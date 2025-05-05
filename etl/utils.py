# etl/utils.py
# Utility functions for database connection, logging, and state management.

import logging
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from sqlalchemy import inspect # Import inspect
import numpy as np # Import numpy
from typing import Union # Import Union
import csv

from config import config # Import configuration instance

logger = logging.getLogger(__name__)

def get_db_engine() -> Engine:
    """
    Creates and returns a SQLAlchemy database engine.
    Handles potential connection errors during creation.
    """
    try:
        engine = create_engine(config.DATABASE_URL, pool_pre_ping=True)
        # Test connection
        with engine.connect() as connection:
            logger.info("Database engine created and connection successful.")
        return engine
    except SQLAlchemyError as e:
        logger.error(f"Error creating database engine: {e}", exc_info=True)
        raise

def get_last_run_timestamp() -> Union[str, None]:  # Use Union for backward compatibility
    """
    Reads the last run timestamp from the state file.
    Returns None if the file doesn't exist or is empty.
    """
    try:
        with open(config.LAST_RUN_TIMESTAMP_FILE, 'r') as f:
            timestamp = f.read().strip()
            if timestamp:
                logger.info(f"Retrieved last run timestamp: {timestamp}")
                return timestamp
            else:
                logger.info("Last run timestamp file is empty.")
                return None
    except FileNotFoundError:
        logger.info(f"Last run timestamp file not found ({config.LAST_RUN_TIMESTAMP_FILE}). Performing full load.")
        return None
    except IOError as e:
        logger.error(f"Error reading last run timestamp file: {e}", exc_info=True)
        return None # Proceed with full load on error

def save_last_run_timestamp(timestamp: str):
    """
    Saves the latest timestamp to the state file.
    """
    try:
        with open(config.LAST_RUN_TIMESTAMP_FILE, 'w') as f:
            f.write(timestamp)
        logger.info(f"Saved last run timestamp: {timestamp}")
    except IOError as e:
        logger.error(f"Error writing last run timestamp file: {e}", exc_info=True)


def upsert_data(engine: Engine, df: pd.DataFrame, table_name: str, unique_key: str):
    """
    Upserts data from a Pandas DataFrame into a PostgreSQL table.
    It uses ON CONFLICT DO UPDATE based on a specified unique key.

    Args:
        engine: SQLAlchemy database engine.
        df: Pandas DataFrame containing the data to upsert.
        table_name: Name of the target table in the database.
        unique_key: The column name that uniquely identifies a row (primary key).
    """
    if df.empty:
        logger.info("DataFrame is empty. No data to upsert.")
        return

    conn = None
    cursor = None
    try:
        conn = engine.raw_connection() # Get underlying DBAPI connection (psycopg2)

        # --- Get actual columns from the database table ---
        inspector = inspect(engine)
        try:
            db_columns = [col['name'] for col in inspector.get_columns(table_name)]
            logger.info(f"Found columns in table '{table_name}': {db_columns}")
        except SQLAlchemyError as e:
             logger.error(f"Could not inspect columns for table '{table_name}': {e}. Assuming all DataFrame columns exist.", exc_info=True)
             # Fallback: Assume all columns exist, which might lead back to the original error
             db_columns = df.columns.tolist()

        # --- Filter DataFrame columns to match database columns ---
        original_cols = df.columns.tolist()
        cols_to_upsert = [col for col in original_cols if col in db_columns]
        dropped_cols = [col for col in original_cols if col not in db_columns]
        if dropped_cols:
            logger.warning(f"Columns from DataFrame not found in table '{table_name}' and will be ignored during upsert: {dropped_cols}")

        df_filtered = df[cols_to_upsert]

        cursor = conn.cursor()

        # Convert Pandas NA and Numpy NaN values to None, which psycopg2 can handle as NULL
        # This must be done before converting to numpy/tuples
        df = df.replace({pd.NA: None, np.nan: None})

        # Prepare data for executemany - convert DataFrame to list of tuples
        data_tuples = [tuple(x) for x in df.to_numpy()]
        # Prepare column names and placeholders using only the filtered columns
        cols_sql = ', '.join([f'"{col}"' for col in cols_to_upsert]) # Quote column names
        update_cols = [f'"{col}" = EXCLUDED."{col}"' for col in cols_to_upsert if col != unique_key]
        update_sql = ', '.join(update_cols)

        # --- Construct and Execute UPSERT ---
        # Construct the UPSERT SQL statement
        # Using ON CONFLICT requires specifying the constraint or the column(s)
        # Assuming the unique_key column has a unique constraint/index
        sql = f"""
        INSERT INTO {table_name} ({cols_sql})
        VALUES %s
        ON CONFLICT ({unique_key}) DO UPDATE
        SET {update_sql};
        """

        # Use psycopg2's execute_values for efficient bulk insertion/upsertion
        from psycopg2.extras import execute_values
        execute_values(cursor, sql, data_tuples, page_size=500) # Adjust page_size as needed
        
        conn.commit()
        logger.info(f"Successfully upserted {len(df)} rows into {table_name}.")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error during upsert operation: {e}", exc_info=True)
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def create_staging_table(engine: Engine, df: pd.DataFrame, table_name: str):
    """
    Creates the staging table if it doesn't exist, inferring types from the DataFrame.
    Uses TEXT type for flexibility initially. dbt will handle proper typing later.
    Adds a primary key constraint on 'incident_number'.
    """
    if df.empty:
        logger.warning("DataFrame is empty. Cannot infer schema to create table.")
        # Attempt to create a minimal table if it doesn't exist
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            incident_number TEXT PRIMARY KEY
            -- Add other essential columns if known, otherwise they'll be added dynamically or assumed TEXT
        );
        """
    else:
        # Generate CREATE TABLE statement dynamically based on DataFrame columns
        # Use TEXT for all columns initially for simplicity in staging
        column_defs = [f'"{col}" TEXT' for col in df.columns if col != 'incident_number']
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            "incident_number" TEXT PRIMARY KEY,
            {', '.join(column_defs)}
        );
        """

    try:
        with engine.connect() as connection:
            connection.execute(text(create_sql))
            logger.info(f"Ensured table {table_name} exists.")
            # Optionally, you could add missing columns here if the table already exists
            # This requires querying INFORMATION_SCHEMA.COLUMNS and ALTER TABLE ADD COLUMN

    except SQLAlchemyError as e:
        logger.error(f"Error creating or checking staging table {table_name}: {e}", exc_info=True)
        raise
