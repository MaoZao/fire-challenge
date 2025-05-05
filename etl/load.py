# etl/load.py
# Handles loading data into the staging table in PostgreSQL.

import pandas as pd
import logging
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from config import config # Import configuration instance
from utils import create_staging_table, upsert_data

logger = logging.getLogger(__name__)

def load_data_to_staging(engine: Engine, df: pd.DataFrame):
    """
    Loads the transformed data into the staging database table.
    Uses an UPSERT (ON CONFLICT DO UPDATE) strategy based on 'incident_number'.

    Args:
        engine: SQLAlchemy database engine.
        df: The transformed Pandas DataFrame to load.
    """
    if df is None or df.empty:
        logger.info("No data provided to load. Skipping database operation.")
        return

    table_name = config.STAGING_TABLE_NAME
    # Ensure 'incident_number' is the cleaned column name if cleaning was applied
    unique_key = 'incident_number' # Assuming this is the cleaned primary key column name

    if unique_key not in df.columns:
        logger.error(f"Critical error: Unique key column '{unique_key}' not found in DataFrame. Cannot load data.")
        raise ValueError(f"Unique key column '{unique_key}' missing.")

    logger.info(f"Starting load process for {df.shape[0]} rows into staging table '{table_name}'...")

    try:
        # 1. Ensure the staging table exists with the correct structure (or inferred structure)
        #    This step also implicitly creates the primary key constraint needed for ON CONFLICT
        create_staging_table(engine, df, table_name)

        # 2. Perform the UPSERT operation
        upsert_data(engine, df, table_name, unique_key)

        logger.info(f"Successfully loaded/updated data into staging table '{table_name}'.")

    except SQLAlchemyError as e:
        logger.error(f"Database error during data loading: {e}", exc_info=True)
        # Depending on requirements, you might want to retry or handle differently
        raise # Re-raise the exception to signal failure in the main script
    except Exception as e:
        logger.error(f"An unexpected error occurred during data loading: {e}", exc_info=True)
        raise
