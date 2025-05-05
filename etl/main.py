# etl/main.py

# Main orchestrator script for the ETL process.

import logging
import sys
import pandas as pd
from datetime import datetime
import os

# Import config first to make settings available
from config import config # Import the instance directly

# Import ETL components and utilities
from utils import get_db_engine, get_last_run_timestamp, save_last_run_timestamp
from extract import fetch_fire_data
from transform import transform_data
from load import load_data_to_staging

# Configure logging using settings from the config object
LOGGING_LEVEL = getattr(logging, config.LOGGING_LEVEL, logging.INFO) # Get level from name
LOGGING_FORMAT = config.LOGGING_FORMAT

# Configure logging for the main script
logging.basicConfig(level=LOGGING_LEVEL, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)

def run_etl():
    """
    Executes the full ETL pipeline: Extract, Transform, Load.
    """
    logger.info("=============================================")
    logger.info("Starting SF Fire Incidents ETL Process...")
    logger.info(f"Run timestamp: {datetime.now().isoformat()}")
    logger.info("=============================================")

    latest_processed_timestamp = None # Track the latest timestamp in the current batch
    engine = None # Initialize engine to None

    try:
        # --- Setup ---
        logger.info("Connecting to database...")
        engine = get_db_engine()
        logger.info("Retrieving last run timestamp...")
        last_run_ts = get_last_run_timestamp()

        # --- Extract ---
        logger.info("Starting Data Extraction...")
        raw_data_df = fetch_fire_data(last_run_ts)

        if raw_data_df is None:
            logger.error("Extraction failed. Aborting ETL process.")
            sys.exit(1) # Exit with error code
        elif raw_data_df.empty:
            logger.info("No new data extracted. ETL process finished.")
            # Optionally update the timestamp even if no new data,
            # to prevent reprocessing if the source hasn't changed
            # Or just exit gracefully
            sys.exit(0)
        else:
            logger.info(f"Extraction successful. Fetched {raw_data_df.shape[0]} records.")
            # Find the latest timestamp in the fetched data to save for the next run
            if config.INCREMENTAL_COLUMN in raw_data_df.columns:
                 # Convert to datetime objects to find the max, handling potential NaTs
                 timestamps = pd.to_datetime(raw_data_df[config.INCREMENTAL_COLUMN], errors='coerce')
                 if not timestamps.isnull().all(): # Check if there are any valid timestamps
                     latest_processed_timestamp = timestamps.max().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] # Format for Socrata
                     logger.info(f"Latest timestamp in fetched data: {latest_processed_timestamp}")
                 else:
                     logger.warning(f"Could not determine latest timestamp from column '{config.INCREMENTAL_COLUMN}'. State will not be updated.")
            else:
                logger.warning(f"Incremental column '{config.INCREMENTAL_COLUMN}' not found in data. Cannot update state.")


        # --- Transform ---
        logger.info("Starting Data Transformation and Quality Checks...")
        transformed_df = transform_data(raw_data_df) # transform_data handles cleaning and DQ

        if transformed_df is None:
            logger.error("Transformation failed. Aborting ETL process.")
            sys.exit(1)
        elif transformed_df.empty:
             logger.info("Dataframe empty after transformation/DQ checks. No data to load.")
             # Still update timestamp if needed
             if latest_processed_timestamp and last_run_ts != latest_processed_timestamp:
                 save_last_run_timestamp(latest_processed_timestamp)
             sys.exit(0)
        else:
            logger.info(f"Transformation successful. Processed {transformed_df.shape[0]} records.")


        # --- Load ---
        logger.info("Starting Data Loading...")
        load_data_to_staging(engine, transformed_df)
        logger.info("Data Loading successful.")

        # --- Post-Load & State Update ---
        # Perform any post-load DQ checks if necessary (e.g., row counts)

        # Update the last run timestamp only if the load was successful
        if latest_processed_timestamp and last_run_ts != latest_processed_timestamp:
            logger.info(f"Updating last run timestamp to: {latest_processed_timestamp}")
            save_last_run_timestamp(latest_processed_timestamp)
        elif not latest_processed_timestamp:
             logger.warning("No valid latest timestamp found in the batch. Last run timestamp file not updated.")
        else:
             logger.info("Latest timestamp matches previous run; no update needed.")


        logger.info("=============================================")
        logger.info("SF Fire Incidents ETL Process Completed Successfully.")
        logger.info("=============================================")

    except Exception as e:
        logger.critical(f"ETL process failed: {e}", exc_info=True)
        logger.info("=============================================")
        logger.info("SF Fire Incidents ETL Process FAILED.")
        logger.info("=============================================")
        sys.exit(1) # Exit with error code
    finally:
        # Ensure the database engine is disposed of properly
        if engine:
            engine.dispose()
            logger.info("Database engine disposed.")

if __name__ == "__main__":
    run_etl()
