# etl/extract.py
# Handles data extraction from the Socrata API.

import requests
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Union

from config import config # Import configuration instance
# from .utils import get_last_run_timestamp # utils imports config, avoid circularity if needed

logger = logging.getLogger(__name__)

def fetch_fire_data(last_run_timestamp: Union[str, None]) -> Union[pd.DataFrame, None]:
    """
    Fetches fire incident data from the Socrata API.
    Implements incremental loading based on the last run timestamp.

    Args:
        last_run_timestamp: The timestamp of the latest record from the previous run.
                            If None, performs a full load.

    Returns:
        A Pandas DataFrame containing the fetched data, or None if an error occurs.
    """
    all_data = []
    offset = 0
    limit = config.BATCH_SIZE

    # Base URL and parameters
    base_url = config.API_ENDPOINT # Already formatted in config class
    params = {
        "$limit": limit,
        "$offset": offset
    }

    # Add app token if available
    if config.API_APP_TOKEN:
        headers = {"X-App-Token": config.API_APP_TOKEN}
    else:
        headers = {}
        logger.warning("API_APP_TOKEN not set. Requests might be throttled.")

    # Add filter for incremental load
    if last_run_timestamp:
        # Ensure the timestamp is in the correct format for Socrata (ISO 8601)
        # Example: '2023-10-27T10:00:00.000'
        # Add a small buffer to avoid potential floating point issues or clock skew
        try:
            # Attempt to parse the timestamp to validate format
            datetime.fromisoformat(last_run_timestamp)
            # Socrata SoQL comparison works with ISO 8601 strings
            params["$where"] = f"{config.INCREMENTAL_COLUMN} > '{last_run_timestamp}'"
            logger.info(f"Fetching data incrementally where {config.INCREMENTAL_COLUMN} > '{last_run_timestamp}'")
        except ValueError:
            logger.error(f"Invalid last_run_timestamp format: {last_run_timestamp}. Performing full load instead.")
            # Optionally remove the $where clause or handle differently
            # For safety, fall back to full load or stop execution
            return None # Or raise an error
    else:
        logger.info("Performing full data fetch (no last run timestamp found).")

    max_iterations = 10 # i need to improve that to make the where work with the offset
    iteration_count = 0
    # Loop to fetch data in batches
    while True:
        params["$offset"] = offset
        try:
            logger.info(f"Fetching batch {iteration_count + 1}/{max_iterations}: offset={offset}, limit={limit}")
            response = requests.get(base_url,  params=params, timeout=60) # Added timeout
            print(f"Response status code: {response.status_code}") # Debugging line
            print(response.text) # Debugging line
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

            data_batch = response.json()

            if not data_batch:
                logger.info("No more data found.")
                break # Exit loop if no data is returned

            all_data.extend(data_batch)
            logger.info(f"Fetched {len(data_batch)} records in this batch. Total fetched: {len(all_data)}")

            # Check if the number of records fetched is less than the limit,
            # indicating the last page
            if len(data_batch) < limit:
                logger.info("Last batch fetched.")
                break

            iteration_count += 1
            if iteration_count >= max_iterations:
                logger.warning(f"Reached maximum iteration limit ({max_iterations}). Stopping fetch.")
                break


            offset += limit # Prepare for the next batch

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from API: {e}", exc_info=True)
            return None # Return None on error
        except Exception as e:
            logger.error(f"An unexpected error occurred during data fetching: {e}", exc_info=True)
            return None

    if not all_data:
        logger.info("No new data fetched.")
        return pd.DataFrame() # Return empty DataFrame if no data

    # Convert list of dictionaries to DataFrame
    try:
        df = pd.DataFrame(all_data)
        logger.info(f"Successfully converted fetched data to DataFrame with shape {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Error converting fetched data to DataFrame: {e}", exc_info=True)
        return None
