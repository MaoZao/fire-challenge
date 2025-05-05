# etl/transform.py
# Handles data transformation and data quality checks.

import pandas as pd
import logging
from typing import Union # Import Union
from config import config # Import configuration instance
from datetime import datetime

logger = logging.getLogger(__name__)

# --- Data Type Conversion ---
# Define expected data types for key columns. Use nullable Pandas types.
# Refer to https://dev.socrata.com/foundry/data.sfgov.org/wr8u-xric for column details
EXPECTED_DTYPES = {
    'incident_number': 'string',
    'exposure_number': 'Int64', # Use nullable integer type
    'id': 'string',
    'address': 'string',
    'incident_date': 'string', # Keep as string for now, parse later if needed
    'call_number': 'string',
    'alarm_dttm': 'string', # Keep as string, parse later
    'arrival_dttm': 'string', # Keep as string, parse later
    'close_dttm': 'string', # Keep as string, parse later
    'city': 'string',
    'zipcode': 'string',
    'battalion': 'string', # Keep as string, will become dimension key
    'station_area': 'string',
    'box': 'string',
    'suppression_units': 'Int64',
    'suppression_personnel': 'Int64',
    'ems_units': 'Int64',
    'ems_personnel': 'Int64',
    'other_units': 'Int64',
    'other_personnel': 'Int64',
    'first_unit_on_scene': 'string',
    'estimated_property_loss': 'Float64', # Use nullable float
    'estimated_contents_loss': 'Float64',
    'fire_fatalities': 'Int64',
    'fire_injuries': 'Int64',
    'civilian_fatalities': 'Int64',
    'civilian_injuries': 'Int64',
    'number_of_alarms': 'Int64',
    'primary_situation': 'string',
    'mutual_aid': 'string',
    'action_taken_primary': 'string',
    'action_taken_secondary': 'string',
    'action_taken_other': 'string',
    'detector_alerted_occupants': 'string',
    'property_use': 'string',
    'area_of_fire_origin': 'string',
    'ignition_cause': 'string',
    'ignition_factor_primary': 'string',
    'ignition_factor_secondary': 'string',
    'heat_source': 'string',
    'item_first_ignited': 'string',
    'human_factors_associated_with_ignition': 'string',
    'structure_type': 'string',
    'structure_status': 'string',
    'floor_of_fire_origin': 'Int64',
    'fire_spread': 'string',
    'no_flame_spead': 'string', # Typo in source? Keep as is for now.
    'number_of_floors_with_minimum_damage': 'Int64',
    'number_of_floors_with_significant_damage': 'Int64',
    'number_of_floors_with_heavy_damage': 'Int64',
    'number_of_floors_with_extreme_damage': 'Int64',
    'detectors_present': 'string',
    'detector_type': 'string',
    'detector_operation': 'string',
    'detector_effectiveness': 'string',
    'detector_failure_reason': 'string',
    'automatic_extinguishing_system_present': 'string',
    'automatic_extinguishing_sytem_type': 'string', # Typo in source?
    'automatic_extinguishing_sytem_perfomance': 'string', # Typo in source?
    'automatic_extinguishing_sytem_failure_reason': 'string', # Typo in source?
    'number_of_sprinkler_heads_operating': 'Int64',
    'supervisor_district': 'string',
    'neighborhood_district': 'string', # Dimension candidate
    'point': 'string', # Geo data, keep as string for now
    'data_as_of': 'string', # Keep as string, parse later
    'data_loaded_at': 'string' # Dimension candidate
    # Add other columns as needed, defaulting to 'string' if unsure
}

# --- Date/Time Columns to Parse ---
DATETIME_COLUMNS = ['incident_date', 'alarm_dttm', 'arrival_dttm', 'close_dttm', 'data_as_of']

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans DataFrame column names: converts to lowercase, replaces spaces with underscores.
    Handles potential duplicate column names after cleaning.
    """
    original_columns = df.columns
    new_columns = [col.lower().replace(' ', '_').replace('-', '_').replace('?', '').replace('.', '') for col in original_columns]

    # Handle potential duplicates after cleaning (e.g., 'Column A' and 'column_a')
    seen = {}
    final_columns = []
    for i, col in enumerate(new_columns):
        if col in seen:
            seen[col] += 1
            final_columns.append(f"{col}_{seen[col]}")
            logger.warning(f"Duplicate column name generated after cleaning: '{original_columns[i]}' -> '{col}'. Renaming to '{final_columns[-1]}'")
        else:
            seen[col] = 0
            final_columns.append(col)

    df.columns = final_columns
    logger.info("Cleaned DataFrame column names.")
    return df


def convert_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts DataFrame columns to specified types defined in EXPECTED_DTYPES.
    Handles potential errors during conversion.
    """
    logger.info("Starting data type conversion...")
    for col, dtype in EXPECTED_DTYPES.items():
        if col in df.columns:
            try:
                # Special handling for datetimes if needed here, but often better in dbt
                if dtype.startswith('Int') or dtype.startswith('Float'):
                     # Use errors='coerce' to turn unparseable values into NaT/NaN
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                elif dtype == 'string':
                     # Convert to pandas StringDtype for nullable strings
                     df[col] = df[col].astype(pd.StringDtype())
                else:
                    # Use standard astype for other types if necessary
                    df[col] = df[col].astype(dtype)
                # logger.debug(f"Converted column '{col}' to {dtype}.")
            except Exception as e:
                logger.warning(f"Could not convert column '{col}' to {dtype}: {e}. Leaving as original type.")
        else:
            logger.warning(f"Expected column '{col}' not found in DataFrame during type conversion.")

    # Parse datetime columns - use errors='coerce' to handle invalid formats gracefully
    for col in DATETIME_COLUMNS:
         if col in df.columns:
             try:
                 # Socrata often uses ISO 8601 format like 'YYYY-MM-DDTHH:MM:SS.sss'
                 df[col] = pd.to_datetime(df[col], errors='coerce')
                 # Convert to string in a consistent format for loading into TEXT column
                 # PostgreSQL can parse ISO 8601 strings effectively
                 df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S.%f').fillna('')
                 logger.debug(f"Parsed and formatted datetime column '{col}'.")
             except Exception as e:
                 logger.warning(f"Could not parse datetime column '{col}': {e}. Leaving as original.")
         else:
             logger.warning(f"Expected datetime column '{col}' not found for parsing.")


    logger.info("Finished data type conversion.")
    return df

def perform_data_quality_checks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Performs basic data quality checks on the DataFrame.

    Args:
        df: The DataFrame to check.

    Returns:
        The DataFrame, potentially with issues logged or flagged.
        (Could also return a separate report or raise errors for critical issues).
    """
    logger.info("Performing data quality checks...")
    if df.empty:
        logger.info("DataFrame is empty. Skipping DQ checks.")
        return df

    issues_found = False

    # 1. Check for missing primary key ('incident_number')
    if 'incident_number' in df.columns:
        missing_pk = df['incident_number'].isnull().sum()
        if missing_pk > 0:
            logger.warning(f"DQ Check FAILED: Found {missing_pk} rows with missing 'incident_number'.")
            # Depending on policy, you might drop these rows:
            # df = df.dropna(subset=['incident_number'])
            # logger.warning(f"Dropped {missing_pk} rows with missing 'incident_number'.")
            issues_found = True
        else:
            logger.info("DQ Check PASSED: 'incident_number' has no missing values.")
    else:
        logger.error("DQ Check FAILED: Critical column 'incident_number' not found!")
        raise ValueError("Missing critical column: incident_number")


    # 2. Check for duplicate incident_number values within the batch
    if 'incident_number' in df.columns:
        duplicates = df[df.duplicated(subset=['incident_number'], keep=False)]
        if not duplicates.empty:
            logger.warning(f"DQ Check WARNING: Found {duplicates.shape[0]} rows with duplicate 'incident_number' within this batch.")
            logger.debug(f"Duplicate incident numbers:\n{duplicates['incident_number'].value_counts()}")
            # Decide how to handle: keep first, keep last, or flag.
            # For upsert, the database conflict handling will manage this, but it's good to log.
            # Example: Keep the last occurrence based on data_as_of if available
            if config.INCREMENTAL_COLUMN in df.columns:
                 df = df.sort_values(config.INCREMENTAL_COLUMN).drop_duplicates(subset=['incident_number'], keep='last')
                 logger.info(f"Resolved duplicates within batch by keeping the latest based on '{config.INCREMENTAL_COLUMN}'.")
            else:
                 df = df.drop_duplicates(subset=['incident_number'], keep='first') # Or 'last'
                 logger.info("Resolved duplicates within batch by keeping the first occurrence.")

            # issues_found = True # Not necessarily a failure if handled
        else:
            logger.info("DQ Check PASSED: No duplicate 'incident_number' values found within the batch.")


    # 3. Check date/time columns for parsing errors (represented as NaT after to_datetime with errors='coerce')
    # This check is implicitly done if you check for nulls after conversion attempt
    # for col in DATETIME_COLUMNS:
    #     if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
    #         parsing_errors = df[col].isnull().sum()
    #         if parsing_errors > 0:
    #             logger.warning(f"DQ Check WARNING: Column '{col}' has {parsing_errors} values that could not be parsed as dates/times.")
    #             # issues_found = True # Decide if this is critical

    # 4. Check for unexpected values in categorical columns (example: battalion)
    if 'battalion' in df.columns:
        # Example: Check if all battalion values start with 'B' followed by digits
        # This is a placeholder - adjust based on actual expected patterns
        # invalid_battalions = df[~df['battalion'].astype(str).str.match(r'^B\d+$', na=False)]['battalion'].unique()
        # if len(invalid_battalions) > 0:
        #     logger.warning(f"DQ Check WARNING: Found potentially invalid battalion codes: {invalid_battalions}")
            # issues_found = True
        pass # Add more specific checks if needed


    if not issues_found:
        logger.info("All basic data quality checks passed.")
    else:
        logger.warning("Some data quality checks raised warnings.")

    return df


def transform_data(df: pd.DataFrame) -> Union[pd.DataFrame, None]:
    """
    Main transformation function orchestrating cleaning and DQ checks.
    """
    if df is None or df.empty:
        logger.info("No data to transform.")
        return pd.DataFrame() # Return empty DataFrame

    logger.info(f"Starting transformation process for DataFrame with shape {df.shape}...")

    # 1. Clean column names
    df = clean_column_names(df)

    # 2. Convert data types (handle potential errors)
    # Convert types *before* DQ checks that might depend on them
    df = convert_data_types(df)

    # 3. Perform Data Quality Checks (before loading)
    df = perform_data_quality_checks(df)

    # 4. Add any other transformations needed before loading
    # Example: Create a composite key if necessary, handle specific missing values, etc.
    # df['load_timestamp'] = datetime.now() # Add metadata if desired

    logger.info(f"Transformation process completed. Output DataFrame shape: {df.shape}")

    return df
