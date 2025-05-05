# d:\GIT\fire-challenge\test_db_connection.py
# A simple script to test the database connection using get_db_engine.

import logging
import sys
import os

# Ensure the 'etl' directory is in the Python path
# This allows importing from 'etl' when running from the root directory
script_dir = os.path.dirname(os.path.abspath(__file__))
etl_dir = os.path.join(script_dir, 'etl')
sys.path.insert(0, etl_dir)

# Now import from the etl package
try:
    from config import config
    from utils import get_db_engine
except ImportError as e:
    print(f"Error importing ETL modules: {e}")
    print("Make sure you are running this script from the project root directory (d:\\GIT\\fire-challenge).")
    sys.exit(1)

# Basic logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Attempting to connect to the database...")
    engine = None
    try:
        # Call the function to get the engine
        engine = get_db_engine()
        # If get_db_engine() completes without error, connection was successful
        logger.info("Successfully obtained database engine and tested connection.")
    except Exception as e:
        logger.error(f"Failed to get database engine: {e}", exc_info=True)
    finally:
        if engine:
            engine.dispose()
            logger.info("Database engine disposed.")