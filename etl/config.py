# etl/config.py
# Loads environment variables from the .env file.

import os
from dotenv import load_dotenv

# Load environment variables from .env file in the parent directory
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=dotenv_path)

class AppConfig:
    """
    Configuration class for the ETL application.
    Loads settings from environment variables and .env file.
    """
    def __init__(self):
        # --- Database Configuration ---
        self.DB_HOST = os.getenv('DB_HOST', 'localhost')
        self.DB_PORT = os.getenv('DB_PORT', '5439')
        self.DB_NAME = os.getenv('DB_NAME', 'sf_fire_db')
        self.DB_USER = os.getenv('DB_USER', 'default_user')
        self.DB_PASSWORD = os.getenv('DB_PASSWORD', 'default_password')
        self.STAGING_TABLE_NAME = os.getenv('STAGING_TABLE_NAME', 'stg_fire_incidents_raw')

        # Construct Database URL for SQLAlchemy
        self.DATABASE_URL = f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        print(f"Database URL: {self.DATABASE_URL}")  # Debugging line
        # --- API Configuration ---
        self.API_ENDPOINT = os.getenv('API_ENDPOINT')
        self.API_APP_TOKEN = os.getenv('API_APP_TOKEN') # Optional app token
        self.SOCRATA_DATASET_ID = "wr8u-xric" # Keep dataset ID here or move to env
        # Ensure API_ENDPOINT uses the dataset ID if it's a template
        if self.API_ENDPOINT and '{dataset_id}' in self.API_ENDPOINT:
             self.API_ENDPOINT = self.API_ENDPOINT.format(dataset_id=self.SOCRATA_DATASET_ID)

        # --- ETL Configuration ---
        batch_size_str = os.getenv('BATCH_SIZE', '2000')
        # Attempt to strip potential inline comments before converting
        batch_size_val = batch_size_str.split('#')[0].strip()
        self.BATCH_SIZE = int(batch_size_val) # Ensure integer type
        # Use response_timestamp for incremental loads
        self.INCREMENTAL_COLUMN = 'response_timestamp'
        self.LAST_RUN_TIMESTAMP_FILE = os.getenv('LAST_RUN_TIMESTAMP_FILE', './etl/last_run_timestamp.txt')

        # --- Logging Configuration ---
        # Define log level and format, but configure in main.py
        self.LOGGING_LEVEL = os.getenv('LOGGING_LEVEL', 'INFO').upper()
        self.LOGGING_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

        # --- Input Validation ---
        if not self.API_ENDPOINT:
            raise ValueError("API_ENDPOINT environment variable is not set or could not be constructed.")

# Create a single instance of the config to be imported by other modules
config = AppConfig()
