import logging
from datetime import datetime, timedelta

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.timezone import datetime
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowFailException # To explicitly fail task

# Import your custom classes
# Adjust the import path based on where you saved the files
# Assuming they are in 'dags/utils/'
try:
    from utils.api import SFFireIncidentsAPI
    from utils.storage import Storage
except ImportError as e:
    logging.error("Could not import custom modules. Ensure sffire_api.py and minio_storage.py are in the PYTHONPATH or a discoverable location (e.g., dags/utils/).")
    # Define dummy classes if import fails, so DAG parsing doesn't completely break
    class SFFireIncidentsAPI: pass
    class Storage: pass
    # Or raise the error if you prefer the DAG file itself to fail parsing
    # raise e
from datetime import timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_email@example.com'], # Optional: Add your email
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2), # Reduced retry delay for example
}

# Define the Python function that will be executed by the PythonOperator
def fetch_and_save_fire_data(s3_folder_path: str, **context):
    """
    Fetches fire incident data from SFGov API and saves it to S3/MinIO.
    Uses the DAG's logical date (ds) for the filename.
    """
    logical_date = context["ds"] # Gets the logical date as YYYY-MM-DD string
    filename = f"sf_fire_incidents_{logical_date}.json"
    full_s3_path = f"{s3_folder_path}{filename}" # Storage class expects folder path ending with /

    logging.info("Initializing SFFireIncidentsAPI...")
    try:
        # Pass the app token explicitly if needed, otherwise it uses env var
        # api = SFFireIncidentsAPI(app_token="YOUR_APP_TOKEN_IF_NOT_IN_ENV")
        api = SFFireIncidentsAPI()
    except Exception as e:
         logging.error(f"Failed to initialize SFFireIncidentsAPI: {e}")
         raise AirflowFailException("Could not initialize the API client.")

    logging.info("Fetching all fire incidents...")
    # Add query parameters here if needed, e.g., filtering by date:
    # query_params = {"$where": f"call_date > '{logical_date}T00:00:00'"}
    # fire_data = api.get_all_fire_incidents(**query_params)
    fire_data = api.get_all_fire_incidents() # Fetch all data

    if fire_data is None:
        logging.error("Failed to fetch data from the API. API returned None.")
        # You might want to skip or fail here depending on requirements
        # For this example, we fail the task if data fetching fails
        raise AirflowFailException("No data received from SF Fire Incidents API.")
    elif not fire_data: # Check if the list is empty
        logging.warning("No fire incidents found for the given criteria (or API returned empty list). Saving empty file.")
        # Decide if saving an empty file is desired, or if the task should skip/fail.
        # Here, we proceed to save the empty list as a JSON file.

    logging.info(f"Successfully fetched {len(fire_data)} records.")

    logging.info("Initializing Storage...")
    try:
        storage = Storage()
    except Exception as e:
         logging.error(f"Failed to initialize Storage client: {e}")
         raise AirflowFailException("Could not initialize the S3/MinIO storage client.")

    # Construct the full S3 path before calling the save method
    full_s3_path = f"{s3_folder_path}{filename}"
    logging.info(f"Attempting to save data to: {full_s3_path}")
    try:
        # Call the simplified save method with the full path
        storage.save_json_to_s3(data=fire_data, s3_path=full_s3_path)
        logging.info(f"Successfully saved data to {full_s3_path}")
    except Exception as e:
        logging.error(f"Failed to save data to S3/MinIO: {e}")
        raise AirflowFailException(f"Failed during S3 upload to {full_s3_path}.")

# Define the DAG
with DAG(
    dag_id='sffire_incidents_to_s3_minio', # Unique ID for the DAG
    default_args=default_args,
    description='Fetch SF Fire Incidents data and save to S3/MinIO',
    schedule=timedelta(days=1),   # How often to run (e.g., daily)
    start_date=datetime(2023, 1, 1),             # Start date for the DAG runs
    tags=['socrata', 's3', 'minio', 'fire-dept'], # Tags for filtering in UI
    catchup=False # Set to True if you want it to run for past missed schedules
) as dag:

    # Define the S3 path where the file will be saved
    # !!! IMPORTANT: Replace 'your-bucket-name' with your actual bucket name !!!
    # Ensure the path ends with a '/'
    S3_DESTINATION_FOLDER = 's3://landing/sf_fire_incidents/'

    # Use a TaskGroup (optional, good for organizing)
    with TaskGroup(group_id='sffire_ingestion') as sffire_ingestion_group:
        # Define the PythonOperator task
        ingest_task = PythonOperator(
            task_id='fetch_and_save_sffire_data',
            python_callable=fetch_and_save_fire_data,
            op_kwargs={'s3_folder_path': S3_DESTINATION_FOLDER}, # Pass the S3 path to the function
        )

    # If you had more tasks, you would define dependencies here, e.g.:
    # start_task >> sffire_ingestion_group >> end_task