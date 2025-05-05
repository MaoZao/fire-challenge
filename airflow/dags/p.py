#!/usr/bin/env python

# make sure to install these packages before running:
# pip install pandas sodapy minio # pandas is used only for potential future use or consistency

import os
import io
import json
import pandas as pd
from sodapy import Socrata
from minio import Minio
from minio.error import S3Error
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_socrata_data(dataset_id="wr8u-xric", limit=2000):
    """
    Fetches data from the Socrata API for a given dataset ID.

    Args:
        dataset_id (str): The ID of the Socrata dataset.
        limit (int): The maximum number of records to fetch.

    Returns:
        list: A list of dictionaries representing the fetched records, or None if an error occurs.
    """
    # Unauthenticated client only works with public data sets. Note 'None'
    # in place of application token, and no username or password:
    socrata_client = Socrata("data.sfgov.org", None)
    logging.info(f"Fetching up to {limit} records from Socrata dataset {dataset_id}...")
    try:
        results = socrata_client.get(dataset_id, limit=limit)
        logging.info(f"Successfully fetched {len(results)} records from Socrata.")
        # Optional: Convert to DataFrame if needed for other steps, but not required for JSON upload
        # results_df = pd.DataFrame.from_records(results)
        # logging.info("Data converted to Pandas DataFrame.")
        # logging.info(results_df.head())
        return results
    except Exception as e:
        logging.error(f"Error fetching data from Socrata: {e}")
        return None

def upload_data_to_minio_json(data, endpoint, access_key, secret_key, bucket_name, object_name, secure=False):
    """
    Uploads data (list of dictionaries) as a JSON file to a MinIO bucket.

    Args:
        data (list): The data to upload (list of dictionaries).
        endpoint (str): MinIO server endpoint URL (e.g., 'minio:9000').
        access_key (str): MinIO access key.
        secret_key (str): MinIO secret key.
        bucket_name (str): The name of the target MinIO bucket.
        object_name (str): The desired name for the JSON file in the bucket.
        secure (bool): Whether to use TLS/SSL for the connection.

    Returns:
        bool: True if upload was successful, False otherwise.
    """
    logging.info(f"Connecting to MinIO at {endpoint}...")
    try:
        minio_client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        # Check if bucket exists
        found = minio_client.bucket_exists(bucket_name)
        if not found:
            logging.warning(f"MinIO bucket '{bucket_name}' does not exist. Attempting to create it.")
            try:
                minio_client.make_bucket(bucket_name)
                logging.info(f"Bucket '{bucket_name}' created successfully.")
            except S3Error as e:
                logging.error(f"Error creating bucket '{bucket_name}': {e}")
                return False
        else:
            logging.info(f"Successfully connected to MinIO and found bucket '{bucket_name}'.")

        # Convert data list to JSON bytes
        logging.info("Converting data to JSON format...")
        json_data = json.dumps(data, indent=4).encode('utf-8') # Using indent for readability in MinIO
        json_buffer = io.BytesIO(json_data)
        file_size = len(json_data)

        logging.info(f"Uploading data as '{object_name}' to bucket '{bucket_name}'...")
        result = minio_client.put_object(
            bucket_name,
            object_name,
            data=json_buffer,
            length=file_size,
            content_type='application/json' # Set correct content type
        )
        logging.info(
            f"Successfully uploaded {object_name} (etag: {result.etag}, version_id: {result.version_id})"
        )
        return True

    except S3Error as e:
        logging.error(f"MinIO S3 Error: {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred during MinIO upload: {e}")
        return False

if __name__ == "__main__":
    # --- Configuration ---
    # Get MinIO connection details from environment variables
    # Ensure these are set in your environment (e.g., via Docker env vars, .env file, or Airflow Connections)
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    # Specify your landing bucket name here
    MINIO_LANDING_BUCKET = os.getenv("MINIO_LANDING_BUCKET", "landing")
    MINIO_SECURE = os.getenv("MINIO_SECURE", "False").lower() == "true"
    SOCRATA_DATASET_ID = "wr8u-xric"
    SOCRATA_RECORD_LIMIT = 2000 # Adjust as needed
    OUTPUT_JSON_FILENAME = "fire_incidents.json" # Name for the file in MinIO

    # --- Execution ---
    socrata_results = fetch_socrata_data(
        dataset_id=SOCRATA_DATASET_ID,
        limit=SOCRATA_RECORD_LIMIT
    )

    if socrata_results:
        success = upload_data_to_minio_json(
            data=socrata_results,
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            bucket_name=MINIO_LANDING_BUCKET,
            object_name=OUTPUT_JSON_FILENAME,
            secure=MINIO_SECURE
        )
        if success:
            logging.info("Script finished successfully.")
        else:
            logging.error("Script finished with errors during MinIO upload.")
            exit(1) # Exit with error code if upload failed
    else:
        logging.error("Script finished with errors during Socrata data fetching.")
        exit(1) # Exit with error code if fetch failed