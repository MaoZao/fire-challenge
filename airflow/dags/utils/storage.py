import json
import logging
import os
from typing import Union, Dict, List, Tuple, Optional
from urllib.parse import urlparse
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

logger = logging.getLogger(__name__)

class Storage:
    """Handles saving data to S3-compatible storage using env vars for config."""
    def __init__(self):
        """Initializes S3 client using MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY."""
        # Consolidate reading env vars
        endpoint_url = os.environ.get('MINIO_ENDPOINT')
        aws_access_key_id = os.environ.get('MINIO_ACCESS_KEY')
        aws_secret_access_key = os.environ.get('MINIO_SECRET_KEY')
        # Use AWS_REGION if set, otherwise default (boto3 might need a region)
        region_name = os.environ.get('AWS_REGION', 'us-east-1')

        # Check for missing required variables
        if not all([endpoint_url, aws_access_key_id, aws_secret_access_key]):
            missing_vars = [
                var for var in ['MINIO_ENDPOINT', 'MINIO_ACCESS_KEY', 'MINIO_SECRET_KEY']
                if not os.environ.get(var)
            ]
            logger.error(f"Missing required S3 environment variables: {', '.join(missing_vars)}")
            raise ValueError(f"Missing required S3 env vars: {', '.join(missing_vars)}.")

        try:
            # Initialize boto3 client
            self.s3_client = boto3.client(
                's3',
                region_name=region_name,
                endpoint_url=endpoint_url,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )
            # Perform a simple check like list_buckets (requires permissions) or head_bucket if possible
            # self.s3_client.list_buckets() # Example check - requires ListAllMyBuckets permission
            logger.info(f"S3 client initialized for endpoint: {endpoint_url}")
        except (NoCredentialsError, PartialCredentialsError) as e:
            logger.error(f"S3 client initialization failed due to credential issues: {e}", exc_info=True)
            self.s3_client = None
            raise RuntimeError("S3 client initialization failed: Invalid or missing credentials.") from e
        except Exception as e:
            logger.error(f"S3 client initialization failed: {e}", exc_info=True)
            self.s3_client = None
            raise RuntimeError("Could not initialize S3 client") from e

    def _parse_s3_path(self, s3_path: str) -> Tuple[str, str]:
        """
        Parses a full 's3://bucket/key/filename.ext' path into (bucket, key).
        """
        try:
            parsed = urlparse(s3_path)
            # Validate scheme
            if parsed.scheme.lower() != 's3':
                raise ValueError(f"Invalid S3 path scheme: '{s3_path}'. Must start with 's3://'.")

            bucket = parsed.netloc
            key = parsed.path.lstrip('/') # Remove leading slash from path

            # Validate bucket and key presence
            if not bucket:
                 raise ValueError(f"Missing bucket name in S3 path: '{s3_path}'.")
            # Validate key looks like a file (not empty, not ending with '/')
            if not key or key.endswith('/'):
                 raise ValueError(f"Invalid or missing object key (filename) in S3 path: '{s3_path}'. Key must not end with '/'.")
            return bucket, key
        except Exception as e:
            logger.error(f"Failed to parse S3 path '{s3_path}': {e}")
            # Raise ValueError to indicate bad input format
            raise ValueError(f"Failed to parse S3 path '{s3_path}': {e}") from e

    def save_json_to_s3(self, data: Union[List, Dict], s3_path: str) -> None:
        """
        Saves a Python list or dictionary as a JSON file to the specified S3 path.

        Args:
            data (Union[list, dict]): The Python object (list or dict) to save as JSON.
            s3_path (str): The full S3 destination path including bucket and filename
                           (e.g., 's3://my-bucket/my-folder/data.json').

        Raises:
            RuntimeError: If the S3 client is not initialized or if the upload fails.
            ValueError: If the provided s3_path is invalid.
            TypeError: If the data cannot be serialized to JSON.
        """
        # Check if client was initialized successfully
        if self.s3_client is None:
             logger.error("S3 client is not initialized. Cannot save data.")
             raise RuntimeError("S3 client not available. Initialization might have failed.")

        logger.info(f"Attempting to save data to S3 path: {s3_path}")

        try:
            bucket, key = self._parse_s3_path(s3_path)
        except ValueError as e: # Catch path parsing/validation errors from _parse_s3_path
             logger.error(f"Save failed due to invalid S3 path '{s3_path}': {e}")
             raise # Re-raise ValueError

        try:
            json_body = json.dumps(data, indent=2, ensure_ascii=False)
        except TypeError as e:
             logger.error(f"Save failed: Data could not be serialized to JSON. Error: {e}", exc_info=True)
             raise # Re-raise TypeError

        try:
            self.s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=json_body, # Pass the JSON string directly
                ContentType='application/json' # Set MIME type for proper handling
            )
            logger.info(f"Successfully saved data to {s3_path}")
        except ClientError as e: # Catch specific boto3 client errors
            logger.error(f"Save failed during S3 upload to {s3_path} (ClientError): {e}", exc_info=True)
            raise RuntimeError(f"Failed to upload data to S3 path {s3_path} due to client error") from e
        except Exception as e: # Catch other unexpected errors during upload
            logger.error(f"Save failed during S3 upload to {s3_path} (Unexpected Error): {e}", exc_info=True)
            raise RuntimeError(f"Failed to upload data to S3 path {s3_path}") from e
