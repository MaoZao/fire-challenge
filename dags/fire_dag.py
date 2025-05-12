import os
import shutil
import time
from datetime import datetime, timedelta
from typing import List, cast

import boto3
import duckdb

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)


def get_s3_folder(
    s3_bucket, s3_folder, local_folder="/opt/airflow/temp/s3folder/"
):
    # TODO: Move AWS credentials to env variables
    s3 = boto3.resource(
        service_name="s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="minio123",
        region_name="us-east-1",
    )
    bucket = s3.Bucket(s3_bucket)
    local_path = os.path.join(local_folder, s3_folder)
    # Delete the local folder if it exists
    if os.path.exists(local_path):
        shutil.rmtree(local_path)

    for obj in bucket.objects.filter(Prefix=s3_folder):
        target = os.path.join(local_path, os.path.relpath(obj.key, s3_folder))
        os.makedirs(os.path.dirname(target), exist_ok=True)
        bucket.download_file(obj.key, target)
        print(f"Downloaded {obj.key} to {target}")


with DAG(
    "fire_sf_to_s3_dag",
    description="Dag to pull San Francisco Fire Incidents data to S3",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    fire_s3_bucket = "fire-sf-bucket"

    create_s3_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket", bucket_name=fire_s3_bucket
    )

    fetch_fire_sf_data_task = BashOperator(
        task_id="fetch_fire_sf_data",
        bash_command="python /opt/airflow/dags/scripts/spark/import.py",
    )

    fire_sf_to_s3 = LocalFilesystemToS3Operator(
        task_id="fire_sf_to_s3",
        filename="/opt/airflow/data/Fire_SF.csv",
        dest_key="raw/Fire_SF.csv",
        dest_bucket=fire_s3_bucket,
        replace=True,
    )

    fire_sf_to_s3_processed = BashOperator(
        task_id="fire_sf_to_s3_processed",
        bash_command=(
            "python /opt/airflow/dags/scripts/spark/process_fire_incidents.py "
            "--input_path s3a://fire-sf-bucket/raw/Fire_SF.csv "
            "--output_path "
            "s3a://fire-sf-bucket/processed/fire_incidents_by_date"
        ),
    )

    get_fire_incidents_processed_to_local = PythonOperator(
        task_id="get_fire_incidents_processed_to_local",
        python_callable=get_s3_folder,
        op_kwargs={
            "s3_bucket": fire_s3_bucket,  # Keep this line under 80 chars
            "s3_folder": "processed/fire_incidents_by_date",  # And this one
            # Also this comment
        },
    )

    @task
    def summarize_fire_incidents():
        time.sleep(30)
        query = """WITH up AS (
          select
            *
          from
            '/opt/airflow/temp/s3folder/processed/fire_incidents_by_date/*.parquet'
        )
                SELECT
            incident_date,
            COUNT(*) AS num_incidents,
            SUM(
                CAST("civilian_injuries" AS INTEGER)
            ) AS total_civilian_injuries
        FROM
            up
        WHERE incident_date IS NOT NULL
        GROUP BY
            incident_date
        ORDER BY
            incident_date
        """
        summary_file_path = "/opt/airflow/data/fire_incident_summary.csv"
        duckdb.sql(query).write_csv(summary_file_path)
        return summary_file_path

    summarize_fire_incidents_task_instance = summarize_fire_incidents()

    @task
    def perform_fire_summary_dq_checks(summary_file_path: str):
        """
        Performs data quality checks on the fire_incident_summary.csv file.
        Returns a list of status strings (e.g., "PASS_X", "FAIL_Y").
        """
        results = []

        if not os.path.exists(summary_file_path):
            results.append(f"FAIL_FILE_DOES_NOT_EXIST: {summary_file_path}")
            return results

        con = None
        try:
            con = duckdb.connect(database=':memory:', read_only=False)
            df = con.execute(
                f"SELECT * FROM read_csv_auto('{summary_file_path}')"
            ).fetchdf()
        except Exception as e:
            results.append(f"FAIL_FILE_READ_ERROR: {str(e)}")
            return results
        finally:
            if con:
                con.close()

        if df.empty:
            results.append("FAIL_FILE_IS_EMPTY")
            return results  # No further column checks if file is empty

        # Check 1: incident_date completeness
        if df['incident_date'].isnull().any():
            results.append("FAIL_INCIDENT_DATE_INCOMPLETE")
        else:
            results.append("PASS_INCIDENT_DATE_COMPLETE")

        # Check 2: num_incidents should be positive
        if not (df['num_incidents'] > 0).all():
            results.append("FAIL_NUM_INCIDENTS_NON_POSITIVE")
        else:
            results.append("PASS_NUM_INCIDENTS_POSITIVE")

        # Check 3: total_civilian_injuries should be non-negative
        if not (df['total_civilian_injuries'] >= 0).all():
            results.append("FAIL_TOTAL_CIVILIAN_INJURIES_NEGATIVE")
        else:
            results.append("PASS_TOTAL_CIVILIAN_INJURIES_NON_NEGATIVE")

        print(f"DQ Check Results: {results}")
        return results

    perform_fire_summary_dq_checks_instance = perform_fire_summary_dq_checks(
        cast(str, summarize_fire_incidents_task_instance)
    )

    stop_pipeline_task = DummyOperator(task_id='stop_pipeline')
    proceed_with_pipeline_task = DummyOperator(task_id='proceed_with_pipeline')

    @task.branch
    def branch_on_dq_results(dq_statuses: List[str]):
        """
        Decides the next step based on Data Quality check results.
        """
        if any("FAIL" in status for status in dq_statuses):
            return stop_pipeline_task.task_id
        return proceed_with_pipeline_task.task_id

    branch_on_dq_results_instance = branch_on_dq_results(
        dq_statuses=cast(List[str], perform_fire_summary_dq_checks_instance)
    )

    (
        create_s3_bucket
        >> fetch_fire_sf_data_task
        >> fire_sf_to_s3
        >> fire_sf_to_s3_processed
        >> get_fire_incidents_processed_to_local
        >> summarize_fire_incidents_task_instance
        >> perform_fire_summary_dq_checks_instance
        >> branch_on_dq_results_instance
    )

    branch_on_dq_results_instance >> [
        proceed_with_pipeline_task,
        stop_pipeline_task,
    ]
