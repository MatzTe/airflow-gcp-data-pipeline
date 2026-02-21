from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from google.cloud import storage

from pipeline.pipeline_runner import run_pipeline

BUCKET_NAME = "pipeline-raw-data-matias-airflow"
RAW_PREFIX = "raw/"

storage_client = storage.Client()


def list_raw_files(**context):
    """
    List CSV files inside raw/ folder.
    """

    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=RAW_PREFIX)

    files = [blob.name for blob in blobs if blob.name.endswith(".csv")]

    return files


def process_files(**context):
    """
    Process each detected file.
    """

    files = context["ti"].xcom_pull(task_ids="list_files")

    if not files:
        print("No files found")
        return

    for file_key in files:
        print(f"Processing: {file_key}")
        run_pipeline(BUCKET_NAME, file_key)


with DAG(
    dag_id="gcs_auto_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False
) as dag:

    list_files = PythonOperator(
        task_id="list_files",
        python_callable=list_raw_files
    )

    process = PythonOperator(
        task_id="process_files",
        python_callable=process_files
    )

    list_files >> process