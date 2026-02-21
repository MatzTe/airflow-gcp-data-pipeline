from google.cloud import storage
import pandas as pd
import io

# Reusable GCS client
storage_client = storage.Client()


def download_file(bucket: str, key: str) -> bytes:
    """
    Download a file from GCS and return raw bytes.
    """

    bucket_obj = storage_client.bucket(bucket)
    blob = bucket_obj.blob(key)

    return blob.download_as_bytes()


def upload_file(bucket: str, key: str, df: pd.DataFrame):
    """
    Upload a DataFrame to GCS as CSV.
    """

    bucket_obj = storage_client.bucket(bucket)
    blob = bucket_obj.blob(key)

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    blob.upload_from_string(
        csv_buffer.getvalue(),
        content_type="text/csv"
    )


def file_exists(bucket: str, key: str) -> bool:
    """
    Check if a file exists in GCS.
    """

    bucket_obj = storage_client.bucket(bucket)
    blob = bucket_obj.blob(key)

    return blob.exists()