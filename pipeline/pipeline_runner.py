import time
import logging
import io
import pandas as pd

from pipeline.storage import download_file, upload_file, file_exists
from pipeline.cleaning import normalize_dataframe
from pipeline.validation import validate_dataframe
from pipeline.utils import calculate_hash

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_into_dataframe(file_bytes: bytes) -> pd.DataFrame:
    """
    Load raw CSV bytes into a Pandas DataFrame.
    """
    return pd.read_csv(io.BytesIO(file_bytes))


def run_pipeline(bucket: str, key: str):
    """
    Main pipeline entrypoint for Airflow.

    Equivalent to lambda_handler but triggered by Airflow.
    """

    start_time = time.time()
    logger.info(f"Processing file: {key}")

    # 1. Download file from GCS
    file_bytes = download_file(bucket, key)

    # 2. Compute content hash (idempotency)
    file_hash = calculate_hash(file_bytes)

    processed_key = f"processed/users_cleaned_{file_hash}.csv"
    error_key = f"errors/invalid_rows_{file_hash}.csv"

    # 3. Skip if already processed
    if file_exists(bucket, processed_key):
        logger.info("File already processed. Skipping.")
        return

    # 4. Load into dataframe
    df = load_into_dataframe(file_bytes)
    total_rows = len(df)

    # 5. Normalize
    df = normalize_dataframe(df)

    # 6. Validate
    try:
        valid_df, invalid_df = validate_dataframe(df)
    except ValueError as e:
        logger.error(f"Schema validation failed: {str(e)}")
        upload_file(bucket, error_key, df)
        return

    # 7. Deduplicate
    valid_df = valid_df.drop_duplicates(subset=["user_id"])

    valid_rows = len(valid_df)
    invalid_rows = len(invalid_df)

    # 8. Upload results
    if valid_rows > 0:
        upload_file(bucket, processed_key, valid_df)

    if invalid_rows > 0:
        upload_file(bucket, error_key, invalid_df)

    # 9. Metrics
    processing_time = round(time.time() - start_time, 2)

    logger.info({
        "file": key,
        "hash": file_hash,
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
        "processing_time_seconds": processing_time
    })