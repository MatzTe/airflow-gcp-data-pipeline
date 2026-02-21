# User Data Pipeline with Airflow on GCP

## Overview

This project implements a production-style data pipeline using Apache Airflow running on Google Cloud Composer.

The pipeline automatically detects CSV files uploaded to a Google Cloud Storage bucket, processes the data, validates the schema, and stores clean and invalid records in separate locations.

The goal of this project is to simulate a real-world data engineering pipeline with idempotent processing, data validation, and structured architecture.

---

## Architecture

Pipeline flow:

User uploads CSV → GCS bucket (raw/) → Airflow DAG → Data Processing → GCS (processed/ and errors/)

Processing steps:

1. A CSV file is uploaded to the raw folder
2. Airflow detects the file
3. The pipeline downloads the file
4. Data is normalized
5. Schema validation is applied
6. Valid and invalid records are separated
7. Results are stored back in the bucket
8. Execution metrics are logged

---

## Key Features

Event-style pipeline with Airflow  
Idempotent processing using file hashing  
Schema validation with business rules  
Data normalization and cleaning  
Deduplication of records  
Structured logging  
Separation of valid and invalid data  
Cloud-based architecture

---

## Tech Stack

Python  
Apache Airflow  
Google Cloud Composer  
Google Cloud Storage  
Pandas

---

## Project Structure

airflow-gcp-data-pipeline/
├── dags/
│   └── user_pipeline_dag.py
│
├── pipeline/
│   ├── pipeline_runner.py
│   ├── cleaning.py
│   ├── validation.py
│   ├── storage.py
│   └── utils.py
│
├── data_sample/
│   └── users_test.csv
│
├── architecture/
│   └── architecture_diagram.png
│
├── requirements.txt
└── README.md

---

## How the Pipeline Works

### Step 1 — File Upload

A CSV file is uploaded to:

raw/users_test.csv

### Step 2 — Airflow Detection

The Airflow DAG waits for the file to appear in the bucket.

### Step 3 — Pipeline Execution

The pipeline performs:

- Data loading
- Data normalization
- Schema validation
- Record filtering
- Deduplication

### Step 4 — Output

Clean data:

processed/users_cleaned_<hash>.csv

Invalid records:

errors/invalid_rows_<hash>.csv

The hash ensures idempotent processing and avoids duplicate runs.

---

## Example Input

Example dataset:

users_test.csv

Columns:

- user_id
- name
- email
- signup_date
- country
- age
- subscription_tier

---

## Data Validation Rules

The pipeline validates:

- Required schema columns
- Email format
- Valid subscription tiers
- Positive age values
- Non-null user identifiers

Invalid rows are automatically separated for traceability.

---

## Why This Project

This project was built to demonstrate practical data engineering concepts such as:

- Pipeline orchestration
- Cloud storage integration
- Data validation and cleaning
- Idempotent processing
- Scalable architecture design

It simulates how ingestion pipelines are implemented in production environments.

---

## Future Improvements

- Automatic detection of new files in raw/
- Processing multiple files dynamically
- Pipeline monitoring and metrics
- CI/CD integration
- Data quality dashboard

---

## Author

Matías Terraza

Data Analyst / Data Engineering focused projects  
Background in philosophy with a transition into data and machine learning.
