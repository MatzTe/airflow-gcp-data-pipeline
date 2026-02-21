# Airflow GCP Data Pipeline

A cloud-based data pipeline that automatically processes CSV files uploaded to Google Cloud Storage using Apache Airflow.

This project demonstrates a real-world Data Engineering workflow including ingestion, validation, cleaning, and storage of processed data.

---

## Project Overview

The pipeline automatically detects new files uploaded to a cloud storage bucket and processes them through a modular Python data pipeline.

Input files are uploaded to:

raw/

The pipeline then:

1. Validates the data
2. Cleans incorrect values
3. Separates valid and invalid records
4. Saves results back to the bucket

Output is stored in:

processed/  
errors/

---

## Architecture

The pipeline follows this flow:

User Upload CSV  
        ↓  
Google Cloud Storage (raw/)  
        ↓  
Apache Airflow DAG  
        ↓  
Python Data Pipeline  
        ↓  
Processed Data + Error Data  
        ↓  
Google Cloud Storage

---

## Technologies Used

- Python
- Apache Airflow
- Google Cloud Platform
- Google Cloud Storage
- Pandas

---

## Project Structure

```
airflow-gcp-data-pipeline/
│
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
├── requirements.txt
└── README.md
```

---

## How the Pipeline Works

### 1. File Upload
A CSV file is uploaded to the cloud storage bucket:

raw/users_test.csv

### 2. Airflow Detection
The DAG runs and detects files inside the raw folder.

### 3. Processing
The pipeline:

- Reads the file
- Validates schema and data quality
- Cleans incorrect values
- Splits valid and invalid rows

### 4. Output

Valid records:

processed/users_clean.csv

Invalid records:

errors/users_errors.csv

---

## DAG Overview

The Airflow DAG is responsible for:

- Monitoring the storage bucket
- Triggering the pipeline
- Running the processing logic
- Logging execution

Main file:

dags/user_pipeline_dag.py

---

## Pipeline Modules

validation.py  
Handles schema validation and data checks.

cleaning.py  
Standardizes and cleans the dataset.

storage.py  
Saves processed and error files to cloud storage.

pipeline_runner.py  
Coordinates the entire pipeline process.

utils.py  
Helper functions used across modules.

---

## Running the Pipeline

1. Upload a CSV file to the raw folder in the bucket
2. Airflow detects the file
3. The DAG runs automatically
4. Processed results appear in processed/ and errors/

You can monitor execution in the Airflow UI.

---

## Example Use Case

This pipeline simulates a common data engineering workflow where incoming datasets must be validated and cleaned before being used for analytics or machine learning.

---

## What This Project Demonstrates

- Data pipeline architecture
- Workflow orchestration
- Modular Python design
- Cloud storage integration
- Data validation strategies
- Production-style project structure

---

## Idempotent Pipeline Design

This pipeline follows an idempotent processing strategy to ensure that files are not processed multiple times.

Each input file is identified using a content hash. Before processing, the pipeline checks whether the file has already been processed by comparing hashes.

This approach guarantees that:

- The same file is not processed more than once
- Re-running the DAG does not create duplicated outputs
- Results remain consistent across executions
- The pipeline can safely recover from failures

The hash-based mechanism allows the pipeline to scale efficiently as new files are added to the storage bucket.

---

## Author

Matías Terraza  
Data Analyst / Analytics Engineer
