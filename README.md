# Airflow GCP Data Pipeline

A simple end-to-end data pipeline built with Apache Airflow and Google Cloud Storage.  
The pipeline automatically processes CSV files uploaded to a GCS bucket, validates the data, cleans it, and stores the results in organized locations.

This project demonstrates orchestration, modular pipeline design, and basic data engineering best practices.

---

## Project Overview

The pipeline works as follows:

1. A CSV file is uploaded to the **raw** folder in a GCS bucket.
2. Airflow triggers the DAG.
3. The pipeline:
   - Validates the data
   - Cleans incorrect rows
   - Separates valid and invalid records
4. Processed data is stored back in the bucket.

---

## Tech Stack

- Apache Airflow
- Python
- Google Cloud Storage
- Pandas
- Docker (optional for deployment)

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
├── architecture/
│   └── architecture_diagram.png
│
├── requirements.txt
└── README.md
```

---

## How the Pipeline Works

### 1. File Upload
A CSV file is uploaded to:

```
raw/users_test.csv
```

### 2. Airflow DAG Trigger
Airflow detects the new file and runs the DAG:

```
user_pipeline_test
```

### 3. Data Processing
The pipeline performs:

- Schema validation
- Data cleaning
- Error detection
- Logging

### 4. Output

Results are stored in:

```
processed/
errors/
```

---

## Example Data Flow

```
Upload CSV → Airflow DAG → Validation → Cleaning → Storage
```

```
raw/ → processing → processed/ + errors/
```

---

## Installation

Clone the repository:

```
git clone https://github.com/your-username/airflow-gcp-data-pipeline.git
cd airflow-gcp-data-pipeline
```

Install dependencies:

```
pip install -r requirements.txt
```

---

## Running the Project

1. Start Airflow
2. Place the DAG inside the Airflow `dags/` folder
3. Upload a CSV file to the bucket
4. Trigger the DAG from the Airflow UI

---

## Example Use Case

This project simulates a common real-world data engineering workflow:

- Raw data ingestion
- Automated validation
- Data quality checks
- Structured storage

It can be extended to:

- Data warehouses
- Streaming pipelines
- Machine learning pipelines

---

## Future Improvements

- Add CI/CD
- Add data quality metrics
- Implement monitoring
- Add unit tests
- Deploy with Terraform

---

## Author

Matías Terraza  
Data / Machine Learning Engineer
