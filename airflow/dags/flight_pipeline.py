from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator, GCSListObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")
GCS_BUCKET = os.getenv("GCS_BUCKET_NAME", "opensky-raw-flights-bucket")
BQ_RAW_DATASET = os.getenv("BQ_RAW_DATASET", "raw_flights")
BQ_RAW_TABLE = "raw_states"

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="flight_pipeline_elt",
    default_args=default_args,
    description="Batch ELT pipeline loading raw Parquet flight data from GCS into BigQuery and running dbt transformations",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    # Task 1: List raw Parquet flight files in GCS
    list_gcs_files = GCSListObjectsOperator(
        task_id="list_gcs_files",
        bucket=GCS_BUCKET,
        prefix="raw/flights/",
    )

    # Task 2: Load raw Parquet records from GCS into BigQuery
    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id="load_gcs_to_bigquery",
        bucket=GCS_BUCKET,
        source_objects=["raw/flights/*.parquet", "raw/flights/*/*.parquet", "raw/flights/*/*/*.parquet", "raw/flights/*/*/*/*.parquet"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{BQ_RAW_TABLE}",
        source_format="PARQUET",
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True,
        ignore_unknown_values=True,
    )

    # Task 3: Clean up ingested Parquet files from GCS
    delete_gcs_processed_files = GCSDeleteObjectsOperator(
        task_id="delete_gcs_objects",
        bucket_name=GCS_BUCKET,
        prefix="raw/flights/",
    )

    # Task 4: Trigger dbt transformations & data validation
    trigger_dbt_transformations = BashOperator(
        task_id="trigger_dbt_models",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir . && dbt test --profiles-dir .",
    )

    list_gcs_files >> load_gcs_to_bq >> delete_gcs_processed_files >> trigger_dbt_transformations
