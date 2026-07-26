# OpenSky End-to-End ELT Streaming Pipeline Documentation

---

## Executive Summary & System Overview

This repository contains a production-grade, end-to-end Streaming ELT (Extract, Load, Transform) data engineering pipeline. It collects real-time flight telemetry state vectors from the global [OpenSky Network API](https://opensky-network.org/apidoc/rest.html), streams them through an event broker, buffers and writes compressed Parquet files to Google Cloud Storage (GCS), orchestrates landing-zone ingestion into Google BigQuery using Apache Airflow, and transforms raw telemetry records into analytical data marts using dbt.

```
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│  OpenSky REST   │ ──1──>│ Python Producer │ ──2──>│  Apache Kafka   │
│       API       │       │  (producer.py)  │       │  (KRaft Broker) │
└─────────────────┘       └─────────────────┘       └─────────────────┘
                                                             │
                                                             3
                                                             ▼
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│ BigQuery Marts  │ <──6──│ Apache Airflow  │ <──5──│ Python Consumer │
│ (marts_flights) │ (dbt) │ (DAG Scheduler) │ (GCS) │  (consumer.py)  │
└─────────────────┘       └─────────────────┘       └─────────────────┘
                                    │                        │
                                    │ (Load & Purge)         │ (Write Parquet)
                                    ▼                        ▼
                          ┌──────────────────┐    ┌──────────────────┐
                          │ BigQuery Raw Zone│    │   GCS Bucket     │
                          │  (raw_flights)   │    │ (Raw Parquet)    │
                          └──────────────────┘    └──────────────────┘
```

---

## 1. The Complete Data Flow Narrative

Here is the step-by-step lifecycle of a single flight record as it moves through the infrastructure:

### Step 1: Live Polling & State Vector Parsing
* **Tool**: `producer/main.py` (`streaming/producer.py`)
* **Process**: Every 10 seconds, the Python producer issues an HTTP `GET` request to `https://opensky-network.org/api/states/all`. 
* **Payload Structure**: The OpenSky API returns a payload containing a unix timestamp `time` and an array of 17-element state vectors (`states`), e.g.:
  ```json
  [
    "4b1812", "SWR123  ", "Switzerland", 1785073800, 1785073807,
    8.5432, 47.3769, 10668.0, false, 245.5, 230.1, 0.0,
    null, 10972.8, "7700", false, 0
  ]
  ```
* **Parsing**: The producer extracts each state vector element safely using positional indices, cleans trailing whitespace from callsigns, converts attributes into a structured JSON dict (e.g. `icao24`, `callsign`, `origin_country`, `latitude`, `longitude`, `baro_altitude`, `velocity`, `on_ground`), injects an `ingested_at` timestamp, and serializes the record as UTF-8 JSON.

### Step 2: Ingesting into Kafka Broker
* **Tool**: Apache Kafka (`kafka` container in KRaft mode)
* **Process**: The producer sends the serialized JSON record to the Kafka topic `opensky_flights_raw` on port `9092`. Kafka retains these records in an append-only log partition, ensuring high throughput, fault tolerance, and decoupling the live API source from downstream processing.

### Step 3: Stream Consumption & Parquet Batching
* **Tool**: `consumer/main.py` (`streaming/consumer.py`)
* **Process**: The Python consumer service runs an event loop subscribed to `opensky_flights_raw` in the consumer group `gcs_parquet_ingestor_group`.
* **Buffering & Serialization**: Every 60 seconds, the consumer flushes its in-memory record buffer into a `pandas.DataFrame`. It enforces strict column data types (`Int64`, `string`, `float64`, `boolean`) to ensure schema consistency, converts the DataFrame to a columnar **Parquet** file using `pyarrow`, and names it `flights_YYYYMMDD_HHMMSS.parquet`.

### Step 4: Storage Data Lake Partitioning
* **Tool**: Google Cloud Storage (`GCS_BUCKET_NAME`)
* **Process**: The consumer uploads the local Parquet file to GCS using a time-partitioned blob hierarchy:
  `gs://<bucket_name>/raw/flights/year=YYYY/month=MM/day=DD/flights_<timestamp>.parquet`
  After a successful upload, the temporary local Parquet file is deleted from `/tmp`.

### Step 5: Orchestration & Lakehouse Ingestion
* **Tool**: Apache Airflow (`flight_pipeline_elt` DAG)
* **Process**: On an hourly schedule, Airflow executes the orchestration DAG:
  1. `list_gcs_files` (`GCSListObjectsOperator`): Scans `raw/flights/` in GCS for landing Parquet objects.
  2. `load_gcs_to_bigquery` (`GCSToBigQueryOperator`): Reads all discovered Parquet files and loads them into BigQuery table `raw_flights.raw_states` using `WRITE_APPEND` disposition and schema auto-detection.
  3. `delete_gcs_objects` (`GCSDeleteObjectsOperator`): Removes the ingested Parquet files from GCS to prevent duplicate loads on subsequent runs.

### Step 6: Analytical Data Mart Transformation
* **Tool**: dbt (data build tool) & BigQuery
* **Process**: After data loading completes, Airflow triggers `dbt run` and `dbt test`:
  1. dbt reads `dbt/models/sources.yml` to target `raw_flights.raw_states` in BigQuery (`asia-southeast1`).
  2. dbt executes `dbt/models/marts/active_flights.sql`, filtering active non-grounded aircraft, aggregating total aircraft count and average velocity per origin country.
  3. The transformed result is written to BigQuery table `marts_flights.active_flights` for downstream dashboards and analytics.

---

## 2. Directory Tree Structure

```
opensky-elt-pipeline/
├── .env                                  # Active environment configuration & GCP parameters
├── .env.example                          # Template for required environment variables
├── .gitignore                            # Version control exclusion rules (secrets, logs, build outputs)
├── deploy.sh                             # GCP VM automated deployment script
├── docker-compose.yml                    # Multi-container orchestration (Kafka, Airflow, Postgres, Microservices)
├── init_project.sh                       # One-liner bash script to recreate folder layout
├── notes/
│   └── architecture_and_flow.md          # Comprehensive architecture & data flow documentation
├── airflow/
│   ├── Dockerfile                        # Custom Airflow container image with pre-installed dbt & GCP libraries
│   └── dags/
│       └── flight_pipeline.py            # Airflow DAG for GCS to BigQuery loading & dbt execution
├── consumer/
│   ├── Dockerfile                        # Docker build specification for the consumer microservice
│   └── main.py                           # Container entrypoint script for Kafka to GCS batching
├── producer/
│   ├── Dockerfile                        # Docker build specification for the producer microservice
│   └── main.py                           # Container entrypoint script for OpenSky API to Kafka streaming
├── streaming/
│   ├── producer.py                       # Core Python producer logic (OpenSky API -> Kafka)
│   └── consumer.py                       # Core Python consumer logic (Kafka -> Parquet -> GCS)
├── dbt/
│   ├── dbt_project.yml                   # dbt project configuration, model paths, and materialization rules
│   ├── profiles.yml                      # dbt BigQuery connection profile (Service Account auth in asia-southeast1)
│   └── models/
│       ├── sources.yml                   # dbt source metadata mapping raw BigQuery tables
│       ├── staging/                      # Directory for staging models (intermediate views)
│       └── marts/
│           └── active_flights.sql        # Datamart SQL model aggregating active aircraft per country
└── terraform/
    ├── main.tf                           # Infrastructure as Code: GCS Bucket & BigQuery Datasets
    ├── variables.tf                      # Terraform variable declarations and default values
    └── outputs.tf                        # Terraform output variables (GCS bucket URL, dataset IDs)
```

---

## 3. File-by-File Breakdown

### Root Configuration & Control Scripts

#### 1. `.env` & `.env.example`
* **Job**: Stores environment configuration parameters such as `GCP_PROJECT_ID`, `GCS_BUCKET_NAME`, `GCP_REGION`, `KAFKA_BOOTSTRAP_SERVERS`, `POSTGRES_USER`, and credential file paths.
* **Connection**: `.env` is loaded automatically by `docker-compose.yml` and `deploy.sh` to inject parameters into containers and Terraform.

#### 2. `.gitignore`
* **Job**: Prevents confidential files (such as GCP Service Account JSON keys in `keys/`, `.env`, local Python caches, dbt targets, and Airflow logs) from being committed to Git.
* **Connection**: Read by `git` during staging and push operations.

#### 3. `docker-compose.yml`
* **Job**: Multi-container specification that spins up 7 services (`postgres`, `kafka`, `airflow-init`, `airflow-webserver`, `airflow-scheduler`, `producer`, `consumer`).
* **Connection**: Connects container networking over `opensky-elt-pipeline_default`, mounts shared code directories (`./dags`, `./dbt`, `./keys`), and passes environment variables.

#### 4. `deploy.sh`
* **Job**: VM provisioning shell script that installs Docker, Docker Compose, and Terraform, clones the repository, sources `.env`, runs `terraform apply`, and boots containers using `docker compose up -d`.
* **Connection**: Serves as the one-click setup script on remote GCP Compute Engine instances.

#### 5. `init_project.sh`
* **Job**: Shell script containing a single `mkdir -p` + `touch` pipeline command to generate the empty directory layout and required files.
* **Connection**: Used during initial setup from an empty workspace.

---

### Streaming Microservices (`streaming/`, `producer/`, `consumer/`)

#### 6. `streaming/producer.py` & `producer/main.py`
* **Job**: Continuously polls the OpenSky API, parses raw state vectors into clean dictionaries, and sends JSON messages to the Kafka topic `opensky_flights_raw`.
* **Connection**: Talks to external OpenSky REST API via HTTP and internal Kafka broker on `kafka:9092`.

#### 7. `producer/Dockerfile`
* **Job**: Container image specification based on `python:3.10-slim`, installing `requests` and `kafka-python`, and copying `main.py`.
* **Connection**: Used by `docker-compose.yml` to build the `opensky-producer` service.

#### 8. `streaming/consumer.py` & `consumer/main.py`
* **Job**: Consumes records from Kafka, accumulates them into a buffer, formats them into a pandas DataFrame every 60 seconds, serializes to Parquet format, and uploads to Google Cloud Storage.
* **Connection**: Connects to Kafka broker on `kafka:9092` and Google Cloud Storage using `google-cloud-storage` library with credentials at `/keys/gcp-key.json`.

#### 9. `consumer/Dockerfile`
* **Job**: Container image specification installing `kafka-python`, `pandas`, `pyarrow`, and `google-cloud-storage`.
* **Connection**: Used by `docker-compose.yml` to build the `opensky-consumer` service.

---

### Orchestration & Airflow (`airflow/`)

#### 10. `airflow/Dockerfile`
* **Job**: Custom Dockerfile based on `apache/airflow:2.8.1-python3.10` that pre-bakes `dbt-bigquery`, `google-cloud-storage`, and `google-cloud-bigquery` directly into the image.
* **Connection**: Provides an instant startup environment for `airflow-webserver`, `airflow-scheduler`, and `airflow-init` without runtime `pip install` delays.

#### 11. `airflow/dags/flight_pipeline.py`
* **Job**: Airflow DAG defined on an hourly schedule (`@hourly`) that orchestrates listing Parquet files in GCS, loading them into BigQuery table `raw_flights.raw_states`, purging processed GCS objects, and triggering dbt transformations.
* **Connection**: Interacts with GCS via `GCSListObjectsOperator` and `GCSDeleteObjectsOperator`, BigQuery via `GCSToBigQueryOperator`, and dbt via `BashOperator`.

---

### Data Transformation & dbt (`dbt/`)

#### 12. `dbt/dbt_project.yml`
* **Job**: Main dbt project configuration file defining project name (`opensky_elt`), profile connection (`opensky_profile`), model directory structure, and materialization policies (views for staging, tables for marts).
* **Connection**: Read by `dbt` CLI commands executed inside the Airflow container.

#### 13. `dbt/profiles.yml`
* **Job**: Defines BigQuery connection credentials (`type: bigquery`, `method: service-account`, `location: asia-southeast1`, `keyfile: /keys/gcp-key.json`).
* **Connection**: Connects dbt directly to Google BigQuery API.

#### 14. `dbt/models/sources.yml`
* **Job**: Declares raw BigQuery tables (`raw_flights.raw_states`) as dbt sources, enabling lineage tracking and reference via `{{ source('raw_flights', 'raw_states') }}`.
* **Connection**: Referenced in dbt SQL models.

#### 15. `dbt/models/marts/active_flights.sql`
* **Job**: Analytical SQL model that queries `raw_flights.raw_states`, filters active non-grounded aircraft, aggregates total aircraft count and average velocity grouped by `origin_country`, and writes the materialized table to `marts_flights.active_flights`.
* **Connection**: Executed by `dbt run` inside BigQuery.

---

### Infrastructure as Code (`terraform/`)

#### 16. `terraform/main.tf`
* **Job**: Provisions Google Cloud infrastructure: GCS storage bucket (`google_storage_bucket.raw_flights_bucket`) with a 30-day lifecycle rule, and two BigQuery datasets (`raw_flights` for raw ingestion and `marts_flights` for transformed analytical datamarts).
* **Connection**: Managed via Google Cloud Provider (`hashicorp/google`) authenticated with service account credentials.

#### 17. `terraform/variables.tf`
* **Job**: Declares input variables (`project_id`, `region`, `location`, `gcs_bucket_name`, `bq_raw_dataset_id`, `bq_marts_dataset_id`) with default fallback values.
* **Connection**: Consumed by `main.tf` and populated via `deploy.sh` or command-line flags.

#### 18. `terraform/outputs.tf`
* **Job**: Exports created GCP resource attributes (GCS Bucket URL, Raw Dataset ID, Marts Dataset ID) upon completion of `terraform apply`.
* **Connection**: Outputs values to the console and environment scripts.
