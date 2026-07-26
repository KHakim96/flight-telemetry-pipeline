terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# GCS Bucket for Raw Flight JSON/Parquet Data
resource "google_storage_bucket" "raw_flights_bucket" {
  name                        = var.gcs_bucket_name
  location                    = var.location
  force_destroy               = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}

# BigQuery Raw Ingestion Dataset
resource "google_bigquery_dataset" "raw_flights" {
  dataset_id                 = var.bq_raw_dataset_id
  friendly_name              = "Raw Flights Dataset"
  description                = "Landing zone for raw OpenSky stream records from GCS consumer"
  location                   = var.location
  delete_contents_on_destroy = true
}

# BigQuery Analytics Marts Dataset
resource "google_bigquery_dataset" "marts_flights" {
  dataset_id                 = var.bq_marts_dataset_id
  friendly_name              = "Marts Flights Dataset"
  description                = "Transformed analytical models, metrics, and dimensional tables built with dbt"
  location                   = var.location
  delete_contents_on_destroy = true
}
