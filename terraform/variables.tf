variable "project_id" {
  description = "The GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region for infrastructure deployment"
  type        = string
  default     = "us-central1"
}

variable "location" {
  description = "GCP Multi-Region Location for Storage & BigQuery"
  type        = string
  default     = "US"
}

variable "gcs_bucket_name" {
  description = "Name of the Google Cloud Storage bucket for raw flight data"
  type        = string
}

variable "bq_raw_dataset_id" {
  description = "BigQuery dataset ID for raw ingestion layer"
  type        = string
  default     = "raw_flights"
}

variable "bq_marts_dataset_id" {
  description = "BigQuery dataset ID for dbt transformation analytics layer"
  type        = string
  default     = "marts_flights"
}
