output "gcs_bucket_url" {
  description = "GCS Bucket URL for Raw Flight Data"
  value       = google_storage_bucket.raw_flights_bucket.url
}

output "raw_dataset_id" {
  description = "BigQuery Raw Dataset ID"
  value       = google_bigquery_dataset.raw_flights.dataset_id
}

output "marts_dataset_id" {
  description = "BigQuery Marts Dataset ID"
  value       = google_bigquery_dataset.marts_flights.dataset_id
}
