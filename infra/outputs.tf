output "gcp_project_id" {
    value = var.project_id
    description = "project id"
}

output "gcp_credentials" {
    value = var.google_credentials_file
    description = "credentials for gcp"
}

output "gcs_aq_bucket_name" {
    value = google_storage_bucket.aq_data_storage.name
    description = "bucket name for air quality data"
}

output "gcs_weather_bucket_name" {
    value = google_storage_bucket.weather_data_storage.name
    description = "bucket name for weather data"
}

output "dev_bq_dataset_name" {
    value = google_bigquery_dataset.dev_dataset.dataset_id
    description = "dataset name for dev environment"
}

output "prod_bq_dataset_name" {
    value = google_bigquery_dataset.prod_dataset.dataset_id
    description = "dataset name for prod environment"
}