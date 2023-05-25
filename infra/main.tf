terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

provider "google" {
  credentials = file(var.google_credentials_file)
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

resource "google_bigquery_dataset" "dev_dataset" {
  dataset_id = "dev"
  location   = var.region
}

resource "google_bigquery_dataset" "prod_dataset" {
  dataset_id = "prod"
  location   = var.region
}

# Air quality Infra
resource "google_storage_bucket" "aq_data_storage" {
  name          = "air-quality-data-storage"
  location      = var.region
  storage_class = "standard"
}

# Weather infra
resource "google_storage_bucket" "weather_data_storage" {
  name          = "weather-data-storage"
  location      = var.region
  storage_class = "standard"
}