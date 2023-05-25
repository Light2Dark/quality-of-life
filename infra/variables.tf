variable "google_credentials_file" {}

variable "project_id" {
    description = "The project ID to deploy to"
    type = string
}

variable "region" {
    default = "asia-southeast1" 
    type = string
}

variable "zone" {
    default = "asia-southeast1-a"
    type = string
}