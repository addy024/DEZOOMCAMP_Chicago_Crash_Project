variable "credentials" {
  description = "My Credentials"
  default     = "" # replace with the key.json location
  #ex: if you have a directory where this file is called keys with your service account json file
  #saved there as my-creds.json you could use default = "./keys/my-creds.json"
}


variable "project" {
  description = "Project"
  default     = "" # replace with the project id
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default = "aisa-south1"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default = "asia"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default = "chicago_crash_warehouse"
}

variable "gcs_bucket_bronze_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default = "chicago_crash_bronze_layer"
}

variable "gcs_bucket_silver_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default = "chicago_crash_silver_layer"
}

variable "gcs_bucket_gold_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default = "chicago_crash_gold_layer"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}