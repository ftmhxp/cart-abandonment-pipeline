terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.5"
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ── Variables ──────────────────────────────────────────────────────────────────

variable "project_id" {
  description = "GCP project ID"
  type        = string
  default     = "cart-abandonment-prod"
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

# ── GCS Bucket ────────────────────────────────────────────────────────────────

resource "google_storage_bucket" "lake" {
  name          = "cart-pipeline-lake"
  location      = "US"
  force_destroy = false

  versioning {
    enabled = true
  }

  uniform_bucket_level_access = true
}

# ── BigQuery Dataset ──────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "cart_pipeline" {
  dataset_id = "cart_pipeline"
  location   = "US"
}

# ── BigQuery Tables ───────────────────────────────────────────────────────────

resource "google_bigquery_table" "abandoned_carts" {
  dataset_id          = google_bigquery_dataset.cart_pipeline.dataset_id
  table_id            = "abandoned_carts"
  deletion_protection = false

  schema = jsonencode([
    { name = "session_id",       type = "STRING",    mode = "NULLABLE" },
    { name = "user_id",          type = "STRING",    mode = "NULLABLE" },
    { name = "cart_value",       type = "FLOAT64",   mode = "NULLABLE" },
    { name = "item_count",       type = "INT64",     mode = "NULLABLE" },
    { name = "first_add_time",   type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "last_event_time",  type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "minutes_inactive", type = "FLOAT64",   mode = "NULLABLE" },
    { name = "recovery_sent",    type = "BOOL",      mode = "NULLABLE" },
    { name = "abandoned_at",     type = "TIMESTAMP", mode = "NULLABLE" },
  ])
}

resource "google_bigquery_table" "hourly_stats" {
  dataset_id          = google_bigquery_dataset.cart_pipeline.dataset_id
  table_id            = "hourly_stats"
  deletion_protection = false

  schema = jsonencode([
    { name = "hour",               type = "INT64",   mode = "NULLABLE" },
    { name = "total_sessions",     type = "INT64",   mode = "NULLABLE" },
    { name = "abandoned_sessions", type = "INT64",   mode = "NULLABLE" },
    { name = "converted_sessions", type = "INT64",   mode = "NULLABLE" },
    { name = "abandonment_rate",   type = "FLOAT64", mode = "NULLABLE" },
    { name = "revenue_lost",       type = "FLOAT64", mode = "NULLABLE" },
  ])
}

# ── Outputs ───────────────────────────────────────────────────────────────────

output "bucket_name" {
  description = "GCS bucket for the Delta lake"
  value       = google_storage_bucket.lake.name
}

output "dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.cart_pipeline.dataset_id
}
