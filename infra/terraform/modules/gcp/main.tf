locals {
  name   = "${var.project_name}-${var.environment}"
  is_prod = var.environment == "prod"
}

# --------------------------- Storage: one bucket per medallion layer ---------------------------
resource "google_storage_bucket" "layer" {
  for_each                    = toset(var.layers)
  name                        = "${local.name}-${each.value}"
  location                    = var.region
  uniform_bucket_level_access = true          # no ACLs, IAM only
  public_access_prevention    = "enforced"
  force_destroy               = !local.is_prod
  labels                      = var.labels

  versioning {
    enabled = local.is_prod
  }

  # Cost: landing/temp age out fast; bronze cools; gold stays hot.
  dynamic "lifecycle_rule" {
    for_each = contains(["landing", "temp"], each.value) ? [1] : []
    content {
      condition {
        age = 30
      }
      action {
        type = "Delete"
      }
    }
  }
  dynamic "lifecycle_rule" {
    for_each = each.value == "bronze" ? [1] : []
    content {
      condition {
        age = 90
      }
      action {
        type          = "SetStorageClass"
        storage_class = "NEARLINE"
      }
    }
  }

  encryption {
    default_kms_key_name = google_kms_crypto_key.data.id
  }
}

resource "google_kms_key_ring" "main" {
  name     = "${local.name}-kr"
  location = var.region
}

resource "google_kms_crypto_key" "data" {
  name            = "${local.name}-cmek"
  key_ring        = google_kms_key_ring.main.id
  rotation_period = "7776000s" # 90d
}

# --------------------------- Event backbone: Pub/Sub ---------------------------
locals {
  topics = ["gc-purchase-events", "gc-cdc-orders", "gc-cdc-subscriptions", "gc-cdc-customers"]
}

resource "google_pubsub_topic" "events" {
  for_each                   = toset(local.topics)
  name                       = "${each.value}-${var.environment}"
  message_retention_duration = "604800s" # 7d replay window
  labels                     = var.labels
  schema_settings {
    schema   = google_pubsub_schema.purchase_event.id
    encoding = "JSON"
  }
}

resource "google_pubsub_schema" "purchase_event" {
  name       = "gc-purchase-event-${var.environment}"
  type       = "AVRO"
  definition = file("${path.module}/schemas/purchase_event.avsc")
}

resource "google_pubsub_topic" "dlq" {
  name   = "gc-purchase-events-dlq-${var.environment}"
  labels = var.labels
}

resource "google_pubsub_subscription" "events" {
  for_each                   = google_pubsub_topic.events
  name                       = "${each.value.name}-sub"
  topic                      = each.value.id
  ack_deadline_seconds       = 60
  enable_exactly_once_delivery = true
  message_retention_duration = "604800s"

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dlq.id
    max_delivery_attempts = 5
  }
}

# --------------------------- Serving warehouse: BigQuery ---------------------------
resource "google_bigquery_dataset" "marts" {
  dataset_id                 = "gaming_commerce_${var.environment}"
  location                   = var.region
  delete_contents_on_destroy = !local.is_prod
  labels                     = var.labels
  default_table_expiration_ms = local.is_prod ? null : 2592000000 # 30d in non-prod
  default_encryption_configuration {
    kms_key_name = google_kms_crypto_key.data.id
  }
}

# --------------------------- Orchestration: Cloud Composer ---------------------------
resource "google_composer_environment" "airflow" {
  name   = "${local.name}-composer"
  region = var.region
  config {
    software_config {
      image_version = "composer-3-airflow-2.10.2"
      pypi_packages = {
        "apache-airflow-providers-databricks" = ""
        "databricks-sdk"                      = ""
      }
      env_variables = {
        CLOUD       = "gcp"
        ENVIRONMENT = var.environment
      }
    }
    workloads_config {
      scheduler {
        cpu        = 1
        memory_gb  = 2
        storage_gb = 1
        count      = local.is_prod ? 2 : 1
      }
      worker {
        cpu        = 1
        memory_gb  = 4
        storage_gb = 5
        min_count  = var.cost_controls.autoscale_min
        max_count  = var.cost_controls.autoscale_max
      }
    }
    environment_size = local.is_prod ? "ENVIRONMENT_SIZE_MEDIUM" : "ENVIRONMENT_SIZE_SMALL"
  }
}

# --------------------------- OLTP source for CDC ---------------------------
resource "google_sql_database_instance" "oltp" {
  name             = "${local.name}-pg"
  database_version = "POSTGRES_16"
  region           = var.region
  settings {
    tier              = local.is_prod ? "db-custom-4-16384" : "db-g1-small"
    availability_type = local.is_prod ? "REGIONAL" : "ZONAL"
    backup_configuration {
      enabled                        = true
      point_in_time_recovery_enabled = true
    }
    database_flags {
      name  = "cloudsql.logical_decoding"   # required by Debezium
      value = "on"
    }
    database_flags {
      name  = "max_replication_slots"
      value = "8"
    }
    ip_configuration {
      ipv4_enabled    = false
      private_network = "default"
    }
  }
  deletion_protection = local.is_prod
}

# --------------------------- Identity: Workload Identity Federation (keyless CI) ---------------------------
resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "${local.name}-gh"
  display_name              = "GitHub Actions"
}

resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-oidc"
  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
    "attribute.ref"        = "assertion.ref"
  }
  # Only this repo, and for prod only the protected ref, may mint tokens.
  attribute_condition = var.environment == "prod" ? "assertion.repository=='ORG/gaming-commerce-lakehouse' && assertion.ref=='refs/heads/main'" : "assertion.repository=='ORG/gaming-commerce-lakehouse'"
  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account" "pipeline" {
  account_id   = "gc-pipeline-${var.environment}"
  display_name = "Gaming Commerce pipeline SA"
}

resource "google_project_iam_member" "pipeline" {
  for_each = toset([
    "roles/storage.objectAdmin",
    "roles/bigquery.dataEditor",
    "roles/pubsub.editor",
    "roles/dataflow.developer",
    "roles/aiplatform.user",
    "roles/secretmanager.secretAccessor",
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# --------------------------- Secrets ---------------------------
resource "google_secret_manager_secret" "app" {
  for_each  = toset(["kaggle-api-token", "postgres-dsn", "databricks-host-gcp", "databricks-token"])
  secret_id = "${each.value}-${var.environment}"
  replication {
    auto {}
  }
  labels = var.labels
}

# --------------------------- Budget guardrail ---------------------------
resource "google_billing_budget" "monthly" {
  billing_account = data.google_project.this.billing_account
  display_name    = "${local.name}-budget"
  budget_filter {
    projects = ["projects/${data.google_project.this.number}"]
  }
  amount {
    specified_amount {
      currency_code = "USD"
      units         = tostring(var.cost_controls.budget_usd_month)
    }
  }
  threshold_rules {
    threshold_percent = 0.5
  }
  threshold_rules {
    threshold_percent = 0.9
  }
  threshold_rules {
    threshold_percent = 1.0
  }
}

data "google_project" "this" {
  project_id = var.project_id
}
