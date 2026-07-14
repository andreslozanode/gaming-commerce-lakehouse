locals {
  name    = "${var.project_name}-${var.environment}"
  short   = substr(replace(var.project_name, "-", ""), 0, 16)
  is_prod = var.environment == "prod"
}

resource "azurerm_resource_group" "main" {
  name     = "rg-${local.name}"
  location = var.location
  tags     = var.tags
}

# --------------------------- Storage: ADLS Gen2, one container per layer ---------------------------
resource "azurerm_storage_account" "lake" {
  name                     = "${local.short}${var.environment}dls"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = local.is_prod ? "ZRS" : "LRS"
  is_hns_enabled           = true # hierarchical namespace = ADLS Gen2, required for Delta
  min_tls_version          = "TLS1_2"
  shared_access_key_enabled = false # Entra ID / Managed Identity only
  tags                     = var.tags

  blob_properties {
    versioning_enabled = local.is_prod
    delete_retention_policy {
      days = local.is_prod ? 30 : 7
    }
  }
}

resource "azurerm_storage_container" "layer" {
  for_each              = toset(var.layers)
  name                  = "${local.short}${var.environment}${each.value}"
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

resource "azurerm_storage_management_policy" "lifecycle" {
  storage_account_id = azurerm_storage_account.lake.id
  rule {
    name    = "landing-expiry"
    enabled = true
    filters {
      blob_types   = ["blockBlob"]
      prefix_match = ["${local.short}${var.environment}landing", "${local.short}${var.environment}temp"]
    }
    actions {
      base_blob {
        delete_after_days_since_modification_greater_than = 30
      }
    }
  }
  rule {
    name    = "bronze-cooldown"
    enabled = true
    filters {
      blob_types   = ["blockBlob"]
      prefix_match = ["${local.short}${var.environment}bronze"]
    }
    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than = 90
      }
    }
  }
}

# --------------------------- Event backbone: Event Hubs (Kafka endpoint) ---------------------------
resource "azurerm_eventhub_namespace" "main" {
  name                     = "evhns-${local.name}"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  sku                      = local.is_prod ? "Standard" : "Basic"
  capacity                 = local.is_prod ? 4 : 1
  auto_inflate_enabled     = local.is_prod
  maximum_throughput_units = local.is_prod ? 20 : 0
  tags                     = var.tags
}

resource "azurerm_eventhub" "topic" {
  for_each = toset(["gc-purchase-events", "gc-purchase-events-dlq", "gc-cdc-orders", "gc-cdc-subscriptions", "gc-cdc-customers"])

  name              = "${each.value}-${var.environment}"
  namespace_id      = azurerm_eventhub_namespace.main.id
  partition_count   = local.is_prod ? 8 : 2
  message_retention = local.is_prod ? 7 : 1
}

resource "azurerm_eventhub_namespace_schema_group" "avro" {
  name                 = "gc-schemas"
  namespace_id         = azurerm_eventhub_namespace.main.id
  schema_compatibility = "Backward"
  schema_type          = "Avro"
}

# --------------------------- Serving warehouse: Synapse ---------------------------
resource "azurerm_synapse_workspace" "main" {
  name                                 = "syn-${local.short}${var.environment}"
  resource_group_name                  = azurerm_resource_group.main.name
  location                             = azurerm_resource_group.main.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.synapse.id
  sql_administrator_login              = "gcadmin"
  sql_administrator_login_password     = random_password.synapse.result
  managed_virtual_network_enabled      = true
  tags                                 = var.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_storage_data_lake_gen2_filesystem" "synapse" {
  name               = "synapse"
  storage_account_id = azurerm_storage_account.lake.id
}

resource "random_password" "synapse" {
  length  = 32
  special = true
}

# --------------------------- OLTP source for CDC ---------------------------
resource "azurerm_postgresql_flexible_server" "oltp" {
  name                   = "psql-${local.name}"
  resource_group_name    = azurerm_resource_group.main.name
  location               = azurerm_resource_group.main.location
  version                = "16"
  sku_name               = local.is_prod ? "GP_Standard_D4ds_v5" : "B_Standard_B1ms"
  storage_mb             = local.is_prod ? 131072 : 32768
  backup_retention_days  = local.is_prod ? 35 : 7
  administrator_login    = "gcadmin"
  administrator_password = random_password.postgres.result
  zone                   = "1"
  tags                   = var.tags
}

resource "azurerm_postgresql_flexible_server_configuration" "logical" {
  name      = "wal_level"
  server_id = azurerm_postgresql_flexible_server.oltp.id
  value     = "logical" # Debezium prerequisite
}

resource "random_password" "postgres" {
  length  = 32
  special = true
}

# --------------------------- Databricks workspace ---------------------------
resource "azurerm_databricks_workspace" "main" {
  name                = "dbw-${local.name}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = local.is_prod ? "premium" : "standard"
  tags                = var.tags
}

# --------------------------- Azure ML (model registry + online endpoints) ---------------------------
resource "azurerm_machine_learning_workspace" "main" {
  name                    = "mlw-${local.name}"
  resource_group_name     = azurerm_resource_group.main.name
  location                = azurerm_resource_group.main.location
  application_insights_id = azurerm_application_insights.main.id
  key_vault_id            = azurerm_key_vault.main.id
  storage_account_id      = azurerm_storage_account.lake.id
  tags                    = var.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_application_insights" "main" {
  name                = "appi-${local.name}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  application_type    = "web"
}

# --------------------------- Secrets: Key Vault ---------------------------
resource "azurerm_key_vault" "main" {
  name                       = "kv-${local.short}${var.environment}"
  resource_group_name        = azurerm_resource_group.main.name
  location                   = azurerm_resource_group.main.location
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  purge_protection_enabled   = local.is_prod
  soft_delete_retention_days = 7
  rbac_authorization_enabled = true
  tags                       = var.tags
}

data "azurerm_client_config" "current" {}

# --------------------------- Keyless CI: federated credential for GitHub OIDC ---------------------------
resource "azurerm_user_assigned_identity" "pipeline" {
  name                = "id-${local.name}-ci"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
}

resource "azurerm_federated_identity_credential" "github" {
  name                = "github-oidc-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  parent_id           = azurerm_user_assigned_identity.pipeline.id
  audience            = ["api://AzureADTokenExchange"]
  issuer              = "https://token.actions.githubusercontent.com"
  subject             = "repo:ORG/gaming-commerce-lakehouse:environment:${var.environment}"
}

resource "azurerm_role_assignment" "pipeline_storage" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.pipeline.principal_id
}

# --------------------------- Budget guardrail ---------------------------
resource "azurerm_consumption_budget_resource_group" "monthly" {
  name              = "budget-${local.name}"
  resource_group_id = azurerm_resource_group.main.id
  amount            = var.cost_controls.budget_usd_month
  time_grain        = "Monthly"

  time_period {
    start_date = formatdate("YYYY-MM-01'T'00:00:00Z", timestamp())
  }

  notification {
    enabled        = true
    threshold      = 90
    operator       = "GreaterThan"
    contact_emails = ["data-platform@example.com"]
  }
}
