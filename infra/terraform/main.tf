locals {
  is_gcp   = var.cloud == "gcp"
  is_azure = var.cloud == "azure"
  name     = "${var.project_name}-${var.environment}"
  layers   = ["landing", "bronze", "silver", "gold", "temp"]

  common_tags = merge(var.tags, {
    environment = var.environment
    cloud       = var.cloud
  })
}

# ---------------------------------------------------------------------------
# GCP stack — created only when cloud = "gcp"
# ---------------------------------------------------------------------------
module "gcp" {
  source = "./modules/gcp"
  count  = local.is_gcp ? 1 : 0

  project_id      = var.gcp_project_id
  region          = var.gcp_region
  environment     = var.environment
  project_name    = var.project_name
  layers          = local.layers
  enable_gpu_pool = var.enable_gpu_pool
  cost_controls   = var.cost_controls
  labels          = local.common_tags
}

# ---------------------------------------------------------------------------
# Azure stack — created only when cloud = "azure"
# ---------------------------------------------------------------------------
module "azure" {
  source = "./modules/azure"
  count  = local.is_azure ? 1 : 0

  subscription_id = var.azure_subscription_id
  location        = var.azure_location
  environment     = var.environment
  project_name    = var.project_name
  layers          = local.layers
  enable_gpu_pool = var.enable_gpu_pool
  cost_controls   = var.cost_controls
  tags            = local.common_tags
}
