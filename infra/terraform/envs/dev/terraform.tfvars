environment = "dev"
project_name = "gaming-commerce-lakehouse"

# cloud is passed on the CLI: -var="cloud=${CLOUD}"
gcp_project_id        = "REPLACE_ME"
gcp_region            = "us-central1"
azure_subscription_id = "REPLACE_ME"
azure_location        = "eastus"

enable_gpu_pool = false

cost_controls = {
  spot_instances   = true
  autoscale_min    = 1
  autoscale_max    = 4
  budget_usd_month = 300
}
