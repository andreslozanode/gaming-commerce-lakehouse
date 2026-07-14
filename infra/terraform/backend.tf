# Remote state, per cloud, per environment. Selected at init time:
#   terraform init -backend-config=envs/${ENVIRONMENT}/backend.${CLOUD}.hcl
terraform {
  backend "gcs" {}   # overridden by -backend=false + azurerm backend block when CLOUD=azure
}
