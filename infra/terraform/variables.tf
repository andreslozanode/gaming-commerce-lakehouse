variable "cloud" {
  type        = string
  description = "gcp | azure — the single toggle that selects the entire target stack"
  validation {
    condition     = contains(["gcp", "azure"], var.cloud)
    error_message = "cloud must be gcp or azure"
  }
}

variable "environment" {
  type = string
  validation {
    condition     = contains(["dev", "qa", "prod"], var.environment)
    error_message = "environment must be dev, qa or prod"
  }
}

variable "project_name" {
  type    = string
  default = "gaming-commerce-lakehouse"
}

variable "gcp_project_id" {
  type    = string
  default = ""
}

variable "gcp_region" {
  type    = string
  default = "us-central1"
}

variable "azure_subscription_id" {
  type    = string
  default = ""
}

variable "azure_location" {
  type    = string
  default = "eastus"
}

variable "enable_gpu_pool" {
  type        = bool
  default     = true
  description = "Provision the A100 pool used by the PyTorch/TensorFlow training jobs"
}

variable "cost_controls" {
  type = object({
    spot_instances    = bool
    autoscale_min     = number
    autoscale_max     = number
    budget_usd_month  = number
  })
  default = {
    spot_instances   = true
    autoscale_min    = 1
    autoscale_max    = 8
    budget_usd_month = 500
  }
}

variable "tags" {
  type = map(string)
  default = {
    project    = "gaming-commerce-lakehouse"
    managed_by = "terraform"
    domain     = "gaming_commerce"
  }
}
