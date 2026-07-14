terraform {
  required_version = ">= 1.9.0"
  required_providers {
    google     = { source = "hashicorp/google", version = "~> 6.12" }
    azurerm    = { source = "hashicorp/azurerm", version = "~> 4.14" }
    databricks = { source = "databricks/databricks", version = "~> 1.60" }
    random     = { source = "hashicorp/random", version = "~> 3.6" }
  }
}
