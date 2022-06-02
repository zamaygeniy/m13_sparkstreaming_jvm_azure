terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~>2.81.0"
    }

    databricks = {
      source = "databrickslabs/databricks"
    }
  }
}
