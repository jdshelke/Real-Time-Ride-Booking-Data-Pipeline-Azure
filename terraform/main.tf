resource "azurerm_resource_group" "res-0" {
  location = "eastus2"
  name     = "RG-Azure-Project"
}
resource "azurerm_databricks_access_connector" "res-15" {
  location            = "uksouth"
  name                = "storage-access-to-databricks"
  resource_group_name = azurerm_resource_group.res-0.name
  identity {
    type = "SystemAssigned"
  }
}
resource "azurerm_databricks_workspace" "res-16" {
  location            = "uksouth"
  name                = "databricks-azure-project"
  resource_group_name = azurerm_resource_group.res-0.name
  sku                 = "premium"
}
resource "azurerm_eventhub_namespace" "res-17" {
  location            = "uksouth"
  name                = "uber-eventhub-ns"
  resource_group_name = azurerm_resource_group.res-0.name
  sku                 = "Basic"
}
resource "azurerm_eventhub_namespace_authorization_rule" "res-18" {
  listen              = true
  manage              = true
  name                = "RootManageSharedAccessKey"
  namespace_name      = "uber-eventhub-ns"
  resource_group_name = azurerm_resource_group.res-0.name
  send                = true
  depends_on = [
    azurerm_eventhub_namespace.res-17,
  ]
}
resource "azurerm_eventhub" "res-19" {
  message_retention = 1
  name              = "uber-ride-events"
  namespace_id      = azurerm_eventhub_namespace.res-17.id
  partition_count   = 2
}
resource "azurerm_eventhub_authorization_rule" "res-20" {
  eventhub_name       = "uber-ride-events"
  listen              = true
  name                = "eventhub-policy"
  namespace_name      = "uber-eventhub-ns"
  resource_group_name = azurerm_resource_group.res-0.name
  send                = true
  depends_on = [
    azurerm_eventhub.res-19,
  ]
}
resource "azurerm_eventhub_consumer_group" "res-21" {
  eventhub_name       = "uber-ride-events"
  name                = "$Default"
  namespace_name      = "uber-eventhub-ns"
  resource_group_name = azurerm_resource_group.res-0.name
  depends_on = [
    azurerm_eventhub.res-19,
  ]
}
resource "azurerm_key_vault" "res-23" {
  location            = "uksouth"
  name                = "jd-azure-project-kv"
  resource_group_name = azurerm_resource_group.res-0.name
  sku_name            = "standard"
  tenant_id           = "fa3126e9-ab3c-485b-bce7-c76913f2142b"
}
resource "azurerm_key_vault_secret" "res-24" {
  key_vault_id = azurerm_key_vault.res-23.id
  name         = "eventhub-connection-string"
  value        = "REPLACE_WITH_SECRET"
}
resource "azurerm_storage_account" "res-67" {
  account_replication_type        = "LRS"
  account_tier                    = "Standard"
  allow_nested_items_to_be_public = false
  is_hns_enabled                  = true
  location                        = "uksouth"
  name                            = "jdazstorageac"
  resource_group_name             = azurerm_resource_group.res-0.name
}
resource "azurerm_storage_container" "res-71" {
  name               = "databricks-metastore"
  storage_account_id = "/subscriptions/99ea9e91-505c-4801-98ac-a07dae7eeedc/resourceGroups/RG-Azure-Project/providers/Microsoft.Storage/storageAccounts/jdazstorageac"
  depends_on = [
    # One of azurerm_storage_account.res-67,azurerm_storage_account_queue_properties.res-76 (can't auto-resolve as their ids are identical)
  ]
}
resource "azurerm_storage_container" "res-73" {
  name               = "real-time-ride-booking-data-pipeline-azure"
  storage_account_id = "/subscriptions/99ea9e91-505c-4801-98ac-a07dae7eeedc/resourceGroups/RG-Azure-Project/providers/Microsoft.Storage/storageAccounts/jdazstorageac"
  depends_on = [
    # One of azurerm_storage_account.res-67,azurerm_storage_account_queue_properties.res-76 (can't auto-resolve as their ids are identical)
  ]
}
resource "azurerm_storage_account_queue_properties" "res-76" {
  storage_account_id = azurerm_storage_account.res-67.id
  hour_metrics {
    version = "1.0"
  }
  logging {
    delete  = false
    read    = false
    version = "1.0"
    write   = false
  }
  minute_metrics {
    version = "1.0"
  }
}
