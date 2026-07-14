output "storage_uris" {
  value = {
    for layer, container in azurerm_storage_container.layer :
    layer => "abfss://${container.name}@${azurerm_storage_account.lake.name}.dfs.core.windows.net"
  }
}
output "eventhub_namespace" { value = azurerm_eventhub_namespace.main.name }
output "synapse_workspace"  { value = azurerm_synapse_workspace.main.name }
output "databricks_host"    { value = azurerm_databricks_workspace.main.workspace_url }
output "key_vault_uri"      { value = azurerm_key_vault.main.vault_uri }
output "ci_identity_id"     { value = azurerm_user_assigned_identity.pipeline.client_id }
