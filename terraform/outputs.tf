# Outputs feed the single root.env. Retrieve with:
#   terraform output -raw <name>
# or dump all as JSON:  terraform output -json > tf_outputs.json

output "environment_id" {
  description = "Confluent Cloud environment id (Flink catalog)"
  value       = confluent_environment.env.id
}

output "kafka_cluster_id" {
  description = "Kafka cluster id (Flink database)"
  value       = confluent_kafka_cluster.kafka.id
}

output "bootstrap_servers" {
  description = "BOOTSTRAP_SERVERS (strip the SASL_SSL:// prefix for clients)"
  value       = confluent_kafka_cluster.kafka.bootstrap_endpoint
}

output "schema_registry_url" {
  description = "SCHEMA_REGISTRY_URL"
  value       = data.confluent_schema_registry_cluster.sr.rest_endpoint
}

output "flink_compute_pool_id" {
  value = confluent_flink_compute_pool.pool.id
}

output "flink_region_rest_endpoint" {
  value = data.confluent_flink_region.main.rest_endpoint
}

# --- client credentials (emulator + backend) -> KAFKA_API_KEY / SECRET ------
output "kafka_api_key" {
  description = "KAFKA_API_KEY"
  value       = confluent_api_key.clients_kafka.id
}

output "kafka_api_secret" {
  description = "KAFKA_API_SECRET"
  value       = confluent_api_key.clients_kafka.secret
  sensitive   = true
}

# --- schema registry credentials -> SCHEMA_REGISTRY_API_KEY / SECRET --------
output "schema_registry_api_key" {
  description = "SCHEMA_REGISTRY_API_KEY"
  value       = confluent_api_key.sr.id
}

output "schema_registry_api_secret" {
  description = "SCHEMA_REGISTRY_API_SECRET"
  value       = confluent_api_key.sr.secret
  sensitive   = true
}

# --- Flink management key (for the CLI / connection creation) ---------------
output "flink_api_key" {
  value = confluent_api_key.flink.id
}

output "flink_api_secret" {
  value     = confluent_api_key.flink.secret
  sensitive = true
}

# --- managed MCP / RTCE (Claude Code) ---------------------------------------
output "mcp_url" {
  description = "Regional, cluster-scoped managed MCP endpoint (written to .env as CC_MCP_URL; referenced by .mcp.json)."
  value       = local.mcp_url
}

output "mcp_reader_service_account" {
  description = "Create a GLOBAL API key owned by THIS service account (Console → Cloud API keys → Add key → Global), then export CC_MCP_AUTH=base64('<key>:<secret>'). The regional MCP server rejects Cloud API keys, so Terraform can't mint this."
  value       = confluent_service_account.mcp_reader.id
}

# --- domain constants -> .env (so the Python apps match Flink) --------------
output "station_name" { value = var.station_name }
output "station_capacity" { value = var.station_capacity }
output "agg_window_seconds" { value = var.agg_window_seconds }
output "pct_low" { value = var.pct_low }
output "pct_high" { value = var.pct_high }
output "pct_critical" { value = var.pct_critical }

# Convenience: the .env lines for the Kafka/SR block. Secrets are redacted
# unless you read them explicitly with `terraform output -raw`.
output "env_block" {
  description = "Paste-ready .env lines (run `terraform output -raw env_block`)"
  sensitive   = true
  value       = <<-EOT
    BOOTSTRAP_SERVERS=${replace(confluent_kafka_cluster.kafka.bootstrap_endpoint, "SASL_SSL://", "")}
    KAFKA_API_KEY=${confluent_api_key.clients_kafka.id}
    KAFKA_API_SECRET=${confluent_api_key.clients_kafka.secret}
    SCHEMA_REGISTRY_URL=${data.confluent_schema_registry_cluster.sr.rest_endpoint}
    SCHEMA_REGISTRY_API_KEY=${confluent_api_key.sr.id}
    SCHEMA_REGISTRY_API_SECRET=${confluent_api_key.sr.secret}
    STATION_NAME=${var.station_name}
    STATION_CAPACITY=${var.station_capacity}
    AGG_WINDOW_SECONDS=${var.agg_window_seconds}
    PCT_LOW=${var.pct_low}
    PCT_HIGH=${var.pct_high}
    PCT_CRITICAL=${var.pct_critical}
  EOT
}
