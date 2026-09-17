locals {
  description = "Tilley Station crowd-management demo — provisioned with Terraform"
}

# Makes names unique per account/run.
resource "random_id" "id" {
  byte_length = 4
}

# ---------------------------------------------------------------------------
# Confluent Cloud / Kafka cluster
# ---------------------------------------------------------------------------
variable "cc_cloud_provider" {
  type    = string
  default = "AWS"
}

variable "cc_cloud_region" {
  type        = string
  default     = "us-east-1" # same region as Bedrock (broadest model availability)
  description = "Confluent Cloud + Flink region"
}

variable "cc_env_name" {
  type    = string
  default = "demo-tilley-station"
}

variable "cc_cluster_name" {
  type    = string
  default = "cc-tilley-main"
}

variable "cc_availability" {
  type    = string
  default = "SINGLE_ZONE"
}

variable "stream_governance" {
  type    = string
  default = "ESSENTIALS"
}

variable "flink_cfu" {
  type        = number
  default     = 10
  description = "Max CFUs for the Flink compute pool (metrics + anomalies + 2 control loops + AI)"
}

variable "state_ttl" {
  type        = string
  default     = "1h"
  description = <<-EOT
    sql.state-ttl for the DML statements — bounds idle keyed state and clears the
    HIGH_STATE_OPERATOR_WITHOUT_TTL warning. Safe for the demo: every key updates
    every ~15s so active state is never idle-evicted. Empty string disables it.
  EOT
}

variable "scan_idle_timeout" {
  type        = string
  default     = "5 s"
  description = <<-EOT
    sql.tables.scan.idle-timeout for the DML statements. Topics have multiple
    partitions but modest traffic, so idle partitions would otherwise stall the
    event-time watermark (Confluent's default idleness grows up to 5 min),
    freezing every windowed / OVER operator (metrics, control loops, anomalies,
    AI). A short fixed timeout keeps the watermark advancing from active
    partitions so the pipeline stays live and control latency stays low.
  EOT
}

# ---------------------------------------------------------------------------
# Topics — underscore names, 6 partitions, short retention
# ---------------------------------------------------------------------------
variable "partitions_count" {
  type    = number
  default = 6
}

variable "topic_retention_ms" {
  type        = number
  default     = 3600000 # 1 hour — crowd data ages fast; bounds topic growth
  description = "Retention for all topics (minutes–an hour)"
}

# ---------------------------------------------------------------------------
# AWS Bedrock (GenAI inference) — native confluent_flink_connection.
# Defaults to the same region as the cluster. The model must be enabled in the
# AWS Bedrock console (Model access) in var.bedrock_region.
# ---------------------------------------------------------------------------
variable "bedrock_region" {
  type        = string
  default     = "us-east-1"
  description = <<-EOT
    AWS region for the Bedrock endpoint — a region where bedrock_model_id is
    enabled for on-demand invocation. us-east-1 has the broadest availability.
    Can differ from the Kafka region, but keeping them the same is simplest.
  EOT
}

variable "bedrock_model_id" {
  type        = string
  default     = "anthropic.claude-sonnet-4-5-20250929-v1:0"
  description = <<-EOT
    Bedrock model id. It is combined with the region-geo prefix into a
    cross-region INFERENCE PROFILE for the endpoint (us-east-1 ->
    us.anthropic.claude-sonnet-4-5-...). Newer Claude models are only invocable
    via an inference profile, so enable this model's profile under Bedrock ->
    Model access in bedrock_region. (Matches ifnesi/retail-ai-demo.)
  EOT
}

# AWS credentials for the Bedrock connection. Set via the root .env as
# TF_VAR_aws_access_key / TF_VAR_aws_secret_key (single secrets file) — Terraform
# reads TF_VAR_* from the environment. These are written to the connection and
# therefore stored (encrypted-at-rest by the backend, but plaintext in a local
# state file) — keep terraform.tfstate secure; it is git-ignored.
variable "aws_access_key" {
  type        = string
  default     = ""
  sensitive   = true
  description = "AWS access key id for the Bedrock connection (TF_VAR_aws_access_key)"
}

variable "aws_secret_key" {
  type        = string
  default     = ""
  sensitive   = true
  description = "AWS secret access key for the Bedrock connection (TF_VAR_aws_secret_key)"
}

variable "aws_session_token" {
  type        = string
  default     = ""
  sensitive   = true
  description = "Optional AWS session token (only for temporary STS credentials)"
}

variable "bedrock_connection_name" {
  type        = string
  default     = "bedrock-conn"
  description = "Name of the native Flink Bedrock connection (referenced by CREATE MODEL). Must be lowercase alphanumeric + hyphens (no underscores)."
}

variable "ai_model_name" {
  type    = string
  default = "ops_advisor"
}

variable "ai_max_tokens" {
  type        = number
  default     = 512
  description = "Bedrock max_tokens for the advisor (required by Anthropic models). The four-line reply is short, so this is plenty."
}

# ---------------------------------------------------------------------------
# Domain constants (station_name, station_capacity, evacuation_flow,
# agg_window_seconds, pct_low/high/critical, evac_warn/crit) are NOT variables:
# they live once in the repo-root demo.config.json, read by both this Terraform
# (local.demo) and the Python emulator/backend. Edit them there.
# ---------------------------------------------------------------------------

variable "min_training_size" {
  type        = number
  default     = 30
  description = "ML_DETECT_ANOMALIES minTrainingSize — model is trained by showtime"
}

variable "write_env_file" {
  type        = bool
  default     = true
  description = <<-EOT
    On apply, write BOOTSTRAP_SERVERS / KAFKA_* / SCHEMA_REGISTRY_* into the
    repo-root .env in place (via write_env.sh, a local-exec provisioner). Set to
    false in CI or when you manage .env yourself.
  EOT
}
