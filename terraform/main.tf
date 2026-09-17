# ===========================================================================
# Tilley Station demo — Confluent Cloud footprint
# Adapted from ifnesi/flink-fraud-detection/terraform.
# ===========================================================================

locals {
  avro_dir = "${path.module}/../emulator/avro"

  # All raw + derived topics. Each gets a topic + a value Avro schema so
  # Flink auto-maps it to a table.
  topics = [
    "train_in_transit",
    "train_in_station",
    "passengers_flow",
    "station_occupancy",
    "demo_control",
    "station_metrics",
    "station_anomalies",
    "signal_state",
    "gateline_state",
    "station_ai_suggestions",
    "operator_questions",
    "operator_answers",
  ]

  # Raw display topics we window / control over — set a zero-lag watermark so
  # windows fire promptly and the control loops react within a few seconds.
  raw_display_topics = [
    "passengers_flow",
    "train_in_station",
    "train_in_transit",
    "station_occupancy",
  ]

  # Bedrock invoke endpoint (region + model id). Reused by the connection and by
  # the model-version hash below so a region/model change rebuilds the model.
  #
  # Newer Claude models (Sonnet 4.5, 3.5+) are ONLY invocable through a
  # cross-region INFERENCE PROFILE, not the bare model id — invoking the bare id
  # returns 404. The profile id is the region-geo prefix + '.' + model id, e.g.
  # us-east-1 -> "us.anthropic.claude-sonnet-4-5-...". This mirrors the working
  # ifnesi/retail-ai-demo (model_prefix = split("-", region)[0]).
  bedrock_model_ref = "${split("-", var.bedrock_region)[0]}.${var.bedrock_model_id}"
  bedrock_endpoint  = "https://bedrock-runtime.${var.bedrock_region}.amazonaws.com/model/${local.bedrock_model_ref}/invoke"

  # Model versioning guard. Confluent Flink CREATE MODEL with an existing name
  # creates a NEW VERSION but leaves the OLD version as the default that
  # ML_PREDICT resolves — so re-applying after a prompt/endpoint change never
  # takes effect and a stale (e.g. wrong-region) version keeps returning 404.
  # Tie the model NAME to a hash of everything that defines the model (the prompt
  # template, the endpoint, and max_tokens): any change yields a fresh name, so
  # CREATE MODEL makes a brand-new model whose only version is the default, and
  # ML_PREDICT (same local) always references it. Unchanged definition => same
  # name => no churn.
  # See: docs.confluent.io/cloud/current/flink/reference/statements/create-model.html#model-versioning
  model_version_hash = substr(sha1(join("|", [
    filesha1("${path.module}/sql/05_ai_model.sql"),
    filesha1("${path.module}/sql/05b_qa_model.sql"),
    local.bedrock_endpoint,
    tostring(var.ai_max_tokens),
    tostring(var.ai_temperature),
  ])), 0, 8)
  model_name    = "${var.ai_model_name}_${local.model_version_hash}"
  qa_model_name = "${var.ai_qa_model_name}_${local.model_version_hash}"

  # Constants injected into the Flink SQL templates. Domain values are Terraform
  # variables (vars.tf) — Terraform's own copy. On apply, write_env.sh writes the
  # resolved values into the repo-root .env so the Python apps use the same
  # numbers (no shared data file). ML/AI params also live in vars.tf.
  sql_vars = {
    station_name       = var.station_name
    station_capacity   = var.station_capacity
    window_seconds     = var.agg_window_seconds
    pct_low            = var.pct_low
    pct_high           = var.pct_high
    pct_critical       = var.pct_critical
    min_training_size  = var.min_training_size
    bedrock_connection = var.bedrock_connection_name
    model_name         = local.model_name
    qa_model_name      = local.qa_model_name
    ai_max_tokens      = var.ai_max_tokens
    ai_temperature     = var.ai_temperature
  }

  flink_statement_properties = {
    "sql.current-catalog"  = confluent_environment.env.id
    "sql.current-database" = confluent_kafka_cluster.kafka.id
  }

  # DML statements additionally get:
  #  * a state TTL to bound idle keyed state, and
  #  * a fixed scan idle-timeout so idle Kafka partitions don't stall the
  #    event-time watermark (which would freeze all windowed / OVER operators).
  # DDL (watermark ALTERs, CREATE MODEL) keeps the base properties.
  flink_statement_properties_dml = merge(
    local.flink_statement_properties,
    var.state_ttl == "" ? {} : { "sql.state-ttl" = var.state_ttl },
    var.scan_idle_timeout == "" ? {} : { "sql.tables.scan.idle-timeout" = var.scan_idle_timeout },
  )
}

# ---------------------------------------------------------------------------
# Organization / Environment / Cluster
# ---------------------------------------------------------------------------
data "confluent_organization" "org" {}

resource "confluent_environment" "env" {
  display_name = "${var.cc_env_name}-${random_id.id.hex}"
  stream_governance {
    package = var.stream_governance
  }
  lifecycle {
    prevent_destroy = false
  }
}

resource "confluent_kafka_cluster" "kafka" {
  display_name = var.cc_cluster_name
  availability = var.cc_availability
  cloud        = var.cc_cloud_provider
  region       = var.cc_cloud_region
  standard {}
  environment {
    id = confluent_environment.env.id
  }
  lifecycle {
    prevent_destroy = false
  }
}

data "confluent_schema_registry_cluster" "sr" {
  environment {
    id = confluent_environment.env.id
  }
  depends_on = [confluent_kafka_cluster.kafka]
}

# ---------------------------------------------------------------------------
# Service accounts
# ---------------------------------------------------------------------------
resource "confluent_service_account" "app_manager" {
  display_name = "app-manager-${random_id.id.hex}"
  description  = "Tilley demo — manages topics, schemas, Flink statements"
}

resource "confluent_service_account" "sr" {
  display_name = "sr-${random_id.id.hex}"
  description  = "Tilley demo — Schema Registry"
}

resource "confluent_service_account" "clients" {
  display_name = "client-${random_id.id.hex}"
  description  = "Tilley demo — emulator + backend Kafka clients"
}

# ---------------------------------------------------------------------------
# Role bindings
# ---------------------------------------------------------------------------
resource "confluent_role_binding" "app_manager_env_admin" {
  principal   = "User:${confluent_service_account.app_manager.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.env.resource_name
}

resource "confluent_role_binding" "sr_env_admin" {
  principal   = "User:${confluent_service_account.sr.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.env.resource_name
}

resource "confluent_role_binding" "clients_cluster_admin" {
  principal   = "User:${confluent_service_account.clients.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.kafka.rbac_crn
}

# ---------------------------------------------------------------------------
# API keys
# ---------------------------------------------------------------------------
resource "confluent_api_key" "app_manager_kafka" {
  display_name = "app-manager-${var.cc_cluster_name}-${random_id.id.hex}"
  description  = "Tilley demo — app manager Kafka key"
  owner {
    id          = confluent_service_account.app_manager.id
    api_version = confluent_service_account.app_manager.api_version
    kind        = confluent_service_account.app_manager.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.kafka.id
    api_version = confluent_kafka_cluster.kafka.api_version
    kind        = confluent_kafka_cluster.kafka.kind
    environment {
      id = confluent_environment.env.id
    }
  }
  depends_on = [confluent_role_binding.app_manager_env_admin]
}

resource "confluent_api_key" "sr" {
  display_name = "sr-${var.cc_cluster_name}-${random_id.id.hex}"
  description  = "Tilley demo — Schema Registry key"
  owner {
    id          = confluent_service_account.sr.id
    api_version = confluent_service_account.sr.api_version
    kind        = confluent_service_account.sr.kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.sr.id
    api_version = data.confluent_schema_registry_cluster.sr.api_version
    kind        = data.confluent_schema_registry_cluster.sr.kind
    environment {
      id = confluent_environment.env.id
    }
  }
  depends_on = [
    confluent_role_binding.sr_env_admin,
    data.confluent_schema_registry_cluster.sr,
  ]
}

resource "confluent_api_key" "clients_kafka" {
  display_name = "clients-${var.cc_cluster_name}-${random_id.id.hex}"
  description  = "Tilley demo — emulator + backend Kafka key"
  owner {
    id          = confluent_service_account.clients.id
    api_version = confluent_service_account.clients.api_version
    kind        = confluent_service_account.clients.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.kafka.id
    api_version = confluent_kafka_cluster.kafka.api_version
    kind        = confluent_kafka_cluster.kafka.kind
    environment {
      id = confluent_environment.env.id
    }
  }
  depends_on = [confluent_role_binding.clients_cluster_admin]
}

resource "confluent_api_key" "flink" {
  display_name = "flink-${var.cc_cluster_name}-${random_id.id.hex}"
  description  = "Tilley demo — Flink management key"
  owner {
    id          = confluent_service_account.app_manager.id
    api_version = confluent_service_account.app_manager.api_version
    kind        = confluent_service_account.app_manager.kind
  }
  managed_resource {
    id          = data.confluent_flink_region.main.id
    api_version = data.confluent_flink_region.main.api_version
    kind        = data.confluent_flink_region.main.kind
    environment {
      id = confluent_environment.env.id
    }
  }
  depends_on = [confluent_role_binding.app_manager_env_admin]
}

# ---------------------------------------------------------------------------
# Write connection details into the repo-root.env on apply.
# Values are passed to write_env.sh as EV_* env vars (not via `terraform output`,
# which is unavailable mid-apply). Secrets flow through the sensitive attributes,
# so Terraform redacts them in the plan/apply log. Toggle with var.write_env_file.
# ---------------------------------------------------------------------------
resource "terraform_data" "write_env" {
  count = var.write_env_file ? 1 : 0

  triggers_replace = [
    confluent_kafka_cluster.kafka.bootstrap_endpoint,
    confluent_api_key.clients_kafka.id,
    confluent_api_key.sr.id,
    data.confluent_schema_registry_cluster.sr.rest_endpoint,
  ]

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    command     = "${path.module}/write_env.sh"
    environment = {
      EV_BOOTSTRAP_SERVERS          = replace(confluent_kafka_cluster.kafka.bootstrap_endpoint, "SASL_SSL://", "")
      EV_KAFKA_API_KEY              = confluent_api_key.clients_kafka.id
      EV_KAFKA_API_SECRET           = confluent_api_key.clients_kafka.secret
      EV_SCHEMA_REGISTRY_URL        = data.confluent_schema_registry_cluster.sr.rest_endpoint
      EV_SCHEMA_REGISTRY_API_KEY    = confluent_api_key.sr.id
      EV_SCHEMA_REGISTRY_API_SECRET = confluent_api_key.sr.secret
      # Domain constants -> .env so the Python apps use the same numbers as Flink.
      EV_STATION_NAME       = var.station_name
      EV_STATION_CAPACITY   = tostring(var.station_capacity)
      EV_AGG_WINDOW_SECONDS = tostring(var.agg_window_seconds)
      EV_PCT_LOW            = tostring(var.pct_low)
      EV_PCT_HIGH           = tostring(var.pct_high)
      EV_PCT_CRITICAL       = tostring(var.pct_critical)
      # Managed MCP endpoint for Claude Code (.mcp.json reads CC_MCP_URL).
      # CC_MCP_AUTH is a Global API key the operator creates by hand (see README);
      # Terraform can't mint a Global key, so it is not written here.
      EV_CC_MCP_URL = local.mcp_url
    }
  }
}

# ---------------------------------------------------------------------------
# Topics + Avro schemas (for_each over all 10) —
# ---------------------------------------------------------------------------
resource "confluent_kafka_topic" "topic" {
  for_each = toset(local.topics)

  kafka_cluster {
    id = confluent_kafka_cluster.kafka.id
  }
  topic_name       = each.key
  rest_endpoint    = confluent_kafka_cluster.kafka.rest_endpoint
  partitions_count = var.partitions_count
  config = {
    "cleanup.policy"      = "delete"
    "retention.ms"        = tostring(var.topic_retention_ms)
    "min.insync.replicas" = "2"
  }
  credentials {
    key    = confluent_api_key.app_manager_kafka.id
    secret = confluent_api_key.app_manager_kafka.secret
  }
  lifecycle {
    prevent_destroy = false
  }
}

resource "confluent_schema" "value" {
  for_each = toset(local.topics)

  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.sr.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.sr.rest_endpoint
  subject_name  = "${each.key}-value"
  format        = "AVRO"
  schema        = file("${local.avro_dir}/${each.key}.avsc")
  credentials {
    key    = confluent_api_key.sr.id
    secret = confluent_api_key.sr.secret
  }
  lifecycle {
    prevent_destroy = false
  }
  depends_on = [confluent_kafka_topic.topic]
}

# ---------------------------------------------------------------------------
# Real-Time Context Engine (RTCE) + managed MCP for Claude Code
# ---------------------------------------------------------------------------
# RTCE exposes each topic to Confluent's managed MCP "context-engine" endpoint,
# so an AI agent (Claude Code) can ask questions about the live data over MCP.
# Enabled on EVERY topic in this demo (each has a registered schema above).
# The managed MCP endpoint is regional + cluster-scoped:
#   https://mcp.<region>.<cloud>.confluent.cloud/mcp/v1/context-engine/
#     organizations/<org>/environments/<env>/kafka-clusters/<lkc>
locals {
  mcp_url = format(
    "https://mcp.%s.%s.confluent.cloud/mcp/v1/context-engine/organizations/%s/environments/%s/kafka-clusters/%s",
    var.cc_cloud_region,
    lower(var.cc_cloud_provider),
    data.confluent_organization.org.id,
    confluent_environment.env.id,
    confluent_kafka_cluster.kafka.id,
  )
}

# Read-only principal the managed MCP server authenticates as. The REGIONAL
# managed-MCP endpoint only accepts a GLOBAL API key (or a Flink key) — NOT a
# Cloud API key, which is the only kind the Terraform provider can mint. So the
# operator creates a Global API key for THIS service account in the Console
# (Cloud API keys → Add key → Global), and exports it base64-encoded as
# CC_MCP_AUTH. The key inherits this SA's read-only RBAC below.
resource "confluent_service_account" "mcp_reader" {
  display_name = "mcp-reader-${random_id.id.hex}"
  description  = "Read-only principal for the Confluent Cloud managed MCP / RTCE endpoint"
  lifecycle {
    prevent_destroy = false
  }
}

# Read-only on every topic + subject, plus environment discovery.
resource "confluent_role_binding" "mcp_read_topics" {
  principal   = "User:${confluent_service_account.mcp_reader.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${confluent_kafka_cluster.kafka.rbac_crn}/kafka=${confluent_kafka_cluster.kafka.id}/topic=*"
}

resource "confluent_role_binding" "mcp_read_subjects" {
  principal   = "User:${confluent_service_account.mcp_reader.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.sr.resource_name}/subject=*"
}

resource "confluent_role_binding" "mcp_data_discovery" {
  principal   = "User:${confluent_service_account.mcp_reader.id}"
  role_name   = "DataDiscovery"
  crn_pattern = confluent_environment.env.resource_name
}

# NOTE: no confluent_api_key for mcp-reader here on purpose. The regional managed
# MCP server rejects Cloud API keys (HTTP 404), and the provider cannot create a
# Global API key — so it is created by hand in the Console (see README) and
# exported as CC_MCP_AUTH.

# One RTCE registration per topic — makes each available to the MCP context
# engine. Requires the registered value schema (above) and an RTCE-supported
# cluster/region. No credentials block: it uses the provider's Cloud API key.
resource "confluent_rtce_topic" "topic" {
  for_each = toset(local.topics)

  cloud       = var.cc_cloud_provider
  region      = var.cc_cloud_region
  topic_name  = each.key
  description = "Tilley Station demo — ${each.key}"

  environment {
    id = confluent_environment.env.id
  }
  kafka_cluster {
    id = confluent_kafka_cluster.kafka.id
  }

  depends_on = [confluent_schema.value]
  lifecycle {
    prevent_destroy = false
  }
}

# ---------------------------------------------------------------------------
# Flink compute pool + region
# ---------------------------------------------------------------------------
resource "confluent_flink_compute_pool" "pool" {
  display_name = "tilley-pool-${random_id.id.hex}"
  cloud        = var.cc_cloud_provider
  region       = var.cc_cloud_region
  max_cfu      = var.flink_cfu
  environment {
    id = confluent_environment.env.id
  }
}

data "confluent_flink_region" "main" {
  cloud  = var.cc_cloud_provider
  region = var.cc_cloud_region
}

# Reusable statement scaffolding is repeated per resource below (Terraform has
# no statement "module" here) — each sets org/env/pool/principal/creds/props.

# ---------------------------------------------------------------------------
# 0) Zero-lag watermarks on the raw display tables (low-latency windows/control)
# ---------------------------------------------------------------------------
resource "confluent_flink_statement" "watermark" {
  for_each = toset(local.raw_display_topics)

  organization {
    id = data.confluent_organization.org.id
  }
  environment {
    id = confluent_environment.env.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.pool.id
  }
  principal {
    id = confluent_service_account.app_manager.id
  }
  statement     = "ALTER TABLE `${each.value}` MODIFY WATERMARK FOR $rowtime AS $rowtime;"
  properties    = local.flink_statement_properties
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }
  depends_on = [confluent_schema.value, confluent_flink_compute_pool.pool]
}

# ---------------------------------------------------------------------------
# station_metrics is written transactionally by the metrics statement. Without
# this, the DOWNSTREAM Flink statements (anomalies, AI advisor) would wait up to
# ~1 min per Flink checkpoint commit before seeing each new window (read-committed
# is the default). read-uncommitted surfaces rows immediately — at-least-once, so
# an occasional duplicate row is possible, which is fine for a live dashboard.
# See: docs.confluent.io/cloud/current/flink/concepts/delivery-guarantees.html
# ---------------------------------------------------------------------------
resource "confluent_flink_statement" "metrics_read_uncommitted" {
  organization {
    id = data.confluent_organization.org.id
  }
  environment {
    id = confluent_environment.env.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.pool.id
  }
  principal {
    id = confluent_service_account.app_manager.id
  }
  statement     = "ALTER TABLE `station_metrics` SET ('kafka.consumer.isolation-level' = 'read-uncommitted');"
  properties    = local.flink_statement_properties
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }
  depends_on = [confluent_schema.value, confluent_flink_compute_pool.pool]
}

# station_anomalies is written transactionally by the anomalies statement and now
# read by the AI advisor (06). Same reasoning as above: read-uncommitted so an
# anomaly reaches the advisor in seconds, not after a ~1 min checkpoint commit.
resource "confluent_flink_statement" "anomalies_read_uncommitted" {
  organization {
    id = data.confluent_organization.org.id
  }
  environment {
    id = confluent_environment.env.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.pool.id
  }
  principal {
    id = confluent_service_account.app_manager.id
  }
  statement     = "ALTER TABLE `station_anomalies` SET ('kafka.consumer.isolation-level' = 'read-uncommitted');"
  properties    = local.flink_statement_properties
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }
  depends_on = [confluent_schema.value, confluent_flink_compute_pool.pool]
}

# ---------------------------------------------------------------------------
# 1) Windowed crowd metrics -> station_metrics
# ---------------------------------------------------------------------------
resource "confluent_flink_statement" "metrics" {
  organization {
    id = data.confluent_organization.org.id
  }
  environment {
    id = confluent_environment.env.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.pool.id
  }
  principal {
    id = confluent_service_account.app_manager.id
  }
  statement     = templatefile("${path.module}/sql/01_metrics.sql", local.sql_vars)
  properties    = local.flink_statement_properties_dml
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }
  depends_on = [confluent_flink_statement.watermark]
}

# ---------------------------------------------------------------------------
# 2) Anomaly detection -> station_anomalies
# ---------------------------------------------------------------------------
resource "confluent_flink_statement" "anomalies" {
  organization {
    id = data.confluent_organization.org.id
  }
  environment {
    id = confluent_environment.env.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.pool.id
  }
  principal {
    id = confluent_service_account.app_manager.id
  }
  statement     = templatefile("${path.module}/sql/02_anomalies.sql", local.sql_vars)
  properties    = local.flink_statement_properties_dml
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }
  depends_on = [confluent_flink_statement.metrics, confluent_flink_statement.metrics_read_uncommitted]
}

# NOTE: signals + gateline are now DETERMINISTIC and owned by the Python emulator
# (it publishes signal_state / gateline_state directly). The former Flink control
# loops (03_signal_control, 04_gateline_control) have been removed — Flink is
# analytics only: windowed metrics, ML anomaly detection, and the advisory AI.

# AI resources used to be gated behind a count (enable_ai_statements). They are
# now always created; these moved blocks migrate any existing [0]-indexed state
# in place, avoiding a destroy/recreate. (No-ops if that state doesn't exist.)
moved {
  from = confluent_flink_connection.bedrock[0]
  to   = confluent_flink_connection.bedrock
}
moved {
  from = confluent_flink_statement.ai_model[0]
  to   = confluent_flink_statement.ai_model
}
moved {
  from = confluent_flink_statement.ai_suggestions[0]
  to   = confluent_flink_statement.ai_suggestions
}

# ---------------------------------------------------------------------------
# Bedrock inference connection (native, confluent provider >= 2.32).
# Always created — one apply provisions it, no CLI, no `confluent login`. AWS
# keys are sensitive vars (from .env as TF_VAR_aws_*). NOTE: connection secrets
# are written to Terraform state, so keep terraform.tfstate secure (git-ignored).
# ---------------------------------------------------------------------------
resource "confluent_flink_connection" "bedrock" {
  organization {
    id = data.confluent_organization.org.id
  }
  environment {
    id = confluent_environment.env.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.pool.id
  }
  principal {
    id = confluent_service_account.app_manager.id
  }
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }

  display_name = var.bedrock_connection_name
  type         = "BEDROCK"
  endpoint     = local.bedrock_endpoint

  aws_access_key    = var.aws_access_key
  aws_secret_key    = var.aws_secret_key
  aws_session_token = var.aws_session_token != "" ? var.aws_session_token : null
}

# ---------------------------------------------------------------------------
# 5) Bedrock model registration (AI).
#    Depends on the connection above, which is created earlier in the same apply.
# ---------------------------------------------------------------------------
resource "confluent_flink_statement" "ai_model" {
  organization {
    id = data.confluent_organization.org.id
  }
  environment {
    id = confluent_environment.env.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.pool.id
  }
  principal {
    id = confluent_service_account.app_manager.id
  }
  statement     = templatefile("${path.module}/sql/05_ai_model.sql", local.sql_vars)
  properties    = local.flink_statement_properties
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }
  depends_on = [confluent_flink_connection.bedrock]
}

# ---------------------------------------------------------------------------
# 6) AI advisor -> station_ai_suggestions.
# ---------------------------------------------------------------------------
resource "confluent_flink_statement" "ai_suggestions" {
  organization {
    id = data.confluent_organization.org.id
  }
  environment {
    id = confluent_environment.env.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.pool.id
  }
  principal {
    id = confluent_service_account.app_manager.id
  }
  statement     = templatefile("${path.module}/sql/06_ai_suggestions.sql", local.sql_vars)
  properties    = local.flink_statement_properties_dml
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }
  depends_on = [
    confluent_flink_statement.ai_model,
    confluent_flink_statement.anomalies,
    confluent_flink_statement.metrics_read_uncommitted,
    confluent_flink_statement.anomalies_read_uncommitted,
  ]
}

# ---------------------------------------------------------------------------
# 5b) Second Bedrock model for the operator Ask-AI feature (conversational).
# ---------------------------------------------------------------------------
resource "confluent_flink_statement" "ai_qa_model" {
  organization {
    id = data.confluent_organization.org.id
  }
  environment {
    id = confluent_environment.env.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.pool.id
  }
  principal {
    id = confluent_service_account.app_manager.id
  }
  statement     = templatefile("${path.module}/sql/05b_qa_model.sql", local.sql_vars)
  properties    = local.flink_statement_properties
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }
  depends_on = [confluent_flink_connection.bedrock]
}

# ---------------------------------------------------------------------------
# 7) Operator Ask-AI: operator_questions (with backend-snapshotted live context)
#    -> Bedrock -> operator_answers. Append-only, no join.
# ---------------------------------------------------------------------------
resource "confluent_flink_statement" "operator_qa" {
  organization {
    id = data.confluent_organization.org.id
  }
  environment {
    id = confluent_environment.env.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.pool.id
  }
  principal {
    id = confluent_service_account.app_manager.id
  }
  statement     = templatefile("${path.module}/sql/07_operator_qa.sql", local.sql_vars)
  properties    = local.flink_statement_properties_dml
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }
  depends_on = [
    confluent_flink_statement.ai_qa_model,
    confluent_schema.value,
    confluent_flink_compute_pool.pool,
  ]
}
