#!/usr/bin/env bash
# Write the Kafka + Schema Registry connection details into the repo-root .env,
# updating each key IN PLACE (no duplicates). Runs two ways:
#
#   * automatically, from the `terraform_data.write_env` local-exec provisioner
#     on `terraform apply` (values are passed in as EV_* environment variables);
#   * manually: `cd terraform && ./write_env.sh` (falls back to `terraform output`).
#
# Idempotent: removes any existing definition of each key (with or without an
# `export ` prefix, empty or not) and writes a single clean `export KEY=VALUE`.
# Safe on macOS bash 3.2.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../.env"
touch "$ENV_FILE"

# Value from EV_<KEY> (set by the provisioner) if present, else `terraform output`.
val() {
  local out
  out="$(printenv "EV_$1" || true)"
  if [ -n "$out" ]; then
    printf '%s' "$out"
  else
    terraform -chdir="$SCRIPT_DIR" output -raw "$2"
  fi
}

update_key() {
  local k="$1" v="$2" tmp
  tmp="$(mktemp)"
  grep -vE "^[[:space:]]*(export[[:space:]]+)?${k}=" "$ENV_FILE" >"$tmp" || true
  mv "$tmp" "$ENV_FILE"
  # Plain KEY=VALUE (no `export`) so the file works as a Docker Compose env_file;
  # `set -a; source .env` still exports it for Terraform / the CLI.
  printf '%s=%s\n' "$k" "$v" >>"$ENV_FILE"
  echo "  set ${k}"
}

echo "Updating ${ENV_FILE} ..."
update_key BOOTSTRAP_SERVERS          "$(val BOOTSTRAP_SERVERS bootstrap_servers | sed 's#^SASL_SSL://##')"
update_key KAFKA_API_KEY              "$(val KAFKA_API_KEY kafka_api_key)"
update_key KAFKA_API_SECRET           "$(val KAFKA_API_SECRET kafka_api_secret)"
update_key SCHEMA_REGISTRY_URL        "$(val SCHEMA_REGISTRY_URL schema_registry_url)"
update_key SCHEMA_REGISTRY_API_KEY    "$(val SCHEMA_REGISTRY_API_KEY schema_registry_api_key)"
update_key SCHEMA_REGISTRY_API_SECRET "$(val SCHEMA_REGISTRY_API_SECRET schema_registry_api_secret)"
# Domain constants (Terraform vars.tf is the source) -> .env for the Python apps.
update_key STATION_NAME       "$(val STATION_NAME station_name)"
update_key STATION_CAPACITY   "$(val STATION_CAPACITY station_capacity)"
update_key AGG_WINDOW_SECONDS "$(val AGG_WINDOW_SECONDS agg_window_seconds)"
update_key PCT_LOW            "$(val PCT_LOW pct_low)"
update_key PCT_HIGH           "$(val PCT_HIGH pct_high)"
update_key PCT_CRITICAL       "$(val PCT_CRITICAL pct_critical)"
echo "Done. Each key now appears exactly once in .env."
