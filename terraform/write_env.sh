#!/usr/bin/env bash
# Write the Kafka + Schema Registry connection details and domain constants into
# the repo-root .env, updating each key STRICTLY IN PLACE. Runs two ways:
#
#   * automatically, from the `terraform_data.write_env` local-exec provisioner
#     on `terraform apply` (values are passed in as EV_* environment variables);
#   * manually: `cd terraform && ./write_env.sh` (falls back to `terraform output`).
#
# It replaces the value on the key's existing line (keeping its position and the
# section comment above it); a key that is not present yet is appended once. A
# value containing whitespace is quoted so `set -a; source .env` stays valid.
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
  # Quote values with whitespace (e.g. STATION_NAME) so `source .env` is valid.
  case "$v" in
    *[[:space:]]*) v="\"$v\"" ;;
  esac
  tmp="$(mktemp)"
  if grep -qE "^[[:space:]]*(export[[:space:]]+)?${k}=" "$ENV_FILE"; then
    # Replace the value on the FIRST existing line in place, keeping its position;
    # drop any later duplicate definitions of the same key. awk gets the value via
    # ENVIRON so no shell metacharacters in secrets are ever interpreted.
    KEY="$k" VAL="$v" awk '
      $0 ~ ("^[[:space:]]*(export[[:space:]]+)?" ENVIRON["KEY"] "=") {
        if (!seen) { print ENVIRON["KEY"] "=" ENVIRON["VAL"]; seen=1 }
        next
      }
      { print }
    ' "$ENV_FILE" >"$tmp"
  else
    cat "$ENV_FILE" >"$tmp"
    printf '%s=%s\n' "$k" "$v" >>"$tmp"
  fi
  mv "$tmp" "$ENV_FILE"
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
echo "Done. Each key updated in place; the file layout is preserved."
