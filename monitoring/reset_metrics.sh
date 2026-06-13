#!/bin/bash
#
# Reset all monitoring data so the pipeline can be re-triggered for the same
# video and only fresh metrics appear in Grafana.
#
# What it does:
#   1. Reads every group currently stored in the Pushgateway.
#   2. Deletes each group via the standard Pushgateway HTTP API
#      (DELETE /metrics/job/<job>). This clears all pushed metrics, including
#      any legacy per-clip groups left over from older runs.
#
# Why this approach:
#   - It uses the Pushgateway's built-in delete API, so it needs NO admin flag
#     and NO container restart.
#   - It changes NO Azure resource, so it never introduces Terraform drift.
#   - Once the Pushgateway is empty, Prometheus marks the old series stale on its
#     next scrape, so sum()/lastNotNull panels immediately reflect only fresh
#     data. (Prometheus runs on an ephemeral TSDB with no persistent volume, so
#     its history is short-lived anyway.)
#
# Usage: ./reset_metrics.sh
#
set -euo pipefail

RESOURCE_GROUP="vana-traffic-rg"
PUSHGATEWAY_APP="vana-pushgateway"

echo "==> Resetting monitoring data..."

# Resolve the Pushgateway public endpoint.
# Note: the Windows az.exe (when invoked from WSL) appends a trailing carriage
# return, so we strip CR/whitespace to keep the URL valid.
PG_FQDN=$(az containerapp show \
  --name "$PUSHGATEWAY_APP" --resource-group "$RESOURCE_GROUP" \
  --query "properties.configuration.ingress.fqdn" -o tsv | tr -d '\r\n ')

if [[ -z "$PG_FQDN" ]]; then
  echo "ERROR: could not resolve Pushgateway ingress FQDN." >&2
  exit 1
fi
echo "--> Pushgateway: https://$PG_FQDN"

# Fetch the current groups and build a DELETE path for each unique job.
curl -s --max-time 30 "https://$PG_FQDN/api/v1/metrics" > /tmp/pg_reset.json

mapfile -t DELETE_PATHS < <(python3 - <<'PY'
import json
d = json.load(open('/tmp/pg_reset.json'))
seen = set()
for grp in d.get('data', []):
    for _, mobj in grp.items():
        if not (isinstance(mobj, dict) and 'metrics' in mobj):
            continue
        for m in mobj['metrics']:
            job = m.get('labels', {}).get('job')
            if job and job not in seen:
                seen.add(job)
                print(f"/metrics/job/{job}")
PY
)

if [[ ${#DELETE_PATHS[@]} -eq 0 ]]; then
  echo "    Pushgateway already empty. Nothing to delete."
else
  for path in "${DELETE_PATHS[@]}"; do
    code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 15 \
      -X DELETE "https://${PG_FQDN}${path}")
    echo "    DELETE ${path} -> ${code}"
  done
fi

# Verify the gateway is empty.
LEFT=$(curl -s --max-time 20 "https://$PG_FQDN/api/v1/metrics" \
  | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('data',[])))")
echo "--> Groups remaining in Pushgateway: $LEFT"

rm -f /tmp/pg_reset.json

echo "==> Done. Prometheus will drop the stale series on its next scrape,"
echo "    so Grafana shows only data from the next pipeline run."
