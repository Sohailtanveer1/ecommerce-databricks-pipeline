#!/usr/bin/env bash
# Register the Debezium Postgres CDC connector with Kafka Connect.
set -euo pipefail
CONNECT=${CONNECT:-http://localhost:8083}
DIR="$(cd "$(dirname "$0")" && pwd)"

echo "waiting for Kafka Connect at $CONNECT ..."
until curl -sf "$CONNECT/connectors" >/dev/null; do sleep 3; done

curl -sf -X POST -H "Content-Type: application/json" \
  --data @"$DIR/pg-connector.json" "$CONNECT/connectors" \
  && echo "  connector registered" \
  || echo "  (already exists? PUT to $CONNECT/connectors/postgres-cdc/config to update)"

echo "status:"
curl -s "$CONNECT/connectors/postgres-cdc/status" | tr ',' '\n' | grep -E 'state|name' || true
