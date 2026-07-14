#!/usr/bin/env bash
# One command to get a working local environment. Assumes Python 3.11 and Docker.
set -euo pipefail

export CLOUD="${CLOUD:-gcp}"
export ENVIRONMENT="${ENVIRONMENT:-dev}"

python -m venv .venv && source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements/dev.txt -e .
pre-commit install

echo "-> starting the local CDC stack (Postgres + Kafka + Debezium + Schema Registry)"
docker compose -f src/gaming_lakehouse/cdc/debezium/docker-compose.yml up -d --wait

echo "-> registering the Debezium connector"
envsubst < src/gaming_lakehouse/cdc/debezium/postgres-connector.json \
  | curl -sS -X POST -H "Content-Type: application/json" --data @- http://localhost:8083/connectors | jq .

echo "-> ready. Try:  make ingest  |  make silver  |  make gold"
