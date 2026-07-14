#!/usr/bin/env bash
# Launch the Beam enrichment pipeline on the runner that matches $CLOUD.
set -euo pipefail
: "${CLOUD:?set CLOUD=gcp|azure}"
: "${ENVIRONMENT:?set ENVIRONMENT=dev|qa|prod}"

if [[ "$CLOUD" == "gcp" ]]; then
  python -m gaming_lakehouse.streaming.beam_enrichment \
    --cloud gcp \
    --runner DataflowRunner \
    --project "${GCP_PROJECT_ID}" \
    --region "${GCP_REGION:-us-central1}" \
    --temp_location "gs://gaming-commerce-lakehouse-${ENVIRONMENT}-temp/beam" \
    --staging_location "gs://gaming-commerce-lakehouse-${ENVIRONMENT}-temp/staging" \
    --input_subscription "projects/${GCP_PROJECT_ID}/subscriptions/gc-purchase-events-sub" \
    --output_table "${GCP_PROJECT_ID}:gaming_commerce_${ENVIRONMENT}.rt_revenue" \
    --enable_streaming_engine \
    --autoscaling_algorithm THROUGHPUT_BASED \
    --max_num_workers "$([[ $ENVIRONMENT == prod ]] && echo 20 || echo 4)" \
    --worker_machine_type n2-standard-4 \
    --experiments=use_runner_v2 \
    --job_name "gc-enrichment-${ENVIRONMENT}"
else
  python -m gaming_lakehouse.streaming.beam_enrichment \
    --cloud azure \
    --runner FlinkRunner \
    --flink_master "${FLINK_MASTER:-flink-jobmanager.airflow.svc:8081}" \
    --environment_type=DOCKER \
    --bootstrap_servers "${EVENTHUBS_NAMESPACE}.servicebus.windows.net:9093" \
    --topic "gc-purchase-events" \
    --output_table "abfss://gc${ENVIRONMENT}gold@gc${ENVIRONMENT}dls.dfs.core.windows.net/rt_revenue" \
    --parallelism "$([[ $ENVIRONMENT == prod ]] && echo 16 || echo 2)" \
    --checkpointing_interval 30000
fi
