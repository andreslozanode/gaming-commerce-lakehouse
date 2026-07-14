.DEFAULT_GOAL := help
SHELL := /bin/bash
CLOUD ?= gcp
ENVIRONMENT ?= dev
export CLOUD ENVIRONMENT

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-24s\033[0m %s\n", $$1, $$2}'

setup: ## Create the venv, install deps and boot the local CDC stack
	./scripts/bootstrap_local.sh

lint: ## ruff + mypy
	ruff check src tests orchestration && ruff format --check src tests && mypy src/gaming_lakehouse --ignore-missing-imports

test: ## Unit tests with coverage gate
	pytest tests/unit -v --cov=gaming_lakehouse --cov-fail-under=80

test-integration: ## End-to-end CDC tests against the docker stack
	pytest tests/integration -m "not gpu" -v

cdc-up: ## Start Postgres + Kafka + Debezium locally
	docker compose -f src/gaming_lakehouse/cdc/debezium/docker-compose.yml up -d --wait

cdc-down: ## Tear the local CDC stack down
	docker compose -f src/gaming_lakehouse/cdc/debezium/docker-compose.yml down -v

ingest: ## Kaggle -> landing -> Bronze
	python -m gaming_lakehouse.ingestion.run_ingest --datasets all

stream: ## Start the Bronze event stream
	python -m gaming_lakehouse.streaming.bronze_stream

simulate: ## Replay purchase events into the broker (200 eps for 60s)
	python -m gaming_lakehouse.streaming.producer_simulator --eps 200 --seconds 60

silver: ## Bronze -> Silver
	python -m gaming_lakehouse.transform.bronze_to_silver --target all

gold: ## Silver -> Gold marts
	python -m gaming_lakehouse.transform.silver_to_gold

features: ## Materialize the ML feature tables
	python -m gaming_lakehouse.ml.features

train-torch: ## Train the two-tower recsys on GPU
	python -m gaming_lakehouse.ml.train_torch_recsys

train-tf: ## Train the TensorFlow revenue forecaster
	python -m gaming_lakehouse.ml.train_tf_forecast

beam: ## Launch the Beam enrichment pipeline on the runner matching $$CLOUD
	./orchestration/beam/run_beam.sh

tf-plan: ## Terraform plan for $$CLOUD / $$ENVIRONMENT
	terraform -chdir=infra/terraform init -reconfigure -backend-config=envs/$(ENVIRONMENT)/backend.$(CLOUD).hcl && \
	terraform -chdir=infra/terraform plan -var="cloud=$(CLOUD)" -var-file=envs/$(ENVIRONMENT)/terraform.tfvars

tf-apply: ## Terraform apply for $$CLOUD / $$ENVIRONMENT
	terraform -chdir=infra/terraform apply -var="cloud=$(CLOUD)" -var-file=envs/$(ENVIRONMENT)/terraform.tfvars

bundle-deploy: ## Deploy the Databricks Asset Bundle
	cd databricks && databricks bundle deploy -t $(ENVIRONMENT) --var="cloud=$(CLOUD)"

docker-gpu: ## Build the CUDA training image
	docker build -f docker/Dockerfile.gpu -t gaming-lakehouse-gpu:local .

clean: ## Remove build artifacts and caches
	rm -rf dist build .pytest_cache .ruff_cache .mypy_cache **/__pycache__ mlruns

.PHONY: help setup lint test test-integration cdc-up cdc-down ingest stream simulate silver gold features train-torch train-tf beam tf-plan tf-apply bundle-deploy docker-gpu clean
