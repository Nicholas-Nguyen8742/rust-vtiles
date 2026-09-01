# Vector tile pipeline — local development entry points (Recommendation 1).
# Full walkthrough: docs/LOCAL_DEV.md · error taxonomy: docs/ERRORS.md.
#
# Common overrides:
#   make run-local PORT=9090
#   make job-status JOB_ID=job_...
#   make replay-job TENANT=tenant-acme JOB_ID=job_... ASSUME_WGS84=1

DATA_DIR     ?= data
HOST         ?= 127.0.0.1
PORT         ?= 8080
TENANT       ?= tenant-acme
JOB_ID       ?=
ASSUME_WGS84 ?= 0
# Sequence 1 US-05 replay audit fields.
REQUESTED_BY ?= make-replay
REASON       ?=
CREATE_NEW_VERSION ?= 0

RELEASE := target/release

.DEFAULT_GOAL := help

.PHONY: help build fixtures setup setup-docker run-local run-local-docker \
        seed job-status smoke replay-job metrics test clean distclean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

build: ## Build the release binaries (vtile-api, vtile)
	cargo build --release -p vtile-api -p vtile-pipeline

fixtures: ## Generate the zipped Shapefile fixtures into tests/fixtures/
	cargo run -q -p vtile-ingest --example gen_fixtures

setup: build fixtures ## Build, generate fixtures, create the local "bucket" dirs
	mkdir -p $(DATA_DIR)/staging $(DATA_DIR)/tiles $(DATA_DIR)/manifests \
		$(DATA_DIR)/quarantine $(DATA_DIR)/jobs

setup-docker: ## Start the docker compose stack (api + worker)
	docker compose up -d --build

run-local: ## Run vtile-api on $(HOST):$(PORT) against ./$(DATA_DIR)
	@if [ -x "$(RELEASE)/vtile-api" ]; then \
		"$(RELEASE)/vtile-api" --data-dir $(DATA_DIR) --host $(HOST) --port $(PORT); \
	else \
		cargo run -q -p vtile-api -- --data-dir $(DATA_DIR) --host $(HOST) --port $(PORT); \
	fi

run-local-docker: ## Run the API from the compose stack (foreground)
	docker compose up --build api

seed: ## Push every fixture through the upload API (API must be running)
	sh scripts/seed.sh

job-status: ## Job status in GET /jobs/{id} shape: make job-status JOB_ID=job_...
	@test -n "$(JOB_ID)" || { echo "usage: make job-status JOB_ID=job_..."; exit 2; }
	@if [ -x "$(RELEASE)/vtile" ]; then \
		"$(RELEASE)/vtile" job-status --data-dir $(DATA_DIR) --job-id $(JOB_ID); \
	else \
		cargo run -q -p vtile-pipeline --bin vtile -- job-status --data-dir $(DATA_DIR) --job-id $(JOB_ID); \
	fi

smoke: ## End-to-end smoke test against a running local API
	sh scripts/smoke.sh

replay-job: ## Replay a job: make replay-job JOB_ID=job_... [ASSUME_WGS84=1] [CREATE_NEW_VERSION=1]
	@test -n "$(JOB_ID)" || { echo "usage: make replay-job TENANT=$(TENANT) JOB_ID=job_... [ASSUME_WGS84=1] [CREATE_NEW_VERSION=1] [REASON=\"...\"]"; exit 2; }
	@if [ -x "$(RELEASE)/vtile" ]; then \
		"$(RELEASE)/vtile" replay --data-dir $(DATA_DIR) --tenant $(TENANT) --job-id $(JOB_ID) --requested-by $(REQUESTED_BY) --reason "$(REASON)" $(if $(filter 1,$(ASSUME_WGS84)),--assume-wgs84,) $(if $(filter 1,$(CREATE_NEW_VERSION)),--create-new-version,); \
	else \
		cargo run -q -p vtile-pipeline --bin vtile -- replay --data-dir $(DATA_DIR) --tenant $(TENANT) --job-id $(JOB_ID) --requested-by $(REQUESTED_BY) --reason "$(REASON)" $(if $(filter 1,$(ASSUME_WGS84)),--assume-wgs84,) $(if $(filter 1,$(CREATE_NEW_VERSION)),--create-new-version,); \
	fi

metrics: ## Idempotency telemetry snapshot from the running API
	curl -s "http://$(HOST):$(PORT)/internal/metrics"

test: ## Run the workspace test suite
	cargo test --workspace

clean: ## Remove local runtime data (uploads, tiles, jobs, quarantine)
	rm -rf $(DATA_DIR)

distclean: clean ## Remove runtime data and build artifacts
	rm -rf target
