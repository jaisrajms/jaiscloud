# ─── Defaults ─────────────────────────────────────────────────────────────────
.DEFAULT_GOAL := help

# Python interpreter used to build the client-conformance venv (see
# test-gcp-python-conformance). Override with `make ... PYTHON=python3.12`.
PYTHON ?= python3

# Plan families in priority order for `make gcp-status-next` (comma-separated);
# other families sort after these, alphabetically. The Java-compat effort owns
# waves W1–W3 today.
SERIES ?= java-compat

# ─── Version ──────────────────────────────────────────────────────────────────
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || \
             grep -oP 'const version = "\K[^"]+' cmd/jaiscloud-aws/main.go 2>/dev/null || \
             echo "dev")

CLOUDS := aws azure gcp

REGISTRY ?=

# ─── Container images ─────────────────────────────────────────────────────────
# Matches const DefaultImage in internal/executor/spark/config.go
SPARK_IMAGE             ?= apache/spark:3.5.0
# Matches python3.12 entry in internal/executor/lambda/config.go runtimeImages
LAMBDA_IMAGE            ?= public.ecr.aws/lambda/python:3.12
# Custom Iceberg-enabled Spark image (must be built locally before use)
SPARK_E2E_ICEBERG_IMAGE ?= spark-iceberg-test
# GCP variant: apache/spark:3.5.0 + iceberg-spark-runtime + gcs-connector (hadoop3);
# the data path is HadoopFileIO, so the iceberg-gcp-bundle is intentionally absent.
SPARK_E2E_ICEBERG_GCP_IMAGE ?= spark-iceberg-gcp-test

# GCP emulator image deployed to the k3d cluster (deploy/k8s/jaiscloud-gcp.yaml).
# `test-e2e-lakehouse-k3d` rebuilds + pushes it before every run so the pipeline
# is never validated against a stale emulator binary.
GCP_REGISTRY   ?= 10.0.100.21:5050
GCP_IMAGE      ?= $(GCP_REGISTRY)/jaiscloud-gcp:compat
# k3d's registry is plain HTTP; buildah defaults to HTTPS, so disable verify.
GCP_PUSH_FLAGS ?= --tls-verify=false
# Seed size for the k3d Lakehouse pipeline e2e. Rendered into the pipeline Job's
# RECORDS env and asserted by the test, so override on the command line:
#   make test-e2e-lakehouse-k3d LAKEHOUSE_RECORDS=1000000
LAKEHOUSE_RECORDS ?= 100

# Spring Cloud GCP sample applications (deploy/k8s/gcp-samples) used by the
# application-level emulator e2e. The manifest hardcodes
# <registry>/jaiscloud-sample-<svc>:<tag>; keep GCP_SAMPLES_TAG in sync with it
# when bumping the upstream release.
GCP_SAMPLES_REGISTRY ?= $(GCP_REGISTRY)
GCP_SAMPLES_TAG      ?= 8.2.1
GCP_SAMPLES_MODULES  := pubsub:spring-cloud-gcp-pubsub-sample \
                        firestore:spring-cloud-gcp-data-firestore-sample \
                        datastore:spring-cloud-gcp-data-datastore-basic-sample

# ─── K8s configuration ────────────────────────────────────────────────────────
K8S_NAMESPACE           ?= jaiscloud
JAISCLOUD_K8S_APISERVER ?= $(shell kubectl config view --context docker-desktop --minify -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null)
JAISCLOUD_K8S_CA_FILE           ?=
JAISCLOUD_K8S_CLIENT_CERT_FILE  ?=
JAISCLOUD_K8S_CLIENT_KEY_FILE   ?=

# ─── Server knobs ─────────────────────────────────────────────────────────────
JAISCLOUD_DSN  ?= postgres://jaiscloud:jaiscloud@localhost:5432/jaiscloud
JAISCLOUD_PORT ?= 4566
JAISCLOUD_HOST ?= http://localhost:$(JAISCLOUD_PORT)

# Per-mode ports for test-integration — allows ephemeral and postgres suites to run in parallel
JAISCLOUD_PORT_EPHEMERAL ?= 4566
JAISCLOUD_PORT_POSTGRES  ?= 4566
_INTEGRATION_MODE        := $(shell printf '%s' '$(MODE)' | tr '[:upper:]' '[:lower:]')
_INTEGRATION_PORT        := $(if $(filter postgres,$(_INTEGRATION_MODE)),$(JAISCLOUD_PORT_POSTGRES),$(JAISCLOUD_PORT_EPHEMERAL))
_INTEGRATION_HOST        := http://localhost:$(_INTEGRATION_PORT)

# ─── Local Postgres container ─────────────────────────────────────────────────
PG_CONTAINER ?= jaiscloud-postgres
PG_VOLUME    ?= jaiscloud-postgres-data
PG_PORT      ?= 5432
PG_USER      ?= jaiscloud
PG_PASSWORD  ?= jaiscloud
PG_DB        ?= jaiscloud

# Narrow any test target to a single test: make test-e2e-emrcontainers-k8s TEST_RUN=TestSparkJob_K8s_CancelJobRun
TEST_RUN ?= .

IMAGE             := jaiscloud-aws
# Public image used by up-docker and up-k8s. Override with a locally built image
# (make docker first) by passing JAISCLOUD_IMAGE=jaiscloud-aws:latest to make.
JAISCLOUD_IMAGE   ?= jaisraj/jaiscloud-aws:latest

.PHONY: lint lint-pagination help build build-all docker docker-all docker-gcp-samples test test-aws test-gcp clean \
        server-memory server-ephemeral server-postgres server-docker server-k8s server-postgres-all \
        server-gcp server-gcp-ephemeral server-gcp-postgres \
        stop-server up-docker down-docker up-k8s down-k8s \
        postgres-up postgres-reset postgres-down \
        test-integration test-integration-gcp \
        test-e2e-emr-docker test-e2e-emrcontainers-k8s test-e2e-eventbridge \
        test-e2e-dpc-docker test-e2e-dpc-k8s \
        test-e2e-lambda-docker test-e2e-lambda-k8s \
        test-e2e-cloudformation test-e2e-kms test-e2e-ssm test-e2e-dynamodb test-e2e-persistence \
        test-e2e-s3-streaming test-e2e-kinesis test-e2e-ecr test-e2e-sfn \
        test-e2e-gcp-persistence test-e2e-iceberg test-e2e-iceberg-gcp \
        test-e2e-lakehouse-k3d \
        test-e2e-gcp-samples-k3d \
        test-e2e-docker-all test-e2e-k8s-all test-e2e test-all test-all-gcp \
        _build-for-e2e _restart-server-memory _wait-docker _wait-postgres \
        _start-k8s _stop-k8s \
        _check-docker-prereq _check-k8s-prereq _check-iceberg-prereq _check-iceberg-gcp-prereq \
        _check-lakehouse-k3d-prereq _check-gcp-samples-prereq _refresh-gcp-image \
        test-gcp-wire-conformance record-gcp-wire-conformance test-gcp-grpc-conformance \
        test-gcp-gcloud-conformance test-gcp-python-conformance \
        test-gcp-differential record-gcp-differential \
        test-gcp-terraform test-gcp-opentofu \
        gen-gcp-fidelity-matrix check-gcp-fidelity-matrix ga-check \
        gcp-status gcp-status-audit gcp-status-coverage gcp-status-lint-plans gcp-status-next gcp-status-check gcp-plan-new

# ─── Help ─────────────────────────────────────────────────────────────────────
# NOTE: 'make --help' and 'make -h' show GNU Make's own flags (cannot be overridden).
#       Use 'make help' or bare 'make' to see JaisCloud targets.

help: ## Show this help  (tip: bare 'make' also works)
	@echo ""
	@echo "\033[1mJaisCloud — available targets\033[0m"
	@echo "  Override variables inline, e.g.:  make test-e2e-emr-docker SPARK_IMAGE=my/spark:4.0"
	@echo ""
	@printf "  \033[1m%-30s  %s\033[0m\n" "Target" "Description"
	@printf "  %-30s  %s\n"  "------------------------------" "---------------------------------------------------"
	@awk 'BEGIN { FS = "##" } \
	  /^##@/ { printf "\n  \033[4m%s\033[0m\n", substr($$0, 5) } \
	  /^[a-zA-Z_-][a-zA-Z0-9_-]*:.*##/ { \
	    t = $$1; sub(/:.*/, "", t); gsub(/[[:space:]]/, "", t); \
	    printf "  \033[36m%-30s\033[0m  %s\n", t, $$2 \
	  }' $(MAKEFILE_LIST)
	@echo ""
	@printf "  \033[1m%-30s  %s\033[0m\n" "Variable" "Default / purpose"
	@printf "  %-30s  %s\n"  "------------------------------" "---------------------------------------------------"
	@printf "  %-30s  %s\n"  "SPARK_IMAGE"   "apache/spark:3.5.0  — Spark container image"
	@printf "  %-30s  %s\n"  "LAMBDA_IMAGE"  "public.ecr.aws/lambda/python:3.12  — Lambda image"
	@printf "  %-30s  %s\n"  "JAISCLOUD_DSN" "postgres://jaiscloud:jaiscloud@localhost:5432/jaiscloud"
	@printf "  %-30s  %s\n"  "K8S_NAMESPACE" "jaiscloud  — K8s namespace for Spark and Lambda jobs"
	@printf "  %-30s  %s\n"  "TEST_RUN"      ".  — go test -run filter (any e2e target)"
	@printf "  %-30s  %s\n"  "PG_CONTAINER"  "jaiscloud-postgres  — docker container name"
	@printf "  %-30s  %s\n"  "PG_VOLUME"     "jaiscloud-postgres-data  — named volume for persistence"
	@printf "  %-30s  %s\n"  "PG_PORT"       "5432  — host port mapped to Postgres 5432"
	@echo ""

##@ Build

build: build-aws  ## Compile jaiscloud-aws (default)

build-all: $(addprefix build-,$(CLOUDS))  ## Compile all cloud binaries (aws, azure, gcp)

build-%:  ## Compile jaiscloud-<cloud>
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o jaiscloud-$* ./cmd/jaiscloud-$*/

docker: docker-aws  ## Build jaiscloud-aws Docker image (default)

docker-all: $(addprefix docker-,$(CLOUDS))  ## Build all cloud Docker images

docker-%:  ## Build jaiscloud-<cloud> Docker image
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg CLOUD=$* \
		--tag jaiscloud-$*:$(VERSION) \
		--tag jaiscloud-$*:latest \
		--file Dockerfile .
ifdef REGISTRY
	docker tag jaiscloud-$*:$(VERSION) $(REGISTRY)/jaiscloud-$*:$(VERSION)
	docker tag jaiscloud-$*:latest     $(REGISTRY)/jaiscloud-$*:latest
endif

docker-gcp-samples: ## Build+push the Spring Cloud GCP sample images (pin: GCP_SAMPLES_TAG)
	@for pair in $(GCP_SAMPLES_MODULES); do \
	  name=$${pair%%:*}; mod=$${pair#*:}; \
	  echo "== build jaiscloud-sample-$$name:$(GCP_SAMPLES_TAG) ($$mod) =="; \
	  docker build --build-arg MODULE=$$mod \
	    -t jaiscloud-sample-$$name:$(GCP_SAMPLES_TAG) \
	    -f deploy/docker/gcp-samples/Dockerfile deploy/docker/gcp-samples || exit 1; \
	  docker tag jaiscloud-sample-$$name:$(GCP_SAMPLES_TAG) \
	    $(GCP_SAMPLES_REGISTRY)/jaiscloud-sample-$$name:$(GCP_SAMPLES_TAG); \
	  docker push $(GCP_PUSH_FLAGS) \
	    $(GCP_SAMPLES_REGISTRY)/jaiscloud-sample-$$name:$(GCP_SAMPLES_TAG) || exit 1; \
	done

clean: ## Remove compiled binaries
	rm -f jaiscloud-aws jaiscloud-azure jaiscloud-gcp

lint: ## Run ARN lint guard + go vet
	@bash scripts/check_no_hardcoded_arn.sh
	@go vet ./...

lint-pagination: ## Heuristic check that List*/Describe* provider methods use pagination
	@go run tools/lint/paginationcheck/main.go ./internal/aws/provider/... ./internal/gcp/provider/...

##@ Unit tests

test: ## Run all unit tests with the race detector  (no server needed)
	go clean -testcache
	go test -race ./internal/...

test-aws: ## Run AWS + shared unit tests (excludes internal/gcp — mirrors CI test-aws)
	go test -race $$(go list ./internal/... | grep -v '/internal/gcp/')

test-gcp: ## Run GCP unit tests incl. the shared Spark/K8s engine (mirrors CI test-gcp)
	go test -race ./internal/gcp/... ./internal/sparkhelpers/... ./internal/k8shelpers/... ./internal/platform/... ./internal/executor/...

##@ Server — foreground (Ctrl-C to stop)

server-memory: build ## Default mode: memory stores + periodic state.json saves, mock executors
	JAISCLOUD_PORT=$(JAISCLOUD_PORT) \
	  ./jaiscloud-aws start

server-ephemeral: build ## Ephemeral mode: no persistence, clean slate on every start (CI/tests)
	JAISCLOUD_PORT=$(JAISCLOUD_PORT) \
	  ./jaiscloud-aws start --ephemeral

server-postgres: build ## Postgres backend, mock executors — requires JAISCLOUD_DSN
	JAISCLOUD_PORT=$(JAISCLOUD_PORT) \
	  ./jaiscloud-aws start --dsn "$(JAISCLOUD_DSN)"

server-gcp: build-gcp ## GCP default mode: memory stores + periodic state.json saves
	JAISCLOUD_PORT=$(JAISCLOUD_PORT) \
	  ./jaiscloud-gcp start

server-gcp-ephemeral: build-gcp ## GCP ephemeral mode: no persistence (CI/tests)
	JAISCLOUD_PORT=$(JAISCLOUD_PORT) \
	  ./jaiscloud-gcp start --ephemeral

server-gcp-postgres: build-gcp ## GCP Postgres backend — requires JAISCLOUD_DSN
	JAISCLOUD_PORT=$(JAISCLOUD_PORT) \
	  ./jaiscloud-gcp start --dsn "$(JAISCLOUD_DSN)"

server-docker: _check-docker-prereq docker ## Persistent mode + Spark and Lambda via Docker (docker-compose, Ctrl-C to stop)
	JAISCLOUD_EXECUTOR_MODE=$(or $(JAISCLOUD_EXECUTOR_MODE),docker) \
	  JAISCLOUD_SPARK_IMAGE=$(SPARK_IMAGE) \
	  JAISCLOUD_LAMBDA_IMAGE=$(LAMBDA_IMAGE) \
	  docker-compose up

server-k8s: _check-k8s-prereq up-k8s ## Persistent mode + Spark and Lambda via K8s  (requires docker-desktop K8s)
	kubectl port-forward -n jaiscloud svc/jaiscloud $(JAISCLOUD_PORT):4566

server-postgres-all: server-k8s ## Alias for server-k8s (postgres backend, all executors via K8s)

stop-server: ## Stop background jaiscloud-aws process and clean up Lambda/Spark resources
	@pkill -f "jaiscloud-aws start" 2>/dev/null && echo "jaiscloud-aws stopped" || echo "jaiscloud-aws was not running"
	@kubectl delete pods -l app=jaiscloud-lambda -n jaiscloud --ignore-not-found 2>/dev/null || true
	@kubectl delete svc -l app=jaiscloud-lambda -n jaiscloud --ignore-not-found 2>/dev/null || true
	@kubectl delete jobs -l app=jaiscloud-spark -n jaiscloud --ignore-not-found 2>/dev/null || true

##@ Containerized server lifecycle

up-docker: _check-docker-prereq ## Start JaisCloud + Postgres via docker-compose (detached)
	JAISCLOUD_IMAGE=$(JAISCLOUD_IMAGE) \
	  JAISCLOUD_EXECUTOR_MODE=$(or $(JAISCLOUD_EXECUTOR_MODE),docker) \
	  JAISCLOUD_SPARK_IMAGE=$(SPARK_IMAGE) \
	  JAISCLOUD_LAMBDA_IMAGE=$(LAMBDA_IMAGE) \
	  docker-compose up -d
	$(MAKE) _wait-docker

down-docker: ## Stop and remove docker-compose services
	docker-compose down --remove-orphans

up-k8s: _check-k8s-prereq ## Deploy JaisCloud + Postgres to K8s  (docker-desktop)
	kubectl apply -f deploy/k8s/namespace.yaml
	kubectl apply -f deploy/k8s/rbac.yaml
	kubectl apply -f deploy/k8s/postgres.yaml
	kubectl apply -f deploy/k8s/jaiscloud.yaml
	@echo "Waiting for jaiscloud deployment..."
	@kubectl rollout status deployment/jaiscloud -n jaiscloud --timeout=120s
	$(MAKE) _wait-docker

down-k8s: ## Remove JaisCloud K8s deployment and clean up Lambda/Spark resources
	@kubectl delete -f deploy/k8s/jaiscloud.yaml --ignore-not-found 2>/dev/null || true
	@kubectl delete pods -l app=jaiscloud-lambda -n jaiscloud --ignore-not-found 2>/dev/null || true
	@kubectl delete svc -l app=jaiscloud-lambda -n jaiscloud --ignore-not-found 2>/dev/null || true
	@kubectl delete jobs -l app=jaiscloud-spark -n jaiscloud --ignore-not-found 2>/dev/null || true

##@ Local Postgres  (standalone container, independent of docker-compose)

postgres-up: _check-docker-prereq ## Start a local Postgres 16 container on port 5432 — creates the jaiscloud db; data persists across restarts
	@if docker ps -q -f name=^/$(PG_CONTAINER)$$ | grep -q . 2>/dev/null; then \
	  echo "$(PG_CONTAINER) is already running on port $(PG_PORT)"; \
	else \
	  docker rm -f $(PG_CONTAINER) > /dev/null 2>&1 || true; \
	  docker volume create $(PG_VOLUME) > /dev/null; \
	  docker run -d \
	    --name $(PG_CONTAINER) \
	    -e POSTGRES_USER=$(PG_USER) \
	    -e POSTGRES_PASSWORD=$(PG_PASSWORD) \
	    -e POSTGRES_DB=$(PG_DB) \
	    -p $(PG_PORT):5432 \
	    -v $(PG_VOLUME):/var/lib/postgresql/data \
	    postgres:16-alpine > /dev/null; \
	  $(MAKE) _wait-postgres; \
	fi

postgres-reset: _check-docker-prereq ## Wipe all Postgres data and start fresh — WARNING: all data is destroyed
	@echo "Resetting $(PG_CONTAINER): stopping container and removing volume $(PG_VOLUME)"
	@docker rm -f $(PG_CONTAINER) > /dev/null 2>&1 || true
	@docker volume rm $(PG_VOLUME) > /dev/null 2>&1 || true
	@$(MAKE) postgres-up

postgres-down: ## Stop and remove the local Postgres container  (volume is kept — data survives)
	@docker rm -f $(PG_CONTAINER) > /dev/null 2>&1 && \
	  echo "$(PG_CONTAINER) stopped (data preserved in volume $(PG_VOLUME))" || \
	  echo "$(PG_CONTAINER) was not running"

##@ Integration tests

# MODE controls the store backend. The value is case-insensitive (postgres/Postgres/POSTGRES all work).
# Note: the variable NAME must be uppercase MODE — Make variable names are case-sensitive.

test-integration: ## Run tests/integration/ — MODE=ephemeral|postgres required; TEST_RUN=TestSQS to target one service
	@if [ -z "$(MODE)" ]; then \
	  printf "\n\033[1mUsage:\033[0m\n"; \
	  printf "  make test-integration \033[36mMODE=ephemeral\033[0m               run against ephemeral in-memory stores (no postgres)\n"; \
	  printf "  make test-integration \033[36mMODE=postgres\033[0m                run against postgres (resets data)\n"; \
	  printf "  make test-integration \033[36mMODE=postgres TEST_RUN=TestS3\033[0m  target a single service\n\n"; \
	  printf "\033[33mMODE is required.\033[0m\n\n"; \
	  exit 1; \
	fi
	@_mode=$$(printf '%s' "$(MODE)" | tr '[:upper:]' '[:lower:]'); \
	if [ "$$_mode" != "ephemeral" ] && [ "$$_mode" != "postgres" ]; then \
	  printf "\033[31mERROR: MODE must be 'ephemeral' or 'postgres', got '$(MODE)'\033[0m\n"; \
	  exit 1; \
	fi; \
	if [ "$$_mode" = "postgres" ]; then \
	  docker info > /dev/null 2>&1 || { printf "\033[31mERROR: Docker is not running\033[0m\n"; exit 1; }; \
	  printf "\n\033[1m┌──────────────────────────────────────────────────────┐\033[0m\n"; \
	  printf   "\033[1m│   JaisCloud Integration Suite — Postgres Backend     │\033[0m\n"; \
	  printf   "\033[1m└──────────────────────────────────────────────────────┘\033[0m\n\n"; \
	  printf "\033[1m[1/4]\033[0m Stopping any running jaiscloud-aws on port $(_INTEGRATION_PORT)...\n"; \
	  lsof -ti tcp:$(_INTEGRATION_PORT) | xargs kill 2>/dev/null || true; \
	  printf "\033[1m[2/4]\033[0m Building jaiscloud-aws... "; \
	  go build -o jaiscloud-aws ./cmd/jaiscloud-aws/ \
	    && printf "\033[32m✓ OK\033[0m\n" \
	    || { printf "\033[31m✗ build failed\033[0m\n"; exit 1; }; \
	  printf "\033[1m[3/4]\033[0m Setting up Postgres...\n"; \
	  if docker ps -q -f name=^/$(PG_CONTAINER)$$ | grep -q . 2>/dev/null; then \
	    printf "  Postgres running — resetting data\n"; \
	    $(MAKE) postgres-reset; \
	  else \
	    printf "  Postgres not running — starting\n"; \
	    $(MAKE) postgres-up; \
	  fi; \
	  printf "\033[1m[4/4]\033[0m Starting jaiscloud-aws with postgres backend...\n"; \
	  printf "  \033[2m$ JAISCLOUD_PORT=$(_INTEGRATION_PORT) ./jaiscloud-aws start --dsn \"$(JAISCLOUD_DSN)\"\033[0m\n"; \
	  JAISCLOUD_PORT=$(_INTEGRATION_PORT) \
	    ./jaiscloud-aws start --dsn "$(JAISCLOUD_DSN)" \
	    > /tmp/jaiscloud-postgres.log 2>&1 & \
	  n=0; until curl -sf $(_INTEGRATION_HOST)/_jaiscloud/health > /dev/null 2>&1; do \
	    n=$$((n+1)); \
	    if [ $$n -ge 30 ]; then \
	      printf "\033[31m  ✗ jaiscloud-aws not healthy — check /tmp/jaiscloud-postgres.log\033[0m\n"; \
	      lsof -ti tcp:$(_INTEGRATION_PORT) | xargs kill 2>/dev/null || true; \
	      exit 1; \
	    fi; \
	    sleep 1; \
	  done; \
	  printf "\033[32m  ✓ Ready → $(_INTEGRATION_HOST)  (log: /tmp/jaiscloud-postgres.log)\033[0m\n"; \
	else \
	  printf "\n\033[1m┌──────────────────────────────────────────────────────┐\033[0m\n"; \
	  printf   "\033[1m│  JaisCloud Integration Suite — Ephemeral (in-memory) │\033[0m\n"; \
	  printf   "\033[1m└──────────────────────────────────────────────────────┘\033[0m\n\n"; \
	  printf "\033[1m[1/3]\033[0m Stopping any running jaiscloud-aws on port $(_INTEGRATION_PORT)...\n"; \
	  lsof -ti tcp:$(_INTEGRATION_PORT) | xargs kill 2>/dev/null || true; \
	  printf "\033[1m[2/3]\033[0m Building jaiscloud-aws... "; \
	  go build -o jaiscloud-aws ./cmd/jaiscloud-aws/ \
	    && printf "\033[32m✓ OK\033[0m\n" \
	    || { printf "\033[31m✗ build failed\033[0m\n"; exit 1; }; \
	  printf "\033[1m[3/3]\033[0m Starting jaiscloud-aws in ephemeral mode...\n"; \
	  printf "  \033[2m$ JAISCLOUD_PORT=$(_INTEGRATION_PORT) ./jaiscloud-aws start --ephemeral\033[0m\n"; \
	  JAISCLOUD_PORT=$(_INTEGRATION_PORT) \
	    ./jaiscloud-aws start --ephemeral \
	    > /tmp/jaiscloud-ephemeral.log 2>&1 & \
	  n=0; until curl -sf $(_INTEGRATION_HOST)/_jaiscloud/health > /dev/null 2>&1; do \
	    n=$$((n+1)); \
	    if [ $$n -ge 30 ]; then \
	      printf "\033[31m  ✗ jaiscloud-aws not healthy — check /tmp/jaiscloud-ephemeral.log\033[0m\n"; \
	      lsof -ti tcp:$(_INTEGRATION_PORT) | xargs kill 2>/dev/null || true; \
	      exit 1; \
	    fi; \
	    sleep 1; \
	  done; \
	  printf "\033[32m  ✓ Ready → $(_INTEGRATION_HOST)  (log: /tmp/jaiscloud-ephemeral.log)\033[0m\n"; \
	fi
	@printf "\n\033[1mRunning integration tests...\033[0m\n\n"
	@go clean -testcache
	@JAISCLOUD_HOST=$(_INTEGRATION_HOST) \
	  go test -v -race -timeout 15m -p 1 -run "$(TEST_RUN)" ./tests/integration/ ./tests/integration/multiaccount/ \
	  > /tmp/integration-results.txt 2>&1; \
	echo $$? > /tmp/integration-exit.txt; \
	awk '\
	  /^=== /       { next } \
	  /^--- PASS:/  { printf "  \033[32m✓ %s\033[0m\n", $$0; n_pass++; next } \
	  /^--- FAIL:/  { printf "  \033[31m✗ %s\033[0m\n", $$0; fails[++n_fail]=$$0; in_fail=1; next } \
	  /^--- /       { in_fail=0; next } \
	  /^    /       { if (in_fail) printf "  \033[33m%s\033[0m\n", $$0; next } \
	  /^(FAIL|ok )/ { next } \
	  { print } \
	  END { \
	    print ""; \
	    print "\033[1m══════════════════════════════════════════════════════\033[0m"; \
	    printf "\033[1mResults:\033[0m  \033[32m%d passed\033[0m", n_pass+0; \
	    if (n_fail+0 > 0) { \
	      printf "  \033[31;1m%d failed\033[0m\n\n", n_fail; \
	      printf "\033[1;31mFailed tests:\033[0m\n"; \
	      for (i=1; i<=n_fail; i++) printf "  \033[31m✗  %s\033[0m\n", fails[i]; \
	    } else { \
	      printf "  \033[32m0 failed  ✓  All tests passed!\033[0m\n"; \
	    } \
	    print "\033[1m══════════════════════════════════════════════════════\033[0m"; \
	  } \
	' /tmp/integration-results.txt; \
	lsof -ti tcp:$(_INTEGRATION_PORT) 2>/dev/null | xargs kill 2>/dev/null || true; \
	exit $$(cat /tmp/integration-exit.txt)

##@ Persistent-mode e2e tests  (start server, run suite, stop server)

test-e2e-emr-docker: _check-docker-prereq ## EMR Docker Spark tests — tests/persistent_mode/aws/emr/ (tag: spark_e2e)
	$(MAKE) up-docker JAISCLOUD_EXECUTOR_MODE=docker JAISCLOUD_SPARK_IMAGE=$(SPARK_IMAGE)
	go clean -testcache
	SPARK_E2E_DOCKER_IMAGE=$(SPARK_IMAGE) JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags spark_e2e -timeout 10m -run "$(TEST_RUN)" ./tests/persistent_mode/aws/emr/
	$(MAKE) down-docker

test-e2e-emrcontainers-k8s: _check-k8s-prereq ## EMR Containers K8s tests — tests/persistent_mode/aws/emrcontainers/ (tag: spark_e2e)
	# Individual tests via TEST_RUN:
	#   TestSparkJob_K8s_StartJobRun_And_Complete
	#   TestSparkJob_K8s_CancelJobRun
	#   TestSparkJob_K8s_MultipleJobRuns_Concurrent
	#   TestSparkJob_K8s_FailedJobRun_ReportsFailure
	$(MAKE) _start-k8s
	go clean -testcache
	SPARK_E2E_SPARK_IMAGE=$(SPARK_IMAGE) SPARK_E2E_K8S_NAMESPACE=$(K8S_NAMESPACE) JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags spark_e2e -timeout 15m -run "$(TEST_RUN)" ./tests/persistent_mode/aws/emrcontainers/
	$(MAKE) _stop-k8s

test-e2e-eventbridge: ## EventBridge notification tests — tests/persistent_mode/aws/eventbridge/ (no Docker/K8s)
	$(MAKE) up-docker JAISCLOUD_EXECUTOR_MODE=mock
	go clean -testcache
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags spark_e2e -timeout 10m -run "$(TEST_RUN)" ./tests/persistent_mode/aws/eventbridge/
	$(MAKE) down-docker

test-e2e-dpc-docker: _check-docker-prereq ## DPC Spark tests via Docker — tests/persistent_mode/aws/dpc/ (tag: spark_e2e)
	$(MAKE) up-docker JAISCLOUD_EXECUTOR_MODE=docker JAISCLOUD_SPARK_IMAGE=$(SPARK_IMAGE)
	go clean -testcache
	SPARK_E2E_DOCKER_IMAGE=$(SPARK_IMAGE) JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags spark_e2e -timeout 10m -run "$(TEST_RUN)" ./tests/persistent_mode/aws/dpc/
	$(MAKE) down-docker

test-e2e-dpc-k8s: _check-k8s-prereq ## DPC Spark tests via K8s — tests/persistent_mode/aws/dpc/ (tag: spark_e2e)
	$(MAKE) _start-k8s
	go clean -testcache
	SPARK_E2E_SPARK_IMAGE=$(SPARK_IMAGE) SPARK_E2E_K8S_NAMESPACE=$(K8S_NAMESPACE) JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags spark_e2e -timeout 15m -run "$(TEST_RUN)" ./tests/persistent_mode/aws/dpc/
	$(MAKE) _stop-k8s

test-e2e-lambda-docker: _check-docker-prereq ## Lambda Docker tests — tests/persistent_mode/aws/lambda/ (tag: lambda_e2e)
	$(MAKE) up-docker JAISCLOUD_EXECUTOR_MODE=docker JAISCLOUD_LAMBDA_IMAGE=$(LAMBDA_IMAGE)
	go clean -testcache
	LAMBDA_E2E_DOCKER_IMAGE=$(LAMBDA_IMAGE) JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags lambda_e2e -timeout 10m -run "$(TEST_RUN)" ./tests/persistent_mode/aws/lambda/
	$(MAKE) down-docker

test-e2e-lambda-k8s: _check-k8s-prereq ## Lambda K8s tests — tests/persistent_mode/aws/lambda/ (tag: lambda_e2e)
	$(MAKE) _start-k8s
	go clean -testcache
	LAMBDA_E2E_K8S_IMAGE=$(LAMBDA_IMAGE) SPARK_E2E_K8S_NAMESPACE=$(K8S_NAMESPACE) JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags lambda_e2e -timeout 15m -run "$(TEST_RUN)" ./tests/persistent_mode/aws/lambda/
	$(MAKE) _stop-k8s

test-e2e-cloudformation: ## CloudFormation e2e tests — tests/persistent_mode/aws/cloudformation/ (tag: cfn_fullmode)
	$(MAKE) up-docker JAISCLOUD_EXECUTOR_MODE=mock
	go clean -testcache
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags cfn_fullmode -timeout 10m -run "$(TEST_RUN)" ./tests/persistent_mode/aws/cloudformation/
	$(MAKE) down-docker

test-e2e-dynamodb: ## DynamoDB GSI/LSI e2e tests — tests/persistent_mode/aws/dynamodb/ (tag: dynamo_fullmode)
	$(MAKE) up-docker JAISCLOUD_EXECUTOR_MODE=mock
	go clean -testcache
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags dynamo_fullmode -timeout 10m -run "$(TEST_RUN)" ./tests/persistent_mode/aws/dynamodb/
	$(MAKE) down-docker

test-e2e-kms: ## KMS/SecretsManager/SSM e2e tests — tests/persistent_mode/aws/kms/ (tag: kms_fullmode)
	$(MAKE) up-docker JAISCLOUD_EXECUTOR_MODE=mock
	go clean -testcache
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags kms_fullmode -timeout 10m -run "$(TEST_RUN)" ./tests/persistent_mode/aws/kms/
	$(MAKE) down-docker

test-e2e-ssm: ## SSM label persistence e2e tests — tests/persistent_mode/aws/ssm/ (tag: ssm_fullmode)
	$(MAKE) up-docker JAISCLOUD_EXECUTOR_MODE=mock
	go clean -testcache
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags ssm_fullmode -timeout 10m -run "$(TEST_RUN)" ./tests/persistent_mode/aws/ssm/
	$(MAKE) down-docker

test-e2e-s3-streaming: ## S3 streaming upload/download e2e tests — tests/persistent_mode/aws/s3/ (tag: s3_fullmode)
	$(MAKE) up-docker JAISCLOUD_EXECUTOR_MODE=mock
	go clean -testcache
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags s3_fullmode -timeout 10m -run "$(TEST_RUN)" ./tests/persistent_mode/aws/s3/
	$(MAKE) down-docker

test-e2e-kinesis: ## Kinesis persistent mode e2e tests — tests/persistent_mode/aws/kinesis/ (tag: kinesis_e2e, requires kinesis-mock binary)
	$(MAKE) up-docker JAISCLOUD_EXECUTOR_MODE=mock
	go clean -testcache
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags kinesis_e2e -timeout 10m -run "$(TEST_RUN)" ./tests/persistent_mode/aws/kinesis/
	$(MAKE) down-docker

test-e2e-ecr: ## ECR persistent mode e2e tests — tests/persistent_mode/aws/ecr/ (tag: ecr_e2e, requires K8s cluster + crane)
	go clean -testcache
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -race -tags ecr_e2e -timeout 15m -run "$(TEST_RUN)" ./tests/persistent_mode/aws/ecr/

test-e2e-sfn: ## Step Functions persistent mode e2e tests — tests/persistent_mode/aws/stepfunctions/ (tag: sfn_e2e)
	go clean -testcache
	JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -race -tags sfn_e2e -timeout 5m -run "$(TEST_RUN)" ./tests/persistent_mode/aws/stepfunctions/

test-e2e-persistence: test-e2e-cloudformation test-e2e-kms ## CloudFormation + KMS persistence tests

test-e2e-gcp-persistence: postgres-up build-gcp ## GCP Postgres persistence tests (requires Docker for Postgres)
	JAISCLOUD_DSN=$(JAISCLOUD_DSN) JAISCLOUD_GCP_PERSIST_PORT=8099 \
	  go test -tags gcp_persistence -p 1 -count=1 -timeout 5m ./tests/persistent_mode/gcp/...
	cd tests/persistent_mode/gcp/parity-grpc && JAISCLOUD_DSN=$(JAISCLOUD_DSN) \
	  go test -tags gcp_persistence -p 1 -count=1 -timeout 5m ./...

##@ GCP integration tests

test-integration-gcp: build-gcp ## Run GCP integration + SDK suites against an ephemeral server (REST :8080 + gRPC :8081)
	@echo "Starting jaiscloud-gcp (ephemeral)..."
	@set -e; \
	  ./jaiscloud-gcp start --port 8080 --grpc-port 8081 --ephemeral > /tmp/jaiscloud-gcp.log 2>&1 & \
	  pid=$$!; \
	  cleanup() { echo "Stopping jaiscloud-gcp..."; kill "$$pid" 2>/dev/null || true; p=$$(lsof -ti tcp:8080 2>/dev/null || true); if [ -n "$$p" ]; then kill $$p 2>/dev/null || true; fi; }; \
	  trap cleanup EXIT INT TERM; \
	  n=0; until curl -sf http://localhost:8080/_jaiscloud/health >/dev/null 2>&1; do \
	    n=$$((n+1)); if [ $$n -ge 30 ]; then echo "ERROR: jaiscloud-gcp not healthy"; cat /tmp/jaiscloud-gcp.log; exit 1; fi; sleep 1; \
	  done; echo "  ready (REST :8080, gRPC :8081)"; \
	  echo "Running raw-HTTP integration tests..."; \
	  go test -race -count=1 -timeout 120s ./tests/integration/gcp/; \
	  echo "Running REST SDK suites..."; \
	  ( cd tests/integration/gcp/sdk && STORAGE_EMULATOR_HOST=http://localhost:8080 go test -count=1 -timeout 120s ./... ); \
	  ( cd tests/integration/gcp/sdk-rest && GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test -count=1 -timeout 120s ./... ); \
	  ( cd tests/integration/gcp/sdk-workflows && GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test -count=1 -timeout 120s ./... ); \
	  ( cd tests/integration/gcp/sdk-dataproc && GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test -count=1 -timeout 120s ./... ); \
	  ( cd tests/integration/gcp/sdk-bigquery && GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test -count=1 -timeout 120s ./... ); \
	  ( cd tests/integration/gcp/sdk-metastore && GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test -count=1 -timeout 120s ./... ); \
	  ( cd tests/integration/gcp/sdk-managed-kafka && GCP_EMULATOR_ENDPOINT=http://localhost:8080/ GCP_EMULATOR_PROJECT=test-project go test -count=1 -timeout 120s ./... ); \
	  ( cd tests/integration/gcp/sdk-clouddns && GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test -count=1 -timeout 120s ./... ); \
	  ( cd tests/integration/gcp/sdk-memorystore && GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test -count=1 -timeout 120s ./... ); \
	  ( cd tests/integration/gcp/sdk-compute && GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test -count=1 -timeout 120s ./... ); \
	  echo "Running gRPC SDK suites..."; \
	  ( cd tests/integration/gcp/sdk-firestore && FIRESTORE_EMULATOR_HOST=localhost:8081 go test -count=1 -timeout 120s ./... ); \
	  ( cd tests/integration/gcp/sdk-monitoring && MONITORING_EMULATOR_HOST=localhost:8081 go test -count=1 -timeout 120s ./... ); \
	  ( cd tests/integration/gcp/sdk-datastore && DATASTORE_EMULATOR_HOST=localhost:8081 GCP_EMULATOR_PROJECT=test-project go test -count=1 -timeout 120s ./... ); \
	  ( cd tests/integration/gcp/sdk-logging && LOGGING_EMULATOR_HOST=localhost:8081 GCP_EMULATOR_PROJECT=test-project go test -count=1 -timeout 120s ./... ); \
	  ( cd tests/integration/gcp/sdk-gcs-grpc && STORAGE_EMULATOR_HOST_GRPC=localhost:8081 GCP_EMULATOR_PROJECT=test-project go test -count=1 -timeout 120s ./... )

test-gcp-wire-conformance: ## Offline GCP wire-conformance harness (Discovery snapshots + recorder; tag: gcp_conformance)
	go test -count=1 -tags gcp_conformance ./tests/gcpconformance/

record-gcp-wire-conformance: ## Record a fresh transcript against an ephemeral emulator, then stop it
	@echo "Building jaiscloud-gcp..."
	@go build -o jaiscloud-gcp ./cmd/jaiscloud-gcp/
	@echo "Starting jaiscloud-gcp (ephemeral)..."
	@set -e; \
	  ./jaiscloud-gcp start --port 8080 --grpc-port 8081 --ephemeral > /tmp/jaiscloud-gcp-conformance.log 2>&1 & \
	  pid=$$!; \
	  cleanup() { echo "Stopping jaiscloud-gcp..."; kill "$$pid" 2>/dev/null || true; p=$$(lsof -ti tcp:8080 2>/dev/null || true); if [ -n "$$p" ]; then kill $$p 2>/dev/null || true; fi; }; \
	  trap cleanup EXIT INT TERM; \
	  n=0; until curl -sf http://localhost:8080/_jaiscloud/health >/dev/null 2>&1; do \
	    n=$$((n+1)); if [ $$n -ge 30 ]; then echo "ERROR: jaiscloud-gcp not healthy"; cat /tmp/jaiscloud-gcp-conformance.log; exit 1; fi; sleep 1; \
	  done; echo "  ready (REST :8080, gRPC :8081)"; \
	  GCP_CONFORMANCE_RECORD=1 GCP_CONFORMANCE_ENDPOINT=http://localhost:8080 go test -tags gcp_conformance -count=1 -v -run TestRecord ./tests/gcpconformance/

# Differential (record/replay) harness: commit goldens captured from REAL GCP,
# then replay them offline against the emulator and report divergences.
#
# Capture requires Application Default Credentials (`gcloud auth
# application-default login`) and a real project with the relevant APIs enabled.
# Override the project with:
#   GCP_DIFFERENTIAL_PROJECT=<project>  (default: parity-diff-jaiscloud)
#   GCP_DIFFERENTIAL_PROJECT_NUMBER=<number>
# All operations are global or multi-region (KMS location=global, BigQuery US,
# Cloud DNS global, Cloud Workflows us-central1), so no region override is
# needed. Dataproc is intentionally excluded. The recorder cleans up every
# created resource except a single fixed KMS keyring/key, which GCP cannot
# delete.
#
# The curated scenario list may grow ahead of a recording: any scenario without
# a committed golden is reported as "pending recording" and skipped by the
# offline replay, so `make test-gcp-differential` stays green until the next
# capture folds it into a golden. Run this target to record (or refresh) all of
# them at once.
record-gcp-differential: ## Capture differential goldens from REAL GCP (needs ADC; see comment for project env)
	@echo "Recording differential goldens from real GCP (project: $${GCP_DIFFERENTIAL_PROJECT:-parity-diff-jaiscloud})..."
	go test -tags gcp_differential -count=1 -v -run TestRecord ./tests/gcpdifferential/ -record

test-gcp-differential: ## Offline differential replay vs an ephemeral emulator (no credentials; tag: gcp_differential)
	@echo "Building jaiscloud-gcp..."
	@go build -o /tmp/jc-differential ./cmd/jaiscloud-gcp/
	@echo "Starting jaiscloud-gcp (ephemeral)..."
	@set -e; \
	  /tmp/jc-differential start --port 8080 --grpc-port 8081 --ephemeral > /tmp/jaiscloud-gcp-differential.log 2>&1 & \
	  pid=$$!; \
	  cleanup() { echo "Stopping jaiscloud-gcp (REST :8080)..."; kill "$$pid" 2>/dev/null || true; p=$$(lsof -ti tcp:8080 2>/dev/null || true); if [ -n "$$p" ]; then kill $$p 2>/dev/null || true; fi; }; \
	  trap cleanup EXIT INT TERM; \
	  n=0; until curl -sf http://localhost:8080/_jaiscloud/health >/dev/null 2>&1; do \
	    n=$$((n+1)); if [ $$n -ge 30 ]; then echo "ERROR: jaiscloud-gcp not healthy"; cat /tmp/jaiscloud-gcp-differential.log; exit 1; fi; sleep 1; \
	  done; echo "  ready (REST :8080)"; \
	  go test -tags gcp_differential -count=1 -v -run 'TestReplay|TestGoldensAreClean|TestGoldenManifest' ./tests/gcpdifferential/

# Opt-in Terraform / OpenTofu compatibility suites — drive the real
# hashicorp/google provider against the emulator (tests/integration/gcp/terraform/).
# Skipped when the toolchain is absent, so they are safe to invoke unconditionally.
test-gcp-terraform: ## Opt-in GCP Terraform compat suite (requires terraform; skips if absent)
	@set -e; \
	  command -v terraform >/dev/null 2>&1 || { echo "SKIP: terraform not installed"; exit 0; }; \
	  echo "Building jaiscloud-gcp..."; \
	  go build -o ./jaiscloud-gcp ./cmd/jaiscloud-gcp/; \
	  echo "Starting jaiscloud-gcp (ephemeral)..."; \
	  ./jaiscloud-gcp start --port 8080 --grpc-port 8081 --ephemeral > /tmp/jaiscloud-gcp-terraform.log 2>&1 & \
	  pid=$$!; \
	  cleanup() { echo "Stopping jaiscloud-gcp..."; kill "$$pid" 2>/dev/null || true; p=$$(lsof -ti tcp:8080 2>/dev/null || true); if [ -n "$$p" ]; then kill $$p 2>/dev/null || true; fi; }; \
	  trap cleanup EXIT INT TERM; \
	  n=0; until curl -sf http://localhost:8080/_jaiscloud/health >/dev/null 2>&1; do \
	    n=$$((n+1)); if [ $$n -ge 30 ]; then echo "ERROR: jaiscloud-gcp not healthy"; cat /tmp/jaiscloud-gcp-terraform.log; exit 1; fi; sleep 1; \
	  done; echo "  ready (REST :8080)"; \
	  TF_BIN=terraform tests/integration/gcp/terraform/run.sh http://localhost:8080 test-project

test-gcp-opentofu: ## Opt-in GCP OpenTofu compat suite (requires tofu; skips if absent)
	@set -e; \
	  command -v tofu >/dev/null 2>&1 || { echo "SKIP: tofu not installed"; exit 0; }; \
	  echo "Building jaiscloud-gcp..."; \
	  go build -o ./jaiscloud-gcp ./cmd/jaiscloud-gcp/; \
	  echo "Starting jaiscloud-gcp (ephemeral)..."; \
	  ./jaiscloud-gcp start --port 8080 --grpc-port 8081 --ephemeral > /tmp/jaiscloud-gcp-opentofu.log 2>&1 & \
	  pid=$$!; \
	  cleanup() { echo "Stopping jaiscloud-gcp..."; kill "$$pid" 2>/dev/null || true; p=$$(lsof -ti tcp:8080 2>/dev/null || true); if [ -n "$$p" ]; then kill $$p 2>/dev/null || true; fi; }; \
	  trap cleanup EXIT INT TERM; \
	  n=0; until curl -sf http://localhost:8080/_jaiscloud/health >/dev/null 2>&1; do \
	    n=$$((n+1)); if [ $$n -ge 30 ]; then echo "ERROR: jaiscloud-gcp not healthy"; cat /tmp/jaiscloud-gcp-opentofu.log; exit 1; fi; sleep 1; \
	  done; echo "  ready (REST :8080)"; \
	  TF_BIN=tofu tests/integration/gcp/terraform/run.sh http://localhost:8080 test-project

test-gcp-grpc-conformance: build-gcp ## gRPC message-level conformance suite via the official Google clients (tests/gcpconformance/grpc)
	@echo "Starting jaiscloud-gcp (ephemeral)..."
	@set -e; \
	  ./jaiscloud-gcp start --port 8080 --grpc-port 8081 --ephemeral > /tmp/jaiscloud-gcp-grpc-conformance.log 2>&1 & \
	  pid=$$!; \
	  cleanup() { echo "Stopping jaiscloud-gcp..."; kill "$$pid" 2>/dev/null || true; p=$$(lsof -ti tcp:8081 2>/dev/null || true); if [ -n "$$p" ]; then kill $$p 2>/dev/null || true; fi; }; \
	  trap cleanup EXIT INT TERM; \
	  n=0; until curl -sf http://localhost:8080/_jaiscloud/health >/dev/null 2>&1; do \
	    n=$$((n+1)); if [ $$n -ge 30 ]; then echo "ERROR: jaiscloud-gcp not healthy"; cat /tmp/jaiscloud-gcp-grpc-conformance.log; exit 1; fi; sleep 1; \
	  done; echo "  ready (REST :8080, gRPC :8081)"; \
	  ( cd tests/gcpconformance/grpc && GCP_EMULATOR_ENDPOINT_GRPC=localhost:8081 go test -count=1 -v -timeout 180s ./... )

test-gcp-gcloud-conformance: ## gcloud CLI client-conformance smoke suite vs ephemeral emulator (tag: gcloud_conformance)
	@echo "Building jaiscloud-gcp -> /tmp/jc-gcloud ..."
	@go build -o /tmp/jc-gcloud ./cmd/jaiscloud-gcp/
	@echo "Starting jaiscloud-gcp (ephemeral)..."
	@set -e; \
	  /tmp/jc-gcloud start --port 8080 --grpc-port 8081 --ephemeral > /tmp/jaiscloud-gcp-gcloud-conformance.log 2>&1 & \
	  pid=$$!; \
	  cleanup() { echo "Stopping jaiscloud-gcp (REST :8080)..."; kill "$$pid" 2>/dev/null || true; p=$$(lsof -ti tcp:8080 2>/dev/null || true); if [ -n "$$p" ]; then kill $$p 2>/dev/null || true; fi; }; \
	  trap cleanup EXIT INT TERM; \
	  n=0; until curl -sf http://localhost:8080/_jaiscloud/health >/dev/null 2>&1; do \
	    n=$$((n+1)); if [ $$n -ge 30 ]; then echo "ERROR: jaiscloud-gcp not healthy"; cat /tmp/jaiscloud-gcp-gcloud-conformance.log; exit 1; fi; sleep 1; \
	  done; echo "  ready (REST :8080)"; \
	  ( cd tests/gcpconformance/gcloud && GCP_EMULATOR_ENDPOINT=http://localhost:8080 go test -tags gcloud_conformance -count=1 -v -timeout 600s ./... )

test-gcp-python-conformance: ## Python google-cloud-* client-conformance suite vs ephemeral emulator (tests/clients/python)
	@echo "Creating Python venv -> tests/clients/python/.venv ..."
	@rm -rf tests/clients/python/.venv
	@$(PYTHON) -m venv tests/clients/python/.venv >/dev/null 2>&1 && [ -x tests/clients/python/.venv/bin/pip ] \
	  || (echo "  ensurepip unavailable; falling back to virtualenv"; rm -rf tests/clients/python/.venv; virtualenv -q tests/clients/python/.venv)
	@tests/clients/python/.venv/bin/python -m pip install -q --upgrade pip
	@tests/clients/python/.venv/bin/python -m pip install -q -r tests/clients/python/requirements.txt
	@echo "Building jaiscloud-gcp -> /tmp/jc-py ..."
	@go build -o /tmp/jc-py ./cmd/jaiscloud-gcp/
	@echo "Starting jaiscloud-gcp (ephemeral)..."
	@set -e; \
	  /tmp/jc-py start --port 8080 --grpc-port 8081 --ephemeral > /tmp/jaiscloud-gcp-python-conformance.log 2>&1 & \
	  pid=$$!; \
	  cleanup() { echo "Stopping jaiscloud-gcp (REST :8080)..."; kill "$$pid" 2>/dev/null || true; p=$$(lsof -ti tcp:8080 2>/dev/null || true); if [ -n "$$p" ]; then kill $$p 2>/dev/null || true; fi; }; \
	  trap cleanup EXIT INT TERM; \
	  n=0; until curl -sf http://localhost:8080/_jaiscloud/health >/dev/null 2>&1; do \
	    n=$$((n+1)); if [ $$n -ge 30 ]; then echo "ERROR: jaiscloud-gcp not healthy"; cat /tmp/jaiscloud-gcp-python-conformance.log; exit 1; fi; sleep 1; \
	  done; echo "  ready (REST :8080, gRPC :8081)"; \
	  tests/clients/python/.venv/bin/python -m pytest -v tests/clients/python

gen-gcp-fidelity-matrix: ## Regenerate docs/fidelity/* (fidelity matrix) from the registry + conformance evidence
	go run -tags gcp_conformance ./tools/fidelitygen -out docs/fidelity

check-gcp-fidelity-matrix: test-gcp-wire-conformance ## Fail if the committed fidelity matrix is stale (regenerate + git diff)
	$(MAKE) gen-gcp-fidelity-matrix
	@git diff --exit-code -- docs/fidelity || \
	  (echo "ERROR: docs/fidelity is stale — run 'make gen-gcp-fidelity-matrix' and commit the result"; exit 1)

gcp-status: ## Rebuild the GCP parity status ledger (plan_docs/STATUS.md + status.json) from all plan docs + git/GitHub state
	@mkdir -p bin
	@go build -o bin/gcpstatus ./tools/gcpstatus
	@bin/gcpstatus -docs plan_docs -out plan_docs/STATUS.md -json plan_docs/status.json -series "$(SERIES)"

gcp-status-check: ## Assess a proposed change against known state: Q="<keywords>" [SERVICE=<svc>] [include-archive=1]; exit 2 = already done, 3 = in flight
	@test -n "$(Q)$(SERVICE)" || { echo 'usage: make gcp-status-check Q="<keywords>" [SERVICE=<svc>]'; exit 2; }
	@mkdir -p bin
	@go build -o bin/gcpstatus ./tools/gcpstatus
	@bin/gcpstatus -docs plan_docs -query "$(Q)" -service "$(SERVICE)" -check $(if $(include-archive),-include-archive,)

gcp-status-audit: ## Classify not-done items: oversight? / unowned / stale-doc / abandoned / claimed-done / unscheduled / scheduled / intentional
	@mkdir -p bin
	@go build -o bin/gcpstatus ./tools/gcpstatus
	@bin/gcpstatus -docs plan_docs -audit $(if $(include-archive),-include-archive,)

gcp-status-coverage: ## Fail if any plan_docs file has status markers but produced no ledger rows (audit blind spots)
	@mkdir -p bin
	@go build -o bin/gcpstatus ./tools/gcpstatus
	@bin/gcpstatus -docs plan_docs -coverage $(if $(include-archive),-include-archive,)

gcp-status-next: ## Print the next actionable items in priority order (N=5, BY=wave|pri, SERIES=a,b)
	@mkdir -p bin
	@go build -o bin/gcpstatus ./tools/gcpstatus
	@bin/gcpstatus -docs plan_docs -next -n $(if $(N),$(N),5) -by $(if $(BY),$(BY),wave) -series "$(SERIES)" $(if $(include-archive),-include-archive,)

gcp-status-lint-plans: ## Fail if any plan-shaped file under plan_docs has no parseable index/detail (skipped the template)
	@mkdir -p bin
	@go build -o bin/gcpstatus ./tools/gcpstatus
	@bin/gcpstatus -docs plan_docs -lint-plans $(if $(include-archive),-include-archive,)

gcp-plan-new: ## Scaffold a preview->GA wave plan from the fidelity matrix: SERVICE=<svc> [EFFORT=ga] [FORCE=1]
	@test -n "$(SERVICE)" || { echo 'usage: make gcp-plan-new SERVICE=<service> [EFFORT=ga]'; exit 2; }
	@mkdir -p bin
	@go build -o bin/gcpstatus ./tools/gcpstatus
	@bin/gcpstatus -new-plan "$(SERVICE)" -effort "$(if $(EFFORT),$(EFFORT),ga)" $(if $(FORCE),-force,)

# One aggregate GA gate: the deterministic, infrastructure-free checks that back docs/GA.md.
# The gRPC and gcloud targets each build + boot an ephemeral emulator on :8080/:8081 and stop it;
# gcloud self-skips when it is not on PATH. Persistence/e2e are intentionally excluded.
ga-check: check-gcp-fidelity-matrix test-gcp-wire-conformance test-gcp-grpc-conformance test-gcp-gcloud-conformance ## One aggregate GA gate: fidelity drift + REST/gRPC/gcloud client conformance (no Docker/Postgres/k8s)
	@echo ""
	@echo "GA gate: offline + client conformance passed"
	@echo "  (grpc/gcloud targets build + boot an ephemeral emulator; this can take a few minutes)"
	@echo "  (persistence/e2e need Docker/Postgres/k8s: make test-e2e-gcp-persistence / test-e2e-lakehouse-k3d)"

test-e2e-iceberg: _check-iceberg-prereq ## Iceberg Glue Catalog tests — tests/persistent_mode/aws/iceberg/ (tag: iceberg_e2e)
	$(MAKE) up-docker JAISCLOUD_EXECUTOR_MODE=mock
	go clean -testcache
	SPARK_E2E_ICEBERG_IMAGE=$(SPARK_E2E_ICEBERG_IMAGE) JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags iceberg_e2e -timeout 30m ./tests/persistent_mode/aws/iceberg/
	$(MAKE) down-docker

# Iceberg-on-Dataproc E2E — external Docker Spark against the emulator's Hive
# Metastore Thrift listener (:9083) + GCS (:8080). The tests run under the
# iceberg_e2e tag and require the Thrift listener, which serves a single global
# catalog. Works against a remote Docker daemon too: SQL is passed via spark-sql
# -e, not a bind mount.
test-e2e-iceberg-gcp: _check-iceberg-gcp-prereq build-gcp ## Iceberg-on-Hive tests — tests/persistent_mode/gcp/iceberg/ (tag: iceberg_e2e)
	@echo "Starting jaiscloud-gcp (ephemeral)..."
	@./jaiscloud-gcp start --port 8080 --grpc-port 8081 --ephemeral > /tmp/jaiscloud-gcp-iceberg.log 2>&1 & \
	  n=0; until curl -sf http://localhost:8080/_jaiscloud/health >/dev/null 2>&1; do \
	    n=$$((n+1)); if [ $$n -ge 30 ]; then echo "ERROR: jaiscloud-gcp not healthy"; cat /tmp/jaiscloud-gcp-iceberg.log; exit 1; fi; sleep 1; \
	  done; echo "  ready (REST :8080)"
	go clean -testcache
	SPARK_E2E_ICEBERG_GCP_IMAGE=$(SPARK_E2E_ICEBERG_GCP_IMAGE) JAISCLOUD_HOST=http://localhost:8080 \
	  go test -v -tags iceberg_e2e -timeout 30m ./tests/persistent_mode/gcp/iceberg/
	@echo "Stopping jaiscloud-gcp..."
	@pkill -f "jaiscloud-gcp start" 2>/dev/null || true

##@ k3d (Kubernetes) e2e

test-e2e-lakehouse-k3d: _check-lakehouse-k3d-prereq _refresh-gcp-image ## Medallion ELT pipeline e2e on k3d — rebuilds the emulator image first (tag: lakehouse_e2e)
	go clean -testcache
	K8S_NAMESPACE=$(K8S_NAMESPACE) LAKEHOUSE_RECORDS=$(LAKEHOUSE_RECORDS) \
	  go test -v -tags lakehouse_e2e -timeout 20m ./tests/persistent_mode/gcp/lakehouse/

test-e2e-gcp-samples-k3d: _check-gcp-samples-prereq _refresh-gcp-image ## Spring Cloud GCP sample apps e2e on k3d (tag: gcpsamples_e2e; run `make docker-gcp-samples` first; SKIP_GCP_IMAGE_REBUILD=1 to reuse the deployed emulator)
	go clean -testcache
	K8S_NAMESPACE=$(K8S_NAMESPACE) \
	  go test -v -tags gcpsamples_e2e -timeout 15m ./tests/persistent_mode/gcp/gcpsamples/

# Rebuild the emulator image from the working tree and roll the deployment so the
# pipeline always runs against the code under test, not whatever happens to be in
# the cluster. A stale image silently broke this suite once already.
# Set SKIP_GCP_IMAGE_REBUILD=1 to reuse the deployed image.
_refresh-gcp-image:
	@if [ "$(SKIP_GCP_IMAGE_REBUILD)" = "1" ]; then \
	  echo "SKIP_GCP_IMAGE_REBUILD=1 — reusing deployed $(GCP_IMAGE)"; \
	else \
	  echo "Rebuilding $(GCP_IMAGE) from $$(git rev-parse --short HEAD) ..."; \
	  docker build --build-arg CLOUD=gcp -t $(GCP_IMAGE) -f Dockerfile . && \
	  docker push $(GCP_PUSH_FLAGS) $(GCP_IMAGE) && \
	  kubectl -n $(K8S_NAMESPACE) rollout restart deployment/jaiscloud-gcp && \
	  kubectl -n $(K8S_NAMESPACE) rollout status deployment/jaiscloud-gcp --timeout=180s; \
	fi

##@ Aggregate test targets

test-e2e-docker-all: test-e2e-emr-docker test-e2e-dpc-docker test-e2e-lambda-docker test-e2e-eventbridge ## All Docker-based e2e suites

test-e2e-k8s-all: test-e2e-emrcontainers-k8s test-e2e-dpc-k8s test-e2e-lambda-k8s ## All K8s-based e2e suites

test-e2e: test-e2e-docker-all test-e2e-k8s-all test-e2e-persistence test-e2e-iceberg ## All e2e suites (Docker + K8s + Persistence + Iceberg)

test-all: test test-integration test-e2e ## Unit tests + integration tests + all e2e suites

test-all-gcp: test-gcp test-integration-gcp test-e2e-gcp-persistence ## GCP unit + integration + persistence

# ─── Internal helpers (not shown in help) ────────────────────────────────────

_build-for-e2e:
	go build -o jaiscloud-aws ./cmd/jaiscloud-aws/

_wait-postgres:
	@echo "Waiting for Postgres on port $(PG_PORT)..."
	@n=0; until docker exec $(PG_CONTAINER) pg_isready -U $(PG_USER) -q 2>/dev/null; do \
	  n=$$((n+1)); \
	  if [ $$n -ge 30 ]; then \
	    echo "ERROR: Postgres did not become ready within 30s — check: docker logs $(PG_CONTAINER)"; \
	    exit 1; \
	  fi; \
	  sleep 1; \
	done; \
	echo "Postgres ready  →  $(JAISCLOUD_DSN)"

_wait-docker:
	@echo "Waiting for jaiscloud on $(JAISCLOUD_HOST)..."
	@n=0; until curl -sf $(JAISCLOUD_HOST)/_jaiscloud/health > /dev/null 2>&1; do \
	  n=$$((n+1)); \
	  if [ $$n -ge 60 ]; then \
	    echo "ERROR: jaiscloud did not become healthy within 60s"; \
	    exit 1; \
	  fi; \
	  sleep 1; \
	done; \
	echo "jaiscloud ready"

_start-k8s: _check-k8s-prereq up-k8s

_stop-k8s: down-k8s

_restart-server-memory: _build-for-e2e
	@pkill -f "jaiscloud-aws start" 2>/dev/null || true
	@sleep 1
	@JAISCLOUD_PORT=$(JAISCLOUD_PORT) \
	  ./jaiscloud-aws start \
	  > /tmp/jaiscloud-e2e.log 2>&1 &
	@echo "Waiting for jaiscloud-aws on $(JAISCLOUD_HOST)..."
	@n=0; until curl -sf $(JAISCLOUD_HOST)/_jaiscloud/health > /dev/null 2>&1; do \
	  n=$$((n+1)); \
	  if [ $$n -ge 30 ]; then \
	    echo "ERROR: jaiscloud-aws did not become healthy — check /tmp/jaiscloud-e2e.log"; \
	    exit 1; \
	  fi; \
	  sleep 1; \
	done; \
	echo "jaiscloud-aws ready (log: /tmp/jaiscloud-e2e.log)"

_check-docker-prereq:
	@docker info > /dev/null 2>&1 || \
	  (echo "ERROR: Docker daemon is not running"; exit 1)

_check-k8s-prereq:
	@kubectl --context docker-desktop cluster-info > /dev/null 2>&1 || \
	  (echo "ERROR: docker-desktop Kubernetes is not reachable — enable Kubernetes in Docker Desktop"; exit 1)

_check-iceberg-prereq:
	@docker image inspect $(SPARK_E2E_ICEBERG_IMAGE) > /dev/null 2>&1 || \
	  (echo "ERROR: image '$(SPARK_E2E_ICEBERG_IMAGE)' not found — build or pull it first"; exit 1)

_check-iceberg-gcp-prereq:
	@docker image inspect $(SPARK_E2E_ICEBERG_GCP_IMAGE) > /dev/null 2>&1 || \
	  (echo "ERROR: image '$(SPARK_E2E_ICEBERG_GCP_IMAGE)' not found — build or pull it first"; exit 1)

_check-lakehouse-k3d-prereq:
	@command -v kubectl > /dev/null 2>&1 || (echo "ERROR: kubectl not found — install kubectl and start a k3d cluster"; exit 1)
	@kubectl get namespace $(K8S_NAMESPACE) > /dev/null 2>&1 || \
	  (echo "ERROR: namespace '$(K8S_NAMESPACE)' not found — start the cluster and deploy the emulator"; exit 1)
	@kubectl -n $(K8S_NAMESPACE) get svc jaiscloud-gcp > /dev/null 2>&1 || \
	  (echo "ERROR: svc/jaiscloud-gcp not found — kubectl apply -f deploy/k8s/jaiscloud-gcp.yaml"; exit 1)

_check-gcp-samples-prereq:
	@command -v kubectl > /dev/null 2>&1 || (echo "ERROR: kubectl not found — install kubectl and start a k3d cluster"; exit 1)
	@kubectl get namespace $(K8S_NAMESPACE) > /dev/null 2>&1 || \
	  (echo "ERROR: namespace '$(K8S_NAMESPACE)' not found — start the cluster and deploy the emulator"; exit 1)
	@kubectl -n $(K8S_NAMESPACE) get svc jaiscloud-gcp > /dev/null 2>&1 || \
	  (echo "ERROR: svc/jaiscloud-gcp not found — kubectl apply -f deploy/k8s/jaiscloud-gcp.yaml"; exit 1)
