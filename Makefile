# ─── Defaults ─────────────────────────────────────────────────────────────────
.DEFAULT_GOAL := help

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

.PHONY: lint lint-pagination help build build-ui test docker clean \
        server-memory server-ephemeral server-postgres server-docker server-k8s server-postgres-all \
        server-ui stop-server up-docker down-docker up-k8s down-k8s \
        postgres-up postgres-reset postgres-down \
        test-integration \
        test-e2e-emr-docker test-e2e-emrcontainers-k8s test-e2e-eventbridge \
        test-e2e-dpc-docker test-e2e-dpc-k8s \
        test-e2e-lambda-docker test-e2e-lambda-k8s \
        test-e2e-cloudformation test-e2e-kms test-e2e-ssm test-e2e-dynamodb test-e2e-persistence \
        test-e2e-iceberg \
        test-e2e-docker-all test-e2e-k8s-all test-e2e test-all \
        _build-for-e2e _restart-server-memory _wait-docker _wait-postgres \
        _start-k8s _stop-k8s \
        _check-docker-prereq _check-k8s-prereq _check-iceberg-prereq

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

build-ui: ## Build frontend assets then compile jaiscloud-aws with embedded UI (-tags ui)
	pnpm --dir ui run build
	CGO_ENABLED=0 go build -tags ui -trimpath -ldflags="-s -w" -o jaiscloud-aws ./cmd/jaiscloud-aws/

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

clean: ## Remove compiled binaries
	rm -f jaiscloud-aws jaiscloud-azure jaiscloud-gcp

lint: ## Run ARN lint guard + go vet
	@bash scripts/check_no_hardcoded_arn.sh
	@go vet ./...

lint-pagination: ## Heuristic check that List*/Describe* provider methods use pagination
	@go run tools/lint/paginationcheck/main.go ./internal/aws/provider/...

##@ Unit tests

test: ## Run all unit tests with the race detector  (no server needed)
	go clean -testcache
	go test -race ./internal/...

##@ Server — foreground (Ctrl-C to stop)

server-ui: build-ui ## Build with embedded UI and start with --ui --ui-open (opens browser)
	JAISCLOUD_PORT=$(JAISCLOUD_PORT) \
	  ./jaiscloud-aws start --ui --ui-open

server-memory: build ## Default mode: memory stores + periodic state.json saves, mock executors
	JAISCLOUD_PORT=$(JAISCLOUD_PORT) \
	  ./jaiscloud-aws start

server-ephemeral: build ## Ephemeral mode: no persistence, clean slate on every start (CI/tests)
	JAISCLOUD_PORT=$(JAISCLOUD_PORT) \
	  ./jaiscloud-aws start --ephemeral

server-postgres: build ## Postgres backend, mock executors — requires JAISCLOUD_DSN
	JAISCLOUD_PORT=$(JAISCLOUD_PORT) \
	  ./jaiscloud-aws start --dsn "$(JAISCLOUD_DSN)"

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

test-e2e-iceberg: _check-iceberg-prereq ## Iceberg Glue Catalog tests — tests/persistent_mode/aws/iceberg/ (tag: iceberg_e2e)
	$(MAKE) up-docker JAISCLOUD_EXECUTOR_MODE=mock
	go clean -testcache
	SPARK_E2E_ICEBERG_IMAGE=$(SPARK_E2E_ICEBERG_IMAGE) JAISCLOUD_HOST=$(JAISCLOUD_HOST) \
	  go test -v -tags iceberg_e2e -timeout 30m ./tests/persistent_mode/aws/iceberg/
	$(MAKE) down-docker

##@ Aggregate test targets

test-e2e-docker-all: test-e2e-emr-docker test-e2e-dpc-docker test-e2e-lambda-docker test-e2e-eventbridge ## All Docker-based e2e suites

test-e2e-k8s-all: test-e2e-emrcontainers-k8s test-e2e-dpc-k8s test-e2e-lambda-k8s ## All K8s-based e2e suites

test-e2e: test-e2e-docker-all test-e2e-k8s-all test-e2e-persistence test-e2e-iceberg ## All e2e suites (Docker + K8s + Persistence + Iceberg)

test-all: test test-integration test-e2e ## Unit tests + integration tests + all e2e suites

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
