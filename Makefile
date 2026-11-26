.PHONY: build build-release test test-s3 coverage clean \
       run stop monitoring-up monitoring-down \
       docker-build kind-create kind-load deploy \
       helm-install helm-upgrade helm-uninstall helm-package helm-push \
       undeploy clean-kind port-forward status \
       build-ui docker-build-ui run-ui \
       lint-sdk test-sdk test-sdk-kind generate-python-stubs \
       bench-search bench-metal bench-cuda \
       build-connectors test-connectors \
       build-operator test-operator docker-build-operator \
       helm-install-operator helm-uninstall-operator \
       generate-operator-pb \
       docs-install docs-serve docs-build

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# PRESET names a configure preset in CMakePresets.json. Each preset builds
# into build/<preset>.
PRESET          ?= dev
BUILD_DIR        = build/$(PRESET)
CLUSTER_NAME    ?= gvdb
IMAGE_NAME      ?= ghcr.io/jonathanberhe/gvdb:latest
HELM_CHART       = deploy/helm/gvdb
HELM_RELEASE    ?= gvdb
HELM_NAMESPACE  ?= gvdb
HELM_REGISTRY   ?= oci://ghcr.io/jonathanberhe/charts
# Extra cache settings for the configure step, for example
# CMAKE_EXTRA="-DGVDB_WITH_S3=ON".
CMAKE_EXTRA     ?=
PYTHON_SDK_DIR   = clients/python
NPROC           := $(shell getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)
CONNECTORS_DIR   = connectors

# ---------------------------------------------------------------------------
# Local development
# ---------------------------------------------------------------------------
build:
	@cmake --preset $(PRESET) $(CMAKE_EXTRA)
	@cmake --build --preset $(PRESET)

build-release:
	@$(MAKE) build PRESET=release

# CPU search latency of the query executor over FAISS segments, built with
# the bench preset (Release, tests and benchmarks). Compare runs on the same
# machine.
bench-search:
	@cmake --preset bench
	@cmake --build --preset bench --target gvdb-search-bench
	@./build/bench/bin/gvdb-search-bench

# The Metal benchmark loads its kernel source from <build>/kernels.
bench-metal:
	@cmake --preset metal
	@cmake --build --preset metal --target gvdb-metal-bench
	@cd build/metal && ./bin/gvdb-metal-bench

bench-cuda:
	@cmake --preset cuda
	@cmake --build --preset cuda --target gvdb-cuda-bench
	@./build/cuda/bin/gvdb-cuda-bench

# The test binaries are isolated (unique temp folders, free ports), so CTest
# runs them in parallel, one per CPU.
test:
	@ctest --preset $(PRESET) --parallel $(NPROC)

# Line coverage of src/ and include/ per file, from the unit and integration
# tests. The report lands in build/coverage/coverage.
coverage:
	@cmake --preset coverage
	@cmake --build --preset coverage
	@tools/coverage.sh build/coverage

# Single-node server of $(PRESET) in the background. Data, log and pid file
# live in $(BUILD_DIR)/run. Prometheus metrics: http://localhost:9090/metrics.
RUN_DIR = $(BUILD_DIR)/run

run: build
	@mkdir -p $(RUN_DIR)/data
	@if [ -f $(RUN_DIR)/gvdb.pid ] && kill -0 $$(cat $(RUN_DIR)/gvdb.pid) 2>/dev/null; then \
		echo "gvdb-single-node already runs (pid $$(cat $(RUN_DIR)/gvdb.pid)). Run 'make stop' first."; exit 1; \
	fi
	@nohup $(BUILD_DIR)/bin/gvdb-single-node --port 50051 --data-dir $(RUN_DIR)/data --node-id 1 \
		> $(RUN_DIR)/gvdb.log 2>&1 & echo $$! > $(RUN_DIR)/gvdb.pid
	@echo "gvdb-single-node started on localhost:50051 (pid $$(cat $(RUN_DIR)/gvdb.pid), log $(RUN_DIR)/gvdb.log)"

stop:
	@if [ -f $(RUN_DIR)/gvdb.pid ] && kill $$(cat $(RUN_DIR)/gvdb.pid) 2>/dev/null; then \
		echo "gvdb-single-node stopped"; else echo "gvdb-single-node does not run"; fi
	@rm -f $(RUN_DIR)/gvdb.pid

# Prometheus on http://localhost:9091 and Grafana on http://localhost:3000
# (admin/admin) with the GVDB dashboard. Prometheus scrapes the server that
# `make run` starts on this host.
monitoring-up:
	@docker compose -f deploy/monitoring/docker-compose.yml up -d

monitoring-down:
	@docker compose -f deploy/monitoring/docker-compose.yml down

# S3 tests against the S3-compatible endpoint from
# test/integration/docker-compose.s3.yml. Needs a build with S3: run
# `make build CMAKE_EXTRA="-DGVDB_WITH_S3=ON"` first.
test-s3:
	@echo "Starting the S3 test endpoint..."
	@docker compose -f $(CURDIR)/test/integration/docker-compose.s3.yml up -d --wait
	@echo "Running C++ S3 tests..."
	@GVDB_TEST_S3_ENDPOINT=http://localhost:9000 ctest --preset $(PRESET) -L s3; status=$$?; \
	echo "Stopping the S3 test endpoint..."; \
	docker compose -f $(CURDIR)/test/integration/docker-compose.s3.yml down; \
	exit $$status

lint-sdk:
	@cd $(PYTHON_SDK_DIR) && uv run ruff check . && uv run ruff format --check .

# The SDK tests start the gvdb-single-node built by $(PRESET).
test-sdk: lint-sdk
	@cd $(PYTHON_SDK_DIR) && GVDB_SERVER_BIN=$(CURDIR)/$(BUILD_DIR)/bin/gvdb-single-node uv run python3 -m pytest tests/ -v

test-sdk-kind:
	@cd $(PYTHON_SDK_DIR) && GVDB_SERVER_ADDR=localhost:50050 uv run python3 -m pytest tests/test_sdk.py -v

generate-python-stubs:
	@cd $(PYTHON_SDK_DIR) && bash generate_proto.sh

clean:
	@rm -rf build

# ---------------------------------------------------------------------------
# Java Connectors (Spark / Flink)
# ---------------------------------------------------------------------------
build-connectors:
	@cd $(CONNECTORS_DIR) && ./gradlew build -x :gvdb-connector-tests:test

test-connectors:
	@cd $(CONNECTORS_DIR) && ./gradlew build

# ---------------------------------------------------------------------------
# Kubernetes Operator (Tier 0b.6)
# ---------------------------------------------------------------------------
OPERATOR_DIR        ?= operator
OPERATOR_IMAGE_NAME ?= ghcr.io/jonathanberhe/gvdb-operator:latest
HELM_OPERATOR_CHART  = deploy/helm/gvdb-operator
HELM_OPERATOR_RELEASE ?= gvdb-operator
HELM_OPERATOR_NAMESPACE ?= gvdb-operator-system

build-operator:
	@cd $(OPERATOR_DIR) && go build ./...

# TestControllers is the envtest ginkgo suite; it needs apiserver binaries
# (installed via `make -C $(OPERATOR_DIR) envtest`). The CI job runs that
# separately; this target skips it so `make test-operator` works locally
# without the asset download.
test-operator:
	@cd $(OPERATOR_DIR) && go test ./internal/... -skip '^TestControllers$$'

docker-build-operator:
	docker build -t $(OPERATOR_IMAGE_NAME) -f $(OPERATOR_DIR)/Dockerfile $(OPERATOR_DIR)

helm-install-operator:
	helm install $(HELM_OPERATOR_RELEASE) $(HELM_OPERATOR_CHART) \
		-n $(HELM_OPERATOR_NAMESPACE) --create-namespace \
		--set image.tag=latest

helm-uninstall-operator:
	helm uninstall $(HELM_OPERATOR_RELEASE) -n $(HELM_OPERATOR_NAMESPACE)

# Regenerate the operator's Go protobuf stubs so they never drift from
# proto/*.proto. Covers vectordb.proto (public
# VectorDBService) and internal.proto (InternalService for operator's
# GetLeaderInfo + GetClusterHealth calls).
generate-operator-pb:
	@echo "Regenerating Go protobuf stubs for the operator..."
	@mkdir -p /tmp/gvdb-pbgen
	@cd proto && protoc \
		--go_out=/tmp/gvdb-pbgen --go_opt=paths=source_relative \
		--go-grpc_out=/tmp/gvdb-pbgen --go-grpc_opt=paths=source_relative \
		--go_opt=Minternal.proto=gvdb/operator/internal/gvdbpb \
		--go_opt=Mvectordb.proto=gvdb/operator/internal/gvdbpb \
		--go-grpc_opt=Minternal.proto=gvdb/operator/internal/gvdbpb \
		--go-grpc_opt=Mvectordb.proto=gvdb/operator/internal/gvdbpb \
		vectordb.proto internal.proto
	@cp /tmp/gvdb-pbgen/vectordb.pb.go $(OPERATOR_DIR)/internal/gvdbpb/vectordb.pb.go
	@cp /tmp/gvdb-pbgen/vectordb_grpc.pb.go $(OPERATOR_DIR)/internal/gvdbpb/vectordb_grpc.pb.go
	@cp /tmp/gvdb-pbgen/internal.pb.go $(OPERATOR_DIR)/internal/gvdbpb/internal.pb.go
	@cp /tmp/gvdb-pbgen/internal_grpc.pb.go $(OPERATOR_DIR)/internal/gvdbpb/internal_grpc.pb.go
	@sed -i.bak 's|^package pb$$|package gvdbpb|' \
		$(OPERATOR_DIR)/internal/gvdbpb/vectordb.pb.go \
		$(OPERATOR_DIR)/internal/gvdbpb/vectordb_grpc.pb.go \
		$(OPERATOR_DIR)/internal/gvdbpb/internal.pb.go \
		$(OPERATOR_DIR)/internal/gvdbpb/internal_grpc.pb.go
	@rm $(OPERATOR_DIR)/internal/gvdbpb/*.bak
	@rm -rf /tmp/gvdb-pbgen

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------
docker-build:
	docker build -t $(IMAGE_NAME) -f deploy/Dockerfile .

# ---------------------------------------------------------------------------
# Helm
# ---------------------------------------------------------------------------
helm-install:
	helm install $(HELM_RELEASE) $(HELM_CHART) -n $(HELM_NAMESPACE) --create-namespace \
		--set image.tag=latest

helm-upgrade:
	helm upgrade $(HELM_RELEASE) $(HELM_CHART) -n $(HELM_NAMESPACE) \
		--set image.tag=latest

helm-uninstall:
	helm uninstall $(HELM_RELEASE) -n $(HELM_NAMESPACE)

helm-package:
	helm package $(HELM_CHART)

helm-push: helm-package
	helm push gvdb-*.tgz $(HELM_REGISTRY)

# ---------------------------------------------------------------------------
# Kind cluster
# ---------------------------------------------------------------------------
kind-create:
	@kind get clusters 2>/dev/null | grep -q "^$(CLUSTER_NAME)$$" || \
		kind create cluster --name $(CLUSTER_NAME) --config deploy/kind-config.yaml

kind-load:
	kind load docker-image $(IMAGE_NAME) --name $(CLUSTER_NAME)

# Full pipeline: build image + create kind cluster + load + helm install
deploy: docker-build kind-create kind-load helm-install

undeploy:
	-helm uninstall $(HELM_RELEASE) -n $(HELM_NAMESPACE) 2>/dev/null
	kubectl delete namespace $(HELM_NAMESPACE) --ignore-not-found

port-forward:
	@echo "Forwarding proxy gRPC to localhost:50050 (Ctrl+C to stop)"
	kubectl port-forward -n $(HELM_NAMESPACE) svc/$(HELM_RELEASE)-proxy 50050:50050

status:
	kubectl get pods -n $(HELM_NAMESPACE) -o wide

# ---------------------------------------------------------------------------
# Documentation (Zensical, https://zensical.org)
# ---------------------------------------------------------------------------
DOCS_VENV = .venv-docs

docs-install:
	@[ -d $(DOCS_VENV) ] || uv venv $(DOCS_VENV)
	@uv pip install --python $(DOCS_VENV)/bin/python -r requirements-docs.txt

docs-serve: docs-install
	@$(DOCS_VENV)/bin/zensical serve

docs-build: docs-install
	@$(DOCS_VENV)/bin/zensical build

# ---------------------------------------------------------------------------
# Web UI
# ---------------------------------------------------------------------------
build-ui:
	@cd ui/web && yarn install --frozen-lockfile && yarn build
	@rm -rf ui/gateway/static/*
	@cp -r ui/web/dist/* ui/gateway/static/
	@cd ui/gateway && CGO_ENABLED=0 go build -ldflags="-s -w" -o gvdb-ui .
	@echo "Built ui/gateway/gvdb-ui ($$(du -h ui/gateway/gvdb-ui | cut -f1))"

docker-build-ui:
	docker build -t gvdb-ui:latest -f ui/Dockerfile ui/

run-ui:
	@cd ui/gateway && ./gvdb-ui $(if $(GVDB_ADDR),--gvdb-addr $(GVDB_ADDR),)

clean-kind:
	kind delete cluster --name $(CLUSTER_NAME)
