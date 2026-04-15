.PHONY: build build-release test test-e2e test-e2e-kind clean \
       docker-build kind-create kind-load deploy apply \
       helm-install helm-upgrade helm-uninstall helm-package helm-push \
       undeploy clean-kind port-forward status \
       build-ui run-ui \
       lint-sdk test-sdk test-sdk-kind generate-python-stubs \
       bench-metal

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BUILD_DIR       ?= build
BUILD_TYPE      ?= Debug
CLUSTER_NAME    ?= gvdb
IMAGE_NAME      ?= gvdb:latest
HELM_CHART       = deploy/helm/gvdb
HELM_RELEASE    ?= gvdb
HELM_NAMESPACE  ?= gvdb
HELM_REGISTRY   ?= oci://ghcr.io/jonathanberhe/charts
K8S_DIR          = deploy/k8s
CMAKE_JOBS      ?= $(shell nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
CMAKE_EXTRA     ?=
E2E_DIR          = test/e2e
PYTHON_SDK_DIR   = clients/python
GVDB_SERVER_ADDR ?= localhost:50051

# ---------------------------------------------------------------------------
# Local development
# ---------------------------------------------------------------------------
build:
	@cmake -S . -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) $(CMAKE_EXTRA)
	@cmake --build $(BUILD_DIR) -j$(CMAKE_JOBS)

build-release:
	@$(MAKE) build BUILD_TYPE=Release

bench-metal:
	@$(MAKE) build CMAKE_EXTRA="-DGVDB_WITH_METAL=ON"
	@cd $(BUILD_DIR) && ./bin/gvdb-metal-bench

test:
	@ctest --test-dir $(BUILD_DIR) --output-on-failure

test-e2e:
	@cd $(E2E_DIR) && GVDB_SERVER_ADDR=$(GVDB_SERVER_ADDR) ./run_all_tests.sh

test-e2e-kind:
	@cd $(E2E_DIR) && GVDB_SERVER_ADDR=localhost:50050 NO_SERVER=true ./run_all_tests.sh

test-s3:
	@echo "Starting MinIO..."
	@docker compose -f test/integration/docker-compose.minio.yml up -d --wait
	@echo "Running C++ S3 integration tests..."
	@cd $(BUILD_DIR) && ctest -R S3 --output-on-failure || true
	@echo "Running Go S3 e2e test..."
	@cd $(E2E_DIR) && GVDB_S3_ENDPOINT=http://localhost:9000 GVDB_SERVER_ADDR=$(GVDB_SERVER_ADDR) ./run_all_tests.sh || true
	@echo "Stopping MinIO..."
	@docker compose -f test/integration/docker-compose.minio.yml down

lint-sdk:
	@cd $(PYTHON_SDK_DIR) && uv run ruff check . && uv run ruff format --check .

test-sdk: lint-sdk
	@cd $(PYTHON_SDK_DIR) && uv run python3 -m pytest tests/ -v

test-sdk-kind:
	@cd $(PYTHON_SDK_DIR) && GVDB_SERVER_ADDR=localhost:50050 uv run python3 -m pytest tests/test_sdk.py -v

generate-python-stubs:
	@cd $(PYTHON_SDK_DIR) && bash generate_proto.sh

clean:
	@rm -rf $(BUILD_DIR)

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------
docker-build:
	docker build -t $(IMAGE_NAME) -f deploy/Dockerfile .

# ---------------------------------------------------------------------------
# Helm
# ---------------------------------------------------------------------------
helm-install:
	helm install $(HELM_RELEASE) $(HELM_CHART) -n $(HELM_NAMESPACE) --create-namespace

helm-upgrade:
	helm upgrade $(HELM_RELEASE) $(HELM_CHART) -n $(HELM_NAMESPACE)

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

# Raw manifests (alternative to Helm, for quick testing)
apply:
	kubectl apply -f $(K8S_DIR)/namespace.yaml
	kubectl apply -f $(K8S_DIR)/configmap.yaml
	kubectl apply -f $(K8S_DIR)/services.yaml
	kubectl apply -f $(K8S_DIR)/coordinator.yaml
	@echo "Waiting for coordinator to be ready..."
	kubectl -n gvdb rollout status statefulset/gvdb-coordinator --timeout=120s
	kubectl apply -f $(K8S_DIR)/data-node.yaml
	kubectl apply -f $(K8S_DIR)/query-node.yaml
	@echo "Waiting for data and query nodes..."
	kubectl -n gvdb rollout status statefulset/gvdb-data-node --timeout=180s
	kubectl -n gvdb rollout status statefulset/gvdb-query-node --timeout=180s
	kubectl apply -f $(K8S_DIR)/proxy.yaml
	kubectl -n gvdb rollout status deployment/gvdb-proxy --timeout=60s
	@echo ""
	@echo "GVDB cluster deployed successfully!"
	@echo "  Run 'make port-forward' to access the proxy at localhost:50050"

undeploy:
	-helm uninstall $(HELM_RELEASE) -n $(HELM_NAMESPACE) 2>/dev/null
	kubectl delete namespace $(HELM_NAMESPACE) --ignore-not-found

port-forward:
	@echo "Forwarding proxy gRPC to localhost:50050 (Ctrl+C to stop)"
	kubectl port-forward -n $(HELM_NAMESPACE) svc/$(HELM_RELEASE)-proxy 50050:50050

status:
	kubectl get pods -n $(HELM_NAMESPACE) -o wide

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
