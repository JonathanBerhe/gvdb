#!/bin/bash
# Master test runner for all end-to-end tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SERVER_BIN="$PROJECT_ROOT/build/bin/gvdb-single-node"
SERVER_PORT=50051
SERVER_DATA_DIR="/tmp/gvdb-integration-test"
SERVER_PID=""

# Cloud-native: use GVDB_SERVER_ADDR or default to localhost:$SERVER_PORT
export GVDB_SERVER_ADDR="${GVDB_SERVER_ADDR:-localhost:$SERVER_PORT}"

# NO_SERVER=true skips starting a local server (for running against external endpoints)
NO_SERVER="${NO_SERVER:-false}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

cleanup() {
    log_info "Cleaning up..."

    # Kill server if running
    if [ -n "$SERVER_PID" ]; then
        log_info "Stopping GVDB server (PID: $SERVER_PID)..."
        kill -15 "$SERVER_PID" 2>/dev/null || true
        sleep 2
        kill -9 "$SERVER_PID" 2>/dev/null || true
    fi

    # Clean test data
    if [ -d "$SERVER_DATA_DIR" ]; then
        log_info "Removing test data directory..."
        rm -rf "$SERVER_DATA_DIR"
    fi

    log_success "Cleanup complete"
}

# Trap signals for cleanup
trap cleanup EXIT INT TERM

check_dependencies() {
    log_info "Checking dependencies..."

    # Check Go
    if ! command -v go &> /dev/null; then
        log_error "Go not found. Please install Go 1.21+"
        exit 1
    fi

    GO_VERSION=$(go version | awk '{print $3}' | sed 's/go//')
    log_info "Found Go version: $GO_VERSION"

    # Check protoc
    if ! command -v protoc &> /dev/null; then
        log_error "protoc not found. Please install Protocol Buffers compiler"
        exit 1
    fi

    # Check server binary (only if we need to start one)
    if [ "$NO_SERVER" = "false" ] && [ ! -f "$SERVER_BIN" ]; then
        log_error "Server binary not found at: $SERVER_BIN"
        log_error "Please build the project first: make build"
        exit 1
    fi

    log_success "All dependencies OK"
}

generate_proto() {
    log_info "Generating Go protobuf stubs..."

    cd "$SCRIPT_DIR"
    chmod +x generate_proto.sh
    ./generate_proto.sh

    if [ $? -ne 0 ]; then
        log_error "Failed to generate protobuf stubs"
        exit 1
    fi

    log_success "Protobuf stubs generated"
}

download_deps() {
    log_info "Downloading Go dependencies..."

    cd "$SCRIPT_DIR"
    go mod download

    if [ $? -ne 0 ]; then
        log_error "Failed to download Go dependencies"
        exit 1
    fi

    log_success "Go dependencies ready"
}

start_server() {
    log_info "Starting GVDB server..."

    # Clean old data
    rm -rf "$SERVER_DATA_DIR"
    mkdir -p "$SERVER_DATA_DIR"

    # Start server in background
    "$SERVER_BIN" \
        --port "$SERVER_PORT" \
        --data-dir "$SERVER_DATA_DIR" \
        --node-id 1 \
        > "$SERVER_DATA_DIR/server.log" 2>&1 &

    SERVER_PID=$!

    log_info "Server PID: $SERVER_PID"

    # Wait for server to be ready
    log_info "Waiting for server to start..."
    MAX_WAIT=30
    WAITED=0

    while [ $WAITED -lt $MAX_WAIT ]; do
        if lsof -Pi :$SERVER_PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
            log_success "Server is ready on port $SERVER_PORT"
            sleep 1  # Extra second to ensure full startup
            return 0
        fi

        sleep 1
        WAITED=$((WAITED + 1))
    done

    log_error "Server failed to start within ${MAX_WAIT}s"
    log_error "Server log:"
    tail -n 50 "$SERVER_DATA_DIR/server.log"
    exit 1
}

build_test() {
    TEST_NAME="$1"
    TEST_FILE="$2"

    log_info "Building $TEST_NAME..."

    cd "$SCRIPT_DIR"
    go build -o "./${TEST_NAME}_test" "$TEST_FILE" helpers.go

    if [ $? -ne 0 ]; then
        log_error "Failed to build $TEST_NAME"
        return 1
    fi

    log_success "$TEST_NAME built"
    return 0
}

run_test() {
    TEST_NAME="$1"
    TEST_BIN="$2"

    echo ""
    echo "======================================================================"
    log_info "Running $TEST_NAME..."
    echo "======================================================================"

    cd "$SCRIPT_DIR"
    "./$TEST_BIN"
    TEST_EXIT_CODE=$?

    if [ $TEST_EXIT_CODE -eq 0 ]; then
        log_success "$TEST_NAME PASSED"
        return 0
    else
        log_error "$TEST_NAME FAILED (exit code: $TEST_EXIT_CODE)"
        return 1
    fi
}

# Main execution
main() {
    echo "======================================================================"
    echo "GVDB End-to-End Test Suite (Go)"
    echo "======================================================================"
    echo ""
    log_info "Server address: $GVDB_SERVER_ADDR"
    echo ""

    # Setup
    check_dependencies
    generate_proto
    download_deps

    # Build tests
    log_info "Building test binaries..."
    build_test "E2E" "e2e.go" || exit 1
    build_test "Metadata" "metadata.go" || exit 1
    build_test "Load" "load.go" || exit 1
    build_test "Fuzzy" "fuzzy.go" || exit 1
    build_test "Sparse_Search" "sparse_search.go" || exit 1
    build_test "TTL" "ttl.go" || exit 1
    build_test "Auto_Index" "auto_index.go" || exit 1
    build_test "RBAC" "rbac.go" || exit 1
    if [ -n "$GVDB_S3_ENDPOINT" ]; then
        build_test "S3_Storage" "s3_storage.go" || exit 1
    fi
    log_success "All tests built"
    echo ""

    # Start server (unless running against external endpoint)
    if [ "$NO_SERVER" = "true" ]; then
        log_info "Skipping server start (NO_SERVER=true)"
    else
        start_server
    fi

    # Track test results
    TOTAL_TESTS=0
    PASSED_TESTS=0
    FAILED_TESTS=0

    # Run tests
    TESTS=(
        "E2E Test:E2E_test"
        "Metadata Filtering Test:Metadata_test"
        "Load Test:Load_test"
        "Fuzzy Test:Fuzzy_test"
        "Sparse Search Test:Sparse_Search_test"
        "TTL Test:TTL_test"
        "Auto-Index Test:Auto_Index_test"
        "RBAC Test:RBAC_test"
    )

    # Add S3 test if endpoint is configured
    if [ -n "$GVDB_S3_ENDPOINT" ]; then
        TESTS+=("S3 Storage Test:S3_Storage_test")
    fi

    for TEST in "${TESTS[@]}"; do
        TEST_NAME="${TEST%:*}"
        TEST_BIN="${TEST#*:}"

        TOTAL_TESTS=$((TOTAL_TESTS + 1))

        if run_test "$TEST_NAME" "$TEST_BIN"; then
            PASSED_TESTS=$((PASSED_TESTS + 1))
        else
            FAILED_TESTS=$((FAILED_TESTS + 1))
        fi
    done

    # Summary
    echo ""
    echo "======================================================================"
    echo "Integration Test Summary"
    echo "======================================================================"
    echo "  Total tests:  $TOTAL_TESTS"
    echo "  Passed:       $PASSED_TESTS"
    echo "  Failed:       $FAILED_TESTS"
    echo "======================================================================"

    if [ $FAILED_TESTS -eq 0 ]; then
        echo ""
        log_success "ALL INTEGRATION TESTS PASSED!"
        echo ""
        return 0
    else
        echo ""
        log_error "$FAILED_TESTS test(s) failed"
        echo ""
        return 1
    fi
}

# Run main
main
EXIT_CODE=$?

exit $EXIT_CODE
