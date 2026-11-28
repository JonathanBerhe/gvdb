#!/usr/bin/env bash
# Generates every checked-in protobuf stub from proto/*.proto:
#
#   clients/python/gvdb/pb     Python SDK (vectordb.proto), with grpcio-tools
#                              from clients/python/uv.lock
#   operator/internal/gvdbpb   Go, package gvdbpb (vectordb.proto, internal.proto)
#
# CMake generates the C++ code at build time, so it is not here.
#
# The Go output depends on the versions of protoc, protoc-gen-go and
# protoc-gen-go-grpc, so the script runs only with the versions in
# tools/proto/versions.env. CI runs it and fails when a stub differs.
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
# shellcheck source=versions.env
source "$root/tools/proto/versions.env"

fail_version() {
  echo "tools/proto/generate.sh: $1 is '$2', but tools/proto/versions.env pins '$3'." >&2
  echo "Install the pinned generators:" >&2
  echo "  protoc $PROTOC_VERSION from https://github.com/protocolbuffers/protobuf/releases/tag/v$PROTOC_VERSION" >&2
  echo "  go install google.golang.org/protobuf/cmd/protoc-gen-go@v$PROTOC_GEN_GO_VERSION" >&2
  echo "  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v$PROTOC_GEN_GO_GRPC_VERSION" >&2
  exit 1
}

have=$(protoc --version 2>/dev/null | awk '{print $2}') || true
[[ "$have" == "$PROTOC_VERSION" ]] || fail_version protoc "${have:-missing}" "$PROTOC_VERSION"
have=$(protoc-gen-go --version 2>/dev/null | awk '{print $2}' | sed 's/^v//') || true
[[ "$have" == "$PROTOC_GEN_GO_VERSION" ]] || fail_version protoc-gen-go "${have:-missing}" "$PROTOC_GEN_GO_VERSION"
have=$(protoc-gen-go-grpc --version 2>/dev/null | awk '{print $2}' | sed 's/^v//') || true
[[ "$have" == "$PROTOC_GEN_GO_GRPC_VERSION" ]] || fail_version protoc-gen-go-grpc "${have:-missing}" "$PROTOC_GEN_GO_GRPC_VERSION"

cd "$root/proto"

# Go stubs for the operator: both files in one package.
operator_pkg='gvdb/operator/internal/gvdbpb;gvdbpb'
protoc -I . \
  --go_out="$root/operator/internal/gvdbpb" --go_opt=paths=source_relative \
  --go_opt="Mvectordb.proto=$operator_pkg" --go_opt="Minternal.proto=$operator_pkg" \
  --go-grpc_out="$root/operator/internal/gvdbpb" --go-grpc_opt=paths=source_relative \
  --go-grpc_opt="Mvectordb.proto=$operator_pkg" --go-grpc_opt="Minternal.proto=$operator_pkg" \
  vectordb.proto internal.proto

# Python stubs for the SDK. The include path maps proto/ to gvdb/pb, so the
# generated code imports "from gvdb.pb import vectordb_pb2".
cd "$root/clients/python"
uv run --frozen python3 -m grpc_tools.protoc \
  -Igvdb/pb="$root/proto" \
  --python_out=. --grpc_python_out=. \
  gvdb/pb/vectordb.proto

echo "Generated the Python and operator stubs from proto/."
