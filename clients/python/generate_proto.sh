#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PROTO_DIR="$PROJECT_ROOT/proto"
OUTPUT_DIR="$SCRIPT_DIR/gvdb/pb"

echo "Generating Python gRPC stubs..."
echo "  Proto dir: $PROTO_DIR"
echo "  Output dir: $OUTPUT_DIR"

cd "$SCRIPT_DIR"
uv run python3 -m grpc_tools.protoc \
    -I"$PROTO_DIR" \
    --python_out="$OUTPUT_DIR" \
    --grpc_python_out="$OUTPUT_DIR" \
    "$PROTO_DIR/vectordb.proto"

# Fix import: grpc_tools.protoc emits "import vectordb_pb2 as vectordb__pb2"
# but the SDK package requires "from gvdb.pb import vectordb_pb2 as vectordb__pb2"
sed -i.bak \
    's/^import vectordb_pb2 as vectordb__pb2$/from gvdb.pb import vectordb_pb2 as vectordb__pb2/' \
    "$OUTPUT_DIR/vectordb_pb2_grpc.py"
rm -f "$OUTPUT_DIR/vectordb_pb2_grpc.py.bak"

echo "✅ Python stubs generated in $OUTPUT_DIR"
