#!/usr/bin/env bash
set -e
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"/proto
python3 -m grpc_tools.protoc -I. --python_out=.. --grpc_python_out=.. kv.proto
echo "Generated Python gRPC modules in project root (kv_pb2.py, kv_pb2_grpc.py)"
