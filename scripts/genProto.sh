#!/usr/bin/env bash
set -e
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"/proto
python3 -m grpc_tools.protoc \
    -I. \
    --python_out=../worker \
    --grpc_python_out=../worker \
    kv.proto

python3 -m grpc_tools.protoc \
    -I. \
    --python_out=../controller \
    --grpc_python_out=../controller \
    kv.proto
