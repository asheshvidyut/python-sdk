#!/bin/bash
set -e
cd "$(dirname "$0")/../.."

# Ensure .agent_venv exists
if [ ! -d ".agent_venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .agent_venv
    source .agent_venv/bin/activate
    pip install grpcio-tools anyio pydantic starlette sse-starlette httpx click pydantic-settings jsonschema httpx-sse
    pip install -e .
else
    source .agent_venv/bin/activate
fi

# Regenerate protos (optional but good practice)
python -m grpc_tools.protoc -I examples/grpc --python_out=examples/grpc --grpc_python_out=examples/grpc examples/grpc/mcp.proto

echo "Starting gRPC Server on 0.0.0.0:50051..."
PYTHONPATH=src python examples/grpc/server.py
