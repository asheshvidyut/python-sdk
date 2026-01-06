#!/bin/bash
set -e
cd "$(dirname "$0")/../.."

# Ensure .agent_venv exists or use it
if [ ! -d ".agent_venv" ]; then
    echo "Please run ./examples/grpc/run_server.sh first to set up the environment."
    exit 1
fi

source .agent_venv/bin/activate

echo "Running gRPC Client..."
PYTHONPATH=src python examples/grpc/client.py
