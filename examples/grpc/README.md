# gRPC Transport Example for MCP

This example demonstrates how to implement a custom gRPC transport for the Model Context Protocol (MCP) Python SDK.

## Overview

- `mcp.proto`: Defines the gRPC service service mirroring the MCP protocol.
- `server.py`: Implements a gRPC server that bridges requests to the `mcp.server.Server`.
- `client.py`: A gRPC client that calls the server to list resources.

## Prerequisites

You need Python 3.10+ and the `grpcio-tools` package.

## Setup & Running

We have provided helper scripts to run the example.

### 1. Run the Server

```bash
chmod +x examples/grpc/run_server.sh
./examples/grpc/run_server.sh
```

This will:
- Create a virtual environment `.agent_venv` (if not exists).
- Install dependencies.
- Install the `mcp` package in editable mode.
- Generate gRPC Python code.
- Start the server on port 50051.

### 2. Run the Client

Open a new terminal and run:

```bash
chmod +x examples/grpc/run_client.sh
./examples/grpc/run_client.sh
```

## Implementation Details

The `server.py` uses `anyio` memory streams to bridge the synchronous/async gRPC methods to the MCP SDK's consume/produce stream interface. It wraps `JSONRPCRequest` objects into `SessionMessage` to be consumed by the SDK's `ServerSession`.

> **Note**: This is an experimental example.
