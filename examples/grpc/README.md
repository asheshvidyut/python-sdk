# gRPC Transport Example for MCP

This example demonstrates how to implement a custom gRPC transport for the Model Context Protocol (MCP) Python SDK.

## Overview

- `mcp.proto`: Defines the gRPC service service mirroring the MCP protocol.
- `server.py`: Implements a gRPC server that bridges requests to the `mcp.server.Server`.
- `client.py`: A gRPC client that calls the server to list resources.

## Prerequisites

You need Python 3.10+ and the `grpcio-tools` package.

## Setup & Running

### Files

- `mcp.proto`: The Protobuf definition for the MCP Service.
- `grpc_server.py`: A reusable gRPC Servicer adapter that wraps any MCP `Server` or `FastMCP` instance.
- `server.py`: An example user application that defines tools/resources using `FastMCP` and runs them over gRPC using the adapter.
- `client.py`: A gRPC client that calls the server's methods (ListResources, ListTools, CallTool).
- `run_server.sh`: Helper script to install dependencies, generate code, and run the server.
- `run_client.sh`: Helper script to run the client.

### How to Run

1.  **Start the Server**:
    ```bash
    ./examples/grpc/run_server.sh
    ```
    This script handles virtual environment creation, dependency installation, Protobuf code generation, and starting the server on port 50051.

2.  **Run the Client** (in a new terminal):
    ```bash
    ./examples/grpc/run_client.sh
    ```
    You should see the client list resources, list tools (including the `add` tool), and successfully call the `add` tool to get the result `30`.

### Implementation Details

- **`GrpcMcpService` Adapter**: This class acts as a bridge. It accepts an `mcp.server.Server` instance. It starts the MCP server loop in the background using `anyio` memory streams.
- **Initialization Handshake**: The adapter automatically handles the MCP initialization sequence (`initialize` -> `notifications/initialized`) so the server is ready to accept requests immediately.
- **Message Mapping**: The adapter converts between gRPC messages (defined in `mcp.proto`) and the internal JSON-RPC messages used by the MCP SDK. It handles `CallTool` content type mapping (Text vs Image).
- **`FastMCP` Integration**: The example `server.py` uses `FastMCP` to define tools and resources, demonstrating how the `GrpcMcpService` adapter can wrap any `mcp.server.Server` instance.
- **Asynchronous Handling**: The adapter uses `anyio` to manage asynchronous operations, allowing it to bridge the gRPC stream interface with the MCP SDK's consume/produce stream interface. It wraps `JSONRPCRequest` objects into `SessionMessage` to be consumed by the SDK's `ServerSession`.

> **Note**: This is an experimental example.
