
"""
This is an example of how a USER would write a FastMCP server and expose it over gRPC
using the pre-built 'GrpcMcpService' adapter components.
"""
import logging
import anyio
import grpc
from mcp.server.fastmcp import FastMCP
from examples.grpc.grpc_server import GrpcMcpService
import examples.grpc.mcp_pb2_grpc as mcp_pb2_grpc

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("user_server")

# 1. Define your FastMCP Server (Business Logic)
mcp = FastMCP("my-fastmcp-app")

@mcp.tool()
def add(a: int, b: int) -> int:
    """Adds two numbers"""
    return a + b

@mcp.resource("file:///example.txt")
def get_example() -> str:
    return "This is an example resource"

# 2. Infrastructure Setup (Boilerplate to run over gRPC)
async def serve():
    # Wrap the internal Low Level Server with the gRPC Adapter
    # Note: FastMCP wraps the low-level server in ._mcp_server
    service = GrpcMcpService(mcp._mcp_server)
    
    # Standard gRPC Server setup
    server = grpc.aio.server()
    mcp_pb2_grpc.add_McpServicer_to_server(service, server)
    server.add_insecure_port('0.0.0.0:50051')
    
    # Start the MCP background loop
    await service.start_background_loop()
    
    logger.info("Starting gRPC server on 0.0.0.0:50051")
    await server.start()
    await server.wait_for_termination()

if __name__ == '__main__':
    try:
        anyio.run(serve)
    except KeyboardInterrupt:
        pass
