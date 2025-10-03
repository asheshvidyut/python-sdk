"""
gRPC server transport for MCP.

This module provides a gRPC transport for MCP servers.
"""

import asyncio
import logging
from typing import AsyncIterator, Sequence

from google.protobuf import json_format
import grpc
from grpc import aio
from grpc_reflection.v1alpha import reflection
from mcp import types
from mcp.proto import mcp_pb2
from mcp.proto import mcp_pb2_grpc
from mcp.server.grpc_session import GrpcSession
from mcp.server.lowlevel.server import RequestContext
from mcp.shared import convert


logger = logging.getLogger(__name__)


class McpServicer(mcp_pb2_grpc.McpServicer):
    """gRPC servicer for MCP protocol."""

    def __init__(self, mcp_server):
        self.mcp_server = mcp_server

    async def ListTools(self, request, context):
        """List tools."""
        try:
            tools = await self.mcp_server.list_tools()
            tool_protos = convert.tool_types_to_protos(tools)

            return mcp_pb2.ListToolsResponse(
                common=mcp_pb2.ResponseFields(protocol_version=mcp_pb2.VERSION_20250326),
                tools=tool_protos,
            )
        except json_format.ParseError as e:
            error_message = f"Failed to parse tool data: {e}"
            logger.error("Error during ListTools: %s", error_message, exc_info=True)
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, error_message)
        except Exception as e:
            logger.error("Error during ListTools: %s", e, exc_info=True)
            # Send an INTERNAL error back to the client
            await context.abort(grpc.StatusCode.INTERNAL, f"An internal error occurred: {e}")

    async def tool_runner(
        self,
        request_iterator: AsyncIterator[mcp_pb2.CallToolRequest],
        response_queue: asyncio.Queue[mcp_pb2.CallToolResponse],
        context: aio.ServicerContext,
    ):
        """Runs the tool and puts the final result on the queue."""
        tool_name = None
        try:
            request = await request_iterator.__anext__()
            if not request.HasField("request"):
                raise grpc.RpcError(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "Initial request cannot be empty.",
                )

            tool_name = request.request.name
            arguments = json_format.MessageToDict(request.request.arguments)

            progress_token = None
            if request.common.HasField("progress") and request.common.progress.progress_token:
                progress_token = request.common.progress.progress_token
            logger.info("Progress token from request: %s", progress_token)

            req_context = RequestContext(
                request_id=progress_token,
                meta=types.RequestParams.Meta(progressToken=progress_token),
                session=GrpcSession(response_queue),
                lifespan_context=context,
            )

            logger.info("Calling tool '%s' with arguments: %s", tool_name, arguments)
            result = await self.mcp_server.call_tool(tool_name, arguments, request_context=req_context)

            if isinstance(result, list) or isinstance(result, tuple):
                for iter_res in result:
                    result_protos = convert.tool_output_to_proto(iter_res)
                    for res in result_protos:
                        await response_queue.put(mcp_pb2.CallToolResponse(result=res))
            else:
                result_protos = convert.tool_output_to_proto(result)
                for res in result_protos:
                    await response_queue.put(mcp_pb2.CallToolResponse(result=res))

        except Exception as e:
            logger.error("Error during tool call: %s", e, exc_info=True)
            # Create a CallToolResult for the error
            result = mcp_pb2.CallToolResponse.Result(
                is_error=True, text=mcp_pb2.TextContent(text=f"Error executing tool {tool_name}: {e}")
            )
            await response_queue.put(mcp_pb2.CallToolResponse(result=result))
        finally:
            await response_queue.put(None)

    async def CallTool(self, request_iterator, context):
        """Call a tool."""

        response_queue = asyncio.Queue()

        tool_task = asyncio.create_task(self.tool_runner(request_iterator, response_queue, context))
        try:
            while True:
                item = await response_queue.get()
                if item is None:
                    break
                if isinstance(item, Exception):
                    logger.error("Error during tool call: %s", item, exc_info=True)
                    continue
                yield item
        except asyncio.CancelledError:
            logger.info("CallTool stream cancelled by client.")
            tool_task.cancel()
            try:
                await tool_task
            except asyncio.CancelledError:
                logger.info("Tool runner task cancelled successfully.")
        finally:
            # Ensure task is cancelled if loop breaks for other reasons or finishes
            if not tool_task.done():
                tool_task.cancel()
                try:
                    await tool_task
                except asyncio.CancelledError:
                    logger.info("Tool runner task cancelled successfully in finally block.")


def _enable_grpc_reflection(server: grpc.Server) -> None:
    """Enables gRPC reflection on the given server."""
    logger.info("gRPC reflection enabled")
    service_names = (
        mcp_pb2.DESCRIPTOR.services_by_name["Mcp"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(service_names, server)


def attach_mcp_server_to_grpc_server(
    mcp_server,  # This is the FastMCP server
    server: grpc.Server,
) -> None:
    """Attach a MCP server to a gRPC server."""
    # Create servicer and add to server
    servicer = McpServicer(mcp_server)
    mcp_pb2_grpc.add_McpServicer_to_server(servicer, server)

    # Enable gRPC reflection
    if mcp_server.settings.grpc_enable_reflection:
        _enable_grpc_reflection(server)


async def create_mcp_grpc_server(
    mcp_server,
    target: str = "127.0.0.1:50051",
) -> aio.Server:
    """Create a simple gRPC server for MCP.

    Args:
        mcp_server: The MCP server instance to handle requests
        target: The target address for the gRPC server.

    Returns:
        Configured gRPC server ready to serve
    """
    server = aio.server(
        migration_thread_pool=mcp_server.settings.grpc_migration_thread_pool,
        handlers=mcp_server.settings.grpc_handlers,
        interceptors=mcp_server.settings.grpc_interceptors,
        options=mcp_server.settings.grpc_options,
        maximum_concurrent_rpcs=mcp_server.settings.grpc_maximum_concurrent_rpcs,
        compression=mcp_server.settings.grpc_compression,
    )

    attach_mcp_server_to_grpc_server(mcp_server, server)

    # Configure server port
    if mcp_server.settings.grpc_credentials:
        server.add_secure_port(target, mcp_server.settings.grpc_credentials)
    else:
        server.add_insecure_port(target)

    # Start gRPC server
    await server.start()
    logger.info("gRPC server started on %s", target)
    return server
