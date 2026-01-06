
import asyncio
import logging
from contextlib import asynccontextmanager
import anyio
import grpc
import mcp.types as types
from mcp.shared.message import SessionMessage
from mcp.client.session import ClientSession

# Import generated gRPC code
try:
    import mcp_pb2
    import mcp_pb2_grpc
except ImportError:
    import examples.grpc.mcp_pb2 as mcp_pb2
    import examples.grpc.mcp_pb2_grpc as mcp_pb2_grpc

logger = logging.getLogger("grpc_client_transport")

@asynccontextmanager
async def grpc_client_channel(channel):
    """
    A transport context manager for MCP ClientSession that runs over a gRPC channel.
    Yields (read_stream, write_stream).
    """
    stub = mcp_pb2_grpc.McpStub(channel)
    
    # Streams for ClientSession
    read_stream_writer, read_stream = anyio.create_memory_object_stream(100)
    write_stream, write_stream_reader = anyio.create_memory_object_stream(100)
    
    async def process_outgoing_messages():
        """Reads JSON-RPC messages from ClientSession and sends them via gRPC."""
        async with write_stream_reader:
            async for message in write_stream_reader:
                # message is SessionMessage -> JSONRPCMessage
                try:
                    json_msg = message.message
                    if isinstance(json_msg.root, types.JSONRPCRequest):
                        req = json_msg.root
                        logger.debug(f"Sending request: {req.method} ID: {req.id}")
                        await handle_request(req, stub, read_stream_writer)
                    elif isinstance(json_msg.root, types.JSONRPCNotification):
                        # Handle notifications (e.g. initialized)
                        logger.debug(f"Sending notification: {json_msg.root.method}")
                        # Provide dummy/no-op implementation for notifications if not critical
                        pass
                except Exception as e:
                    logger.error(f"Error processing outgoing message: {e}", exc_info=True)

    async def handle_request(req, stub, result_writer):
        """Dispatches JSON-RPC requests to appropriate gRPC methods."""
        response_result = None
        error = None
        
        try:
            if req.method == "initialize":
                # Fake initialize response because gRPC transport doesn't need handshake 
                # OR forward to server if server supports generic method (it doesn't in our proto)
                # But our GridServer expects initialization?
                # Actually, our `GrpcMcpService` adapter allows one-way bridge.
                # BUT `ClientSession` expects full protocol.
                # In this specific gRPC example, we defined Typed methods.
                # So we simulate the handshake locally or map to something?
                # The MCP SDK Client REQUIRES initialize.
                # Let's return a synthetic response.
                response_result = types.InitializeResult(
                    protocolVersion="2024-11-05",
                    capabilities=types.ServerCapabilities(),
                    serverInfo=types.Implementation(name="grpc-server", version="1.0")
                ).model_dump(by_alias=True)
            
            elif req.method == "resources/list":
                grpc_resp = await stub.ListResources(mcp_pb2.ListResourcesRequest())
                # Map gRPC response to JSON-RPC result
                resources = []
                for r in grpc_resp.resources:
                    resources.append(types.Resource(
                        uri=r.uri, name=r.name, mimeType=r.mime_type
                    ))
                response_result = types.ListResourcesResult(resources=resources).model_dump(by_alias=True)

            elif req.method == "tools/list":
                grpc_resp = await stub.ListTools(mcp_pb2.ListToolsRequest())
                tools = []
                for t in grpc_resp.tools:
                    tools.append(types.Tool(
                        name=t.name, description=t.description, inputSchema={} # Simplified
                    ))
                response_result = types.ListToolsResult(tools=tools).model_dump(by_alias=True)

            elif req.method == "tools/call":
                # Map params to gRPC
                params = req.params # dict
                name = params.get("name")
                args = params.get("arguments", {})
                
                from google.protobuf import struct_pb2
                args_struct = struct_pb2.Struct()
                args_struct.update(args)
                
                req_proto = mcp_pb2.CallToolRequest.Request(name=name, arguments=args_struct)
                
                # Consume stream
                content = []
                is_error = False
                async for chunk in stub.CallTool(mcp_pb2.CallToolRequest(request=req_proto)):
                    is_error = chunk.is_error
                    for c in chunk.content:
                        if c.HasField("text"):
                            content.append(types.TextContent(type="text", text=c.text.text))
                        elif c.HasField("image"):
                            content.append(types.ImageContent(type="image", data=c.image.data, mimeType=c.image.mime_type))
                
                response_result = types.CallToolResult(content=content, isError=is_error).model_dump(by_alias=True)

            else:
                raise ValueError(f"Method not supported by gRPC transport: {req.method}")

            # Send successful response back to ClientSession
            resp = types.JSONRPCResponse(
                jsonrpc="2.0",
                id=req.id,
                result=response_result
            )
            await result_writer.send(SessionMessage(types.JSONRPCMessage(root=resp)))

        except Exception as e:
            logger.error(f"RPC failed: {e}")
            err_resp = types.JSONRPCError(
                jsonrpc="2.0",
                id=req.id,
                error=types.ErrorData(code=-32603, message=str(e))
            )
            await result_writer.send(SessionMessage(types.JSONRPCMessage(root=err_resp)))

    async with anyio.create_task_group() as tg:
        tg.start_soon(process_outgoing_messages)
        yield read_stream, write_stream
        tg.cancel_scope.cancel()
