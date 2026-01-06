
import asyncio
import logging
import grpc
import anyio
import mcp.types as types
from mcp.shared.message import SessionMessage
from mcp.server import Server

# Import generated gRPC code
# Assuming these are in the same directory or PYTHONPATH
try:
    import mcp_pb2
    import mcp_pb2_grpc
except ImportError:
    # Fallback for when running directly
    import examples.grpc.mcp_pb2 as mcp_pb2
    import examples.grpc.mcp_pb2_grpc as mcp_pb2_grpc

logger = logging.getLogger("grpc_mcp_server")

class GrpcMcpService(mcp_pb2_grpc.McpServicer):
    """
    Adapts gRPC requests to an existing MCP Server instance.
    """
    def __init__(self, mcp_server: Server):
        self.mcp_server = mcp_server
        # Create memory streams for bridging gRPC -> MCP
        self.read_stream_writer, self.read_stream = anyio.create_memory_object_stream(100)
        self.write_stream, self.write_stream_reader = anyio.create_memory_object_stream(100)
        self.pending_responses = {}

    async def start_background_loop(self):
        """Starts the MCP server loop in the background."""
        logger.info("Initializing MCP Server...")
        init_options = self.mcp_server.create_initialization_options()
        
        async def run_mcp():
            try:
                await self.mcp_server.run(
                    self.read_stream,
                    self.write_stream,
                    init_options
                )
            except Exception as e:
                logger.error(f"MCP Server loop failed: {e}", exc_info=True)

        async def process_outgoing_messages():
            logger.info("Starting outgoing message processor...")
            async with self.write_stream_reader:
                async for message in self.write_stream_reader:
                    # Message type: SessionMessage
                    # Contains: JSONRPCMessage (Request, Response, Notification)
                    try:
                        json_rpc_message = message.message
                        logger.debug(f"SDK emitted message raw: {message}")
                        
                        # Use the Pydantic model to identify message type
                        if hasattr(json_rpc_message.root, 'id'):
                            msg_id = json_rpc_message.root.id
                            if msg_id in self.pending_responses:
                                logger.info(f"Processing response for ID: {msg_id}")
                                fut = self.pending_responses.pop(msg_id)
                                if not fut.done():
                                    fut.set_result(json_rpc_message.root)
                            else:
                                logger.warning(f"Received response for unknown ID: {msg_id}")
                        else:
                            # Notifications
                            logger.debug(f"Received notification: {json_rpc_message}")

                    except Exception as e:
                       logger.error(f"Error processing outgoing message: {e}", exc_info=True)
            logger.info("Outgoing message processor channel closed")

        # Start tasks
        loop = asyncio.get_running_loop()
        loop.create_task(run_mcp())
        loop.create_task(process_outgoing_messages())
        
        # Perform Initialization Handshake
        await self._initialize_mcp_session()

    async def _initialize_mcp_session(self):
        """Performs the MCP initialization handshake (JSON-RPC over MemoryStream)."""
        req_id = "init_0"
        init_req = types.JSONRPCRequest(
            jsonrpc="2.0",
            id=req_id,
            method="initialize",
            params=types.InitializeRequestParams(
                protocolVersion="2024-11-05",
                capabilities=types.ClientCapabilities(
                    experimental={},
                    sampling={}
                ),
                clientInfo=types.Implementation(name="grpc-adapter", version="1.0.0")
            ).model_dump(by_alias=True)
        )
        
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self.pending_responses[req_id] = fut
        
        # Send Initialize Request
        await self.read_stream_writer.send(SessionMessage(types.JSONRPCMessage(root=init_req)))
        
        # Wait for Initialize Response
        await fut 
        
        # Send Initialized Notification
        notif = types.JSONRPCNotification(
            jsonrpc="2.0",
            method="notifications/initialized",
            params=types.NotificationParams().model_dump(by_alias=True)
        )
        await self.read_stream_writer.send(SessionMessage(types.JSONRPCMessage(root=notif)))
        logger.info("MCP Server Initialized")

    # --- gRPC Handlers ---

    async def ListResources(self, request, context):
        return await self._unary_call(
            request, context, "resources/list", mcp_pb2.ListResourcesResponse, "resources", self._map_resource
        )

    async def ListTools(self, request, context):
        return await self._unary_call(
            request, context, "tools/list", mcp_pb2.ListToolsResponse, "tools", self._map_tool
        )

    async def CallTool(self, request, context):
        import google.protobuf.json_format
        
        # Access nested 'request' message
        tool_req = request.request
        arguments = google.protobuf.json_format.MessageToDict(tool_req.arguments)
        
        req_id = f"call_tool_{id(request)}"
        params = types.CallToolRequestParams(name=tool_req.name, arguments=arguments).model_dump(by_alias=True)
        
        response = await self._send_request(req_id, "tools/call", params, context)
        if not response: return # Error already handled
        
        try:
            result = response.result
            content_list = []
            for c in result.get("content", []):
               c_type = c.get("type")
               if c_type == "text":
                   content_list.append(mcp_pb2.CallToolResponse.Content(
                       text=mcp_pb2.TextContent(text=c.get("text", ""))
                   ))
               elif c_type == "image":
                   content_list.append(mcp_pb2.CallToolResponse.Content(
                       image=mcp_pb2.ImageContent(
                           data=c.get("data", b""),
                           mime_type=c.get("mimeType", "")
                       )
                   ))
               # Add other types as needed
            yield mcp_pb2.CallToolResponse(content=content_list, is_error=result.get("isError", False))
        except Exception as e:
            logger.error(f"Error processing CallTool result: {e}", exc_info=True)
            await context.abort(grpc.StatusCode.INTERNAL, str(e))

    # --- Helper Methods ---

    async def _unary_call(self, request, context, method, response_cls, list_key, map_func):
        req_id = f"{method.replace('/', '_')}_{id(request)}"
        response = await self._send_request(req_id, method, {}, context)
        if not response: return response_cls()

        try:
            result = response.result
            items_pb = [map_func(item) for item in result.get(list_key, [])]
            return response_cls(**{list_key: items_pb}, common=mcp_pb2.ResponseFields())
        except Exception as e:
            logger.error(f"Error mapping {method} response: {e}", exc_info=True)
            await context.abort(grpc.StatusCode.INTERNAL, str(e))
            raise

    async def _send_request(self, req_id, method, params, context):
        json_rpc_req = types.JSONRPCRequest(jsonrpc="2.0", id=req_id, method=method, params=params)
        
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self.pending_responses[req_id] = fut
        
        await self.read_stream_writer.send(SessionMessage(types.JSONRPCMessage(root=json_rpc_req)))
        
        try:
            return await asyncio.wait_for(fut, timeout=10.0)
        except asyncio.TimeoutError:
            await context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, "Timeout waiting for MCP SDK")
            return None

    def _map_resource(self, res):
        return mcp_pb2.Resource(
            uri=res.get("uri"),
            name=res.get("name", ""),
            description=res.get("description", ""),
            mime_type=res.get("mimeType", "")
        )

    def _map_tool(self, tool):
        return mcp_pb2.Tool(
            name=tool.get("name"),
            description=tool.get("description", "")
        )
