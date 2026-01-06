import logging
import asyncio
from concurrent import futures
import grpc
import anyio

# Import generated classes
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import mcp_pb2
import mcp_pb2_grpc

from mcp.server import Server
from mcp.types import JSONRPCMessage, JSONRPCRequest, JSONRPCResponse
from mcp.shared.message import SessionMessage
import mcp.types as types

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("grpc_mcp_server")

class GrpcMcpService(mcp_pb2_grpc.McpServicer):
    """
    Implements the Mcp gRPC service by proxying requests to an instance of mcp.server.Server.
    """
    
    def __init__(self, mcp_server: Server):
        self.server = mcp_server
        # We need a way to pump messages into the server and get responses out.
        # Since mcp.server.Server is designed to run with a transport, we need to "fake" a transport
        # for each request or maintain a persistent session?
        
        # MCP Server is usually 1:1 with a client connection (via run context manager).
        # But gRPC is stateless/multiplexed. 
        # Ideally, we should create a NEW Session for each gRPC call? No, that's expensive.
        # We need a Persistent Session.
        # For simplicity in this example, we will treat the gRPC service as a SINGLE SESSION stateful server.
        
        self.read_stream_writer, self.read_stream = anyio.create_memory_object_stream(100)
        self.write_stream, self.write_stream_reader = anyio.create_memory_object_stream(100)
        
        # We also need a way to map pending response IDs to futures, to "await" the result from the SDK
        self.pending_responses: dict[str | int, asyncio.Future] = {}
        
        # Background task to route outgoing messages from SDK to gRPC responses?
        # Wait, for unary gRPC calls, we need to wait for the SPECIFIC response.
        
        self._started = False

    async def start_background_loop(self):
        """Starts the MCP server loop in the background."""
        if self._started:
            return
        self._started = True
        
        # Start the MCP Server's run loop
        # We use a task group to manage it
        self.tg = anyio.create_task_group()
        await self.tg.__aenter__()
        
        # Start the server with our streams
        self.tg.start_soon(
            self.server.run,
            self.read_stream,
            self.write_stream,
            self.server.create_initialization_options()
        )
        
        # Start a loop to read FROM the server (responses to our proxied requests)
        self.tg.start_soon(self._process_outgoing_messages)
        
        # Initialize the MCP Server
        # We need to send an 'initialize' request
        logger.info("Initializing MCP Server...")
        init_req = types.JSONRPCRequest(
            jsonrpc="2.0",
            id="init_0",
            method="initialize",
            params=types.InitializeRequestParams(
                protocolVersion="2024-11-05", # Use a supported version
                capabilities=types.ClientCapabilities(),
                clientInfo=types.Implementation(name="grpc-gateway", version="1.0.0")
            ).model_dump(by_alias=True, exclude_none=True)
        )
        
        # Prepare future for init
        loop = asyncio.get_running_loop()
        self.init_future = loop.create_future()
        self.pending_responses["init_0"] = self.init_future
        
        await self.read_stream_writer.send(SessionMessage(types.JSONRPCMessage(root=init_req)))
        
        # Wait for initialization response
        await self.init_future
        
        # Send initialized notification
        init_notif = types.JSONRPCNotification(
            jsonrpc="2.0",
            method="notifications/initialized",
            params={}
        )
        await self.read_stream_writer.send(SessionMessage(types.JSONRPCMessage(root=init_notif)))
        logger.info("MCP Server Initialized")
        
    async def _process_outgoing_messages(self):
        """Reads messages from the SDK and fulfills pending gRPC futures."""
        logger.info("Starting outgoing message processor...")
        async with self.write_stream_reader:
            async for msg in self.write_stream_reader:
                logger.debug(f"SDK emitted message raw: {msg}")
                try:
                    # Check if it's a response to a request we sent
                    if isinstance(msg, SessionMessage) and msg.message.root.id is not None:
                         req_id = msg.message.root.id
                         logger.info(f"Processing response for ID: {req_id}")
                         if req_id in self.pending_responses:
                             self.pending_responses[req_id].set_result(msg.message.root)
                             del self.pending_responses[req_id]
                         else:
                             # It might be the init response
                             if req_id == "init_0":
                                 logger.info("Received init response")
                             else:
                                 logger.warning(f"Received response for unknown ID: {req_id}")
                    else:
                        logger.info(f"Received non-response message: {msg}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
        logger.info("Outgoing message processor channel closed")

    # --- Implement gRPC Methods ---
    
    async def ListResources(self, request, context):
        logger.info("ListResources called via gRPC")
        # 1. Construct JSON-RPC Request
        req_id = "list_resources_1" # In a real app, generate unique IDs
        
        json_rpc_req = types.JSONRPCRequest(
            jsonrpc="2.0",
            id=req_id,
            method="resources/list",
            params={} 
            # Note: We are ignoring 'cursor' and other fields for this simple example
        )
        
        # 2. Setup Future
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self.pending_responses[req_id] = fut
        logger.info(f"Sending JSON-RPC request: {req_id}")
        
        # 3. Send to SDK
        # Wrap in JSONRPCMessage (RootModel)
        msg_wrapper = types.JSONRPCMessage(root=json_rpc_req)
        await self.read_stream_writer.send(SessionMessage(msg_wrapper))
        
        try:
            response = await asyncio.wait_for(fut, timeout=10.0)
        except asyncio.TimeoutError:
            await context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, "Timeout waiting for MCP SDK")
            return mcp_pb2.ListResourcesResponse()
            
        # 5. Convert JSON-RPC Response to gRPC Response
        try:
            # response.result is a dict
            result = response.result
            logger.info(f"Got result from SDK: {result}")
            
            # Map 'resources' list
            resources_pb = []
            for res in result.get("resources", []):
                resources_pb.append(mcp_pb2.Resource(
                    uri=res.get("uri"),
                    name=res.get("name", ""),
                    description=res.get("description", ""),
                    mime_type=res.get("mimeType", "")
                ))
            
            resp = mcp_pb2.ListResourcesResponse(
                resources=resources_pb,
                common=mcp_pb2.ResponseFields()
            )
            logger.info(f"Returning gRPC response: {resp}")
            return resp
        except Exception as e:
            logger.error(f"Error constructing response: {e}", exc_info=True)
            raise

    # Implement other methods similarly...
    # For brevity, I only implemented ListResources to prove the point.

async def serve():
    # 1. Setup MCP Server
    mcp_server = Server("grpc-example-server")
    
    @mcp_server.list_resources()
    async def list_resources():
        return [
            types.Resource(
                uri="file:///example.txt",
                name="Example Resource",
                mimeType="text/plain"
            )
        ]

    # 2. Setup gRPC Service
    service = GrpcMcpService(mcp_server)
    
    # 3. Start gRPC Server
    server = grpc.aio.server()
    mcp_pb2_grpc.add_McpServicer_to_server(service, server)
    server.add_insecure_port('0.0.0.0:50051')
    
    # Needs to start the MCP loop inside the event loop
    await service.start_background_loop()
    
    logger.info("Starting gRPC server on port 50051...")
    await server.start()
    await server.wait_for_termination()

if __name__ == '__main__':
    # Use anyio.run to ensure compatible event loop
    try:
        anyio.run(serve)
    except KeyboardInterrupt:
        pass
