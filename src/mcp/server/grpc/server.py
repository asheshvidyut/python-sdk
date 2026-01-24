"""
gRPC server transport for MCP.

This module implements the server-side gRPC transport for MCP, allowing
an MCP server to be exposed over gRPC with support for native streaming
and bidirectional communication.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import uuid
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from fnmatch import fnmatch
from typing import Any, TypeVar

import grpc
from google.protobuf import struct_pb2
from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from pydantic import AnyUrl

import mcp.types as types
from mcp.server.lowlevel.server import Server, request_ctx
from mcp.server.transport_session import ServerTransportSession
from mcp.shared.context import RequestContext
from mcp.v1.mcp_pb2 import (
    CallToolResponse,
    CallToolWithProgressResponse,
    CompleteResponse,
    CompletionResult,
    Content,
    GetPromptResponse,
    ImageContent,
    InitializeResponse,
    ListPromptsResponse,
    ListResourcesResponse,
    ListResourceTemplatesResponse,
    ListToolsResponse,
    PingResponse,
    ProgressNotification,
    ProgressToken,
    Prompt,
    PromptArgument,
    PromptMessage,
    ReadResourceChunkedResponse,
    ReadResourceResponse,
    Resource,
    ResourceChangeType,
    ResourceContents,
    ResourceTemplate,
    Role,
    ServerCapabilities,
    ServerInfo,
    SessionResponse,
    StreamEnd,
    StreamError,
    StreamPromptCompletionResponse,
    StreamToolCallsResponse,
    TextContent,
    Tool,
    ToolResult,
    WatchResourcesResponse,
)
from mcp.v1.mcp_pb2_grpc import McpServiceServicer, add_McpServiceServicer_to_server

logger = logging.getLogger(__name__)

PROGRESS_POLL_TIMEOUT_SECONDS = 0.1
WATCH_RESOURCES_POLL_TIMEOUT_SECONDS = 1.0

LifespanResultT = TypeVar("LifespanResultT")
RequestT = TypeVar("RequestT")


class GrpcServerSession(ServerTransportSession):
    """
    gRPC implementation of ServerTransportSession.

    This session implementation handles the context for gRPC requests,
    bridging the gap between the abstract ServerSession interface and
    gRPC's execution model.
    """

    def __init__(self) -> None:
        # In gRPC, we don't manage the stream lifecycle the same way as
        # the persistent connection in stdio/SSE, as many RPCs are unary.
        # This session object acts primarily as a handle for capabilities.
        self._client_params: types.InitializeRequestParams | None = None
        self._resource_watchers: list[tuple[list[str], asyncio.Queue[WatchResourcesResponse]]] = []

    @property
    def client_params(self) -> types.InitializeRequestParams | None:
        return self._client_params

    def check_client_capability(self, capability: types.ClientCapabilities) -> bool:
        """Check if the client supports a specific capability."""
        if self._client_params is None:
            return False

        # Get client capabilities from initialization params
        client_caps = self._client_params.capabilities

        # Check each specified capability in the passed in capability object
        if capability.roots is not None:
            if client_caps.roots is None:
                return False
            if capability.roots.listChanged and not client_caps.roots.listChanged:
                return False

        if capability.sampling is not None:
            if client_caps.sampling is None:
                return False

        if capability.elicitation is not None:
            if client_caps.elicitation is None:
                return False

        if capability.experimental is not None:
            if client_caps.experimental is None:
                return False
            # Check each experimental capability
            for exp_key, exp_value in capability.experimental.items():
                if exp_key not in client_caps.experimental or client_caps.experimental[exp_key] != exp_value:
                    return False

        return True

    async def send_log_message(
        self,
        level: types.LoggingLevel,
        data: Any,
        logger_name: str | None = None,
        related_request_id: types.RequestId | None = None,
    ) -> None:
        # For unary RPCs, we can't push log messages back easily unless
        # we are in the Session bidirectional stream.
        # TODO: Implement side-channel logging for Session stream
        logger.warning("Log message dropped (not implemented for unary gRPC): %s: %s", level, data)

    async def send_progress_notification(
        self,
        progress_token: str | int,
        progress: float,
        total: float | None = None,
        message: str | None = None,
        related_request_id: str | None = None,
    ) -> None:
        # This is handled by specific streaming RPCs (CallToolWithProgress)
        # or the Session stream. If called from a unary context, we log warning.
        logger.warning("Progress notification dropped (not implemented for unary gRPC): %s", progress)

    async def send_resource_updated(self, uri: AnyUrl) -> None:
        if not self._resource_watchers:
            return

        timestamp = Timestamp()
        timestamp.FromDatetime(datetime.now(timezone.utc))
        uri_value = str(uri)

        notification = WatchResourcesResponse(
            uri=uri_value,
            change_type=ResourceChangeType.RESOURCE_CHANGE_TYPE_MODIFIED,
            timestamp=timestamp,
        )
        for patterns, queue in list(self._resource_watchers):
            if any(fnmatch(uri_value, pattern) for pattern in patterns):
                await queue.put(notification)

    async def send_resource_list_changed(self) -> None:
        logger.warning("Resource list changed notification dropped (not implemented for unary gRPC)")

    async def send_tool_list_changed(self) -> None:
        logger.warning("Tool list changed notification dropped (not implemented for unary gRPC)")

    async def send_prompt_list_changed(self) -> None:
        logger.warning("Prompt list changed notification dropped (not implemented for unary gRPC)")

    async def list_roots(self) -> types.ListRootsResult:
        logger.warning("List roots request dropped (not implemented for unary gRPC)")
        return types.ListRootsResult(roots=[])

    async def elicit(
        self,
        message: str,
        requested_schema: types.ElicitRequestedSchema,
        related_request_id: types.RequestId | None = None,
    ) -> types.ElicitResult:
        raise NotImplementedError("Elicitation not implemented for unary gRPC")

    async def send_ping(self) -> types.EmptyResult:
        logger.warning("Ping request dropped (not implemented for unary gRPC)")
        return types.EmptyResult()

    def register_resource_watch(
        self,
        patterns: list[str],
    ) -> asyncio.Queue[WatchResourcesResponse]:
        queue: asyncio.Queue[WatchResourcesResponse] = asyncio.Queue()
        self._resource_watchers.append((patterns, queue))
        return queue

    def unregister_resource_watch(
        self,
        queue: asyncio.Queue[WatchResourcesResponse],
    ) -> None:
        self._resource_watchers = [(patterns, q) for patterns, q in self._resource_watchers if q is not queue]


class McpGrpcServicer(McpServiceServicer):
    """
    Implements the McpService gRPC definition by delegating to an MCP Server instance.
    """

    def __init__(self, server: Server[Any, Any]):
        self._server = server
        self._session = GrpcServerSession()
        self._peer_sessions: dict[str, GrpcServerSession] = {}

    def _get_peer_session(self, context: grpc.ServicerContext) -> GrpcServerSession:
        peer = context.peer() or "unknown"
        session = self._peer_sessions.get(peer)
        if session is None:
            session = GrpcServerSession()
            self._peer_sessions[peer] = session
        return session

    # -------------------------------------------------------------------------
    # Type Conversion Helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _dict_to_struct(d: dict[str, Any] | None) -> struct_pb2.Struct:
        """Convert a Python dict to protobuf Struct."""
        struct = struct_pb2.Struct()
        if d:
            struct.update(d)
        return struct

    @staticmethod
    def _struct_to_dict(struct: struct_pb2.Struct) -> dict[str, Any]:
        """Convert protobuf Struct to Python dict."""
        return MessageToDict(struct)

    def _convert_content_to_proto(
        self, content: types.TextContent | types.ImageContent | types.EmbeddedResource
    ) -> Any:
        """Convert MCP Content to proto Content."""
        if isinstance(content, types.TextContent):
            return Content(text=TextContent(text=content.text))
        elif isinstance(content, types.ImageContent):
            return Content(image=ImageContent(data=content.data, mime_type=content.mimeType))
        # TODO: Handle EmbeddedResource
        return Content()

    def _convert_tool_to_proto(self, tool: types.Tool) -> Any:
        """Convert MCP Tool to proto Tool."""
        return Tool(
            name=tool.name, description=tool.description or "", input_schema=self._dict_to_struct(tool.inputSchema)
        )

    def _convert_resource_to_proto(self, resource: types.Resource) -> Any:
        """Convert MCP Resource to proto Resource."""
        return Resource(
            uri=str(resource.uri),
            name=resource.name,
            description=resource.description or "",
            mime_type=resource.mimeType or "",
        )

    def _convert_prompt_to_proto(self, prompt: types.Prompt) -> Any:
        """Convert MCP Prompt to proto Prompt."""
        return Prompt(
            name=prompt.name,
            description=prompt.description or "",
            arguments=[
                PromptArgument(name=arg.name, description=arg.description or "", required=arg.required or False)
                for arg in (prompt.arguments or [])
            ],
        )

    @staticmethod
    def _make_session_response(in_reply_to: str, **kwargs: Any) -> SessionResponse:
        return SessionResponse(
            message_id=str(uuid.uuid4()),
            in_reply_to=in_reply_to,
            **kwargs,
        )

    async def _emit_stream_error(
        self,
        queue: asyncio.Queue[SessionResponse],
        in_reply_to: str,
        *,
        code: int,
        message: str,
    ) -> None:
        await queue.put(
            self._make_session_response(
                in_reply_to=in_reply_to,
                stream_error=StreamError(code=code, message=message),
            )
        )

    async def _session_emit(
        self,
        queue: asyncio.Queue[SessionResponse],
        request_id: str,
        **payload: Any,
    ) -> None:
        await queue.put(
            self._make_session_response(
                in_reply_to=request_id,
                **payload,
            )
        )

    async def _iter_prompt_completion_chunks(
        self,
        name: str,
        arguments: dict[str, str],
    ) -> AsyncIterator[tuple[str, bool, str | None]]:
        handler = getattr(self._server, "_stream_prompt_completion_handler", None)
        if handler is None:
            raise NotImplementedError("StreamPromptCompletion handler not registered")

        result = handler(name, arguments)
        if asyncio.iscoroutine(result):
            result = await result

        async def iter_chunks() -> AsyncIterator[Any]:
            if hasattr(result, "__aiter__"):
                async for item in result:  # type: ignore[assignment]
                    yield item
            else:
                for item in result:  # type: ignore[assignment]
                    yield item

        async for chunk in iter_chunks():
            normalized = types.StreamPromptCompletionChunk.coerce(chunk)
            yield normalized.token, normalized.isFinal, normalized.finishReason

    def _convert_request_meta(self, meta: Any | None) -> types.RequestParams.Meta | None:
        if meta is None:
            return None
        data = {
            "trace_id": getattr(meta, "trace_id", None),
            "correlation_id": getattr(meta, "correlation_id", None),
        }
        extra = dict(getattr(meta, "extra", {}))
        data = {k: v for k, v in data.items() if v}
        if not data and not extra:
            return None
        return types.RequestParams.Meta(progressToken=None, **data, **extra)

    async def _execute_handler(
        self,
        request_type: type,
        request_obj: Any,
        context: grpc.ServicerContext,
        *,
        meta: types.RequestParams.Meta | None = None,
        session: GrpcServerSession | None = None,
    ) -> Any:
        """
        Execute a registered handler for the given request type.
        Sets up the request context needed by the handler.
        """
        handler = self._server.request_handlers.get(request_type)
        if not handler:
            await context.abort(grpc.StatusCode.UNIMPLEMENTED, f"Method {request_type.__name__} not implemented")
        else:
            active_session = session or self._get_peer_session(context)
            # Set up request context
            # We use a unique ID for each request
            token = request_ctx.set(
                RequestContext(
                    request_id=str(uuid.uuid4()),
                    meta=meta,
                    session=active_session,
                    lifespan_context={},
                )
            )

            try:
                result = await handler(request_obj)
                return result
            except Exception as e:
                logger.exception("Error handling gRPC request")
                await context.abort(grpc.StatusCode.INTERNAL, str(e))
            finally:
                request_ctx.reset(token)

    async def _execute_handler_for_session(
        self,
        request_type: type,
        request_obj: Any,
        *,
        request_id: str,
        meta: types.RequestParams.Meta | None = None,
        session: GrpcServerSession,
    ) -> Any:
        handler = self._server.request_handlers.get(request_type)
        if not handler:
            raise NotImplementedError(f"Method {request_type.__name__} not implemented")

        token = request_ctx.set(
            RequestContext(
                request_id=request_id,
                meta=meta,
                session=session,
                lifespan_context={},
            )
        )
        try:
            return await handler(request_obj)
        finally:
            request_ctx.reset(token)

    # -------------------------------------------------------------------------
    # RPC Implementations
    # -------------------------------------------------------------------------

    async def Initialize(self, request, context):
        """Initialize the session."""

        # Populate session with client params
        roots_cap = None
        if request.capabilities.HasField("roots"):
            roots_cap = types.RootsCapability(listChanged=request.capabilities.roots.list_changed)

        sampling_cap = None
        if request.capabilities.HasField("sampling"):
            sampling_cap = types.SamplingCapability()

        experimental_cap = None
        if request.capabilities.HasField("experimental"):
            experimental_cap = dict(request.capabilities.experimental.capabilities.items())

        session = self._get_peer_session(context)
        session._client_params = types.InitializeRequestParams(
            protocolVersion=request.protocol_version,
            capabilities=types.ClientCapabilities(
                roots=roots_cap,
                sampling=sampling_cap,
                experimental=experimental_cap,
            ),
            clientInfo=types.Implementation(
                name=request.client_info.name,
                version=request.client_info.version,
            ),
        )

        # Convert proto to internal options
        # We manually construct what the server expects for initialization
        # The Server.create_initialization_options normally takes internal types
        # Here we are just bridging the handshake

        init_opts = self._server.create_initialization_options()

        # Convert internal ServerCapabilities to proto ServerCapabilities
        caps = ServerCapabilities()
        if init_opts.capabilities.prompts:
            caps.prompts.list_changed = init_opts.capabilities.prompts.listChanged or False
        if init_opts.capabilities.resources:
            caps.resources.subscribe = init_opts.capabilities.resources.subscribe or False
            caps.resources.list_changed = init_opts.capabilities.resources.listChanged or False
        if init_opts.capabilities.tools:
            caps.tools.list_changed = init_opts.capabilities.tools.listChanged or False

        return InitializeResponse(
            protocol_version=types.LATEST_PROTOCOL_VERSION,
            server_info=ServerInfo(name=init_opts.server_name, version=init_opts.server_version),
            capabilities=caps,
            instructions=init_opts.instructions or "",
        )

    async def Ping(self, request, context):
        """Ping."""
        await self._execute_handler(types.PingRequest, types.PingRequest(), context)
        return PingResponse()

    async def ListTools(self, request, context):
        """
        List available tools.

        Note: The underlying Server implementation currently collects all tools
        into a list before returning. While we stream the response to the client,
        true end-to-end streaming requires updates to mcp.server.lowlevel.server
        to support async generators.
        """
        req = types.ListToolsRequest(params=types.PaginatedRequestParams(cursor=None))
        meta = self._convert_request_meta(request.meta) if request.HasField("meta") else None
        result = await self._execute_handler(types.ListToolsRequest, req, context, meta=meta)

        if isinstance(result, types.ServerResult):
            # result.root is ListToolsResult
            tools_result = result.root
            for tool in tools_result.tools:
                yield ListToolsResponse(tool=self._convert_tool_to_proto(tool))

    async def CallTool(self, request, context):
        """Call a tool (unary)."""
        req = types.CallToolRequest(
            params=types.CallToolRequestParams(name=request.name, arguments=self._struct_to_dict(request.arguments))
        )

        result = await self._execute_handler(types.CallToolRequest, req, context)

        if isinstance(result, types.ServerResult):
            call_result = result.root
            return CallToolResponse(
                content=[self._convert_content_to_proto(c) for c in call_result.content], is_error=call_result.isError
            )
        return CallToolResponse(is_error=True)

    async def CallToolWithProgress(self, request, context):
        """Call a tool with streaming progress updates."""
        # This requires a special handler execution that captures progress notifications
        # and streams them back.

        # We need a custom session that intercepts progress
        progress_queue = asyncio.Queue()

        class StreamingSession(GrpcServerSession):
            async def send_progress_notification(
                self, progress_token, progress, total=None, message=None, related_request_id=None
            ):
                token_msg = ProgressToken()
                if isinstance(progress_token, int):
                    token_msg.int_token = progress_token
                else:
                    token_msg.string_token = str(progress_token)

                await progress_queue.put(
                    CallToolWithProgressResponse(
                        progress=ProgressNotification(
                            progress_token=token_msg, progress=progress, total=total or 0.0, message=message or ""
                        )
                    )
                )

        req = types.CallToolRequest(
            params=types.CallToolRequestParams(name=request.name, arguments=self._struct_to_dict(request.arguments))
        )

        handler = self._server.request_handlers.get(types.CallToolRequest)
        if not handler:
            await context.abort(grpc.StatusCode.UNIMPLEMENTED, "Tool execution not implemented")
        else:

            async def run_handler():
                streaming_session = StreamingSession()
                # Inherit client params/capabilities from the main session
                streaming_session._client_params = self._session._client_params

                token = request_ctx.set(
                    RequestContext(
                        request_id=str(uuid.uuid4()),
                        meta=None,
                        session=streaming_session,
                        lifespan_context={},
                    )
                )
                try:
                    result = await handler(req)
                    return result
                finally:
                    request_ctx.reset(token)

            # Run handler in background task while we stream queue
            task = asyncio.create_task(run_handler())

            try:
                while not task.done():
                    try:
                        update = await asyncio.wait_for(
                            progress_queue.get(),
                            timeout=PROGRESS_POLL_TIMEOUT_SECONDS,
                        )
                        yield update
                    except asyncio.TimeoutError:
                        continue

                try:
                    result = task.result()
                except Exception as e:
                    logger.exception("Error in streaming tool call")
                    await context.abort(grpc.StatusCode.INTERNAL, str(e))
                    return

                if isinstance(result, types.ServerResult):
                    call_result = result.root
                    yield CallToolWithProgressResponse(
                        result=ToolResult(
                            content=[self._convert_content_to_proto(c) for c in call_result.content],
                            is_error=call_result.isError,
                        )
                    )
            finally:
                if not task.done():
                    task.cancel()
                # Drain any remaining progress
                while not progress_queue.empty():
                    yield progress_queue.get_nowait()

    async def StreamToolCalls(self, request_iterator, context):
        """
        Stream multiple tool calls with parallel execution.

        Client streams tool call requests, server streams results as they complete.
        Results may arrive in different order than requests.
        """
        active_tasks: dict[str, asyncio.Task[StreamToolCallsResponse]] = {}
        response_queue: asyncio.Queue[StreamToolCallsResponse | None] = asyncio.Queue()

        async def execute_tool(request_id: str, name: str, arguments: dict[str, Any]) -> None:
            """Execute a single tool and put result in queue."""
            try:
                req = types.CallToolRequest(params=types.CallToolRequestParams(name=name, arguments=arguments))
                result = await self._execute_handler(types.CallToolRequest, req, context)

                if isinstance(result, types.ServerResult):
                    call_result = result.root
                    await response_queue.put(
                        StreamToolCallsResponse(
                            request_id=request_id,
                            success=ToolResult(
                                content=[self._convert_content_to_proto(c) for c in call_result.content],
                                is_error=call_result.isError,
                            ),
                        )
                    )
                else:
                    await response_queue.put(
                        StreamToolCallsResponse(
                            request_id=request_id,
                            error=StreamError(
                                code=types.INTERNAL_ERROR,
                                message="Unexpected tool result type",
                            ),
                        )
                    )
            except Exception as e:
                logger.exception("Error executing tool %s", name)
                await response_queue.put(
                    StreamToolCallsResponse(
                        request_id=request_id,
                        error=StreamError(
                            code=types.INTERNAL_ERROR,
                            message=str(e),
                        ),
                    )
                )
            finally:
                active_tasks.pop(request_id, None)

        async def consume_requests() -> None:
            """Consume incoming requests and spawn tasks."""
            async for request in request_iterator:
                request_id = request.request_id or str(uuid.uuid4())
                arguments = self._struct_to_dict(request.arguments)
                task = asyncio.create_task(execute_tool(request_id, request.name, arguments))
                active_tasks[request_id] = task

            # Wait for all tasks to complete
            if active_tasks:
                await asyncio.gather(*list(active_tasks.values()), return_exceptions=True)
            await response_queue.put(None)  # Signal completion

        producer = asyncio.create_task(consume_requests())

        try:
            while True:
                response = await response_queue.get()
                if response is None:
                    break
                yield response
        finally:
            producer.cancel()
            for task in list(active_tasks.values()):
                task.cancel()

    async def ListResources(self, request, context):
        """
        List resources.

        Note: Currently buffers all resources from the Server handler.
        Future optimization: Support async iterators in Server handlers.
        """
        req = types.ListResourcesRequest(params=types.PaginatedRequestParams(cursor=None))
        meta = self._convert_request_meta(request.meta) if request.HasField("meta") else None
        result = await self._execute_handler(
            types.ListResourcesRequest,
            req,
            context,
            meta=meta,
        )

        if isinstance(result, types.ServerResult):
            res_result = result.root
            for r in res_result.resources:
                yield ListResourcesResponse(resource=self._convert_resource_to_proto(r))

    async def ListResourceTemplates(self, request, context):
        """
        List resource templates.

        Note: Currently buffers results from the Server handler.
        """
        req = types.ListResourceTemplatesRequest(params=types.PaginatedRequestParams(cursor=None))
        meta = self._convert_request_meta(request.meta) if request.HasField("meta") else None
        result = await self._execute_handler(
            types.ListResourceTemplatesRequest,
            req,
            context,
            meta=meta,
        )

        if isinstance(result, types.ServerResult):
            res_result = result.root
            for t in res_result.resourceTemplates:
                yield ListResourceTemplatesResponse(
                    resource_template=ResourceTemplate(
                        uri_template=t.uriTemplate,
                        name=t.name,
                        description=t.description or "",
                        mime_type=t.mimeType or "",
                    )
                )

    async def ReadResource(self, request, context):
        """Read a resource."""
        req = types.ReadResourceRequest(params=types.ReadResourceRequestParams(uri=AnyUrl(request.uri)))
        result = await self._execute_handler(types.ReadResourceRequest, req, context)

        if isinstance(result, types.ServerResult):
            read_result = result.root
            contents = []
            for c in read_result.contents:
                msg = ResourceContents(uri=str(c.uri), mime_type=c.mimeType or "")
                if isinstance(c, types.TextResourceContents):
                    msg.text = c.text
                elif isinstance(c, types.BlobResourceContents):
                    msg.blob = base64.b64decode(c.blob)
                contents.append(msg)

            return ReadResourceResponse(contents=contents)
        return ReadResourceResponse()

    async def ReadResourceChunked(self, request, context):
        """
        Read a resource in chunks.

        Note: The underlying read_resource handler currently returns the full content
        (or a full list of contents), which we then chunk. True streaming from the
        source is not yet supported by the Server class.
        """
        async for chunk in self._read_resource_chunked_impl(
            request,
            context,
            request_id=str(uuid.uuid4()),
            session=self._session,
            use_session=False,
        ):
            yield chunk

    async def _read_resource_chunked_impl(
        self,
        request,
        context,
        *,
        request_id: str,
        session: GrpcServerSession,
        use_session: bool,
    ) -> AsyncIterator[ReadResourceChunkedResponse]:
        req = types.ReadResourceRequest(params=types.ReadResourceRequestParams(uri=AnyUrl(request.uri)))

        # We reuse the standard ReadResource handler
        # Note: Ideally the handler would support yielding chunks, but for now
        # we get the full result and stream it back.
        if use_session:
            result = await self._execute_handler_for_session(
                types.ReadResourceRequest,
                req,
                request_id=request_id,
                session=session,
            )
        else:
            result = await self._execute_handler(types.ReadResourceRequest, req, context)

        if isinstance(result, types.ServerResult):
            read_result = result.root
            for c in read_result.contents:
                uri = str(c.uri)
                mime_type = c.mimeType or ""

                if isinstance(c, types.TextResourceContents):
                    text = c.text
                    # Chunk text to ensure messages stay within reasonable limits
                    # 8192 chars * 4 bytes/char (max utf8) = ~32KB, well within default 4MB limit
                    chunk_size = 8192
                    if not text:
                        yield ReadResourceChunkedResponse(
                            uri=uri,
                            mime_type=mime_type,
                            text_chunk="",
                            is_final=True,
                        )
                    else:
                        for i in range(0, len(text), chunk_size):
                            chunk = text[i : i + chunk_size]
                            is_last = (i + chunk_size) >= len(text)
                            yield ReadResourceChunkedResponse(
                                uri=uri,
                                mime_type=mime_type,
                                text_chunk=chunk,
                                is_final=is_last,
                            )

                elif isinstance(c, types.BlobResourceContents):
                    # Blob is base64 encoded in the Pydantic model
                    # But gRPC expects raw bytes in blob_chunk
                    blob_data = base64.b64decode(c.blob)

                    # 64KB chunk size for binary data
                    chunk_size = 64 * 1024
                    if not blob_data:
                        yield ReadResourceChunkedResponse(
                            uri=uri,
                            mime_type=mime_type,
                            blob_chunk=b"",
                            is_final=True,
                        )
                    else:
                        for i in range(0, len(blob_data), chunk_size):
                            chunk = blob_data[i : i + chunk_size]
                            is_last = (i + chunk_size) >= len(blob_data)
                            yield ReadResourceChunkedResponse(
                                uri=uri,
                                mime_type=mime_type,
                                blob_chunk=chunk,
                                is_final=is_last,
                            )

    async def WatchResources(self, request, context):
        """Stream resource change notifications."""
        patterns = list(request.uri_patterns)
        session = self._get_peer_session(context)
        queue = session.register_resource_watch(patterns)

        try:
            if request.include_initial:
                req = types.ListResourcesRequest(params=types.PaginatedRequestParams(cursor=None))
                result = await self._execute_handler(types.ListResourcesRequest, req, context)
                if isinstance(result, types.ServerResult):
                    timestamp = Timestamp()
                    timestamp.FromDatetime(datetime.now(timezone.utc))
                    for resource in result.root.resources:
                        uri_value = str(resource.uri)
                        if any(fnmatch(uri_value, pattern) for pattern in patterns):
                            yield WatchResourcesResponse(
                                uri=uri_value,
                                change_type=ResourceChangeType.RESOURCE_CHANGE_TYPE_CREATED,
                                timestamp=timestamp,
                            )

            while True:
                try:
                    notification = await asyncio.wait_for(
                        queue.get(),
                        timeout=WATCH_RESOURCES_POLL_TIMEOUT_SECONDS,
                    )
                    yield notification
                except asyncio.TimeoutError:
                    # Check if client disconnected
                    if context.cancelled():
                        break
                    continue
                except asyncio.CancelledError:
                    break
        finally:
            session.unregister_resource_watch(queue)

    async def ListPrompts(self, request, context):
        """
        List prompts.

        Note: Currently buffers results from the Server handler.
        """
        req = types.ListPromptsRequest(params=types.PaginatedRequestParams(cursor=None))
        meta = self._convert_request_meta(request.meta) if request.HasField("meta") else None
        result = await self._execute_handler(
            types.ListPromptsRequest,
            req,
            context,
            meta=meta,
        )

        if isinstance(result, types.ServerResult):
            prompts_result = result.root
            for p in prompts_result.prompts:
                yield ListPromptsResponse(prompt=self._convert_prompt_to_proto(p))

    async def GetPrompt(self, request, context):
        """Get a prompt."""
        req = types.GetPromptRequest(
            params=types.GetPromptRequestParams(name=request.name, arguments=dict(request.arguments))
        )
        result = await self._execute_handler(types.GetPromptRequest, req, context)

        if isinstance(result, types.ServerResult):
            prompt_result = result.root
            messages = []
            for m in prompt_result.messages:
                role = Role.ROLE_USER if m.role == "user" else Role.ROLE_ASSISTANT

                messages.append(PromptMessage(role=role, content=self._convert_content_to_proto(m.content)))

            return GetPromptResponse(description=prompt_result.description or "", messages=messages)
        return GetPromptResponse()

    async def Complete(self, request, context):
        """Autocomplete."""
        # Map proto reference to internal reference
        ref: types.PromptReference | types.ResourceTemplateReference
        if request.HasField("prompt_ref"):
            ref = types.PromptReference(name=request.prompt_ref.name)
        else:
            ref = types.ResourceTemplateReference(uri=request.resource_template_ref.uri)

        req = types.CompleteRequest(
            params=types.CompleteRequestParams(
                ref=ref, argument=types.CompletionArgument(name=request.argument_name, value=request.argument_value)
            )
        )

        result = await self._execute_handler(types.CompleteRequest, req, context)

        if isinstance(result, types.ServerResult):
            comp_result = result.root.completion
            return CompleteResponse(
                completion=CompletionResult(
                    values=comp_result.values, total=comp_result.total or 0, has_more=comp_result.hasMore or False
                )
            )
        return CompleteResponse()

    async def StreamPromptCompletion(self, request, context):
        """Stream prompt completion tokens."""
        arguments = dict(request.arguments)
        try:
            async for token, is_final, finish_reason in self._iter_prompt_completion_chunks(
                request.name,
                arguments,
            ):
                yield StreamPromptCompletionResponse(
                    token=token,
                    is_final=is_final,
                    finish_reason=finish_reason or "",
                )
        except NotImplementedError:
            await context.abort(
                grpc.StatusCode.UNIMPLEMENTED,
                "StreamPromptCompletion handler not registered",
            )

    async def _handle_session_initialize(
        self,
        request,
        context,
        *,
        session: GrpcServerSession,
    ) -> InitializeResponse:
        """Handle Initialize within a Session, updating the session's client params."""
        # Populate session with client params
        roots_cap = None
        if request.capabilities.HasField("roots"):
            roots_cap = types.RootsCapability(listChanged=request.capabilities.roots.list_changed)

        sampling_cap = None
        if request.capabilities.HasField("sampling"):
            sampling_cap = types.SamplingCapability()

        experimental_cap = None
        if request.capabilities.HasField("experimental"):
            experimental_cap = dict(request.capabilities.experimental.capabilities.items())

        session._client_params = types.InitializeRequestParams(
            protocolVersion=request.protocol_version,
            capabilities=types.ClientCapabilities(
                roots=roots_cap,
                sampling=sampling_cap,
                experimental=experimental_cap,
            ),
            clientInfo=types.Implementation(
                name=request.client_info.name,
                version=request.client_info.version,
            ),
        )

        init_opts = self._server.create_initialization_options()

        caps = ServerCapabilities()
        if init_opts.capabilities.prompts:
            caps.prompts.list_changed = init_opts.capabilities.prompts.listChanged or False
        if init_opts.capabilities.resources:
            caps.resources.subscribe = init_opts.capabilities.resources.subscribe or False
            caps.resources.list_changed = init_opts.capabilities.resources.listChanged or False
        if init_opts.capabilities.tools:
            caps.tools.list_changed = init_opts.capabilities.tools.listChanged or False

        return InitializeResponse(
            protocol_version=types.LATEST_PROTOCOL_VERSION,
            server_info=ServerInfo(
                name=init_opts.server_name,
                version=init_opts.server_version,
            ),
            capabilities=caps,
            instructions=init_opts.instructions or "",
        )

    async def _handle_session_list_tools(
        self,
        request,
        *,
        request_id: str,
        response_queue: asyncio.Queue[SessionResponse],
        session: GrpcServerSession,
    ) -> None:
        req = types.ListToolsRequest(params=types.PaginatedRequestParams(cursor=None))
        meta = self._convert_request_meta(request.list_tools.meta) if request.list_tools.HasField("meta") else None
        result = await self._execute_handler_for_session(
            types.ListToolsRequest,
            req,
            request_id=request_id,
            meta=meta,
            session=session,
        )
        if isinstance(result, types.ServerResult):
            for tool in result.root.tools:
                await self._session_emit(
                    response_queue,
                    request_id,
                    list_tools=ListToolsResponse(tool=self._convert_tool_to_proto(tool)),
                )
            await self._session_emit(
                response_queue,
                request_id,
                stream_end=StreamEnd(request_id=request_id),
            )
        else:
            await self._emit_stream_error(
                response_queue,
                request_id,
                code=types.INTERNAL_ERROR,
                message="Unexpected list_tools result",
            )

    async def _handle_session_list_resources(
        self,
        request,
        *,
        request_id: str,
        response_queue: asyncio.Queue[SessionResponse],
        session: GrpcServerSession,
    ) -> None:
        req = types.ListResourcesRequest(params=types.PaginatedRequestParams(cursor=None))
        meta = (
            self._convert_request_meta(request.list_resources.meta) if request.list_resources.HasField("meta") else None
        )
        result = await self._execute_handler_for_session(
            types.ListResourcesRequest,
            req,
            request_id=request_id,
            meta=meta,
            session=session,
        )
        if isinstance(result, types.ServerResult):
            for resource in result.root.resources:
                await self._session_emit(
                    response_queue,
                    request_id,
                    list_resources=ListResourcesResponse(resource=self._convert_resource_to_proto(resource)),
                )
            await self._session_emit(
                response_queue,
                request_id,
                stream_end=StreamEnd(request_id=request_id),
            )
        else:
            await self._emit_stream_error(
                response_queue,
                request_id,
                code=types.INTERNAL_ERROR,
                message="Unexpected list_resources result",
            )

    async def _handle_session_list_resource_templates(
        self,
        request,
        *,
        request_id: str,
        response_queue: asyncio.Queue[SessionResponse],
        session: GrpcServerSession,
    ) -> None:
        req = types.ListResourceTemplatesRequest(params=types.PaginatedRequestParams(cursor=None))
        meta = (
            self._convert_request_meta(request.list_resource_templates.meta)
            if request.list_resource_templates.HasField("meta")
            else None
        )
        result = await self._execute_handler_for_session(
            types.ListResourceTemplatesRequest,
            req,
            request_id=request_id,
            meta=meta,
            session=session,
        )
        if isinstance(result, types.ServerResult):
            for template in result.root.resourceTemplates:
                await self._session_emit(
                    response_queue,
                    request_id,
                    list_resource_templates=ListResourceTemplatesResponse(
                        resource_template=ResourceTemplate(
                            uri_template=template.uriTemplate,
                            name=template.name,
                            description=template.description or "",
                            mime_type=template.mimeType or "",
                        )
                    ),
                )
            await self._session_emit(
                response_queue,
                request_id,
                stream_end=StreamEnd(request_id=request_id),
            )
        else:
            await self._emit_stream_error(
                response_queue,
                request_id,
                code=types.INTERNAL_ERROR,
                message="Unexpected list_resource_templates result",
            )

    async def _handle_session_list_prompts(
        self,
        request,
        *,
        request_id: str,
        response_queue: asyncio.Queue[SessionResponse],
        session: GrpcServerSession,
    ) -> None:
        req = types.ListPromptsRequest(params=types.PaginatedRequestParams(cursor=None))
        meta = self._convert_request_meta(request.list_prompts.meta) if request.list_prompts.HasField("meta") else None
        result = await self._execute_handler_for_session(
            types.ListPromptsRequest,
            req,
            request_id=request_id,
            meta=meta,
            session=session,
        )
        if isinstance(result, types.ServerResult):
            for prompt in result.root.prompts:
                await self._session_emit(
                    response_queue,
                    request_id,
                    list_prompts=ListPromptsResponse(prompt=self._convert_prompt_to_proto(prompt)),
                )
            await self._session_emit(
                response_queue,
                request_id,
                stream_end=StreamEnd(request_id=request_id),
            )
        else:
            await self._emit_stream_error(
                response_queue,
                request_id,
                code=types.INTERNAL_ERROR,
                message="Unexpected list_prompts result",
            )

    async def _handle_session_read_resource_chunked(
        self,
        request,
        context,
        *,
        request_id: str,
        response_queue: asyncio.Queue[SessionResponse],
        session: GrpcServerSession,
    ) -> None:
        stream = self._read_resource_chunked_impl(
            request.read_resource_chunked,
            context,
            request_id=request_id,
            session=session,
            use_session=True,
        )
        async for chunk in stream:
            await self._session_emit(
                response_queue,
                request_id,
                resource_chunk=chunk,
            )
        await self._session_emit(
            response_queue,
            request_id,
            stream_end=StreamEnd(request_id=request_id),
        )

    async def _handle_session_watch_resources(
        self,
        request,
        *,
        request_id: str,
        response_queue: asyncio.Queue[SessionResponse],
        session: GrpcServerSession,
    ) -> None:
        patterns = list(request.watch_resources.uri_patterns)
        queue = session.register_resource_watch(patterns)
        try:
            if request.watch_resources.include_initial:
                req = types.ListResourcesRequest(params=types.PaginatedRequestParams(cursor=None))
                result = await self._execute_handler_for_session(
                    types.ListResourcesRequest,
                    req,
                    request_id=request_id,
                    session=session,
                )
                if isinstance(result, types.ServerResult):
                    timestamp = Timestamp()
                    timestamp.FromDatetime(datetime.now(timezone.utc))
                    for resource in result.root.resources:
                        uri_value = str(resource.uri)
                        if any(fnmatch(uri_value, pattern) for pattern in patterns):
                            await self._session_emit(
                                response_queue,
                                request_id,
                                resource_notification=WatchResourcesResponse(
                                    uri=uri_value,
                                    change_type=ResourceChangeType.RESOURCE_CHANGE_TYPE_CREATED,
                                    timestamp=timestamp,
                                ),
                            )

            while True:
                try:
                    notification = await asyncio.wait_for(
                        queue.get(),
                        timeout=WATCH_RESOURCES_POLL_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    break

                await self._session_emit(
                    response_queue,
                    request_id,
                    resource_notification=notification,
                )
        finally:
            session.unregister_resource_watch(queue)

    async def _handle_session_stream_prompt_completion(
        self,
        request,
        *,
        request_id: str,
        response_queue: asyncio.Queue[SessionResponse],
    ) -> None:
        arguments = dict(request.stream_prompt_completion.arguments)
        try:
            async for token, is_final, finish_reason in self._iter_prompt_completion_chunks(
                request.stream_prompt_completion.name,
                arguments,
            ):
                await self._session_emit(
                    response_queue,
                    request_id,
                    completion_chunk=StreamPromptCompletionResponse(
                        token=token,
                        is_final=is_final,
                        finish_reason=finish_reason or "",
                    ),
                )
            await self._session_emit(
                response_queue,
                request_id,
                stream_end=StreamEnd(request_id=request_id),
            )
        except NotImplementedError:
            await self._emit_stream_error(
                response_queue,
                request_id,
                code=types.METHOD_NOT_FOUND,
                message="StreamPromptCompletion handler not registered",
            )

    async def Session(self, request_iterator, context):
        """Handle bidirectional Session stream with explicit StreamEnd signals."""
        # Create a dedicated session for this connection.
        # Client should send Initialize first to set up capabilities.
        # If they don't, capability checks will return False.
        session = GrpcServerSession()
        response_queue: asyncio.Queue[SessionResponse] = asyncio.Queue()
        active_tasks: dict[str, asyncio.Task[None]] = {}
        stop_sentinel: object = object()

        async def handle_request(request) -> None:
            request_id = request.message_id or str(uuid.uuid4())
            payload_type = request.WhichOneof("payload")

            try:
                if payload_type != "initialize" and session._client_params is None:
                    await self._emit_stream_error(
                        response_queue,
                        request_id,
                        code=types.INVALID_REQUEST,
                        message="Session not initialized",
                    )
                    return

                if payload_type == "initialize":
                    response = await self._handle_session_initialize(
                        request.initialize,
                        context,
                        session=session,
                    )
                    await self._session_emit(
                        response_queue,
                        request_id,
                        initialize=response,
                    )
                elif payload_type == "ping":
                    response = await self.Ping(request.ping, context)
                    await self._session_emit(
                        response_queue,
                        request_id,
                        ping=response,
                    )
                elif payload_type == "call_tool":
                    response = await self.CallTool(request.call_tool, context)
                    await self._session_emit(
                        response_queue,
                        request_id,
                        call_tool=response,
                    )
                elif payload_type == "read_resource":
                    response = await self.ReadResource(request.read_resource, context)
                    await self._session_emit(
                        response_queue,
                        request_id,
                        read_resource=response,
                    )
                elif payload_type == "get_prompt":
                    response = await self.GetPrompt(request.get_prompt, context)
                    await self._session_emit(
                        response_queue,
                        request_id,
                        get_prompt=response,
                    )
                elif payload_type == "complete":
                    response = await self.Complete(request.complete, context)
                    await self._session_emit(
                        response_queue,
                        request_id,
                        complete=response,
                    )
                elif payload_type == "list_tools":
                    await self._handle_session_list_tools(
                        request,
                        request_id=request_id,
                        response_queue=response_queue,
                        session=session,
                    )
                elif payload_type == "list_resources":
                    await self._handle_session_list_resources(
                        request,
                        request_id=request_id,
                        response_queue=response_queue,
                        session=session,
                    )
                elif payload_type == "list_resource_templates":
                    await self._handle_session_list_resource_templates(
                        request,
                        request_id=request_id,
                        response_queue=response_queue,
                        session=session,
                    )
                elif payload_type == "list_prompts":
                    await self._handle_session_list_prompts(
                        request,
                        request_id=request_id,
                        response_queue=response_queue,
                        session=session,
                    )
                elif payload_type == "read_resource_chunked":
                    await self._handle_session_read_resource_chunked(
                        request,
                        context,
                        request_id=request_id,
                        response_queue=response_queue,
                        session=session,
                    )
                elif payload_type == "watch_resources":
                    await self._handle_session_watch_resources(
                        request,
                        request_id=request_id,
                        response_queue=response_queue,
                        session=session,
                    )
                elif payload_type == "stream_prompt_completion":
                    await self._handle_session_stream_prompt_completion(
                        request,
                        request_id=request_id,
                        response_queue=response_queue,
                    )
                else:
                    await self._emit_stream_error(
                        response_queue,
                        request_id,
                        code=types.METHOD_NOT_FOUND,
                        message=f"Unsupported session payload: {payload_type}",
                    )
            except Exception as exc:
                logger.exception("Session handler error")
                await self._emit_stream_error(
                    response_queue,
                    request_id,
                    code=types.INTERNAL_ERROR,
                    message=str(exc),
                )
            finally:
                active_tasks.pop(request_id, None)

        async def consume_requests() -> None:
            async for request in request_iterator:
                payload_type = request.WhichOneof("payload")
                if payload_type == "cancel":
                    cancel_id = request.cancel.request_id
                    task = active_tasks.pop(cancel_id, None)
                    if task:
                        task.cancel()
                        await response_queue.put(
                            self._make_session_response(
                                in_reply_to=cancel_id,
                                stream_end=StreamEnd(request_id=cancel_id),
                            )
                        )
                    continue

                if not request.message_id:
                    request.message_id = str(uuid.uuid4())

                task = asyncio.create_task(handle_request(request))
                active_tasks[request.message_id] = task

            if active_tasks:
                # Copy values to avoid race with tasks removing themselves
                await asyncio.gather(*list(active_tasks.values()), return_exceptions=True)
            await response_queue.put(stop_sentinel)  # type: ignore[arg-type]

        producer = asyncio.create_task(consume_requests())

        while True:
            response = await response_queue.get()
            if response is stop_sentinel:
                break
            yield response

        producer.cancel()


async def start_grpc_server(
    server: Server, address: str = "[::]:50051", ssl_key_chain: tuple[bytes, bytes] | None = None
) -> grpc.aio.Server:
    """
    Start a gRPC server serving the given MCP server instance.

    Args:
        server: The MCP server instance (from mcp.server.lowlevel.server)
        address: The address to bind to (default: "[::]:50051")
        ssl_key_chain: Optional (private_key, certificate_chain) for SSL/TLS

    Returns:
        The started grpc.aio.Server instance.
    """
    grpc_server = grpc.aio.server()
    servicer = McpGrpcServicer(server)
    add_McpServiceServicer_to_server(servicer, grpc_server)

    if ssl_key_chain:
        server_credentials = grpc.ssl_server_credentials([ssl_key_chain])
        grpc_server.add_secure_port(address, server_credentials)
    else:
        grpc_server.add_insecure_port(address)

    logger.info("Starting MCP gRPC server on %s", address)
    await grpc_server.start()
    return grpc_server
