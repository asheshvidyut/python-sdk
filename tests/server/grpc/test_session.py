import asyncio

import pytest
from google.protobuf import struct_pb2
from pydantic import AnyUrl

import mcp.types as types
from mcp.server.grpc.server import McpGrpcServicer
from mcp.server.lowlevel.helper_types import ReadResourceContents
from mcp.server.lowlevel.server import Server
from mcp.v1.mcp_pb2 import (
    CallToolWithProgressRequest,
    CancelRequest,
    ClientInfo,
    InitializeRequest,
    ListToolsRequest,
    ReadResourceChunkedRequest,
    SessionRequest,
    StreamPromptCompletionRequest,
    StreamToolCallsRequest,
    WatchResourcesRequest,
)


class DummyContext:
    async def abort(self, code, message):  # pragma: no cover - should not be called
        raise RuntimeError(f"{code}: {message}")

    def peer(self) -> str:
        return "test-peer"

    def cancelled(self) -> bool:
        return False


def session_initialize_request(message_id: str) -> SessionRequest:
    init = InitializeRequest(protocol_version="2025-06-18")
    init.client_info.CopyFrom(ClientInfo(name="test-client", version="0.0"))
    return SessionRequest(message_id=message_id, initialize=init)


@pytest.mark.anyio
async def test_session_list_tools_stream_end():
    server = Server("test-session-list-tools")

    @server.list_tools()
    async def list_tools() -> list[types.Tool]:
        return [
            types.Tool(
                name="session_tool",
                description="Session tool",
                inputSchema={"type": "object"},
            )
        ]

    servicer = McpGrpcServicer(server)

    async def requests():
        yield session_initialize_request("init-1")
        yield SessionRequest(message_id="req-1", list_tools=ListToolsRequest())

    responses = [response async for response in servicer.Session(requests(), DummyContext())]

    assert any(r.WhichOneof("payload") == "list_tools" for r in responses)
    end = next(r for r in responses if r.WhichOneof("payload") == "stream_end")
    assert end.in_reply_to == "req-1"
    assert end.stream_end.request_id == "req-1"


@pytest.mark.anyio
async def test_session_requires_initialize():
    server = Server("test-session-requires-init")

    @server.list_tools()
    async def list_tools() -> list[types.Tool]:
        return []

    servicer = McpGrpcServicer(server)

    async def requests():
        yield SessionRequest(message_id="req-1", list_tools=ListToolsRequest())

    responses = [response async for response in servicer.Session(requests(), DummyContext())]

    error = next(r for r in responses if r.WhichOneof("payload") == "stream_error")
    assert error.in_reply_to == "req-1"
    assert error.stream_error.code == types.INVALID_REQUEST


@pytest.mark.anyio
async def test_session_read_resource_chunked_stream_end():
    server = Server("test-session-read-resource")

    @server.read_resource()
    async def read_resource(uri: AnyUrl) -> list[ReadResourceContents]:
        return [
            ReadResourceContents(
                content="Chunked content",
                mime_type="text/plain",
            )
        ]

    servicer = McpGrpcServicer(server)

    async def requests():
        yield session_initialize_request("init-2")
        yield SessionRequest(
            message_id="req-2",
            read_resource_chunked=ReadResourceChunkedRequest(uri="file:///test/resource.txt"),
        )

    responses = [response async for response in servicer.Session(requests(), DummyContext())]

    assert any(r.WhichOneof("payload") == "resource_chunk" for r in responses)
    end = next(r for r in responses if r.WhichOneof("payload") == "stream_end")
    assert end.in_reply_to == "req-2"
    assert end.stream_end.request_id == "req-2"


@pytest.mark.anyio
async def test_session_cancel_emits_stream_end():
    server = Server("test-session-cancel")
    block = asyncio.Event()

    @server.list_tools()
    async def list_tools() -> list[types.Tool]:
        await block.wait()
        return []

    servicer = McpGrpcServicer(server)

    async def requests():
        yield SessionRequest(message_id="req-4", list_tools=ListToolsRequest())
        yield SessionRequest(cancel=CancelRequest(request_id="req-4"))

    responses = [response async for response in servicer.Session(requests(), DummyContext())]

    end = next(r for r in responses if r.WhichOneof("payload") == "stream_end")
    assert end.in_reply_to == "req-4"


@pytest.mark.anyio
async def test_session_watch_resources_streams_updates():
    server = Server("test-session-watch-resources")

    @server.list_resources()
    async def list_resources() -> list[types.Resource]:
        return [
            types.Resource(
                uri="file:///test/resource.txt",
                name="test_resource",
                mime_type="text/plain",
            )
        ]

    servicer = McpGrpcServicer(server)

    async def requests():
        yield session_initialize_request("init-3")
        yield SessionRequest(
            message_id="req-3",
            watch_resources=WatchResourcesRequest(
                uri_patterns=["file:///test/*.txt"],
                include_initial=True,
            ),
        )

    responses = []

    async def collect():
        async for response in servicer.Session(requests(), DummyContext()):
            responses.append(response)
            if response.WhichOneof("payload") == "resource_notification":
                break

    await collect()

    assert any(
        r.WhichOneof("payload") == "resource_notification" for r in responses
    )


@pytest.mark.anyio
async def test_watch_resources_rpc_streams_updates():
    server = Server("test-watch-resources-rpc")

    @server.list_resources()
    async def list_resources() -> list[types.Resource]:
        return []

    servicer = McpGrpcServicer(server)

    async def run_watch():
        async for response in servicer.WatchResources(
            WatchResourcesRequest(uri_patterns=["file:///watch/*.txt"]),
            DummyContext(),
        ):
            return response
        return None

    task = asyncio.create_task(run_watch())
    await asyncio.sleep(0)
    await servicer._get_peer_session(DummyContext()).send_resource_updated(
        "file:///watch/file.txt"
    )
    response = await asyncio.wait_for(task, timeout=1.0)

    assert response is not None
    assert response.uri == "file:///watch/file.txt"


@pytest.mark.anyio
async def test_session_stream_prompt_completion_streams_tokens():
    server = Server("test-session-prompt-completion")

    @server.stream_prompt_completion()
    async def stream_prompt_completion(name: str, arguments: dict[str, str] | None):
        yield types.StreamPromptCompletionChunk(token="hello")
        yield types.StreamPromptCompletionChunk(token="world", isFinal=True, finishReason="stop")

    servicer = McpGrpcServicer(server)

    async def requests():
        yield session_initialize_request("init-5")
        yield SessionRequest(
            message_id="req-5",
            stream_prompt_completion=StreamPromptCompletionRequest(
                name="test",
                arguments={"q": "value"},
            ),
        )

    responses = [response async for response in servicer.Session(requests(), DummyContext())]

    tokens = [
        r.completion_chunk.token
        for r in responses
        if r.WhichOneof("payload") == "completion_chunk"
    ]
    assert tokens == ["hello", "world"]
    end = next(r for r in responses if r.WhichOneof("payload") == "stream_end")
    assert end.in_reply_to == "req-5"


@pytest.mark.anyio
async def test_stream_tool_calls_rpc_streams_results():
    server = Server("test-stream-tool-calls")

    @server.call_tool()
    async def call_tool(name: str, arguments: dict | None) -> list[types.TextContent]:
        return [types.TextContent(type="text", text=f"{name}:{arguments.get('msg')}")]

    servicer = McpGrpcServicer(server)

    async def requests():
        args_a = struct_pb2.Struct()
        args_a.update({"msg": "a"})
        args_b = struct_pb2.Struct()
        args_b.update({"msg": "b"})
        yield StreamToolCallsRequest(request_id="r1", name="tool", arguments=args_a)
        yield StreamToolCallsRequest(request_id="r2", name="tool", arguments=args_b)

    responses = [response async for response in servicer.StreamToolCalls(requests(), DummyContext())]

    by_id = {response.request_id: response for response in responses}
    assert set(by_id) == {"r1", "r2"}
    assert by_id["r1"].success.content[0].text.text == "tool:a"
    assert by_id["r2"].success.content[0].text.text == "tool:b"


@pytest.mark.anyio
async def test_call_tool_with_progress_rpc_streams_updates():
    server = Server("test-call-tool-progress")

    @server.call_tool()
    async def call_tool(name: str, arguments: dict | None) -> list[types.TextContent]:
        ctx = server.request_context
        if ctx.session:
            await ctx.session.send_progress_notification(
                progress_token="p",
                progress=0.5,
                total=1.0,
                message="half",
            )
        return [types.TextContent(type="text", text="done")]

    servicer = McpGrpcServicer(server)

    args = struct_pb2.Struct()
    args.update({"msg": "x"})
    request = CallToolWithProgressRequest(name="tool", arguments=args)
    responses = [response async for response in servicer.CallToolWithProgress(request, DummyContext())]

    assert any(r.WhichOneof("update") == "progress" for r in responses)
    result = next(r for r in responses if r.WhichOneof("update") == "result")
    assert result.result.content[0].text.text == "done"
