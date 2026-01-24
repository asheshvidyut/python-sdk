import asyncio

import pytest
from pydantic import AnyUrl

import mcp.types as types
from mcp.server.grpc.server import McpGrpcServicer
from mcp.server.lowlevel.helper_types import ReadResourceContents
from mcp.server.lowlevel.server import Server
from mcp.v1.mcp_pb2 import (
    CancelRequest,
    ListToolsRequest,
    ReadResourceChunkedRequest,
    SessionRequest,
    StreamPromptCompletionRequest,
    WatchResourcesRequest,
)


class DummyContext:
    async def abort(self, code, message):  # pragma: no cover - should not be called
        raise RuntimeError(f"{code}: {message}")


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
        yield SessionRequest(message_id="req-1", list_tools=ListToolsRequest())

    responses = [response async for response in servicer.Session(requests(), DummyContext())]

    assert any(r.WhichOneof("payload") == "list_tools" for r in responses)
    end = next(r for r in responses if r.WhichOneof("payload") == "stream_end")
    assert end.in_reply_to == "req-1"
    assert end.stream_end.request_id == "req-1"


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
                uri=AnyUrl("file:///test/resource.txt"),
                name="test_resource",
                mimeType="text/plain",
            )
        ]

    servicer = McpGrpcServicer(server)

    async def requests():
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
    await servicer._session.send_resource_updated(AnyUrl("file:///watch/file.txt"))
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
