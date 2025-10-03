import asyncio
import base64
import json
import socket
from collections.abc import Generator
from io import BytesIO

import pytest

from mcp import types
from mcp.client.grpc_transport_session import GRPCTransportSession
from mcp.server.fastmcp.server import FastMCP
from mcp.server.grpc import create_mcp_grpc_server
from mcp.shared.exceptions import McpError


def setup_test_server(port: int) -> FastMCP:
    """Set up a FastMCP server for testing."""
    mcp = FastMCP(
        name="Test gRPC Server",
        instructions="A test MCP server for gRPC transport.",
        host="127.0.0.1",
        port=port,
    )

    @mcp.tool()
    def greet(name: str) -> str:
        greeting = f"Hello, {name}! Welcome to the Simple gRPC Server!"
        return greeting

    @mcp.tool()
    def test_tool(a: int, b: int) -> int:
        """A test tool that adds two numbers."""
        return a + b

    @mcp.tool()
    def failing_tool():
        """A tool that always fails."""
        raise ValueError("This tool always fails")

    @mcp.tool()
    async def blocking_tool():
        """A tool that blocks until cancelled."""
        await asyncio.sleep(10)

    @mcp.tool()
    def get_image() -> types.ImageContent:
        from PIL import Image as PILImage

        img = PILImage.new("RGB", (1, 1), color="red")
        buf = BytesIO()
        img.save(buf, format="PNG")
        return types.ImageContent(
            type="image", data=base64.b64encode(buf.getvalue()).decode("utf-8"), mimeType="image/png"
        )

    @mcp.tool()
    def get_audio() -> types.AudioContent:
        return types.AudioContent(
            type="audio", data=base64.b64encode(b"fake wav data").decode("utf-8"), mimeType="audio/wav"
        )

    @mcp.tool()
    def get_resource_link() -> types.ResourceLink:
        return types.ResourceLink(name="resourcelink", type="resource_link", uri="test://example/link")

    @mcp.tool()
    def get_embedded_text_resource() -> types.EmbeddedResource:
        return types.EmbeddedResource(
            type="resource",
            resource=types.TextResourceContents(
                uri="test://example/embeddedtext", mimeType="text/plain", text="some text"
            ),
        )

    @mcp.tool()
    def get_embedded_blob_resource() -> types.EmbeddedResource:
        return types.EmbeddedResource(
            type="resource",
            resource=types.BlobResourceContents(
                uri="test://example/embeddedblob",
                mimeType="application/octet-stream",
                blob=base64.b64encode(b"blobdata").decode("utf-8"),
            ),
        )

    @mcp.tool()
    def get_untyped_object() -> dict:
        class UntypedObject:
            def __str__(self):
                return "UntypedObject()"

        return {"result": str(UntypedObject())}

    return mcp


def setup_empty_test_server(port: int) -> FastMCP:
    """Set up a FastMCP server with no tools for testing."""
    mcp = FastMCP(
        name="Empty Test gRPC Server",
        instructions="A test MCP server with no tools.",
        host="127.0.0.1",
        port=port,
    )
    return mcp


@pytest.fixture
def server_port() -> int:
    """Find an available port for the server."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
async def grpc_server(server_port: int) -> Generator[None, None, None]:
    """Start a gRPC server in process."""
    server_instance = setup_test_server(server_port)
    server = await create_mcp_grpc_server(target=f"127.0.0.1:{server_port}", mcp_server=server_instance)

    yield server

    await server.stop(grace=1)
    # Add a small delay to allow gRPC channels to close
    await asyncio.sleep(0.1)


@pytest.fixture
async def empty_grpc_server(server_port: int) -> Generator[None, None, None]:
    """Start a gRPC server in process with no tools."""
    server_instance = setup_empty_test_server(server_port)
    server = await create_mcp_grpc_server(target=f"127.0.0.1:{server_port}", mcp_server=server_instance)

    yield server

    await server.stop(grace=1)
    # Add a small delay to allow gRPC channels to close
    await asyncio.sleep(0.1)


@pytest.mark.anyio
async def test_list_tools_grpc_transport(grpc_server: None, server_port: int):
    """Test GRPCTransportSession.list_tools()."""
    transport = GRPCTransportSession(target=f"127.0.0.1:{server_port}")
    try:
        list_tools_result = await transport.list_tools()

        assert list_tools_result is not None
        assert len(list_tools_result.tools) == 10

        tools_by_name = {tool.name: tool for tool in list_tools_result.tools}

        expected_tools = {
            "greet": {
                "name": "greet",
                "description": "",
                "inputSchema": {
                    "properties": {"name": {"title": "Name", "type": "string"}},
                    "required": ["name"],
                    "title": "greetArguments",
                    "type": "object",
                },
                "outputSchema": {
                    "properties": {"result": {"title": "Result", "type": "string"}},
                    "required": ["result"],
                    "title": "greetOutput",
                    "type": "object",
                },
            },
            "test_tool": {
                "name": "test_tool",
                "description": "A test tool that adds two numbers.",
                "inputSchema": {
                    "properties": {
                        "a": {"title": "A", "type": "integer"},
                        "b": {"title": "B", "type": "integer"},
                    },
                    "required": ["a", "b"],
                    "title": "test_toolArguments",
                    "type": "object",
                },
                "outputSchema": {
                    "properties": {"result": {"title": "Result", "type": "integer"}},
                    "required": ["result"],
                    "title": "test_toolOutput",
                    "type": "object",
                },
            },
            "failing_tool": {
                "name": "failing_tool",
                "description": "A tool that always fails.",
                "inputSchema": {"properties": {}, "title": "failing_toolArguments", "type": "object"},
                "outputSchema": {},
            },
            "blocking_tool": {
                "name": "blocking_tool",
                "description": "A tool that blocks until cancelled.",
                "inputSchema": {"properties": {}, "title": "blocking_toolArguments", "type": "object"},
                "outputSchema": {},
            },
            "get_image": {
                "name": "get_image",
                "description": "",
                "inputSchema": {"properties": {}, "title": "get_imageArguments", "type": "object"},
                "outputSchema": types.ImageContent.model_json_schema(),
            },
            "get_audio": {
                "name": "get_audio",
                "description": "",
                "inputSchema": {"properties": {}, "title": "get_audioArguments", "type": "object"},
                "outputSchema": types.AudioContent.model_json_schema(),
            },
            "get_resource_link": {
                "name": "get_resource_link",
                "description": "",
                "inputSchema": {"properties": {}, "title": "get_resource_linkArguments", "type": "object"},
                "outputSchema": types.ResourceLink.model_json_schema(),
            },
            "get_embedded_text_resource": {
                "name": "get_embedded_text_resource",
                "description": "",
                "inputSchema": {"properties": {}, "title": "get_embedded_text_resourceArguments", "type": "object"},
                "outputSchema": types.EmbeddedResource.model_json_schema(),
            },
            "get_embedded_blob_resource": {
                "name": "get_embedded_blob_resource",
                "description": "",
                "inputSchema": {"properties": {}, "title": "get_embedded_blob_resourceArguments", "type": "object"},
                "outputSchema": types.EmbeddedResource.model_json_schema(),
            },
            "get_untyped_object": {
                "name": "get_untyped_object",
                "description": "",
                "inputSchema": {"properties": {}, "title": "get_untyped_objectArguments", "type": "object"},
                "outputSchema": {},
            },
        }

        assert tools_by_name.keys() == expected_tools.keys()

        for tool_name, tool in tools_by_name.items():
            expected_tool = expected_tools[tool_name]
            assert tool.name == expected_tool["name"]
            assert tool.description == expected_tool["description"]
            assert tool.inputSchema == expected_tool["inputSchema"]
            assert tool.outputSchema == expected_tool["outputSchema"]
    finally:
        await transport.close()


@pytest.mark.anyio
async def test_list_tools_grpc_empty_tran(empty_grpc_server: None, server_port: int):
    """Test GRPCTransportSession.list_tools() with no tools."""
    transport = GRPCTransportSession(target=f"127.0.0.1:{server_port}")
    try:
        list_tools_result = await transport.list_tools()
        assert list_tools_result is not None
        assert len(list_tools_result.tools) == 0
    finally:
        await transport.close()


@pytest.mark.anyio
async def test_list_tools_grpc_transport_failure(server_port: int):
    """Test GRPCTransportSession.list_tools() when no server is running."""
    transport = GRPCTransportSession(target=f"127.0.0.1:{server_port + 1}")
    try:
        with pytest.raises(McpError) as e:
            await transport.list_tools()
        assert e.value.error.code == -32603  # types.INTERNAL_ERROR
        assert "grpc.RpcError - Failed to list tools" in e.value.error.message
        assert "StatusCode.UNAVAILABLE" in e.value.error.message
        assert "Connection refused" in e.value.error.message
    finally:
        await transport.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "tool_name, tool_args, expected_content, expected_structured_content",
    [
        (
            "greet",
            {"name": "World"},
            [{"type": "text", "text": "Hello, World! Welcome to the Simple gRPC Server!"}],
            {"result": "Hello, World! Welcome to the Simple gRPC Server!"},
        ),
        (
            "test_tool",
            {"a": 2, "b": 3},
            [{"type": "text", "text": "5"}],
            {"result": 5},
        ),
        (
            "get_image",
            {},
            [
                {
                    "type": "image",
                    "data": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC",  # noqa: E501
                    "mimeType": "image/png",
                },
                {
                    "type": "image",
                    "data": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC",  # noqa: E501
                    "mimeType": "image/png",
                },
            ],
            {
                "data": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC",
                "mimeType": "image/png",
                "annotations": None,
                "_meta": None,
                "type": "image",
            },
        ),
        (
            "get_audio",
            {},
            [
                {"type": "audio", "data": base64.b64encode(b"fake wav data").decode("utf-8"), "mimeType": "audio/wav"},
                {"type": "audio", "data": base64.b64encode(b"fake wav data").decode("utf-8"), "mimeType": "audio/wav"},
            ],
            {
                "data": "ZmFrZSB3YXYgZGF0YQ==",
                "mimeType": "audio/wav",
                "annotations": None,
                "_meta": None,
                "type": "audio",
            },
        ),
        (
            "get_resource_link",
            {},
            [
                {"type": "resource_link", "uri": "test://example/link", "name": "resourcelink"},
                {"type": "resource_link", "uri": "test://example/link", "name": "resourcelink"},
            ],
            {
                "name": "resourcelink",
                "title": None,
                "uri": "test://example/link",
                "description": None,
                "mimeType": None,
                "size": None,
                "annotations": None,
                "_meta": None,
                "type": "resource_link",
            },
        ),
        (
            "get_embedded_text_resource",
            {},
            [
                {
                    "type": "resource",
                    "resource": {
                        "type": "text",
                        "uri": "test://example/embeddedtext",
                        "mimeType": "text/plain",
                        "text": "some text",
                    },
                },
                {
                    "type": "resource",
                    "resource": {
                        "type": "text",
                        "uri": "test://example/embeddedtext",
                        "mimeType": "text/plain",
                        "text": "some text",
                    },
                },
            ],
            {
                "type": "resource",
                "resource": {
                    "uri": "test://example/embeddedtext",
                    "mimeType": "text/plain",
                    "text": "some text",
                    "_meta": None,
                },
                "annotations": None,
                "_meta": None,
            },
        ),
        (
            "get_embedded_blob_resource",
            {},
            [
                {
                    "type": "resource",
                    "resource": {
                        "type": "blob",
                        "uri": "test://example/embeddedblob",
                        "mimeType": "application/octet-stream",
                        "blob": base64.b64encode(b"blobdata").decode("utf-8"),
                    },
                },
                {
                    "type": "resource",
                    "resource": {
                        "type": "blob",
                        "uri": "test://example/embeddedblob",
                        "mimeType": "application/octet-stream",
                        "blob": base64.b64encode(b"blobdata").decode("utf-8"),
                    },
                },
            ],
            {
                "type": "resource",
                "resource": {
                    "uri": "test://example/embeddedblob",
                    "mimeType": "application/octet-stream",
                    "blob": base64.b64encode(b"blobdata").decode("utf-8"),
                    "_meta": None,
                },
                "annotations": None,
                "_meta": None,
            },
        ),
        (
            "get_untyped_object",
            {},
            [{"type": "text", "text": json.dumps({"result": "UntypedObject()"}, indent=2)}],
            None,
        ),
    ],
)
async def test_call_tool_grpc_transport_success(
    grpc_server: None,
    server_port: int,
    tool_name: str,
    tool_args: dict,
    expected_content: list,
    expected_structured_content: dict,
):
    """Test GRPCTransportSession.call_tool() for successful calls."""
    transport = GRPCTransportSession(target=f"127.0.0.1:{server_port}")
    try:
        result = await transport.call_tool(tool_name, tool_args)
        assert result is not None
        assert not result.isError
        assert len(result.content) == len(expected_content)
        for i, content_block in enumerate(result.content):
            expected = expected_content[i]
            assert content_block.type == expected["type"]
            if expected["type"] == "text":
                assert content_block.text == expected["text"]
            elif expected["type"] == "image":
                assert content_block.data == expected["data"]
                assert content_block.mimeType == expected["mimeType"]
            elif expected["type"] == "audio":
                assert content_block.data == expected["data"]
                assert content_block.mimeType == expected["mimeType"]
            elif expected["type"] == "resource_link":
                assert str(content_block.uri) == expected["uri"]
                assert content_block.name == expected["name"]
            elif expected["type"] == "resource":
                assert str(content_block.resource.uri) == expected["resource"]["uri"]
                assert content_block.resource.mimeType == expected["resource"]["mimeType"]

        if result.structuredContent is not None and expected_structured_content is not None:
            assert result.structuredContent == expected_structured_content
        else:
            assert result.structuredContent is None or result.structuredContent == {}
    finally:
        await transport.close()


@pytest.mark.anyio
async def test_call_tool_grpc_transport_failing_tool(grpc_server: None, server_port: int):
    """Test GRPCTransportSession.call_tool() when the tool raises an exception."""
    transport = GRPCTransportSession(target=f"127.0.0.1:{server_port}")
    try:
        result = await transport.call_tool("failing_tool", {})

        assert result is not None
        assert result.isError
        assert len(result.content) == 1
        assert "Error executing tool failing_tool: This tool always fails" in result.content[0].text
    finally:
        await transport.close()


@pytest.mark.anyio
async def test_call_tool_grpc_transport_failure(server_port: int):
    """Test GRPCTransportSession.call_tool() when the transport fails."""
    transport = GRPCTransportSession(target=f"127.0.0.1:{server_port + 1}")
    try:
        with pytest.raises(McpError) as e:
            await transport.call_tool("greet", {"name": "Test"})
        assert e.value.error.code == -32603  # types.INTERNAL_ERROR
        assert "grpc.RpcError - Failed to call tool" in e.value.error.message
        assert "StatusCode.UNAVAILABLE" in e.value.error.message
        assert "Connection refused" in e.value.error.message
    finally:
        await transport.close()


@pytest.mark.anyio
async def test_send_notification_cancel(grpc_server: None, server_port: int):
    """Test GRPCTransportSession.send_notification() for cancellation."""
    transport = GRPCTransportSession(target=f"127.0.0.1:{server_port}")
    try:
        request_id = transport._request_counter + 1
        cancel_notification = types.ClientNotification(
            root=types.CancelledNotification(
                method="notifications/cancelled",
                params=types.CancelledNotificationParams(requestId=request_id),
            )
        )

        call_tool_task = asyncio.create_task(transport.call_tool("blocking_tool", {}))
        await asyncio.sleep(0.2)  # give call_tool time to start and populate _running_calls
        await transport.send_notification(cancel_notification)

        with pytest.raises(McpError) as e:
            await call_tool_task
        assert e.value.error.code == types.REQUEST_CANCELLED
        assert 'Tool call "blocking_tool" was cancelled' in e.value.error.message
    finally:
        await transport.close()
