import asyncio
import socket
from collections.abc import Generator
import json
import os

import grpc
import pytest
from google.protobuf import json_format
from google.protobuf import struct_pb2
from mcp import types
from mcp.client.grpc_transport_session import GRPCTransportSession
from mcp.proto import mcp_pb2, mcp_pb2_grpc
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
        """A simple greeting tool."""
        greeting = f"Hello, {name}! Welcome to the Simple gRPC Server!"
        return greeting

    @mcp.tool()
    def test_tool(a: int, b: int) -> int:
        """A test tool that adds two numbers."""
        return a + b

    @mcp.tool()
    def failing_tool() -> str:
        """A tool that always fails."""
        raise ValueError("This tool is designed to fail.")

    @mcp.tool()
    def list_tool() -> list[str]:
        """A tool that returns a list of strings."""
        return ["one", "two"]

    @mcp.tool()
    def dict_tool() -> dict:
        """A tool that returns a dict."""
        return {"key": "value"}

    @mcp.tool()
    async def blocking_tool():
        """A tool that blocks until cancelled."""
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            raise

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

    yield
    await server.stop(None)


@pytest.fixture
async def grpc_stub(server_port: int) -> Generator[mcp_pb2_grpc.McpStub, None, None]:
    """Create a gRPC client stub."""
    async with grpc.aio.insecure_channel(f"127.0.0.1:{server_port}") as channel:
        stub = mcp_pb2_grpc.McpStub(channel)
        yield stub


@pytest.mark.skipif("COVERAGE_OUTPUT_FILE" in os.environ, reason="Crashes under coverage")
@pytest.mark.anyio
async def test_list_tools_grpc(grpc_server: None, grpc_stub: mcp_pb2_grpc.McpStub):
    """Test ListTools via gRPC."""
    request = mcp_pb2.ListToolsRequest()
    response = await grpc_stub.ListTools(request)

    assert response is not None
    assert len(response.tools) == 6
    assert response.common.protocol_version == mcp_pb2.VERSION_20250326

    tools_by_name = {tool.name: tool for tool in response.tools}

    expected_tools = {
        "greet": {
            "name": "greet",
            "description": "A simple greeting tool.",
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
            "inputSchema": {
                "properties": {},
                "title": "failing_toolArguments",
                "type": "object",
            },
            "outputSchema": {
                "properties": {"result": {"title": "Result", "type": "string"}},
                "required": ["result"],
                "title": "failing_toolOutput",
                "type": "object",
            },
        },
        "list_tool": {
            "name": "list_tool",
            "description": "A tool that returns a list of strings.",
            "inputSchema": {
                "properties": {},
                "title": "list_toolArguments",
                "type": "object",
            },
            "outputSchema": {
                "properties": {
                    "result": {
                        "items": {"type": "string"},
                        "title": "Result",
                        "type": "array",
                    }
                },
                "required": ["result"],
                "title": "list_toolOutput",
                "type": "object",
            },
        },
        "dict_tool": {
            "name": "dict_tool",
            "description": "A tool that returns a dict.",
            "inputSchema": {
                "properties": {},
                "title": "dict_toolArguments",
                "type": "object",
            },
            "outputSchema": {},
        },
        "blocking_tool": {
            "name": "blocking_tool",
            "description": "A tool that blocks until cancelled.",
            "inputSchema": {
                "properties": {},
                "title": "blocking_toolArguments",
                "type": "object",
            },
            "outputSchema": {},
        },
    }

    assert tools_by_name.keys() == expected_tools.keys()

    for tool_name, tool in tools_by_name.items():
        expected_tool = expected_tools[tool_name]
        assert tool.name == expected_tool["name"]
        assert tool.description == expected_tool["description"]
        assert json_format.MessageToDict(tool.input_schema) == expected_tool["inputSchema"]
        assert json_format.MessageToDict(tool.output_schema) == expected_tool["outputSchema"]


def setup_failing_test_server(port: int) -> FastMCP:
    """Set up a FastMCP server that fails on list_tools."""
    mcp = FastMCP(
        name="Failing Test gRPC Server",
        host="127.0.0.1",
        port=port,
    )

    async def failing_list_tools():
        raise RuntimeError("This is an intentional error")

    mcp.list_tools = failing_list_tools
    return mcp


@pytest.fixture
def failing_server_port() -> int:
    """Find an available port for the server."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
async def failing_grpc_server(failing_server_port: int) -> Generator[None, None, None]:
    """Start a gRPC server in process that fails on list_tools."""
    server_instance = setup_failing_test_server(failing_server_port)
    server = await create_mcp_grpc_server(target=f"127.0.0.1:{failing_server_port}", mcp_server=server_instance)

    yield
    await server.stop(None)


@pytest.fixture
async def failing_grpc_stub(
    failing_server_port: int,
) -> Generator[mcp_pb2_grpc.McpStub, None, None]:
    """Create a gRPC client stub for failing server."""
    async with grpc.aio.insecure_channel(f"127.0.0.1:{failing_server_port}") as channel:
        stub = mcp_pb2_grpc.McpStub(channel)
        yield stub


@pytest.mark.skipif("COVERAGE_OUTPUT_FILE" in os.environ, reason="Crashes under coverage")
@pytest.mark.anyio
async def test_list_tools_grpc_error(failing_grpc_server: None, failing_grpc_stub: mcp_pb2_grpc.McpStub):
    """Test ListTools via gRPC when server handler raises an error."""
    request = mcp_pb2.ListToolsRequest()
    with pytest.raises(grpc.aio.AioRpcError) as excinfo:
        await failing_grpc_stub.ListTools(request)

    assert excinfo.value.code() == grpc.StatusCode.INTERNAL
    assert "This is an intentional error" in excinfo.value.details()


@pytest.mark.skipif("COVERAGE_OUTPUT_FILE" in os.environ, reason="Crashes under coverage")
@pytest.mark.anyio
async def test_call_tool_grpc_greet(grpc_server: None, grpc_stub: mcp_pb2_grpc.McpStub):
    """Test CallTool via gRPC with greet tool."""
    tool_name = "greet"
    arguments = {"name": "Test"}
    args_struct = struct_pb2.Struct()
    json_format.ParseDict(arguments, args_struct)

    request = mcp_pb2.CallToolRequest(request=mcp_pb2.CallToolRequest.Request(name=tool_name, arguments=args_struct))

    async def request_iterator():
        yield request

    responses = []
    async for response in grpc_stub.CallTool(request_iterator()):
        responses.append(response)

    assert len(responses) == 2
    assert responses[0].result.text.text == "Hello, Test! Welcome to the Simple gRPC Server!"
    assert not responses[0].result.is_error
    assert responses[1].result.structured_content["result"] == "Hello, Test! Welcome to the Simple gRPC Server!"
    assert not responses[1].result.is_error


@pytest.mark.skipif("COVERAGE_OUTPUT_FILE" in os.environ, reason="Crashes under coverage")
@pytest.mark.anyio
async def test_call_tool_grpc_test_tool(grpc_server: None, grpc_stub: mcp_pb2_grpc.McpStub):
    """Test CallTool via gRPC with test_tool."""
    tool_name = "test_tool"
    arguments = {"a": 1, "b": 2}
    args_struct = struct_pb2.Struct()
    json_format.ParseDict(arguments, args_struct)

    request = mcp_pb2.CallToolRequest(request=mcp_pb2.CallToolRequest.Request(name=tool_name, arguments=args_struct))

    async def request_iterator():
        yield request

    responses = []
    async for response in grpc_stub.CallTool(request_iterator()):
        responses.append(response)

    assert len(responses) == 2
    assert responses[0].result.text.text == "3"
    assert not responses[0].result.is_error
    assert responses[1].result.structured_content["result"] == 3
    assert not responses[1].result.is_error


@pytest.mark.skipif("COVERAGE_OUTPUT_FILE" in os.environ, reason="Crashes under coverage")
@pytest.mark.anyio
async def test_call_failing_tool_grpc(grpc_server: None, grpc_stub: mcp_pb2_grpc.McpStub):
    """Test CallTool with a tool that raises an error."""
    tool_name = "failing_tool"
    arguments = {}
    args_struct = struct_pb2.Struct()
    json_format.ParseDict(arguments, args_struct)

    request = mcp_pb2.CallToolRequest(request=mcp_pb2.CallToolRequest.Request(name=tool_name, arguments=args_struct))

    async def request_iterator():
        yield request

    responses = []
    async for response in grpc_stub.CallTool(request_iterator()):
        responses.append(response)

    assert len(responses) == 1
    assert responses[0].result.is_error
    assert "Error executing tool failing_tool: This tool is designed to fail." in responses[0].result.text.text


@pytest.mark.skipif("COVERAGE_OUTPUT_FILE" in os.environ, reason="Crashes under coverage")
@pytest.mark.anyio
async def test_call_tool_grpc_list_tool(grpc_server: None, grpc_stub: mcp_pb2_grpc.McpStub):
    """Test CallTool via gRPC with list_tool."""
    tool_name = "list_tool"
    arguments = {}
    args_struct = struct_pb2.Struct()
    json_format.ParseDict(arguments, args_struct)

    request = mcp_pb2.CallToolRequest(request=mcp_pb2.CallToolRequest.Request(name=tool_name, arguments=args_struct))

    async def request_iterator():
        yield request

    responses = []
    async for response in grpc_stub.CallTool(request_iterator()):
        responses.append(response)

    assert len(responses) == 3
    assert responses[0].result.text.text == "one"
    assert not responses[0].result.is_error
    assert responses[1].result.text.text == "two"
    assert not responses[1].result.is_error
    assert responses[2].result.structured_content["result"] == ["one", "two"]
    assert not responses[2].result.is_error


@pytest.mark.skipif("COVERAGE_OUTPUT_FILE" in os.environ, reason="Crashes under coverage")
@pytest.mark.anyio
async def test_call_tool_grpc_no_initial_request(grpc_server: None, grpc_stub: mcp_pb2_grpc.McpStub):
    """Test CallTool via gRPC with no initial request."""
    request = mcp_pb2.CallToolRequest()

    async def request_iterator():
        yield request

    responses = []
    async for response in grpc_stub.CallTool(request_iterator()):
        responses.append(response)

    assert len(responses) == 1
    assert responses[0].result.is_error
    assert "Initial request cannot be empty." in responses[0].result.text.text


@pytest.mark.skipif("COVERAGE_OUTPUT_FILE" in os.environ, reason="Crashes under coverage")
@pytest.mark.anyio
async def test_call_tool_grpc_dict_tool(grpc_server: None, grpc_stub: mcp_pb2_grpc.McpStub):
    """Test CallTool via gRPC with dict_tool."""
    tool_name = "dict_tool"
    arguments = {}
    args_struct = struct_pb2.Struct()
    json_format.ParseDict(arguments, args_struct)

    request = mcp_pb2.CallToolRequest(request=mcp_pb2.CallToolRequest.Request(name=tool_name, arguments=args_struct))

    async def request_iterator():
        yield request

    responses = []
    async for response in grpc_stub.CallTool(request_iterator()):
        responses.append(response)

    assert len(responses) == 1
    assert json.loads(responses[0].result.text.text) == {"key": "value"}
    assert not responses[0].result.is_error


@pytest.mark.skipif("COVERAGE_OUTPUT_FILE" in os.environ, reason="Crashes under coverage")
@pytest.mark.anyio
async def test_server_handles_client_cancellation(grpc_server: None, server_port: int):
    """
    Test that McpServicer.CallTool's cancellation handling is triggered
    when a client cancels a call. This test indirectly verifies that
    the `except asyncio.CancelledError` block in `McpServicer.CallTool`
    is entered, and that it correctly cancels the tool-running task.
    """
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
        await asyncio.sleep(0.2)  # give call_tool time to start
        await transport.send_notification(cancel_notification)

        with pytest.raises(McpError) as e:
            await call_tool_task
        assert e.value.error.code == types.REQUEST_CANCELLED
        # If we reach here without timeout, it means server-side
        # cancellation handling in McpServicer.CallTool worked and cancelled
        # the tool task running blocking_tool, allowing call_tool to
        # receive cancellation from server and raise McpError quickly.
    finally:
        await transport.close()
