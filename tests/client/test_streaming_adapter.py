import pytest

import mcp.types as types
from mcp.client.streaming_adapter import StreamingAdapter


@pytest.mark.anyio
async def test_streaming_adapter_uses_native_stream() -> None:
    class NativeTransport:
        async def _stream_list_tools_native(self, *, cursor=None, params=None):
            yield types.Tool(
                name="native",
                description=None,
                inputSchema={"type": "object"},
            )

        async def list_tools(self, *args, **kwargs):  # pragma: no cover - should not be called
            raise AssertionError("list_tools should not be called for native streaming")

    adapter = StreamingAdapter(NativeTransport())
    tools = [tool async for tool in adapter.stream_list_tools()]

    assert [tool.name for tool in tools] == ["native"]


@pytest.mark.anyio
async def test_streaming_adapter_cursor_fallback() -> None:
    class CursorTransport:
        def __init__(self) -> None:
            self._calls = 0

        async def list_tools(self, cursor=None, params=None):
            self._calls += 1
            if self._calls == 1:
                return types.ListToolsResult(
                    tools=[
                        types.Tool(
                            name="page1",
                            description=None,
                            inputSchema={"type": "object"},
                        )
                    ],
                    nextCursor="next",
                )
            return types.ListToolsResult(
                tools=[
                    types.Tool(
                        name="page2",
                        description=None,
                        inputSchema={"type": "object"},
                    )
                ],
                nextCursor=None,
            )

    transport = CursorTransport()
    adapter = StreamingAdapter(transport)
    tools = [tool async for tool in adapter.stream_list_tools()]

    assert [tool.name for tool in tools] == ["page1", "page2"]
