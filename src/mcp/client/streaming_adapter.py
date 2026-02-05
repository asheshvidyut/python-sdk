from __future__ import annotations

from collections.abc import AsyncIterator

import mcp.types as types
from mcp.client.transport_session import ClientTransportSession


class StreamingAdapter:
    """Provides a streaming interface over any transport."""

    def __init__(self, transport: ClientTransportSession) -> None:
        self._transport = transport

    async def stream_list_tools(
        self,
        *,
        cursor: str | None = None,
        params: types.PaginatedRequestParams | None = None,
    ) -> AsyncIterator[types.Tool]:
        native = getattr(self._transport, "_stream_list_tools_native", None)
        if callable(native):
            async for tool in native(cursor=cursor, params=params):
                yield tool
            return

        cursor_value = params.cursor if params else cursor
        current_params = params
        while True:
            result = await self._transport.list_tools(
                cursor=None if current_params else cursor_value,
                params=current_params,
            )
            for tool in result.tools:
                yield tool
            cursor_value = result.next_cursor
            if cursor_value is None:
                break
            if current_params is not None:
                current_params = types.PaginatedRequestParams(
                    cursor=cursor_value,
                    meta=current_params.meta,
                )

    async def list_tools(
        self,
        *,
        cursor: str | None = None,
        params: types.PaginatedRequestParams | None = None,
    ) -> types.ListToolsResult:
        tools = [tool async for tool in self.stream_list_tools(cursor=cursor, params=params)]
        return types.ListToolsResult(tools=tools, next_cursor=None)

    async def stream_list_resources(
        self,
        *,
        cursor: str | None = None,
    ) -> AsyncIterator[types.Resource]:
        native = getattr(self._transport, "_stream_list_resources_native", None)
        if callable(native):
            async for resource in native(cursor=cursor):
                yield resource
            return

        cursor_value = cursor
        while True:
            result = await self._transport.list_resources(cursor=cursor_value)
            for resource in result.resources:
                yield resource
            cursor_value = result.next_cursor
            if cursor_value is None:
                break

    async def list_resources(
        self,
        *,
        cursor: str | None = None,
    ) -> types.ListResourcesResult:
        resources = [
            resource async for resource in self.stream_list_resources(cursor=cursor)
        ]
        return types.ListResourcesResult(resources=resources, next_cursor=None)

    async def stream_list_resource_templates(
        self,
        *,
        cursor: str | None = None,
    ) -> AsyncIterator[types.ResourceTemplate]:
        native = getattr(self._transport, "_stream_list_resource_templates_native", None)
        if callable(native):
            async for template in native(cursor=cursor):
                yield template
            return

        cursor_value = cursor
        while True:
            result = await self._transport.list_resource_templates(cursor=cursor_value)
            for template in result.resourceTemplates:
                yield template
            cursor_value = result.next_cursor
            if cursor_value is None:
                break

    async def list_resource_templates(
        self,
        *,
        cursor: str | None = None,
    ) -> types.ListResourceTemplatesResult:
        templates = [
            template
            async for template in self.stream_list_resource_templates(cursor=cursor)
        ]
        return types.ListResourceTemplatesResult(
            resourceTemplates=templates,
            next_cursor=None,
        )

    async def stream_list_prompts(
        self,
        *,
        cursor: str | None = None,
    ) -> AsyncIterator[types.Prompt]:
        native = getattr(self._transport, "_stream_list_prompts_native", None)
        if callable(native):
            async for prompt in native(cursor=cursor):
                yield prompt
            return

        cursor_value = cursor
        while True:
            result = await self._transport.list_prompts(cursor=cursor_value)
            for prompt in result.prompts:
                yield prompt
            cursor_value = result.next_cursor
            if cursor_value is None:
                break

    async def list_prompts(
        self,
        *,
        cursor: str | None = None,
    ) -> types.ListPromptsResult:
        prompts = [prompt async for prompt in self.stream_list_prompts(cursor=cursor)]
        return types.ListPromptsResult(prompts=prompts, next_cursor=None)
