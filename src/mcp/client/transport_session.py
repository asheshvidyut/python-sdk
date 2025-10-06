from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Any

from pydantic import AnyUrl

from mcp import types
from mcp.shared.session import ProgressFnT


class TransportSession(ABC):
    """Abstract base class for communication transports."""

    @abstractmethod
    async def initialize(self) -> types.InitializeResult:
        """Send an initialize request."""
        ...

    @abstractmethod
    async def send_ping(self): ...

    @abstractmethod
    async def send_progress_notification(
        self,
        progress_token: str | int,
        progress: float,
        total: float | None = None,
        message: str | None = None,
    ) -> None: ...

    @abstractmethod
    async def set_logging_level(
        self,
        level: types.LoggingLevel,
    ) -> types.EmptyResult:
        """Send a logging/setLevel request."""
        ...

    @abstractmethod
    async def list_resources(
        self,
        cursor: str | None = None,
    ) -> types.ListResourcesResult:
        """Send a resources/list request."""
        ...

    @abstractmethod
    async def list_resource_templates(
        self,
        cursor: str | None = None,
    ) -> types.ListResourceTemplatesResult:
        """Send a resources/templates/list request."""
        ...

    @abstractmethod
    async def read_resource(self, uri: AnyUrl) -> types.ReadResourceResult:
        """Send a resources/read request."""
        ...

    @abstractmethod
    async def subscribe_resource(self, uri: AnyUrl) -> types.EmptyResult:
        """Send a resources/subscribe request."""
        ...

    @abstractmethod
    async def unsubscribe_resource(self, uri: AnyUrl) -> types.EmptyResult:
        """Send a resources/unsubscribe request."""
        ...

    @abstractmethod
    async def call_tool(
        self,
        name: str,
        arguments: Any | None = None,
        read_timeout_seconds: timedelta | None = None,
        progress_callback: ProgressFnT | None = None,
    ) -> types.CallToolResult:
        """Send a tools/call request with optional progress callback support."""
        ...

    @abstractmethod
    async def _validate_tool_result(
        self,
        name: str,
        result: types.CallToolResult,
    ) -> None:
        """Validate the structured content of a tool result against its output
        schema."""
        ...

    @abstractmethod
    async def list_prompts(
        self,
        cursor: str | None = None,
    ) -> types.ListPromptsResult:
        """Send a prompts/list request."""
        ...

    @abstractmethod
    async def get_prompt(
        self,
        name: str,
        arguments: dict[str, str] | None = None,
    ) -> types.GetPromptResult:
        """Send a prompts/get request."""
        ...

    @abstractmethod
    async def complete(
        self,
        ref: types.ResourceTemplateReference | types.PromptReference,
        argument: dict[str, str],
        context_arguments: dict[str, str] | None = None,
    ) -> types.CompleteResult:
        """Send a completion/complete request."""
        ...

    @abstractmethod
    async def list_tools(
        self,
        cursor: str | None = None,
    ) -> types.ListToolsResult:
        """Send a tools/list request."""
        ...

    @abstractmethod
    async def send_roots_list_changed(self) -> None:
        """Send a roots/list_changed notification."""
        ...
