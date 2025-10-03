from datetime import timedelta
import logging
import asyncio

import grpc
from grpc import aio
from google.protobuf import json_format
from jsonschema import ValidationError, SchemaError
from jsonschema.validators import validate

from mcp_grpc import types
from mcp_grpc.client.session_common import ElicitationFnT
from mcp_grpc.client.session_common import ListRootsFnT
from mcp_grpc.client.session_common import LoggingFnT
from mcp_grpc.client.session_common import MessageHandlerFnT
from mcp_grpc.client.session_common import SamplingFnT
from mcp_grpc.client.session_common import _validate_tool_result
from mcp_grpc.client.transport_session import TransportSession
from mcp_grpc.proto import mcp_pb2
from mcp_grpc.proto import mcp_pb2_grpc
from mcp_grpc.shared import convert
from mcp_grpc.shared.exceptions import McpError

from typing import Any

from mcp_grpc.shared.session import ProgressFnT
from mcp_grpc.types import ErrorData
from pydantic import AnyUrl


logger = logging.getLogger(__name__)


class GRPCTransportSession(TransportSession):
    """gRPC-based implementation of the Transport session.

    This class handles communication with the MCP gRPC server.
    """

    def __init__(
        self,
        target: str,
        channel_credential: grpc.ChannelCredentials | None = None,
        sampling_callback: SamplingFnT | None = None,
        elicitation_callback: ElicitationFnT | None = None,
        list_roots_callback: ListRootsFnT | None = None,
        logging_callback: LoggingFnT | None = None,
        message_handler: MessageHandlerFnT | None = None,
        client_info: types.Implementation | None = None,
        **kwargs,
    ) -> None:
      """Initialize the gRPC transport session."""
      logger.info("Creating GRPCTransportSession for target: %s", target)
      if channel_credential is not None:
          channel = aio.secure_channel(target, channel_credential, **kwargs)
      else:
          channel = aio.insecure_channel(target, **kwargs)

      stub = mcp_pb2_grpc.McpStub(channel)
      self.grpc_stub = stub
      self._channel = channel
      self._tool_output_schemas = {}
      self._request_counter = 0
      self._progress_callbacks: dict[str | int, ProgressFnT] = {}
      self._running_calls: dict[str | int, aio.Call] = {}
      logger.info("GRPCTransportSession created.")

    # TODO(asheshvidyut): Look into relevance of this API
    # b/448290917
    async def close(self) -> None:
      """Close the gRPC channel."""
      logger.info("Closing GRPCTransportSession channel.")
      await self._channel.close()
      logger.info("GRPCTransportSession channel closed.")

    def _cancel_request(self, request_id: str | int):
        """Cancel a running request by its ID."""
        call = self._running_calls.get(request_id)
        if call:
            logger.info("Cancelling request_id: %s", request_id)
            call.cancel()

    async def send_notification(self, notification: types.ClientNotification) -> None:
        """Sends a notification.

        If the notification is for cancellation, it triggers gRPC cancellation
        for the corresponding request.
        """
        if isinstance(notification.root, types.CancelledNotification):
            request_id = notification.root.params.requestId
            logger.info(
                "Received cancellation notification for request_id: %s",
                request_id,
            )
            self._cancel_request(request_id)
        else:
            logger.warning(
                "GRPCTransportSession.send_notification received unhandled "
                "notification type: %s",
                type(notification.root),
            )

    async def initialize(self) -> types.InitializeResult:
        """Send an initialize request."""
        ...

    async def send_ping(self):
      ...


    async def send_progress_notification(
        self,
        progress_token: str | int,
        progress: float,
        total: float | None = None,
        message: str | None = None,
    ) -> None:
      ...


    async def set_logging_level(self,
        level: types.LoggingLevel) -> types.EmptyResult:
      """Send a logging/setLevel request."""
      ...


    async def list_resources(self,
        cursor: str | None = None) -> types.ListResourcesResult:
      """Send a resources/list request."""
      ...


    async def list_resource_templates(self,
        cursor: str | None = None) -> types.ListResourceTemplatesResult:
        """Send a resources/templates/list request."""
        ...



    async def read_resource(self, uri: AnyUrl) -> types.ReadResourceResult:
        """Send a resources/read request."""
        ...


    async def subscribe_resource(self, uri: AnyUrl) -> types.EmptyResult:
        """Send a resources/subscribe request."""
        ...


    async def unsubscribe_resource(self, uri: AnyUrl) -> types.EmptyResult:
        """Send a resources/unsubscribe request."""
        ...

    async def request_generator(self,
                                name: str,
                                request_id: int,
                                arguments: Any | None = None):
        """Yields the single tool call request."""
        logger.info(
            "Calling tool '%s' with request_id: %s", name, request_id
        )
        yield types.CallToolRequestParams(
            name=name,
            arguments=arguments or {},
            _meta=types.RequestParams.Meta(
                progressToken=request_id
            ),
        )


    async def call_tool(
        self,
        name: str,
        arguments: Any | None = None,
        read_timeout_seconds: timedelta | None = None,
        progress_callback: ProgressFnT | None = None,
    ) -> types.CallToolResult:
        """Send a tools/call request with optional progress callback support."""
        self._request_counter = 1
        request_id = self._request_counter

        if progress_callback:
            self._progress_callbacks[request_id] = progress_callback

        proto_results = []
        try:
            request_iterator = convert.generate_call_tool_requests(
                self.request_generator(name, request_id, arguments)
            )
            # TODO(asheshvidyut): Add timeout support for CallTool
            call = self.grpc_stub.CallTool(
                request_iterator
            )
            self._running_calls[request_id] = call
            async for response in call:

                if response.common.HasField("progress"):
                    progress_proto = response.common.progress
                    progress_token = progress_proto.progress_token

                    if progress_token in self._progress_callbacks:
                        callback = self._progress_callbacks[progress_token]
                        await callback(
                            progress_proto.progress,
                            progress_proto.total or None,
                            progress_proto.message or None,
                        )
                    continue

                if response.HasField("result"):
                    proto_results.append(response.result)

            final_result = convert.proto_result_to_types_result(proto_results)

        except asyncio.CancelledError as e:
            raise McpError(
                ErrorData(
                    code=types.REQUEST_CANCELLED,
                    message=f'Tool call "{name}" was cancelled',
                )
            ) from e
        except json_format.ParseError as e:
            error_message = f'Failed to parse tool proto: {e}'
            logger.error(error_message, exc_info=True)
            raise McpError(
                ErrorData(code=types.PARSE_ERROR, message=error_message)
            ) from e
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.CANCELLED:
                raise McpError(
                    ErrorData(
                        code=types.REQUEST_CANCELLED,
                        message=f'Tool call "{name}" was cancelled',
                    )
                ) from e
            error_message = f'grpc.RpcError - Failed to call tool "{name}": {e}'
            logger.error(error_message, exc_info=True)
            raise McpError(
                ErrorData(code=types.INTERNAL_ERROR, message=error_message)
            ) from e
        except Exception as e:
            error_message = (
                f'An unexpected error occurred during CallTool: {e}'
            )
            logger.error(error_message, exc_info=True)
            raise McpError(
                ErrorData(code=types.INTERNAL_ERROR, message=error_message)
            ) from e
        finally:
            self._running_calls.pop(request_id, None)
            self._progress_callbacks.pop(request_id, None)

        return await self._validate_and_return_result(name, final_result)

    async def _validate_and_return_result(
        self, name: str, final_result: types.CallToolResult | None
    ) -> types.CallToolResult:
        """Validate and return the tool result."""
        if final_result is None:
            raise McpError(
                ErrorData(
                    code=types.INTERNAL_ERROR,
                    message="Tool call did not produce a result.",
                )
            )

        # TODO(asheshvidyut): check and verify all the error handling cases
        # b/448303754
        if not final_result.isError:
            try:
                await self._validate_tool_result(name, final_result)
            except RuntimeError as e:
                error_message = f'Tool result validation failed for "{name}": {e}'
                logger.error(error_message, exc_info=True)
                raise McpError(
                    ErrorData(code=types.INTERNAL_ERROR, message=error_message)
                ) from e
        return final_result

    async def _validate_tool_result(self, name: str,
        result: types.CallToolResult) -> None:
        """Validate the structured content of a tool result against its output schema."""
        if name not in self._tool_output_schemas:
            # refresh output schema cache
            await self.list_tools()

        if name in self._tool_output_schemas:
            output_schema = self._tool_output_schemas.get(name)
            await _validate_tool_result(output_schema, name, result)
        else:
            logger.warning(
                "Tool %s not listed by server, cannot validate any structured"
                " content",
                name,
            )

    async def list_prompts(self,
        cursor: str | None = None) -> types.ListPromptsResult:
        """Send a prompts/list request."""
        ...


    async def get_prompt(self, name: str,
        arguments: dict[str, str] | None = None) -> types.GetPromptResult:
        """Send a prompts/get request."""
        ...


    async def complete(
        self,
        ref: types.ResourceTemplateReference | types.PromptReference,
        argument: dict[str, str],
        context_arguments: dict[str, str] | None = None,
    ) -> types.CompleteResult:
        """Send a completion/complete request."""
        ...


    async def list_tools(
        self,
        cursor: str | None = None,
    ) -> types.ListToolsResult:
        """Send a tools/list request."""
        request = mcp_pb2.ListToolsRequest(
            common=mcp_pb2.RequestFields(
                protocol_version=mcp_pb2.VERSION_20250326,
                cursor=cursor,
            )
        )

        try:
            # Send the request using gRPC stub
            response = await self.grpc_stub.ListTools(request)

            # Convert gRPC response to ListToolsResult
            tools = convert.tool_protos_to_types(response.tools)

            # Cache tool output schemas for future validation
            for tool in tools:
                self._tool_output_schemas[tool.name] = tool.outputSchema
        except json_format.ParseError as e:
            error_message = f'Failed to parse tool proto: {e}'
            logger.error(error_message, exc_info=True)
            error_data = ErrorData(
                code=types.PARSE_ERROR,
                message=error_message,
            )
            raise McpError(error_data) from e
        except RuntimeError as e:
            error_message = f'Failed to convert tool proto to type: {e}'
            logger.error(error_message, exc_info=True)
            error_data = ErrorData(
                code=types.INTERNAL_ERROR,
                message=error_message,
            )
            raise McpError(error_data) from e
        except grpc.RpcError as e:
            error_message = f'grpc.RpcError - Failed to list tools: {e}'
            logger.error(error_message, exc_info=True)
            error_data = ErrorData(
                code=types.INTERNAL_ERROR,
                message=error_message,
            )
            raise McpError(error_data) from e
        return types.ListToolsResult(tools=tools)

    async def send_roots_list_changed(self) -> None:
        """Send a roots/list_changed notification."""
        ...