"""Defines the session object for gRPC tool calls."""

import asyncio
import logging

from mcp.proto import mcp_pb2

logger = logging.getLogger(__name__)


class GrpcSession:
    """A session object for gRPC tool calls that uses a queue."""

    def __init__(self, response_queue: asyncio.Queue):
        self._response_queue = response_queue

    async def send_log_message(self, level, data, **kwargs):
        """Logs tool messages to the server console."""
        logger.info("Tool log (%s): %s", level, data)

    async def send_progress_notification(self, progress_token, progress, total, message):
        """Puts a progress notification onto the response queue."""
        progress_proto = mcp_pb2.ProgressNotification(
            progress_token=str(progress_token),
            progress=progress,
        )
        if total is not None:
            progress_proto.total = total
        if message is not None:
            progress_proto.message = message

        response = mcp_pb2.CallToolResponse(common=mcp_pb2.ResponseFields(progress=progress_proto))
        await self._response_queue.put(response)
