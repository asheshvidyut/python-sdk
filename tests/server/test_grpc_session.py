import asyncio
import unittest.mock

import pytest

from mcp.proto import mcp_pb2
from mcp.server import grpc_session


@pytest.mark.anyio
async def test_send_log_message():
    """Test that send_log_message logs to the logger."""
    queue = asyncio.Queue()
    session = grpc_session.GrpcSession(queue)

    with unittest.mock.patch.object(grpc_session.logger, "info") as mock_info:
        await session.send_log_message("info", "test message", extra_kwarg="value")
        mock_info.assert_called_once_with("Tool log (%s): %s", "info", "test message")


@pytest.mark.anyio
async def test_send_progress_notification_all_fields():
    """Test send_progress_notification with all fields."""
    queue = asyncio.Queue()
    session = grpc_session.GrpcSession(queue)

    await session.send_progress_notification("token1", 50, 100, "In progress")

    response = await queue.get()
    assert isinstance(response, mcp_pb2.CallToolResponse)
    assert response.common.progress.progress_token == "token1"
    assert response.common.progress.progress == 50
    assert response.common.progress.total == 100
    assert response.common.progress.message == "In progress"
    assert queue.empty()


@pytest.mark.anyio
async def test_send_progress_notification_minimal_fields():
    """Test send_progress_notification with minimal fields."""
    queue = asyncio.Queue()
    session = grpc_session.GrpcSession(queue)

    await session.send_progress_notification("token2", 75, None, None)

    response = await queue.get()
    assert isinstance(response, mcp_pb2.CallToolResponse)
    assert response.common.progress.progress_token == "token2"
    assert response.common.progress.progress == 75
    assert response.common.progress.total == 0  # Default for int32 in proto3
    assert response.common.progress.message == ""  # Default for string in proto3
    assert queue.empty()
