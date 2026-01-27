"""
FastMCP server that streams prompt completion tokens.
"""

import asyncio

from mcp import StreamPromptCompletionChunk
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("Streaming Prompt Completion")


@mcp.stream_prompt_completion()
async def stream_prompt_completion(name: str, arguments: dict[str, str] | None):
    query = (arguments or {}).get("q", "")
    tokens = [f"Prompt {name}: ", query, " ...done"]

    for token in tokens[:-1]:
        yield StreamPromptCompletionChunk(token=token)
        await asyncio.sleep(0.05)

    yield StreamPromptCompletionChunk(
        token=tokens[-1],
        isFinal=True,
        finishReason="stop",
    )
