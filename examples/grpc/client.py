import logging
import asyncio
import grpc
import sys
import os

# Import generated classes
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import mcp_pb2
import mcp_pb2_grpc

from mcp.client.session import ClientSession
from examples.grpc.grpc_client import grpc_client_channel

async def run():
    print("Connecting to gRPC server...")
    async with grpc.aio.insecure_channel('localhost:50051') as channel:
        # Use our custom gRPC transport adapter
        async with grpc_client_channel(channel) as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream) as session:
                print("Initializing...")
                await session.initialize()
                
                print("\nCalling ListResources...")
                resources = await session.list_resources()
                print("Resources received:")
                for res in resources.resources:
                    print(f"- Name: {res.name}")
                    print(f"  URI: {res.uri}")
                    print(f"  MimeType: {res.mimeType}")

                print("\nCalling ListTools...")
                tools = await session.list_tools()
                print("Tools received:")
                for tool in tools.tools:
                    print(f"- Name: {tool.name}")
                    print(f"  Description: {tool.description}")

                print("\nCalling CallTool 'add' with a=10, b=20...")
                result = await session.call_tool("add", arguments={"a": 10, "b": 20})
                print("Tool Response:")
                for content in result.content:
                    if content.type == "text":
                         print(f"  Type: text")
                         print(f"  Text: {content.text}")
                    elif content.type == "image":
                         print(f"  Type: image")
                         print(f"  Data: {len(content.data)} bytes")

if __name__ == '__main__':
    logging.basicConfig()
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
