import logging
import asyncio
import grpc
import sys
import os

# Import generated classes
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import mcp_pb2
import mcp_pb2_grpc

async def run():
    print("Connecting to gRPC server...")
    async with grpc.aio.insecure_channel('localhost:50051') as channel:
        stub = mcp_pb2_grpc.McpStub(channel)
        
        print("Calling ListResources...")
        try:
            response = await stub.ListResources(mcp_pb2.ListResourcesRequest())
            print("Resources received:")
            for res in response.resources:
                print(f"- Name: {res.name}")
                print(f"  URI: {res.uri}")
                print(f"  MimeType: {res.mime_type}")
        except grpc.RpcError as e:
            print(f"RPC failed: {e}")

if __name__ == '__main__':
    logging.basicConfig()
    asyncio.run(run())
