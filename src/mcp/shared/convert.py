"""Utilities for converting between MCP types and protobuf messages."""

import base64
import logging
from typing import Any, Sequence
from collections.abc import AsyncGenerator

from google.protobuf import json_format
from google.protobuf.message import Message

from mcp import types
from mcp.types import ErrorData
from mcp.shared.exceptions import McpError
from mcp.proto import mcp_pb2
from google.protobuf import struct_pb2

logger = logging.getLogger(__name__)


def tool_proto_to_type(tool_proto: Message) -> types.Tool:
  """Converts a Tool protobuf message to a types.Tool object."""
  try:
    input_schema = json_format.MessageToDict(tool_proto.input_schema)
    output_schema = json_format.MessageToDict(tool_proto.output_schema)
  except json_format.ParseError as e:
    error_message = f'Failed to parse tool schema for {tool_proto.name}: {e}'
    logger.error(error_message, exc_info=True)
    raise e

  return types.Tool(
      name=tool_proto.name,
      description=tool_proto.description,
      inputSchema=input_schema,
      outputSchema=output_schema,
  )


def tool_type_to_proto(tool: types.Tool) -> mcp_pb2.Tool:
  """Converts a types.Tool object to a Tool protobuf message."""
  input_schema_dict = getattr(tool, "inputSchema", {})
  input_schema = struct_pb2.Struct()
  if input_schema_dict:
    try:
      json_format.ParseDict(input_schema_dict, input_schema)
    except json_format.ParseError as e:
      error_message = f'Failed to parse inputSchema for tool {tool.name}: {e}'
      logger.error(error_message, exc_info=True)
      raise e

  output_schema_dict = getattr(tool, "outputSchema", {})
  output_schema = struct_pb2.Struct()
  if output_schema_dict:
    try:
      json_format.ParseDict(output_schema_dict, output_schema)
    except json_format.ParseError as e:
      error_message = f'Failed to parse outputSchema for tool {tool.name}: {e}'
      logger.error(error_message, exc_info=True)
      raise e

  # TODO(asheshvidyut): Add annotations once usecase is clear
  return mcp_pb2.Tool(
      name=tool.name,
      title=tool.title,
      description=tool.description,
      input_schema=input_schema,
      output_schema=output_schema,
  )


def tool_types_to_protos(tools: list[types.Tool]) -> list[mcp_pb2.Tool]:
  """Converts a list of types.Tool to a list of Tool protos."""
  return [tool_type_to_proto(tool) for tool in tools]


def tool_protos_to_types(tool_protos: list[Message]) -> list[types.Tool]:
  """Converts a list of Tool protos to a list of types.Tool."""
  return [tool_proto_to_type(tool_proto) for tool_proto in tool_protos]


def _populate_result_from_content_block(
    content_block: dict, result: mcp_pb2.CallToolResponse.Result
) -> bool:
  """Populates the result proto from a single content block."""
  if not isinstance(content_block, dict):
    return False

  content_type = content_block.get("type")
  match content_type:
    case "text":
      if "text" in content_block and isinstance(content_block["text"], str):
        result.text.text = content_block["text"]
        return True
    case "image":
      image_data = content_block
      if "data" in image_data and "mimeType" in image_data:
        result.image.data = base64.b64decode(image_data["data"])
        result.image.mime_type = image_data["mimeType"]
        return True
    case "audio":
      audio_data = content_block
      if "data" in audio_data and "mimeType" in audio_data:
        result.audio.data = base64.b64decode(audio_data["data"])
        result.audio.mime_type = audio_data["mimeType"]
        return True
    case "resource": # EmbeddedResource
      embedded_resource_data = content_block
      if "resource" in embedded_resource_data and isinstance(
          embedded_resource_data["resource"], dict
      ):
        resource_contents = embedded_resource_data["resource"]
        if "uri" in resource_contents:
          result.embedded_resource.contents.uri = str(resource_contents["uri"])
        if "mimeType" in resource_contents:
          result.embedded_resource.contents.mime_type = (
              resource_contents["mimeType"]
          )
        if "text" in resource_contents:
          result.embedded_resource.contents.text = resource_contents["text"]
          return True
        elif "blob" in resource_contents:
          result.embedded_resource.contents.blob = base64.b64decode(
              resource_contents["blob"]
          )
          return True
    case "resource_link": # ResourceLink
      resource_link_data = content_block
      if "uri" in resource_link_data:
        result.resource_link.uri = str(resource_link_data["uri"])
        if "name" in resource_link_data:
          result.resource_link.name = resource_link_data["name"]
        return True
  return False


def _is_content_block(item: Any) -> bool:
  """Check if an item looks like a content block."""
  if isinstance(
      item,
      (
          types.TextContent,
          types.ImageContent,
          types.AudioContent,
          types.EmbeddedResource,
          types.ResourceLink,
      ),
  ):
    return True
  if hasattr(item, "model_dump"):
    try:
      d = item.model_dump()
      return isinstance(d, dict) and "type" in d
    except Exception:
      return False
  return isinstance(item, dict) and "type" in item


def _handle_string_tool_output(tool_output: str):
  """Handles a string tool output."""
  result = mcp_pb2.CallToolResponse.Result()
  result.structured_content.fields["result"].string_value = tool_output
  return result

def _handle_dict_tool_output(tool_output: dict):
  if _is_content_block(tool_output):
    result = mcp_pb2.CallToolResponse.Result()
    if _populate_result_from_content_block(tool_output, result):
      return [result]
  if "text" in tool_output and all(k in ("text", "is_error") for k in tool_output):
    result = mcp_pb2.CallToolResponse.Result()
    result.text.text = tool_output["text"]
    if "is_error" in tool_output:
      result.is_error = tool_output["is_error"]
    return result

  if "structuredContent" in tool_output and len(tool_output) == 1:
    result = mcp_pb2.CallToolResponse.Result()
    try:
      json_format.ParseDict(
          tool_output["structuredContent"], result.structured_content
      )
    except json_format.ParseError as e:
      logger.error(
          "Failed to parse structuredContent from dict: %s", e, exc_info=True
      )
    return result

  if "content" in tool_output or "structuredContent" in tool_output or "isError" in tool_output:
    results = []
    is_error = tool_output.get("isError", False)
    if (
        "structuredContent" in tool_output
        and tool_output["structuredContent"] is not None
    ):
      result = mcp_pb2.CallToolResponse.Result()
      if is_error:
        result.is_error = is_error
      try:
        json_format.ParseDict(
            tool_output["structuredContent"], result.structured_content
        )
      except json_format.ParseError as e:
        logger.error(
            "Failed to parse structuredContent from dict: %s", e, exc_info=True
        )
      results.append(result)
    elif is_error and not tool_output.get("content"):
      result = mcp_pb2.CallToolResponse.Result(is_error=is_error)
      results.append(result)
      return results

    if "content" in tool_output and isinstance(
        tool_output["content"], Sequence
    ):
      for content_block in tool_output["content"]:
        result = mcp_pb2.CallToolResponse.Result()
        if _populate_result_from_content_block(content_block, result):
          if is_error:
            result.is_error = is_error
          results.append(result)
      if results or tool_output.get("content"):
        return results
    if results:
      return results

  result = mcp_pb2.CallToolResponse.Result()
  try:
    json_format.ParseDict(tool_output, result.structured_content)
    return result
  except Exception as e:
    logger.warning("Could not parse dict as structured content: %s", e)
    return None


def tool_output_to_proto(
    tool_output: Any,
) -> list[mcp_pb2.CallToolResponse.Result]:
  """Converts tool output to a list of CallToolResponse.Result protos."""
  logger.info("tool_output_to_proto: tool_output=%s", tool_output)

  if tool_output is None:
    return []

  if isinstance(tool_output, list) or isinstance(tool_output, tuple):
    results = []
    for item in tool_output:
      results.extend(tool_output_to_proto(item))
    return results

  if isinstance(tool_output, str):
    return [_handle_string_tool_output(tool_output)]

  result = mcp_pb2.CallToolResponse.Result()
  if isinstance(tool_output, bool):
    result.structured_content.fields["result"].bool_value = tool_output
    return [result]
  elif isinstance(tool_output, int):
    result.structured_content.fields["result"].number_value = tool_output
    return [result]
  elif isinstance(tool_output, float):
    result.structured_content.fields["result"].number_value = tool_output
    return [result]
  elif isinstance(tool_output, dict):
    dict_result = _handle_dict_tool_output(tool_output)
    if isinstance(dict_result, list):
      return dict_result
    elif dict_result:
      return [dict_result]
  elif _is_content_block(tool_output):
    data = tool_output
    if hasattr(data, "model_dump"):
      try:
        data = data.model_dump()
      except Exception:
        result.text.text = str(tool_output)
        return [result]
    if _populate_result_from_content_block(data, result):
      return [result]
    elif hasattr(tool_output, "model_dump"):
      dict_result = _handle_dict_tool_output(data)
      if isinstance(dict_result, list):
        return dict_result
      elif dict_result:
        return [dict_result]
    elif data:
      try:
        json_format.ParseDict(data, result.structured_content)
        return [result]
      except Exception as e:
        logger.warning("Could not parse model_dump as structured content: %s", e)

  try:
    result.text.text = str(tool_output)
    return [result]
  except Exception as e:
    logger.warning(
        "Could not convert tool output of type %s to proto: %s",
        type(tool_output),
        e,
    )

  return []


def proto_result_to_types_result(
    proto_results: list[mcp_pb2.CallToolResponse.Result],
) -> types.CallToolResult:
  """Converts a CallToolResponse.Result proto to a types.CallToolResult."""
  content = []
  is_error = False
  structured_content = None
  for proto_result in proto_results:
    if proto_result.HasField("text"):
      content.append(
          types.TextContent(type="text", text=proto_result.text.text)
      )
    elif proto_result.HasField("image"):
      content.append(types.ImageContent(
          type="image",
          data=base64.b64encode(proto_result.image.data).decode('utf-8'),
          mimeType=proto_result.image.mime_type
      ))
    elif proto_result.HasField("audio"):
      content.append(types.AudioContent(
          type="audio",
          data=base64.b64encode(proto_result.audio.data).decode('utf-8'),
          mimeType=proto_result.audio.mime_type
      ))
    elif proto_result.HasField("embedded_resource"):
      resource_contents = proto_result.embedded_resource.contents
      res_content = None
      if resource_contents.text:
        res_content = types.TextResourceContents(
            uri=resource_contents.uri,
            mimeType=resource_contents.mime_type,
            text=resource_contents.text,
        )
      elif resource_contents.blob:
        res_content = types.BlobResourceContents(
            uri=resource_contents.uri,
            mimeType=resource_contents.mime_type,
            blob=base64.b64encode(resource_contents.blob).decode("utf-8"),
        )
      if res_content:
        content.append(
            types.EmbeddedResource(type="resource", resource=res_content)
        )
    elif proto_result.HasField("resource_link"):
      content.append(types.ResourceLink(
          name=proto_result.resource_link.name,
          type="resource_link", uri=proto_result.resource_link.uri
      ))

    if proto_result.HasField("structured_content"):
      structured_content = json_format.MessageToDict(
          proto_result.structured_content
      )
    is_error = is_error or proto_result.is_error
  return types.CallToolResult(
      content=content,
      structuredContent=structured_content,
      isError=is_error,
  )

async def generate_call_tool_requests(
    requests: AsyncGenerator[
        types.CallToolRequestParams | types.ProgressNotification, None
    ],
) -> AsyncGenerator[mcp_pb2.CallToolRequest, None]:
  """Generates CallToolRequest protos from a stream of request params."""
  async for request_params in requests:
    request = mcp_pb2.CallToolRequest()
    request.common.protocol_version = mcp_pb2.VERSION_20250326

    if isinstance(request_params, types.CallToolRequestParams):
      name = request_params.name
      arguments = request_params.arguments
      meta = request_params.meta
      if meta and meta.progressToken is not None:
        logger.info(
            "Forwarding progressToken %s for tool %s",
            meta.progressToken,
            name,
        )
        request.common.progress.progress_token = str(
            meta.progressToken
        )
      args_struct = None
      if arguments:
        try:
          args_struct = json_format.ParseDict(
              arguments, struct_pb2.Struct()
          )
        except json_format.ParseError as e:
          error_message = f'Failed to parse tool arguments for "{name}": {e}'
          logger.error(error_message, exc_info=True)
          raise McpError(
              ErrorData(
                  code=types.PARSE_ERROR, message=error_message
              )
          ) from e
      request.request.name = name
      if args_struct:
        request.request.arguments.CopyFrom(args_struct)

    elif isinstance(request_params, types.ProgressNotification):
      progress_token = request_params.params.progressToken
      progress = request_params.params.progress
      total = request_params.params.total
      message = request_params.params.message

      request.common.progress.progress_token = progress_token
      request.common.progress.progress = progress
      if total is not None:
        request.common.progress.total = total
      if message is not None:
        request.common.progress.message = message

    yield request