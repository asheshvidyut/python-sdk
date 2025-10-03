"""Test conversion utilities."""

import unittest
import unittest.mock

import pytest
from google.protobuf import json_format, struct_pb2

from mcp import types
from mcp.proto import mcp_pb2
from mcp.shared import convert
from mcp.shared.exceptions import McpError


def test_tool_proto_to_type_valid():
    """Test conversion of a valid tool proto to types.Tool."""
    input_schema = {"type": "object", "properties": {"a": {"type": "string"}}}
    output_schema = {"type": "object", "properties": {"b": {"type": "number"}}}
    tool_proto = mcp_pb2.Tool(
        name="test_tool",
        description="A test tool",
        input_schema=json_format.ParseDict(input_schema, struct_pb2.Struct()),
        output_schema=json_format.ParseDict(output_schema, struct_pb2.Struct()),
    )
    expected_tool_type = types.Tool(
        name="test_tool",
        description="A test tool",
        inputSchema=input_schema,
        outputSchema=output_schema,
    )

    converted_tool = convert.tool_proto_to_type(tool_proto)

    assert converted_tool == expected_tool_type


def test_tool_proto_to_type_empty_schemas():
    """Test conversion with empty input and output schemas."""
    tool_proto = mcp_pb2.Tool(
        name="empty_tool",
        description="No schemas",
        input_schema=json_format.ParseDict({}, struct_pb2.Struct()),
        output_schema=json_format.ParseDict({}, struct_pb2.Struct()),
    )
    expected_tool_type = types.Tool(name="empty_tool", description="No schemas", inputSchema={}, outputSchema={})

    converted_tool = convert.tool_proto_to_type(tool_proto)

    assert converted_tool == expected_tool_type


def test_tool_proto_to_type_invalid_schema_json():
    """Test error handling with invalid JSON in schema."""
    # Create a proto with a schema that will cause json_format.MessageToDict to fail.
    # This is a bit tricky as Struct naturally handles valid JSON.
    # We'll mock json_format.MessageToDict to simulate a ParseError.
    input_schema = {"type": "object", "properties": {"a": {"type": "string"}}}
    output_schema = {"type": "object", "properties": {"b": {"type": "number"}}}
    tool_proto = mcp_pb2.Tool(
        name="bad_tool",
        description="Bad schema tool",
        input_schema=json_format.ParseDict(input_schema, struct_pb2.Struct()),
        output_schema=json_format.ParseDict(output_schema, struct_pb2.Struct()),
    )

    with pytest.raises(json_format.ParseError) as excinfo:
        with unittest.mock.patch.object(
            json_format,
            "MessageToDict",
            side_effect=json_format.ParseError("Invalid JSON"),
        ):
            convert.tool_proto_to_type(tool_proto)

    assert str(excinfo.value) == "Invalid JSON"


def test_tool_type_to_proto_valid():
    """Test conversion of a valid types.Tool to a proto message."""
    input_schema = {"type": "object", "properties": {"a": {"type": "string"}}}
    output_schema = {"type": "object", "properties": {"b": {"type": "number"}}}
    tool_type = types.Tool(
        name="test_tool",
        description="A test tool",
        inputSchema=input_schema,
        outputSchema=output_schema,
    )
    expected_tool_proto = mcp_pb2.Tool(
        name="test_tool",
        description="A test tool",
        input_schema=json_format.ParseDict(input_schema, struct_pb2.Struct()),
        output_schema=json_format.ParseDict(output_schema, struct_pb2.Struct()),
    )

    converted_tool_proto = convert.tool_type_to_proto(tool_type)
    assert converted_tool_proto == expected_tool_proto


def test_tool_type_to_proto_invalid_input_schema():
    """Test error handling with invalid input schema in tool_type_to_proto."""
    tool_type = types.Tool(
        name="bad_input_tool",
        description="Tool with bad input schema",
        inputSchema={"type": "invalid"},
        outputSchema={},
    )

    with pytest.raises(json_format.ParseError) as excinfo:
        with unittest.mock.patch.object(
            json_format,
            "ParseDict",
            side_effect=[json_format.ParseError("Invalid input schema"), None],
        ):
            convert.tool_type_to_proto(tool_type)

    assert str(excinfo.value) == "Invalid input schema"


def test_tool_type_to_proto_invalid_output_schema():
    """Test error handling with invalid output schema in tool_type_to_proto."""
    tool_type = types.Tool(
        name="bad_output_tool",
        description="Tool with bad output schema",
        inputSchema={},
        outputSchema={"type": "invalid"},
    )

    with pytest.raises(json_format.ParseError) as excinfo:
        with unittest.mock.patch.object(
            json_format,
            "ParseDict",
            side_effect=json_format.ParseError("Invalid output schema"),
        ):
            convert.tool_type_to_proto(tool_type)

    assert str(excinfo.value) == "Invalid output schema"


def test_tool_types_to_protos():
    """Test conversion of a list of types.Tool objects."""
    tool_types = [
        types.Tool(name="tool1", description="Tool 1", inputSchema={}, outputSchema={}),
        types.Tool(
            name="tool2",
            description="Tool 2",
            inputSchema={"type": "string"},
            outputSchema={"type": "number"},
        ),
    ]
    expected_protos = [
        mcp_pb2.Tool(
            name="tool1",
            description="Tool 1",
            input_schema=json_format.ParseDict({}, struct_pb2.Struct()),
            output_schema=json_format.ParseDict({}, struct_pb2.Struct()),
        ),
        mcp_pb2.Tool(
            name="tool2",
            description="Tool 2",
            input_schema=json_format.ParseDict({"type": "string"}, struct_pb2.Struct()),
            output_schema=json_format.ParseDict({"type": "number"}, struct_pb2.Struct()),
        ),
    ]

    converted_protos = convert.tool_types_to_protos(tool_types)
    for converted_proto, expected_proto in zip(converted_protos, expected_protos):
        assert converted_proto == expected_proto


def test_tool_protos_to_types():
    """Test conversion of a list of proto messages."""
    tool_protos = [
        mcp_pb2.Tool(
            name="tool1",
            description="Tool 1",
            input_schema=json_format.ParseDict({}, struct_pb2.Struct()),
            output_schema=json_format.ParseDict({}, struct_pb2.Struct()),
        ),
        mcp_pb2.Tool(
            name="tool2",
            description="Tool 2",
            input_schema=json_format.ParseDict({"type": "string"}, struct_pb2.Struct()),
            output_schema=json_format.ParseDict({"type": "number"}, struct_pb2.Struct()),
        ),
    ]
    expected_types = [
        types.Tool(name="tool1", description="Tool 1", inputSchema={}, outputSchema={}),
        types.Tool(
            name="tool2",
            description="Tool 2",
            inputSchema={"type": "string"},
            outputSchema={"type": "number"},
        ),
    ]

    converted_types = convert.tool_protos_to_types(tool_protos)
    assert converted_types == expected_types


def test_tool_output_to_proto_structured_content():
    """Test conversion of tool output with structured_content."""
    tool_output = {"structuredContent": {"key": "value", "number": 123}}
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 1
    assert converted_proto[0].structured_content == json_format.ParseDict(
        tool_output["structuredContent"], struct_pb2.Struct()
    )


def test_tool_output_to_proto_text_and_error():
    """Test conversion of tool output with text and error flag."""
    tool_output = {"text": "An error occurred", "is_error": True}
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 1
    assert converted_proto[0].text.text == tool_output["text"]
    assert converted_proto[0].is_error == tool_output["is_error"]


def test_tool_output_to_proto_string():
    """Test conversion of tool output as a simple string."""
    tool_output = "Just a string output"
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 1
    assert converted_proto[0].structured_content.fields["result"].string_value == tool_output


def test_tool_output_to_proto_image_content():
    """Test conversion of tool output with image content."""
    tool_output = {"content": [{"type": "image", "data": "aGVsbG8=", "mimeType": "image/png"}]}
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 1
    assert converted_proto[0].image.data == b"hello"
    assert converted_proto[0].image.mime_type == "image/png"


def test_tool_output_to_proto_audio_content():
    """Test conversion of tool output with audio content."""
    tool_output = {"content": [{"type": "audio", "data": "aGVsbG8=", "mimeType": "audio/mpeg"}]}
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 1
    assert converted_proto[0].audio.data == b"hello"
    assert converted_proto[0].audio.mime_type == "audio/mpeg"


def test_tool_output_to_proto_image_content_object():
    """Test conversion of tool output as an ImageContent object."""
    tool_output = types.ImageContent(type="image", data="aGVsbG8=", mimeType="image/png")
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 1
    assert converted_proto[0].image.data == b"hello"
    assert converted_proto[0].image.mime_type == "image/png"


def test_tool_output_to_proto_audio_content_object():
    """Test conversion of tool output as an AudioContent object."""
    tool_output = types.AudioContent(type="audio", data="aGVsbG8=", mimeType="audio/mpeg")
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 1
    assert converted_proto[0].audio.data == b"hello"
    assert converted_proto[0].audio.mime_type == "audio/mpeg"


def test_tool_output_to_proto_image_content_missing_data():
    """Test image content block missing data field."""
    tool_output = {"content": [{"type": "image", "mimeType": "image/png"}]}
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert converted_proto == []


def test_tool_output_to_proto_image_content_missing_mime():
    """Test image content block missing mimeType field."""
    tool_output = {"content": [{"type": "image", "data": "aGVsbG8="}]}
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert converted_proto == []


def test_tool_output_to_proto_audio_content_missing_data():
    """Test audio content block missing data field."""
    tool_output = {"content": [{"type": "audio", "mimeType": "audio/mpeg"}]}
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert converted_proto == []


def test_tool_output_to_proto_audio_content_missing_mime():
    """Test audio content block missing mimeType field."""
    tool_output = {"content": [{"type": "audio", "data": "aGVsbG8="}]}
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert converted_proto == []


def test_tool_output_to_proto_embedded_resource_text():
    """Test conversion of tool output with embedded resource (text)."""
    tool_output = {
        "content": [
            {
                "type": "resource",
                "resource": {
                    "uri": "test://resource",
                    "mimeType": "text/plain",
                    "text": "Embedded text content",
                },
            }
        ]
    }
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 1
    assert converted_proto[0].embedded_resource.contents.uri == "test://resource"
    assert converted_proto[0].embedded_resource.contents.mime_type == "text/plain"
    assert converted_proto[0].embedded_resource.contents.text == "Embedded text content"


def test_tool_output_to_proto_embedded_resource_blob():
    """Test conversion of tool output with embedded resource (blob)."""
    tool_output = {
        "content": [
            {
                "type": "resource",
                "resource": {
                    "uri": "test://resource",
                    "mimeType": "application/octet-stream",
                    "blob": "YmxvYg==",
                },
            }
        ]
    }
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 1
    assert converted_proto[0].embedded_resource.contents.uri == "test://resource"
    assert converted_proto[0].embedded_resource.contents.mime_type == "application/octet-stream"
    assert converted_proto[0].embedded_resource.contents.blob == b"blob"


def test_tool_output_to_proto_embedded_text_resource_object():
    """Test conversion of tool output as an EmbeddedResource object with text."""
    tool_output = types.EmbeddedResource(
        type="resource",
        resource=types.TextResourceContents(uri="test://resource", mimeType="text/plain", text="hello"),
    )
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 1
    assert converted_proto[0].embedded_resource.contents.uri == "test://resource"
    assert converted_proto[0].embedded_resource.contents.mime_type == "text/plain"
    assert converted_proto[0].embedded_resource.contents.text == "hello"


def test_tool_output_to_proto_embedded_blob_resource_object():
    """Test conversion of tool output as an EmbeddedResource object with blob."""
    tool_output = types.EmbeddedResource(
        type="resource",
        resource=types.BlobResourceContents(uri="test://resource", mimeType="app/foo", blob="aGVsbG8="),
    )
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 1
    assert converted_proto[0].embedded_resource.contents.uri == "test://resource"
    assert converted_proto[0].embedded_resource.contents.mime_type == "app/foo"
    assert converted_proto[0].embedded_resource.contents.blob == b"hello"


def test_tool_output_to_proto_resource_link():
    """Test conversion of tool output with a resource link."""
    tool_output = {"content": [{"type": "resource_link", "uri": "test://link"}]}
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 1
    assert converted_proto[0].resource_link.uri == "test://link"


def test_tool_output_to_proto_resource_link_object():
    """Test conversion of tool output as a ResourceLink object."""
    tool_output = types.ResourceLink(type="resource_link", name="", uri="test://link")
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 1
    assert converted_proto[0].resource_link.uri == "test://link"


def test_tool_output_to_proto_resource_link_missing_uri():
    """Test resource link content block missing uri field."""
    tool_output = {"content": [{"type": "resource_link", "name": "bad link"}]}
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert converted_proto == []


def test_tool_output_to_proto_multiple_content_blocks():
    """Test conversion with multiple content blocks, only the first should be used."""
    tool_output = {
        "content": [
            {"type": "image", "data": "aGVsbG8=", "mimeType": "image/png"},
            {"type": "text", "text": "This should be ignored"},
        ]
    }
    converted_protos = convert.tool_output_to_proto(tool_output)
    assert len(converted_protos) == 2
    assert converted_protos[0].image.data == b"hello"
    assert converted_protos[0].image.mime_type == "image/png"
    assert converted_protos[1].text.text == "This should be ignored"


def test_tool_output_to_proto_empty_dict():
    """Test conversion of an empty dictionary tool output."""
    tool_output = {}
    expected_proto = mcp_pb2.CallToolResponse.Result()
    json_format.ParseDict({}, expected_proto.structured_content)
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert converted_proto[0] == expected_proto


def test_tool_output_to_proto_tuple():
    """Test conversion of tool output as a tuple."""
    tool_output = ([{"type": "text", "text": "hello"}], {"key": "value"})
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 2
    assert converted_proto[0].text.text == "hello"
    assert converted_proto[1].structured_content == json_format.ParseDict({"key": "value"}, struct_pb2.Struct())


def test_tool_output_to_proto_list_content_blocks():
    """Test conversion of tool output as a list of content blocks."""
    tool_output = [{"type": "text", "text": "hello"}]
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 1
    assert converted_proto[0].text.text == "hello"


def test_tool_output_to_proto_list_of_other_values():
    """Test conversion of tool output as a list of other values."""
    tool_output = [1, 2, 3]
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 3
    assert converted_proto[0].structured_content == json_format.ParseDict({"result": 1}, struct_pb2.Struct())
    assert converted_proto[1].structured_content == json_format.ParseDict({"result": 2}, struct_pb2.Struct())
    assert converted_proto[2].structured_content == json_format.ParseDict({"result": 3}, struct_pb2.Struct())


def test_tool_output_to_proto_int():
    """Test conversion of tool output as an integer."""
    tool_output = 123
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 1
    assert converted_proto[0].structured_content == json_format.ParseDict({"result": 123}, struct_pb2.Struct())


def test_tool_output_to_proto_float():
    """Test conversion of tool output as a float."""
    tool_output = 123.45
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 1
    assert pytest.approx(converted_proto[0].structured_content.fields["result"].number_value) == 123.45


def test_tool_output_to_proto_bool():
    """Test conversion of tool output as a boolean."""
    tool_output = True
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 1
    assert converted_proto[0].structured_content.fields["result"].bool_value is True


def test_tool_output_to_proto_text_content_dict():
    """Test conversion of tool output as dict resembling text content."""
    tool_output = {"type": "text", "text": "hello from dict"}
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 1
    assert converted_proto[0].text.text == "hello from dict"


def test_tool_output_to_proto_text_only_dict():
    """Test conversion of tool output as dict with only text field."""
    tool_output = {"text": "hello from dict"}
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 1
    assert converted_proto[0].text.text == "hello from dict"
    assert converted_proto[0].is_error is False


def test_tool_output_to_proto_is_error_with_content():
    """Test conversion of tool output with isError and content."""
    tool_output = {
        "isError": True,
        "content": [{"type": "text", "text": "An error occurred"}],
    }
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 1
    assert converted_proto[0].text.text == "An error occurred"
    assert converted_proto[0].is_error is True


def test_tool_output_to_proto_is_error_no_content():
    """Test conversion of tool output with isError and no content."""
    tool_output = {"isError": True}
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 1
    assert converted_proto[0].is_error is True
    assert not converted_proto[0].HasField("text")
    assert not converted_proto[0].HasField("structured_content")


def test_tool_output_to_proto_is_error_with_structured_content():
    """Test conversion of tool output with isError and structuredContent."""
    tool_output = {"isError": True, "structuredContent": {"key": "value"}}
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 1
    assert converted_proto[0].is_error is True
    assert converted_proto[0].structured_content == json_format.ParseDict({"key": "value"}, struct_pb2.Struct())


def test_tool_output_to_proto_bad_structured_content():
    """Test conversion of tool output with bad structuredContent."""
    tool_output = {"structuredContent": {"bad": float("inf")}}
    with unittest.mock.patch.object(json_format, "ParseDict", side_effect=json_format.ParseError("Bad JSON")):
        converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 1
    # Expect an empty structured_content if parsing fails
    assert converted_proto[0].structured_content == struct_pb2.Struct()


def test_tool_output_to_proto_text_content_object():
    """Test conversion of tool output as a TextContent object."""
    tool_output = types.TextContent(type="text", text="hello from object")
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 1
    assert converted_proto[0].text.text == "hello from object"


def test_tool_output_to_proto_mixed_list():
    """Test conversion of tool output as a list of mixed types."""
    tool_output = [1, "two", types.TextContent(type="text", text="three")]
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert len(converted_proto) == 3
    assert converted_proto[0].structured_content.fields["result"].number_value == 1
    assert converted_proto[1].structured_content.fields["result"].string_value == "two"
    assert converted_proto[2].text.text == "three"


def test_tool_output_to_proto_none():
    """Test conversion of tool output as None."""
    tool_output = None
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert converted_proto == []


def test_tool_output_to_proto_invalid_content_block():
    """Test conversion of tool output with invalid content block."""
    tool_output = {"content": [123]}
    converted_proto = convert.tool_output_to_proto(tool_output)
    assert converted_proto == []


class MockPydanticWithDump:
    def __str__(self):
        return "MockPydanticWithDump()"

    def model_dump(self):
        return {"type": "mock", "value": 123}


class MockPydanticWithFailingDump:
    def __str__(self):
        return "MockPydanticWithFailingDump()"

    def model_dump(self):
        raise ValueError("dump failed")


def test_tool_output_to_proto_model_dump_with_type():
    """Test tool_output_to_proto with model_dump that returns a dict with type."""
    tool_output = MockPydanticWithDump()
    result = convert.tool_output_to_proto(tool_output)
    assert len(result) == 1
    assert result[0].structured_content == json_format.ParseDict({"type": "mock", "value": 123}, struct_pb2.Struct())


def test_tool_output_to_proto_model_dump_exception():
    """Test tool_output_to_proto with model_dump that raises exception."""
    tool_output = MockPydanticWithFailingDump()
    result = convert.tool_output_to_proto(tool_output)
    assert len(result) == 1
    assert result[0].text.text == "MockPydanticWithFailingDump()"


def test_proto_result_to_types_result_text():
    """Test conversion of proto result with text to types.CallToolResult."""
    proto_result = mcp_pb2.CallToolResponse.Result()
    proto_result.text.text = "hello"
    types_result = convert.proto_result_to_types_result([proto_result])
    assert types_result.content == [types.TextContent(type="text", text="hello")]
    assert types_result.structuredContent is None
    assert types_result.isError is False


def test_proto_result_to_types_result_image():
    """Test conversion of proto result with image to types.CallToolResult."""
    proto_result = mcp_pb2.CallToolResponse.Result()
    proto_result.image.data = b"hello"
    proto_result.image.mime_type = "image/png"
    types_result = convert.proto_result_to_types_result([proto_result])
    assert types_result.content == [types.ImageContent(type="image", data="aGVsbG8=", mimeType="image/png")]
    assert types_result.structuredContent is None
    assert types_result.isError is False


def test_proto_result_to_types_result_audio():
    """Test conversion of proto result with audio to types.CallToolResult."""
    proto_result = mcp_pb2.CallToolResponse.Result()
    proto_result.audio.data = b"hello"
    proto_result.audio.mime_type = "audio/mpeg"
    types_result = convert.proto_result_to_types_result([proto_result])
    assert types_result.content == [types.AudioContent(type="audio", data="aGVsbG8=", mimeType="audio/mpeg")]
    assert types_result.structuredContent is None
    assert types_result.isError is False


def test_proto_result_to_types_result_embedded_resource_text():
    """Test conversion of proto result with text resource to types.CallToolResult."""
    proto_result = mcp_pb2.CallToolResponse.Result()
    proto_result.embedded_resource.contents.uri = "test://resource"
    proto_result.embedded_resource.contents.mime_type = "text/plain"
    proto_result.embedded_resource.contents.text = "hello"
    types_result = convert.proto_result_to_types_result([proto_result])
    assert types_result.content[0].type == "resource"
    assert str(types_result.content[0].resource.uri) == "test://resource"
    assert types_result.content[0].resource.mimeType == "text/plain"
    assert types_result.content[0].resource.text == "hello"
    assert types_result.structuredContent is None
    assert types_result.isError is False


def test_proto_result_to_types_result_embedded_resource_blob():
    """Test conversion of proto result with blob resource to types.CallToolResult."""
    proto_result = mcp_pb2.CallToolResponse.Result()
    proto_result.embedded_resource.contents.uri = "test://resource"
    proto_result.embedded_resource.contents.mime_type = "application/octet-stream"
    proto_result.embedded_resource.contents.blob = b"blob"
    types_result = convert.proto_result_to_types_result([proto_result])
    assert types_result.content[0].type == "resource"
    assert str(types_result.content[0].resource.uri) == "test://resource"
    assert types_result.content[0].resource.mimeType == "application/octet-stream"
    assert types_result.content[0].resource.blob == "YmxvYg=="
    assert types_result.structuredContent is None
    assert types_result.isError is False


def test_proto_result_to_types_result_resource_link():
    """Test conversion of proto result with resource link to types.CallToolResult."""
    proto_result = mcp_pb2.CallToolResponse.Result()
    proto_result.resource_link.uri = "test://link"
    proto_result.resource_link.name = "Test Link"
    types_result = convert.proto_result_to_types_result([proto_result])
    assert types_result.content == [types.ResourceLink(type="resource_link", uri="test://link", name="Test Link")]
    assert types_result.structuredContent is None
    assert types_result.isError is False


def test_proto_result_to_types_result_resource_link_no_name():
    """Test conversion of proto result with resource link but no name."""
    proto_result = mcp_pb2.CallToolResponse.Result()
    proto_result.resource_link.uri = "test://link"
    types_result = convert.proto_result_to_types_result([proto_result])
    assert types_result.content == [types.ResourceLink(type="resource_link", uri="test://link", name="")]
    assert types_result.structuredContent is None
    assert types_result.isError is False


def test_proto_result_to_types_result_structured_content():
    """Test conversion of proto result with structured content to types.CallToolResult."""
    proto_result = mcp_pb2.CallToolResponse.Result()
    json_format.ParseDict({"key": "value"}, proto_result.structured_content)
    types_result = convert.proto_result_to_types_result([proto_result])
    assert types_result.content == []
    assert types_result.structuredContent == {"key": "value"}
    assert types_result.isError is False


def test_proto_result_to_types_result_error():
    """Test conversion of proto result with error to types.CallToolResult."""
    proto_result = mcp_pb2.CallToolResponse.Result(is_error=True)
    types_result = convert.proto_result_to_types_result([proto_result])
    assert types_result.content == []
    assert types_result.structuredContent is None
    assert types_result.isError is True


@pytest.mark.anyio
async def test_generate_call_tool_requests_call():
    """Test generation of call tool requests."""

    async def requests_gen():
        yield types.CallToolRequestParams(
            name="test_tool",
            arguments={"arg1": "value1"},
        )

    generator = convert.generate_call_tool_requests(requests_gen())
    request = await generator.__anext__()
    assert request.request.name == "test_tool"
    assert request.request.arguments == json_format.ParseDict({"arg1": "value1"}, struct_pb2.Struct())


@pytest.mark.anyio
async def test_generate_call_tool_requests_progress():
    """Test generation of progress notification requests."""

    async def requests_gen():
        yield types.ProgressNotification(
            method="notifications/progress",
            params=types.ProgressNotificationParams(
                progressToken="token1", progress=50, total=100, message="In progress"
            ),
        )

    generator = convert.generate_call_tool_requests(requests_gen())
    request = await generator.__anext__()
    assert request.common.progress.progress_token == "token1"
    assert request.common.progress.progress == 50
    assert request.common.progress.total == 100
    assert request.common.progress.message == "In progress"


@pytest.mark.anyio
async def test_generate_call_tool_requests_progress_minimal():
    """Test generation of progress notification requests with minimal fields."""

    async def requests_gen():
        yield types.ProgressNotification(
            method="notifications/progress",
            params=types.ProgressNotificationParams(progressToken="token1", progress=50, total=None, message=None),
        )

    generator = convert.generate_call_tool_requests(requests_gen())
    request = await generator.__anext__()
    assert request.common.progress.progress_token == "token1"
    assert request.common.progress.progress == 50
    assert request.common.progress.total == 0
    assert request.common.progress.message == ""


@pytest.mark.anyio
async def test_generate_call_tool_requests_progress_no_message():
    """Test generation of progress notification requests with no message."""

    async def requests_gen():
        yield types.ProgressNotification(
            method="notifications/progress",
            params=types.ProgressNotificationParams(progressToken="token1", progress=50, total=100, message=None),
        )

    generator = convert.generate_call_tool_requests(requests_gen())
    request = await generator.__anext__()
    assert request.common.progress.progress_token == "token1"
    assert request.common.progress.progress == 50
    assert request.common.progress.total == 100
    assert request.common.progress.message == ""


@pytest.mark.anyio
async def test_generate_call_tool_requests_progress_no_total():
    """Test generation of progress notification requests with no total."""

    async def requests_gen():
        yield types.ProgressNotification(
            method="notifications/progress",
            params=types.ProgressNotificationParams(
                progressToken="token1", progress=50, total=None, message="In progress"
            ),
        )

    generator = convert.generate_call_tool_requests(requests_gen())
    request = await generator.__anext__()
    assert request.common.progress.progress_token == "token1"
    assert request.common.progress.progress == 50
    assert request.common.progress.total == 0
    assert request.common.progress.message == "In progress"


@pytest.mark.anyio
async def test_generate_call_tool_requests_bad_args():
    """Test generation of call tool requests with bad arguments."""

    async def requests_gen():
        yield types.CallToolRequestParams(
            name="test_tool",
            arguments={"arg1": set()},  # set is not serializable to JSON
        )

    generator = convert.generate_call_tool_requests(requests_gen())
    with pytest.raises(McpError):
        await generator.__anext__()
