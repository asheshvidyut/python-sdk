"""
gRPC server implementation.

This module provides a complete MCP server example using gRPC transport.
Can be used both as an educational example and a practical working server.
"""
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

import argparse
from typing import Annotated, TypedDict, Any

import asyncio
from mcp.server.fastmcp import Context, FastMCP
from mcp.server.session import ServerSession
from pydantic import BaseModel, Field
from io import BytesIO
from mcp.server.fastmcp.utilities.types import Image


class ShrimpTank(BaseModel):

  class Shrimp(BaseModel):
    name: Annotated[str, Field(max_length=10)]

  shrimp: list[Shrimp]

logger = logging.getLogger(__name__)


def setup_server(port: int) -> FastMCP:
  """Set up the FastMCP server with comprehensive features."""
  mcp = FastMCP(
      name="Simple gRPC Server",
      instructions=(
          "A simple MCP server demonstrating gRPC transport capabilities."
      ),
      target=f"127.0.0.1:{port}",
      grpc_enable_reflection=True,
      dependencies=["Pillow"]
  )

  @mcp.tool()
  def add(a: int, b: int) -> int:
    """Add two numbers together."""
    # Create an instance of the expected message type
    result = a + b
    logger.info("Adding %s  %s = %s", a, b, result)
    return result

  @mcp.tool()
  def greet(name: str) -> str:
    """Greet someone by name."""
    # Create an instance of the expected message type
    greeting = f"Hello, {name}! Welcome to the Simple gRPC Server!"
    logger.info("Greeting %s", name)
    # Use generated proto message if available
    return greeting

  @mcp.tool()
  def name_shrimp(
      tank: ShrimpTank,
      # You can use pydantic Field in function signatures for validation.
      extra_names: Annotated[list[str], Field(max_length=10)],
  ) -> list[str]:
    """List all shrimp names in the tank."""
    return [shrimp.name for shrimp in tank.shrimp] + extra_names

  @mcp.tool()
  async def download_file(
      filename: str,
      size_mb: float,
      ctx: Context[ServerSession, None],
  ) -> str:
    """
    Simulate downloading a file with progress updates.

    Args:
        filename: Name of the file to download
        size_mb: Size of the file in MB
    """
    await ctx.info(f"Starting download of {filename} ({size_mb} MB)")

    # Simulate download with progress updates
    total_bytes = int(size_mb * 1024 * 1024)  # Convert MB to bytes
    chunk_size = 64 * 1024  # 64KB chunks
    downloaded = 0

    while downloaded < total_bytes:
      # Simulate network delay
      await asyncio.sleep(0.1)

      # Calculate progress
      progress = downloaded / total_bytes
      remaining_mb = (total_bytes - downloaded) / (1024 * 1024)

      # Update progress
      print(
          "🔄 Sending progress: "
          f"{progress:.2f} - {downloaded / (1024 * 1024):.1f} MB downloaded"
      )
      await ctx.report_progress(
          progress=progress,
          total=1.0,
          message=(
              f"Downloaded {downloaded / (1024 * 1024):.1f} MB, "
              f"{remaining_mb:.1f} MB remaining"
          ),
      )

      # Simulate chunk download
      chunk = min(chunk_size, total_bytes - downloaded)
      downloaded = chunk

    print("🔄 Sending final progress: 100% - Download completed")
    await ctx.report_progress(
        progress=1.0,
        total=1.0,
        message="Download completed successfully",
    )

    print(f"Successfully downloaded {filename} ({size_mb} MB)")
    return f"Successfully downloaded {filename} ({size_mb} MB)"

  # Using Pydantic models for rich structured data
  class WeatherData(BaseModel):
      """Weather information structure."""

      temperature: float = Field(description="Temperature in Celsius")
      humidity: float = Field(description="Humidity percentage")
      condition: str
      wind_speed: float

  @mcp.tool()
  def get_weather(city: str) -> WeatherData:
      """Get weather for a city - returns structured data."""
      # Simulated weather data
      return WeatherData(
          temperature=22.5,
          humidity=45.0,
          condition="sunny",
          wind_speed=5.2,
      )

  # Using TypedDict for simpler structures
  class LocationInfo(TypedDict):
      latitude: float
      longitude: float
      name: str

  @mcp.tool()
  def get_location(address: str) -> LocationInfo:
      """Get location coordinates"""
      return LocationInfo(latitude=51.5074, longitude=-0.1278, name="London, UK")

  # Using dict[str, Any] for flexible schemas
  @mcp.tool()
  def get_statistics(data_type: str) -> dict[str, float]:
      """Get various statistics"""
      return {"mean": 42.5, "median": 40.0, "std_dev": 5.2}

  # Ordinary classes with type hints work for structured output
  class UserProfile:
      name: str
      age: int
      email: str | None = None

      def __init__(self, name: str, age: int, email: str | None = None):
          self.name = name
          self.age = age
          self.email = email

  @mcp.tool()
  def get_user(user_id: str) -> UserProfile:
      """Get user profile - returns structured data"""
      return UserProfile(name="Alice", age=30, email="alice@example.com")

  @mcp.tool()
  def get_image():
    from PIL import Image as PILImage
    img = PILImage.new('RGB', (1, 1), color = 'red')
    buf = BytesIO()
    img.save(buf, format='PNG')
    return ["image", Image(data=buf.getvalue(), format="png")]

  # Classes WITHOUT type hints cannot be used for structured output
  class UntypedConfig:
      def __init__(self, setting1, setting2):  # type: ignore[reportMissingParameterType]
          self.setting1 = setting1
          self.setting2 = setting2

  @mcp.tool()
  def get_config() -> UntypedConfig:
      """This returns unstructured output - no schema generated"""
      return UntypedConfig("value1", "value2")

  # Lists and other types are wrapped automatically
  @mcp.tool()
  def list_cities() -> list[str]:
      """Get a list of cities"""
      return ["London", "Paris", "Tokyo"]
      # Returns: {"result": ["London", "Paris", "Tokyo"]}

  @mcp.tool()
  def get_temperature(city: str) -> float:
      """Get temperature as a simple float"""
      return 22.5
      # Returns: {"result": 22.5}

  return mcp


def main(port=50051):
  """Run the simple gRPC server."""
  # Set up the server with all features
  mcp = setup_server(port)

  logger.info("Starting Simple gRPC Server...")
  logger.info("Server will be available on localhost:%s", port)
  logger.info("Press Ctrl-C to stop the server")

  try:
    # Run the server with gRPC transport
    mcp.run(transport="grpc")
  except KeyboardInterrupt:
    logger.info("Server stopped")


if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="MCP gRPC Server")
  parser.add_argument(
      "--port", type=int, default=50051, help="Server port (default: 50051)"
  )

  args = parser.parse_args()

  main(port=args.port)