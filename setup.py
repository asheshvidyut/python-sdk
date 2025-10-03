
from setuptools.command.build_py import build_py as _build_py

# The content you want to write into the file
old_import = "import mcp_pb2 as mcp__pb2"
new_import = "import mcp.proto.mcp_pb2 as mcp__pb2"
TARGET_FILE = "src/mcp/proto/mcp_pb2_grpc.py"


class build_py(_build_py):
    """Custom build_py command to write the version file."""

    def run(self):
        print(f"--- Replacing content of {TARGET_FILE} ---")
        # Make sure the target directory exists
        self.mkpath(self.build_lib)

        # Write the new content to the file
        with open(TARGET_FILE) as f:
            content = f.read()

        if old_import not in content:
            print(f"The line '{old_import}' was not found in {TARGET_FILE}.")
            return

        print(f"Replacing import in {TARGET_FILE}...")
        new_content = content.replace(old_import, new_import)

        with open(TARGET_FILE, "w") as file:
            file.write(new_content)

        # Let the original build command do its job
        super().run()


# This is what setuptools will use
cmdclass = {"build_py": build_py}
