"""Discovery generate module for file generation."""

# Handle graceful import of modules that may not exist yet
try:
    from .assembler import build_output_path, write_discovery_files
    _has_assembler = True
except ImportError:
    _has_assembler = False

try:
    from .ddl_generator import generate_ddl_file
    _has_ddl_generator = True
except ImportError:
    _has_ddl_generator = False

try:
    from .metadata_generator import generate_metadata_json
    _has_metadata_generator = False
except ImportError:
    _has_metadata_generator = False

try:
    from .manifest_generator import generate_manifest
    _has_manifest_generator = False
except ImportError:
    _has_manifest_generator = False

# Build __all__ dynamically based on available modules
__all__ = []
if _has_assembler:
    __all__.extend(["build_output_path", "write_discovery_files"])
if _has_ddl_generator:
    __all__.append("generate_ddl_file")
if _has_metadata_generator:
    __all__.append("generate_metadata_json")
if _has_manifest_generator:
    __all__.append("generate_manifest")
