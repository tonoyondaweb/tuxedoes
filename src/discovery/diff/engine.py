"""Structural diff engine for comparing Snowflake metadata discovery states."""

import hashlib
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Any, Optional


@dataclass
class DiffResult:
    """Result of a structural diff comparison."""
    has_changes: bool
    added_objects: List[str] = field(default_factory=list)
    removed_objects: List[str] = field(default_factory=list)
    modified_objects: List[str] = field(default_factory=list)
    summary: str = ""

    def __str__(self) -> str:
        """Human-readable summary of changes."""
        if self.summary:
            return self.summary

        parts = []
        if self.added_objects:
            parts.append(f"Added: {len(self.added_objects)} objects")
        if self.removed_objects:
            parts.append(f"Removed: {len(self.removed_objects)} objects")
        if self.modified_objects:
            parts.append(f"Modified: {len(self.modified_objects)} objects")

        if not parts:
            return "No changes detected"

        return "; ".join(parts)


@dataclass
class ObjectState:
    """State representation of a single Snowflake object."""
    object_type: str
    fully_qualified_name: str
    ddl_hash: str
    column_count: int
    constraint_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


class DiffEngine:
    """Engine for performing structural diffs on Snowflake metadata states."""

    def __init__(self):
        """Initialize the diff engine."""
        self._object_types = [
            "tables",
            "views",
            "procedures",
            "functions",
            "streams",
            "tasks",
            "stages",
            "pipes",
            "sequences"
        ]

    def compare(
        self,
        current_state: Dict[str, Dict[str, Any]],
        previous_state: Dict[str, Dict[str, Any]]
    ) -> DiffResult:
        """Compare two metadata states and return structural diff.

        Args:
            current_state: Current discovery state (format from extract_current_state)
            previous_state: Previous discovery state (format from extract_current_state)

        Returns:
            DiffResult with added, removed, and modified objects
        """
        added_objects = []
        removed_objects = []
        modified_objects = []

        for object_type in self._object_types:
            if object_type not in current_state and object_type not in previous_state:
                continue

            current_objects = current_state.get(object_type, {})
            previous_objects = previous_state.get(object_type, {})

            all_keys = set(current_objects.keys()) | set(previous_objects.keys())

            for object_key in all_keys:
                current = current_objects.get(object_key)
                previous = previous_objects.get(object_key)

                if previous is None:
                    # Object was added
                    added_objects.append(f"{object_type[:-1].upper()}: {object_key}")
                elif current is None:
                    # Object was removed
                    removed_objects.append(f"{object_type[:-1].upper()}: {object_key}")
                elif self._has_object_changes(current, previous):
                    # Object was modified
                    change_details = self._get_change_details(current, previous)
                    modified_objects.append(
                        f"{object_type[:-1].upper()}: {object_key} ({change_details})"
                    )

        has_changes = bool(added_objects or removed_objects or modified_objects)
        summary = str(DiffResult(has_changes, added_objects, removed_objects, modified_objects))

        return DiffResult(
            has_changes=has_changes,
            added_objects=added_objects,
            removed_objects=removed_objects,
            modified_objects=modified_objects,
            summary=summary
        )

    def _has_object_changes(
        self,
        current: Dict[str, Any],
        previous: Dict[str, Any]
    ) -> bool:
        """Check if an object has structural changes.

        Args:
            current: Current object state
            previous: Previous object state

        Returns:
            True if object has structural changes, False otherwise
        """
        # Compare DDL hashes (covers DDL changes)
        if current.get("ddl_hash") != previous.get("ddl_hash"):
            return True

        # Compare column counts
        if current.get("column_count") != previous.get("column_count"):
            return True

        # Compare constraint counts
        if current.get("constraint_count") != previous.get("constraint_count"):
            return True

        return False

    def _get_change_details(
        self,
        current: Dict[str, Any],
        previous: Dict[str, Any]
    ) -> str:
        """Get human-readable description of changes between two object states.

        Args:
            current: Current object state
            previous: Previous object state

        Returns:
            Human-readable change description
        """
        changes = []

        # Check column count changes
        current_cols = current.get("column_count", 0)
        previous_cols = previous.get("column_count", 0)
        if current_cols != previous_cols:
            diff = current_cols - previous_cols
            if diff > 0:
                changes.append(f"+{diff} columns")
            else:
                changes.append(f"{diff} columns")

        # Check constraint count changes
        current_cons = current.get("constraint_count", 0)
        previous_cons = previous.get("constraint_count", 0)
        if current_cons != previous_cons:
            diff = current_cons - previous_cons
            if diff > 0:
                changes.append(f"+{diff} constraints")
            else:
                changes.append(f"{diff} constraints")

        # Check DDL changes (if other metrics are same)
        if not changes and current.get("ddl_hash") != previous.get("ddl_hash"):
            changes.append("DDL changed")

        return ", ".join(changes) if changes else "schema changed"


def compute_state_hash(metadata: Dict[str, Any]) -> str:
    """Compute a consistent hash for a metadata state.

    Hash is computed from all DDLs and schemas, suitable for quick comparison.

    Args:
        metadata: Metadata dictionary with object types as keys

    Returns:
        SHA256 hash string
    """
    # Sort keys for consistent hash generation
    sorted_items = []

    for object_type in sorted(metadata.keys()):
        objects = metadata[object_type]
        if isinstance(objects, dict):
            for object_key in sorted(objects.keys()):
                obj = objects[object_key]
                # Hash DDL if present
                ddl = obj.get("ddl", "")
                if ddl:
                    sorted_items.append(f"{object_type}:{object_key}:{ddl}")

                # Hash column list if present
                columns = obj.get("columns", [])
                if columns:
                    # Sort columns by name for consistency
                    sorted_columns = sorted(columns, key=lambda x: x.get("name", ""))
                    columns_str = json.dumps(sorted_columns, sort_keys=True)
                    sorted_items.append(f"{object_type}:{object_key}:columns:{columns_str}")

    # Join all items and compute hash
    content = "|".join(sorted_items)
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def load_previous_state(repo_path: str) -> Dict[str, Dict[str, Any]]:
    """Load previous discovery state from repository.

    Reads existing discovery DDL files and constructs state dictionary.

    Args:
        repo_path: Path to the repository root

    Returns:
        Dictionary with object types as keys and object states as values
    """
    state = {
        "tables": {},
        "views": {},
        "procedures": {},
        "functions": {},
        "streams": {},
        "tasks": {},
        "stages": {},
        "pipes": {},
        "sequences": {}
    }

    # Look for discovery output directory
    discovery_dir = Path(repo_path) / "discovery"
    if not discovery_dir.exists():
        return state

    # Scan for object-type subdirectories
    for object_type_dir in discovery_dir.iterdir():
        if not object_type_dir.is_dir():
            continue

        object_type = object_type_dir.name
        if object_type not in state:
            continue

        # Read DDL files in object type directory
        for ddl_file in object_type_dir.glob("*.sql"):
            try:
                with open(ddl_file, "r", encoding="utf-8") as f:
                    content = f.read()

                # Parse object key from filename
                # File format: DB.SCHEMA.OBJECT.sql
                filename = ddl_file.stem
                object_key = filename

                # Parse DDL content to extract structural info
                state[object_type][object_key] = {
                    "ddl": content,
                    "ddl_hash": hashlib.sha256(content.encode("utf-8")).hexdigest(),
                    "column_count": _extract_column_count(content, object_type),
                    "constraint_count": _extract_constraint_count(content, object_type)
                }
            except Exception as e:
                # Log error but continue processing other files
                # In production, you might want to collect these errors
                pass

    return state


def _extract_column_count(ddl: str, object_type: str) -> int:
    """Extract column count from DDL statement.

    Args:
        ddl: DDL statement
        object_type: Type of object (table, view, etc.)

    Returns:
        Number of columns
    """
    # Simple column detection: count comma-separated items in column list
    # This is a basic heuristic and may not cover all edge cases
    if object_type in ["tables", "views"]:
        # Look for column definition section
        # Pattern: CREATE TABLE name (...) or CREATE VIEW name AS SELECT ...
        import re

        # For tables: count columns between parentheses
        if object_type == "tables":
            match = re.search(r"CREATE TABLE[^(]+\((.*?)\)\s*;", ddl, re.DOTALL | re.IGNORECASE)
            if match:
                columns_section = match.group(1)
                # Count non-constraint lines
                lines = [line.strip() for line in columns_section.split("\n")]
                columns = [line for line in lines if line and not line.upper().startswith(("CONSTRAINT", "PRIMARY", "FOREIGN", "UNIQUE", "CHECK"))]
                return len(columns)

        # For views: extract columns from SELECT or create statement
        if object_type == "views":
            # Try to find column list if explicit: CREATE VIEW name (col1, col2, ...)
            match = re.search(r"CREATE VIEW[^(]+\(([^)]+)\)", ddl, re.IGNORECASE)
            if match:
                columns = [c.strip() for c in match.group(1).split(",")]
                return len(columns)

    return 0


def _extract_constraint_count(ddl: str, object_type: str) -> int:
    """Extract constraint count from DDL statement.

    Args:
        ddl: DDL statement
        object_type: Type of object (table, view, etc.)

    Returns:
        Number of constraints
    """
    if object_type != "tables":
        return 0

    # Count constraint definitions
    # Pattern: CONSTRAINT name TYPE, PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK
    import re

    # Find all PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK occurrences
    # This counts both named and unnamed constraints
    primary_keys = len(re.findall(r"PRIMARY\s+KEY", ddl, re.IGNORECASE))
    foreign_keys = len(re.findall(r"FOREIGN\s+KEY", ddl, re.IGNORECASE))
    uniques = len(re.findall(r"UNIQUE", ddl, re.IGNORECASE))
    checks = len(re.findall(r"CHECK\s*\(", ddl, re.IGNORECASE))

    # Subtract named constraints (avoid double counting)
    # Named constraints: CONSTRAINT name PRIMARY KEY|FOREIGN KEY|UNIQUE|CHECK
    named = len(re.findall(r"CONSTRAINT\s+\w+\s+(PRIMARY|FOREIGN|UNIQUE|CHECK)", ddl, re.IGNORECASE))

    # Total = (all occurrences) - (named constraints which were already counted in the all counts)
    # Actually, named constraints contain the keywords too, so we need to count differently
    # Let's count all occurrences and that's it (each constraint has one PRIMARY/FOREIGN/UNIQUE/CHECK keyword)
    return primary_keys + foreign_keys + uniques + checks


def extract_current_state(extraction_results: Dict[str, List[Any]]) -> Dict[str, Dict[str, Any]]:
    """Extract current state from discovery extraction results.

    Formats extraction results into the structure expected by compare().

    Args:
        extraction_results: Dictionary of object_type -> list of metadata objects

    Returns:
        Dictionary with object types as keys and object states as values
    """
    state = {}

    for object_type, objects in extraction_results.items():
        if not objects:
            state[object_type] = {}
            continue

        type_key = object_type.lower() if object_type.endswith("s") else object_type.lower() + "s"

        if type_key not in state:
            state[type_key] = {}

        for obj in objects:
            # Extract fully qualified name
            fully_qualified_name = _get_qualified_name(obj, object_type)

            # Extract DDL
            ddl = _get_ddl(obj)

            # Count columns
            columns = _get_columns(obj)
            column_count = len(columns)

            # Count constraints
            constraints = _get_constraints(obj)
            constraint_count = len(constraints)

            state[type_key][fully_qualified_name] = {
                "ddl": ddl,
                "ddl_hash": hashlib.sha256(ddl.encode("utf-8")).hexdigest(),
                "column_count": column_count,
                "constraint_count": constraint_count,
                "columns": columns,
                "constraints": constraints
            }

    return state


def _get_qualified_name(obj: Any, object_type: str) -> str:
    """Extract fully qualified name from metadata object.

    Args:
        obj: Metadata object
        object_type: Type of object

    Returns:
        Fully qualified name (DB.SCHEMA.NAME)
    """
    if hasattr(obj, "database") and hasattr(obj, "schema") and hasattr(obj, "name"):
        return f"{obj.database}.{obj.schema}.{obj.name}"
    elif hasattr(obj, "name"):
        return obj.name
    else:
        return str(obj)


def _get_ddl(obj: Any) -> str:
    """Extract DDL from metadata object.

    Args:
        obj: Metadata object

    Returns:
        DDL string
    """
    if hasattr(obj, "ddl"):
        return obj.ddl
    else:
        return ""


def _get_columns(obj: Any) -> List[Dict[str, Any]]:
    """Extract column information from metadata object.

    Args:
        obj: Metadata object

    Returns:
        List of column dictionaries
    """
    if hasattr(obj, "columns"):
        return [{"name": col.name, "type": col.data_type} for col in obj.columns]
    else:
        return []


def _get_constraints(obj: Any) -> List[Dict[str, Any]]:
    """Extract constraint information from metadata object.

    Args:
        obj: Metadata object

    Returns:
        List of constraint dictionaries
    """
    if hasattr(obj, "constraints"):
        return [{"name": c.name, "type": c.type} for c in obj.constraints]
    else:
        return []
