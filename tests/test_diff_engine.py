"""Tests for structural comparison."""

import pytest

from discovery.diff.engine import (
    DiffEngine,
    DiffResult,
    ObjectState,
    compute_state_hash,
    load_previous_state,
    extract_current_state,
)


def test_diff_engine_initialization():
    """Test DiffEngine initialization."""
    engine = DiffEngine()

    assert engine is not None
    assert hasattr(engine, '_object_types')
    assert len(engine._object_types) == 9


def test_diff_result_initialization():
    """Test DiffResult initialization."""
    result = DiffResult(
        has_changes=True,
        added_objects=["TABLE: db.schema.table1"],
        removed_objects=["VIEW: db.schema.view1"],
        modified_objects=["TABLE: db.schema.table2 (+1 columns)"],
        summary="Changes detected"
    )

    assert result.has_changes is True
    assert len(result.added_objects) == 1
    assert len(result.removed_objects) == 1
    assert len(result.modified_objects) == 1
    assert result.summary == "Changes detected"


def test_diff_result_str_no_changes():
    """Test DiffResult string representation with no changes."""
    result = DiffResult(has_changes=False)

    assert str(result) == "No changes detected"


def test_diff_result_str_with_changes():
    """Test DiffResult string representation with changes."""
    result = DiffResult(
        has_changes=True,
        added_objects=["TABLE: db.schema.table1"],
        removed_objects=["VIEW: db.schema.view1"],
        modified_objects=[]
    )

    result_str = str(result)
    assert "Added: 1 objects" in result_str
    assert "Removed: 1 objects" in result_str


def test_diff_result_str_modified():
    """Test DiffResult string representation with modified objects."""
    result = DiffResult(
        has_changes=True,
        added_objects=[],
        removed_objects=[],
        modified_objects=["TABLE: db.schema.table (+1 columns)"]
    )

    result_str = str(result)
    assert "Modified: 1 objects" in result_str


def test_object_state_initialization():
    """Test ObjectState initialization."""
    state = ObjectState(
        object_type="TABLE",
        fully_qualified_name="DB.SCHEMA.TABLE",
        ddl_hash="abc123",
        column_count=3,
        constraint_count=1
    )

    assert state.object_type == "TABLE"
    assert state.fully_qualified_name == "DB.SCHEMA.TABLE"
    assert state.ddl_hash == "abc123"
    assert state.column_count == 3
    assert state.constraint_count == 1


def test_compare_identical_states():
    """Test comparing identical states."""
    engine = DiffEngine()

    state = {
        "tables": {
            "DB.SCHEMA.TABLE": {
                "ddl": "CREATE TABLE TABLE (id INT);",
                "ddl_hash": "abc123",
                "column_count": 1,
                "constraint_count": 0
            }
        }
    }

    result = engine.compare(state, state)

    assert result.has_changes is False
    assert len(result.added_objects) == 0
    assert len(result.removed_objects) == 0
    assert len(result.modified_objects) == 0


def test_compare_added_object():
    """Test detecting added object."""
    engine = DiffEngine()

    previous = {
        "tables": {}
    }

    current = {
        "tables": {
            "DB.SCHEMA.NEW_TABLE": {
                "ddl": "CREATE TABLE NEW_TABLE (id INT);",
                "ddl_hash": "abc123",
                "column_count": 1,
                "constraint_count": 0
            }
        }
    }

    result = engine.compare(current, previous)

    assert result.has_changes is True
    assert len(result.added_objects) == 1
    assert "TABLE: DB.SCHEMA.NEW_TABLE" in result.added_objects[0]
    assert len(result.removed_objects) == 0
    assert len(result.modified_objects) == 0


def test_compare_removed_object():
    """Test detecting removed object."""
    engine = DiffEngine()

    previous = {
        "tables": {
            "DB.SCHEMA.OLD_TABLE": {
                "ddl": "CREATE TABLE OLD_TABLE (id INT);",
                "ddl_hash": "abc123",
                "column_count": 1,
                "constraint_count": 0
            }
        }
    }

    current = {
        "tables": {}
    }

    result = engine.compare(current, previous)

    assert result.has_changes is True
    assert len(result.removed_objects) == 1
    assert "TABLE: DB.SCHEMA.OLD_TABLE" in result.removed_objects[0]
    assert len(result.added_objects) == 0
    assert len(result.modified_objects) == 0


def test_compare_modified_object_ddl_change():
    """Test detecting object modified by DDL change."""
    engine = DiffEngine()

    previous = {
        "tables": {
            "DB.SCHEMA.TABLE": {
                "ddl": "CREATE TABLE TABLE (id INT);",
                "ddl_hash": "abc123",
                "column_count": 1,
                "constraint_count": 0
            }
        }
    }

    current = {
        "tables": {
            "DB.SCHEMA.TABLE": {
                "ddl": "CREATE TABLE TABLE (id INT, name VARCHAR);",
                "ddl_hash": "def456",
                "column_count": 2,
                "constraint_count": 0
            }
        }
    }

    result = engine.compare(current, previous)

    assert result.has_changes is True
    assert len(result.modified_objects) == 1
    assert "TABLE: DB.SCHEMA.TABLE" in result.modified_objects[0]
    assert "DDL changed" in result.modified_objects[0]


def test_compare_modified_object_column_count_change():
    """Test detecting object modified by column count change."""
    engine = DiffEngine()

    previous = {
        "tables": {
            "DB.SCHEMA.TABLE": {
                "ddl": "CREATE TABLE TABLE (id INT);",
                "ddl_hash": "abc123",
                "column_count": 1,
                "constraint_count": 0
            }
        }
    }

    current = {
        "tables": {
            "DB.SCHEMA.TABLE": {
                "ddl": "CREATE TABLE TABLE (id INT);",
                "ddl_hash": "abc123",
                "column_count": 2,
                "constraint_count": 0
            }
        }
    }

    result = engine.compare(current, previous)

    assert result.has_changes is True
    assert len(result.modified_objects) == 1
    assert "column count" in result.modified_objects[0].lower()


def test_compare_modified_object_constraint_count_change():
    """Test detecting object modified by constraint count change."""
    engine = DiffEngine()

    previous = {
        "tables": {
            "DB.SCHEMA.TABLE": {
                "ddl": "CREATE TABLE TABLE (id INT);",
                "ddl_hash": "abc123",
                "column_count": 1,
                "constraint_count": 0
            }
        }
    }

    current = {
        "tables": {
            "DB.SCHEMA.TABLE": {
                "ddl": "CREATE TABLE TABLE (id INT);",
                "ddl_hash": "abc123",
                "column_count": 1,
                "constraint_count": 1
            }
        }
    }

    result = engine.compare(current, previous)

    assert result.has_changes is True
    assert len(result.modified_objects) == 1
    assert "constraint count" in result.modified_objects[0].lower()


def test_compare_multiple_changes():
    """Test detecting multiple changes."""
    engine = DiffEngine()

    previous = {
        "tables": {
            "DB.SCHEMA.TABLE1": {
                "ddl": "CREATE TABLE TABLE1 (id INT);",
                "ddl_hash": "abc123",
                "column_count": 1,
                "constraint_count": 0
            },
            "DB.SCHEMA.TABLE2": {
                "ddl": "CREATE TABLE TABLE2 (id INT);",
                "ddl_hash": "def456",
                "column_count": 1,
                "constraint_count": 0
            }
        }
    }

    current = {
        "tables": {
            "DB.SCHEMA.TABLE1": {
                "ddl": "CREATE TABLE TABLE1 (id INT, name VARCHAR);",
                "ddl_hash": "xyz789",
                "column_count": 2,
                "constraint_count": 0
            },
            "DB.SCHEMA.TABLE3": {
                "ddl": "CREATE TABLE TABLE3 (id INT);",
                "ddl_hash": "new123",
                "column_count": 1,
                "constraint_count": 0
            }
        }
    }

    result = engine.compare(current, previous)

    assert result.has_changes is True
    assert len(result.added_objects) == 1  # TABLE3
    assert len(result.removed_objects) == 1  # TABLE2
    assert len(result.modified_objects) == 1  # TABLE1


def test_compare_different_object_types():
    """Test comparing states with different object types."""
    engine = DiffEngine()

    previous = {
        "tables": {
            "DB.SCHEMA.TABLE": {
                "ddl": "CREATE TABLE TABLE (id INT);",
                "ddl_hash": "abc123",
                "column_count": 1,
                "constraint_count": 0
            }
        }
    }

    current = {
        "tables": {
            "DB.SCHEMA.TABLE": {
                "ddl": "CREATE TABLE TABLE (id INT);",
                "ddl_hash": "abc123",
                "column_count": 1,
                "constraint_count": 0
            }
        },
        "views": {
            "DB.SCHEMA.VIEW": {
                "ddl": "CREATE VIEW VIEW AS SELECT * FROM TABLE;",
                "ddl_hash": "view123",
                "column_count": 1,
                "constraint_count": 0
            }
        }
    }

    result = engine.compare(current, previous)

    assert result.has_changes is True
    assert len(result.added_objects) == 1
    assert "VIEW: DB.SCHEMA.VIEW" in result.added_objects[0]


def test_compute_state_hash():
    """Test state hash computation."""
    state = {
        "tables": {
            "DB.SCHEMA.TABLE": {
                "ddl": "CREATE TABLE TABLE (id INT);",
                "ddl_hash": "abc123",
                "column_count": 1,
                "constraint_count": 0
            }
        }
    }

    hash1 = compute_state_hash(state)
    hash2 = compute_state_hash(state)

    assert hash1 == hash2
    assert isinstance(hash1, str)
    assert len(hash1) == 64  # SHA256 hex digest


def test_compute_state_hash_different_states():
    """Test that different states produce different hashes."""
    state1 = {
        "tables": {
            "DB.SCHEMA.TABLE1": {
                "ddl": "CREATE TABLE TABLE1 (id INT);",
                "ddl_hash": "abc123",
                "column_count": 1,
                "constraint_count": 0
            }
        }
    }

    state2 = {
        "tables": {
            "DB.SCHEMA.TABLE2": {
                "ddl": "CREATE TABLE TABLE2 (id INT);",
                "ddl_hash": "def456",
                "column_count": 1,
                "constraint_count": 0
            }
        }
    }

    hash1 = compute_state_hash(state1)
    hash2 = compute_state_hash(state2)

    assert hash1 != hash2


def test_compute_state_hash_empty_state():
    """Test hash computation for empty state."""
    state = {
        "tables": {},
        "views": {},
        "procedures": {}
    }

    result = compute_state_hash(state)

    assert isinstance(result, str)
    assert len(result) == 64


def test_diff_engine_all_object_types():
    """Test that DiffEngine handles all supported object types."""
    engine = DiffEngine()

    expected_types = [
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

    assert sorted(engine._object_types) == sorted(expected_types)


def test_compare_with_empty_states():
    """Test comparing when both states are empty."""
    engine = DiffEngine()

    previous = {
        "tables": {},
        "views": {}
    }

    current = {
        "tables": {},
        "views": {}
    }

    result = engine.compare(current, previous)

    assert result.has_changes is False
    assert len(result.added_objects) == 0
    assert len(result.removed_objects) == 0
    assert len(result.modified_objects) == 0


def test_compare_with_missing_object_type_in_previous():
    """Test comparing when object type exists only in current."""
    engine = DiffEngine()

    previous = {
        "tables": {
            "DB.SCHEMA.TABLE": {
                "ddl": "CREATE TABLE TABLE (id INT);",
                "ddl_hash": "abc123",
                "column_count": 1,
                "constraint_count": 0
            }
        }
    }

    current = {
        "tables": {
            "DB.SCHEMA.TABLE": {
                "ddl": "CREATE TABLE TABLE (id INT);",
                "ddl_hash": "abc123",
                "column_count": 1,
                "constraint_count": 0
            }
        },
        "streams": {
            "DB.SCHEMA.STREAM": {
                "ddl": "CREATE STREAM STREAM ON TABLE TABLE;",
                "ddl_hash": "stream123",
                "column_count": 0,
                "constraint_count": 0
            }
        }
    }

    result = engine.compare(current, previous)

    assert result.has_changes is True
    assert len(result.added_objects) == 1
    assert "STREAM: DB.SCHEMA.STREAM" in result.added_objects[0]


def test_diff_result_summary_field():
    """Test that DiffResult summary field is set correctly."""
    result = DiffResult(
        has_changes=True,
        added_objects=["TABLE: db.schema.table"],
        summary="Custom summary"
    )

    assert result.summary == "Custom summary"
    assert str(result) == "Custom summary"


def test_get_change_details_ddl():
    """Test getting change details for DDL change."""
    engine = DiffEngine()

    current = {
        "ddl_hash": "new_hash",
        "column_count": 1,
        "constraint_count": 0
    }

    previous = {
        "ddl_hash": "old_hash",
        "column_count": 1,
        "constraint_count": 0
    }

    details = engine._get_change_details(current, previous)

    assert "DDL changed" in details


def test_get_change_details_columns():
    """Test getting change details for column change."""
    engine = DiffEngine()

    current = {
        "ddl_hash": "same_hash",
        "column_count": 2,
        "constraint_count": 0
    }

    previous = {
        "ddl_hash": "same_hash",
        "column_count": 1,
        "constraint_count": 0
    }

    details = engine._get_change_details(current, previous)

    assert "column count" in details.lower()


def test_get_change_details_constraints():
    """Test getting change details for constraint change."""
    engine = DiffEngine()

    current = {
        "ddl_hash": "same_hash",
        "column_count": 1,
        "constraint_count": 1
    }

    previous = {
        "ddl_hash": "same_hash",
        "column_count": 1,
        "constraint_count": 0
    }

    details = engine._get_change_details(current, previous)

    assert "constraint count" in details.lower()


def test_get_change_details_multiple():
    """Test getting change details for multiple changes."""
    engine = DiffEngine()

    current = {
        "ddl_hash": "new_hash",
        "column_count": 2,
        "constraint_count": 1
    }

    previous = {
        "ddl_hash": "old_hash",
        "column_count": 1,
        "constraint_count": 0
    }

    details = engine._get_change_details(current, previous)

    assert "DDL changed" in details
    assert "column count" in details.lower()
    assert "constraint count" in details.lower()


def test_has_object_changes_no_changes():
    """Test _has_object_changes with no changes."""
    engine = DiffEngine()

    current = {
        "ddl_hash": "same_hash",
        "column_count": 1,
        "constraint_count": 0
    }

    previous = {
        "ddl_hash": "same_hash",
        "column_count": 1,
        "constraint_count": 0
    }

    assert not engine._has_object_changes(current, previous)


def test_has_object_changes_with_changes():
    """Test _has_object_changes with changes."""
    engine = DiffEngine()

    current = {
        "ddl_hash": "new_hash",
        "column_count": 1,
        "constraint_count": 0
    }

    previous = {
        "ddl_hash": "old_hash",
        "column_count": 1,
        "constraint_count": 0
    }

    assert engine._has_object_changes(current, previous)


def test_compare_empty_previous_non_empty_current():
    """Test comparing when previous is empty and current has objects."""
    engine = DiffEngine()

    previous = {
        "tables": {}
    }

    current = {
        "tables": {
            "DB.SCHEMA.TABLE": {
                "ddl": "CREATE TABLE TABLE (id INT);",
                "ddl_hash": "abc123",
                "column_count": 1,
                "constraint_count": 0
            }
        }
    }

    result = engine.compare(current, previous)

    assert result.has_changes is True
    assert len(result.added_objects) == 1


def test_compare_non_empty_previous_empty_current():
    """Test comparing when previous has objects and current is empty."""
    engine = DiffEngine()

    previous = {
        "tables": {
            "DB.SCHEMA.TABLE": {
                "ddl": "CREATE TABLE TABLE (id INT);",
                "ddl_hash": "abc123",
                "column_count": 1,
                "constraint_count": 0
            }
        }
    }

    current = {
        "tables": {}
    }

    result = engine.compare(current, previous)

    assert result.has_changes is True
    assert len(result.removed_objects) == 1
