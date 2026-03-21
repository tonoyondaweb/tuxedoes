#!/usr/bin/env python3
"""Test script for DDL generator."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from discovery.generate.ddl_generator import generate_ddl_file
from discovery.types import TableMetadata, ConstraintMetadata

# Create test metadata with constraints
pk_constraint = ConstraintMetadata(
    name="pk_users_id",
    type="PRIMARY KEY",
    columns=["id"],
    referenced_table=None,
    referenced_columns=None
)

fk_constraint = ConstraintMetadata(
    name="fk_users_department",
    type="FOREIGN KEY",
    columns=["department_id"],
    referenced_table="departments",
    referenced_columns=["id"]
)

tm = TableMetadata(
    name='users',
    schema='PUBLIC',
    database='ANALYTICS',
    ddl='CREATE OR REPLACE TABLE ANALYTICS.PUBLIC.users (\n  id INT NOT NULL,\n  name VARCHAR(100),\n  email VARCHAR(255),\n  department_id INT\n);',
    columns=[],
    row_count=1000,
    bytes=50000,
    last_ddl='2025-01-01 10:00:00',
    clustering_key='id',
    constraints=[pk_constraint, fk_constraint],
    tags=[],
    masking_policies=[],
    search_optimization=False,
    variant_schema=None
)

print(generate_ddl_file(tm))
