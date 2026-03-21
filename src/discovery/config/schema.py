"""Pydantic models for Snowflake discovery configuration."""

from typing import Literal, Optional
from pydantic import BaseModel, Field, field_validator, model_validator

# Valid Snowflake object types
SNOWFLAKE_OBJECT_TYPES = {
    "TABLE",
    "VIEW",
    "PROCEDURE",
    "FUNCTION",
    "STREAM",
    "TASK",
    "DYNAMIC_TABLE",
    "STAGE",
    "PIPE",
    "SEQUENCE",
    "EXTERNAL_TABLE",
}


class VariantSamplingConfig(BaseModel):
    """Configuration for VARIANT column adaptive sampling."""

    # Sample sizes based on row count thresholds
    small_table_threshold: int = Field(
        default=1000,
        description="Row count threshold for small tables (sample all)",
        gt=0,
    )
    small_table_sample_size: Optional[int] = Field(
        default=None,
        description="Sample size for small tables (None = sample all)",
    )
    medium_table_threshold: int = Field(
        default=100000,
        description="Row count threshold for medium tables",
        gt=0,
    )
    medium_table_sample_size: int = Field(
        default=1000,
        description="Sample size for medium tables (1K-100K rows)",
        gt=0,
    )
    large_table_threshold: int = Field(
        default=1000000,
        description="Row count threshold for large tables",
        gt=0,
    )
    large_table_sample_size: int = Field(
        default=5000,
        description="Sample size for large tables (100K-1M rows)",
        gt=0,
    )
    extra_large_sample_size: int = Field(
        default=10000,
        description="Sample size for extra large tables (>1M rows)",
        gt=0,
    )
    min_confidence: float = Field(
        default=0.5,
        description="Minimum confidence threshold for including inferred fields (0.0-1.0)",
        ge=0.0,
        le=1.0,
    )

    @field_validator("small_table_threshold")
    @classmethod
    def thresholds_increasing(cls, v, info):
        if info.data:
            thresholds = [
                info.data.get("small_table_threshold", v),
                info.data.get("medium_table_threshold", 100000),
                info.data.get("large_table_threshold", 1000000),
            ]
            if thresholds != sorted(thresholds):
                raise ValueError("Thresholds must be in increasing order")
        return v


class SchemaConfig(BaseModel):
    """Configuration for a specific schema within a database."""

    name: str = Field(
        ...,
        description="Schema name (e.g., 'PUBLIC', 'STAGING')",
    )
    include_types: list[str] = Field(
        default_factory=list,
        description="List of object types to include (empty = all)",
    )
    exclude_types: list[str] = Field(
        default_factory=list,
        description="List of object types to exclude",
    )

    @field_validator("include_types", "exclude_types")
    @classmethod
    def validate_object_types(cls, v):
        if not v:
            return v
        invalid = set(v) - SNOWFLAKE_OBJECT_TYPES
        if invalid:
            raise ValueError(
                f"Invalid Snowflake object types: {', '.join(sorted(invalid))}. "
                f"Valid types: {', '.join(sorted(SNOWFLAKE_OBJECT_TYPES))}"
            )
        return v

    @model_validator(mode="after")
    def no_type_conflicts(self):
        if self.include_types and self.exclude_types:
            conflicts = set(self.include_types) & set(self.exclude_types)
            if conflicts:
                raise ValueError(
                    f"Types cannot be both included and excluded: {', '.join(sorted(conflicts))}"
                )
        return self


class TargetConfig(BaseModel):
    """Configuration for a database target to discover."""

    database: str = Field(
        ...,
        description="Database name to discover",
    )
    schemas: list[SchemaConfig] = Field(
        ...,
        description="List of schemas to discover",
        min_length=1,
    )

    @model_validator(mode="after")
    def unique_schema_names(self):
        schema_names = [s.name for s in self.schemas]
        if len(schema_names) != len(set(schema_names)):
            raise ValueError(f"Duplicate schema names in database {self.database}")
        return self


class OutputConfig(BaseModel):
    """Configuration for output generation."""

    base_path: str = Field(
        default="discovery",
        description="Base directory for discovery output",
    )
    sql_comments: bool = Field(
        default=True,
        description="Include metadata comments in .sql files",
    )
    json_metadata: bool = Field(
        default=True,
        description="Generate .json metadata files alongside .sql",
    )


class DiscoveryConfig(BaseModel):
    """Top-level configuration for Snowflake environment discovery."""

    targets: list[TargetConfig] = Field(
        ...,
        description="List of database targets to discover",
        min_length=1,
    )
    variant_sampling: VariantSamplingConfig = Field(
        default_factory=lambda: VariantSamplingConfig(),
        description="Configuration for VARIANT column adaptive sampling",
    )
    output: OutputConfig = Field(
        default_factory=lambda: OutputConfig(),
        description="Configuration for output generation",
    )

    @model_validator(mode="after")
    def unique_database_names(self):
        database_names = [t.database for t in self.targets]
        if len(database_names) != len(set(database_names)):
            raise ValueError("Duplicate database names in targets")
        return self

    def get_config_hash(self) -> str:
        """Generate hash of config for change detection."""
        import hashlib
        import json

        config_dict = self.model_dump(mode="json", exclude_none=True)
        config_json = json.dumps(config_dict, sort_keys=True)
        return hashlib.sha256(config_json.encode()).hexdigest()
