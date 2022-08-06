"""Module for PyDB data models."""
from enum import Enum
from typing import Type

from pydantic import BaseModel, Field

from pydantic_db._types import ModelType


class RelationType(Enum):
    """Table relationship types."""

    ONE_TO_MANY = 1
    MANY_TO_MANY = 2


class MTMData(BaseModel):
    """Stores information about MTM relationships."""

    tablename: str | None = None
    table_a: str | None = None
    table_b: str | None = None
    table_a_column: str | None = None
    table_b_column: str | None = None


class Relationship(BaseModel):
    """Relationship data."""

    foreign_table: str
    relationship_type: RelationType
    mtm_data: MTMData | None = None


class PyDBTable(BaseModel):
    """Table metadata."""

    model: Type[ModelType]
    tablename: str
    pk: str
    indexed: list[str]
    unique: list[str]
    unique_constraints: list[list[str]]
    columns: list[str]
    # Column to relationship.
    relationships: dict[str, Relationship]
    back_references: dict[str, str]


class TableMap(BaseModel):
    """Map tablename to table data and model to table data."""

    name_to_data: dict[str, PyDBTable] = Field(default_factory=lambda: {})
    model_to_data: dict[ModelType, PyDBTable] = Field(default_factory=lambda: {})
