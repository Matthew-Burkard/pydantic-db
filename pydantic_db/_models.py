"""Module for PyDB data models."""
from typing import Generic, Type

from pydantic import BaseModel, Field
from pydantic.generics import GenericModel

from pydantic_db._types import ModelType


class Relationship(BaseModel):
    """Relationship data."""

    foreign_table: str
    back_references: str | None = None


class PyDBTableMeta(GenericModel, Generic[ModelType]):
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

    name_to_data: dict[str, PyDBTableMeta] = Field(  # type: ignore
        default_factory=lambda: {}
    )
    model_to_data: dict[ModelType, PyDBTableMeta] = Field(  # type: ignore
        default_factory=lambda: {}
    )
