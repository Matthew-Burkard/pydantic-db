"""Module providing classes to store table metadata."""
from enum import auto, Enum
from typing import Generic, Type

from pydantic import BaseModel
from pydantic.generics import GenericModel

from pydantic_db._types import ModelType


# PyCharm thinks auto() takes an argument.
# noinspection PyArgumentList
class RelationshipType(Enum):
    """Table relationship types."""

    ONE_TO_MANY = auto()
    MANY_TO_MANY = auto()


class Relation(BaseModel):
    """Describes a relationship from one table to another."""

    foreign_table: str
    back_references: str | None = None
    type: RelationshipType


class PyDBTableMeta(GenericModel, Generic[ModelType]):
    """Class to store table information."""

    name: str
    model: Type[ModelType]
    pk: str
    indexed: list[str]
    unique: list[str]
    unique_constraints: list[list[str]]
    columns: list[str]
    relationships: dict[str, Relation]
    back_references: dict[str, str]
