"""Module providing PyDBTable."""
from typing import Generic, Type

from pydantic.generics import GenericModel

from pydantic_db._types import ModelType


class PyDBTableMeta(GenericModel, Generic[ModelType]):
    """Class to store table information."""

    name: str
    model: Type[ModelType]
    pk: str
    indexed: list[str]
    unique: list[str]
    unique_constraints: list[list[str]]
    columns: list[str]
    relationships: dict[str, str]
