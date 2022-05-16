"""Utility functions used throughout the project."""
from typing import Any, Type

from pydantic_db._types import ModelType


def tablename_from_model(model: Type[ModelType], schema: dict[str, Any]) -> str:
    """Get a tablename from the model and schema."""
    return [tablename for tablename, data in schema.items() if data.model == model][0]
