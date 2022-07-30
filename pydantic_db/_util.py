"""Utility functions used throughout the project."""
from typing import Any, Type

from pydantic_db._types import ModelType


def tablename_from_model(model: Type[ModelType], schema: dict[str, Any]) -> str:
    """Get a tablename from the model and schema."""
    try:
        return [tablename for tablename, data in schema.items() if data.model == model][0]
    except IndexError:
        print(model)


def get_joining_tablename(
    table: str, column: str, other_table: str, other_column: str
) -> str:
    """Get the name of a table joining two tables in an MTM relation."""
    return f"{table}.{column}-to-{other_table}.{other_column}"
