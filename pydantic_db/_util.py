"""Utility functions used throughout the project."""
from typing import Any

from pydantic import BaseModel


def tablename_from_model(model: BaseModel, schema: dict[str, Any]) -> str | None:
    """Get a tablename from the model and schema."""
    try:
        return [
            tablename for tablename, data in schema.items() if data.model == model.type_
        ][0]
    except IndexError:
        return None
