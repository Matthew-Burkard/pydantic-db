"""Utility functions used throughout the project."""
from typing import Type

from pydantic import BaseModel

from pydantic_db._models import TableMap
from pydantic_db._types import ModelType


def tablename_from_model(model: Type[ModelType], table_map: TableMap) -> str:
    """Get a tablename from the model and schema."""
    return [
        tablename
        for tablename, data in table_map.name_to_data.items()
        if data.model == model
    ][0]


def tablename_from_model_instance(model: BaseModel, table_map: TableMap) -> str:
    """Get a tablename from a model instance."""
    # noinspection PyTypeHints
    return [k for k, v in table_map.name_to_data.items() if isinstance(model, v.model)][
        0
    ]
