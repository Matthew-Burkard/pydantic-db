"""Utility functions used throughout the project."""
import json
from typing import Any, Type
from uuid import UUID

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


def py_type_to_sql(table_map: TableMap, value: Any) -> Any:
    """Get value as SQL compatible type."""
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    if isinstance(value, BaseModel) and type(value) in table_map.model_to_data:
        tablename = tablename_from_model_instance(value, table_map)
        return py_type_to_sql(
            table_map, value.__dict__[table_map.name_to_data[tablename].pk]
        )
    if isinstance(value, BaseModel):
        return value.json()
    return value
