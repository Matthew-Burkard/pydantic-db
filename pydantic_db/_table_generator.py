"""Module providing SQLAlchemyTableGenerator."""
import uuid
from types import UnionType
from typing import Any, get_args, get_origin

from pydantic import BaseModel, ConstrainedStr
from pydantic.fields import ModelField
from sqlalchemy import (  # type: ignore
    Column,
    Float,
    ForeignKey,
    Integer,
    JSON,
    MetaData,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.dialects import postgresql  # type: ignore
from sqlalchemy.ext.asyncio import AsyncEngine  # type: ignore

from pydantic_db._models import PyDBTableMeta, TableMap
from pydantic_db._util import tablename_from_model
from pydantic_db.errors import TypeConversionError


class DBTableGenerator:
    """Generate SQL Alchemy tables from pydantic models."""

    def __init__(
        self, engine: AsyncEngine, metadata: MetaData, table_map: TableMap
    ) -> None:
        self._engine = engine
        self._metadata = metadata
        self._table_map = table_map
        self._tables: list[str] = []

    async def init(self) -> None:
        """Generate SQL Alchemy tables."""
        for tablename, table_data in self._table_map.name_to_data.items():
            unique_constraints = (
                UniqueConstraint(*cols, name=f"{'_'.join(cols)}_constraint")
                for cols in table_data.unique_constraints
            )
            self._tables.append(tablename)
            Table(
                tablename,
                self._metadata,
                *self._get_columns(table_data),
                *unique_constraints,
            )
        async with self._engine.begin() as conn:
            await conn.run_sync(self._metadata.create_all)

    def _get_columns(
        self, table_data: PyDBTableMeta
    ) -> tuple[Column[Any] | Column, ...]:
        columns = []
        for field_name, field in table_data.model.__fields__.items():
            kwargs = {
                "primary_key": field_name == table_data.pk,
                "index": field_name in table_data.indexed,
                "unique": field_name in table_data.unique,
                "nullable": not field.required,
            }
            column = self._get_column(field_name, field, **kwargs)
            if column is not None:
                columns.append(column)
        return tuple(columns)

    def _get_column(
        self, field_name: str, field: ModelField, **kwargs
    ) -> Column | None:
        outer_origin = get_origin(field.outer_type_)
        origin = get_origin(field.type_)
        if outer_origin and outer_origin == list:
            return self._get_column_from_type_args(field_name, field, **kwargs)
        if origin:
            if origin == UnionType:
                if (
                    column := self._get_column_from_type_args(
                        field_name, field, **kwargs
                    )
                ) is not None:
                    return column
                else:
                    raise TypeConversionError(field.type_)
            else:
                raise TypeConversionError(field.type_)
        if get_origin(field.outer_type_) == dict:
            return Column(field_name, JSON, **kwargs)
        if issubclass(field.type_, BaseModel):
            return Column(field_name, JSON, **kwargs)
        if field.type_ is uuid.UUID:
            col_type = (
                postgresql.UUID if self._engine.name == "postgres" else String(36)
            )
            return Column(field_name, col_type, **kwargs)
        if field.type_ is str or issubclass(field.type_, ConstrainedStr):
            return Column(field_name, String(field.field_info.max_length), **kwargs)
        if field.type_ is int:
            return Column(field_name, Integer, **kwargs)
        if field.type_ is float:
            return Column(field_name, Float, **kwargs)
        # Catchall for dict/list or any other.
        return Column(field_name, JSON, **kwargs)

    def _get_column_from_type_args(
        self, field_name: str, field: ModelField, **kwargs
    ) -> Column | None:
        for arg in get_args(field.type_):
            if arg in [it.model for it in self._table_map.name_to_data.values()]:
                foreign_table = tablename_from_model(arg, self._table_map)
                foreign_data = self._table_map.name_to_data[foreign_table]
                return Column(
                    field_name,
                    ForeignKey(f"{foreign_table}.{foreign_data.pk}"),
                    **kwargs,
                )
        return None
