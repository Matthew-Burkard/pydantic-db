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

from pydantic_db._table import PyDBTableMeta, RelationType
from pydantic_db._util import tablename_from_model
from pydantic_db.errors import TypeConversionError


class SQLAlchemyTableGenerator:
    """Generate SQL Alchemy tables from pydantic models."""

    def __init__(
        self, engine: AsyncEngine, metadata: MetaData, schema: dict[str, PyDBTableMeta]
    ) -> None:
        self._engine = engine
        self._metadata = metadata
        self._schema = schema
        self._mtm: dict[str, str] = {}  # {table_a name: table_b field name}

    async def init(self) -> None:
        """Generate SQL Alchemy tables."""
        for tablename, table_data in self._schema.items():
            unique_constraints = (
                UniqueConstraint(*cols, name=f"{'_'.join(cols)}_constraint")
                for cols in table_data.unique_constraints
            )
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
            column = self._get_column(table_data, field_name, field, **kwargs)
            if column is not None:
                columns.append(column)
        return tuple(columns)

    def _get_column(
        self, table_data: PyDBTableMeta, field_name: str, field: ModelField, **kwargs
    ) -> Column | None:
        outer_origin = get_origin(field.outer_type_)
        origin = get_origin(field.type_)
        if outer_origin and outer_origin == list:
            return self._get_column_from_type_args(
                table_data, field_name, field, **kwargs
            )
        if origin:
            if origin == UnionType:
                if (
                    column := self._get_column_from_type_args(
                        table_data, field_name, field, **kwargs
                    )
                ) is not None:
                    return column
                else:
                    raise TypeConversionError(field.type_)
            else:
                raise TypeConversionError(field.type_)
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
        if field.type_ is dict:
            return Column(field_name, JSON, **kwargs)
        if field.type_ is list:
            return Column(field_name, JSON, **kwargs)

    def _get_column_from_type_args(
        self, table_data: PyDBTableMeta, field_name: str, field: ModelField, **kwargs
    ) -> Column | None:
        # Get foreign table name from schema.
        if back_reference := table_data.back_references.get(field_name):
            foreign_table = tablename_from_model(field.type_, self._schema)
            if (
                table_data.relationships[field_name].relation_type
                != RelationType.MANY_TO_MANY
            ):
                # This field is not a column.
                return
            if self._mtm.get(f"{table_data.name}.{back_reference}") == field_name:
                # This mtm has already been made.
                return
            # Create joining table.
            self._mtm[f"{foreign_table}.{field_name}"] = back_reference
            Table(
                table_data.relationships[field_name].mtm_table,
                self._metadata,
                *self._get_mtm_columns(table_data.name, foreign_table),
            )
            return
        for arg in get_args(field.type_):
            if arg in [it.model for it in self._schema.values()]:
                foreign_table = tablename_from_model(arg, self._schema)
                foreign_data = self._schema[foreign_table]
                return Column(
                    field_name,
                    ForeignKey(f"{foreign_table}.{foreign_data.pk}"),
                    **kwargs,
                )

    def _get_mtm_columns(self, table_a: str, table_b: str) -> list[Column]:
        table_a_pk = self._schema[table_a].pk
        table_b_pk = self._schema[table_b].pk
        table_a_col_name = table_a
        table_b_col_name = table_b
        if table_a == table_b:
            table_a_col_name = f"{table_a}_a"
            table_b_col_name = f"{table_b}_b"
        columns = [
            Column(
                table_a_col_name,
                ForeignKey(f"{table_a}.{table_a_pk}"),
            ),
            Column(
                table_b_col_name,
                ForeignKey(f"{table_b}.{table_b_pk}"),
            ),
        ]
        return columns
