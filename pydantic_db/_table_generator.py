"""Module providing SQLAlchemyTableGenerator."""
import uuid
from typing import Any

from pydantic import BaseModel, ConstrainedStr
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

from pydantic_db._table import PyDBTableMeta


class SQLAlchemyTableGenerator:
    """Generate SQL Alchemy tables from pydantic models."""

    def __init__(
        self, engine: AsyncEngine, metadata: MetaData, schema: dict[str, PyDBTableMeta]
    ) -> None:
        self._engine = engine
        self._metadata = metadata
        self._schema = schema

    async def init(self) -> None:
        """Generate SQL Alchemy tables."""
        for tablename, table_data in self._schema.items():
            constraints = [
                [
                    f"{c}_id" if f"{c}_id" in table_data.relationships else c
                    for c in cols
                ]
                for cols in table_data.unique_constraints
            ]
            unique_constraints = (
                UniqueConstraint(*cols, name=f"{'_'.join(cols)}_constraint")
                for cols in constraints
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
        for k, v in table_data.model.__fields__.items():
            kwargs = {
                "primary_key": k == table_data.pk,
                "index": k in table_data.indexed,
                "unique": k in table_data.unique,
            }
            if issubclass(v.type_, BaseModel):
                if v.type_ in [it.model for it in self._schema.values()]:
                    foreign_table = self._tablename_from_model(v.type_)
                    pk = self._schema[foreign_table].pk
                    columns.append(
                        Column(f"{k}_id", ForeignKey(f"{foreign_table}.{pk}"), **kwargs)
                    )
                else:
                    columns.append(Column(k, JSON, **kwargs))
            elif v.type_ is uuid.UUID:
                col_type = (
                    postgresql.UUID if self._engine.name == "postgres" else String(36)
                )
                columns.append(Column(k, col_type, **kwargs))
            elif v.type_ is str or issubclass(v.type_, ConstrainedStr):
                columns.append(Column(k, String(v.field_info.max_length), **kwargs))
            elif v.type_ is int:
                columns.append(Column(k, Integer, **kwargs))
            elif v.type_ is float:
                columns.append(Column(k, Float, **kwargs))
            elif v.type_ is dict:
                columns.append(Column(k, JSON, **kwargs))
            elif v.type_ is list:
                columns.append(Column(k, JSON, **kwargs))
        return tuple(columns)

    def _tablename_from_model(self, model: Any) -> str:
        for tablename, v in self._schema.items():
            if v.model == model:
                return tablename
        raise ValueError("Given model is not a table.")
