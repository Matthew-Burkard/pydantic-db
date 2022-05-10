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
)
from sqlalchemy.dialects import postgresql  # type: ignore
from sqlalchemy.ext.asyncio import AsyncEngine  # type: ignore

from pydantic_db._table import PyDBTableMeta


class SQLAlchemyTableGenerator:
    """Generate SQL Alchemy tables from pydantic models."""

    def __init__(self, engine: AsyncEngine, schema: dict[str, PyDBTableMeta]) -> None:
        self._engine = engine
        self._schema = schema
        self._metadata = MetaData()

    async def init(self) -> None:
        """Generate SQL Alchemy tables."""
        for tablename, table_data in self._schema.items():
            Table(tablename, self._metadata, *self._get_columns(table_data))
        async with self._engine.begin() as conn:
            # TODO Remove drop_all
            await conn.run_sync(self._metadata.drop_all)
            await conn.run_sync(self._metadata.create_all)

    def _get_columns(
        self, table_data: PyDBTableMeta
    ) -> tuple[Column[Any] | Column, ...]:
        columns = []
        for k, v in table_data.model.__fields__.items():
            pk = k == table_data.pk
            index = k in table_data.indexed
            if issubclass(v.type_, BaseModel):
                if v.type_ in [it.model for it in self._schema.values()]:
                    foreign_table = self._tablename_from_model(v.type_)
                    columns.append(
                        Column(
                            f"{k}_id", ForeignKey(f"{foreign_table}.id"), index=index
                        )
                    )
                else:
                    columns.append(Column(k, JSON, index=index))
            elif v.type_ is uuid.UUID:
                col_type = (
                    postgresql.UUID if self._engine.name == "postgres" else String(36)
                )
                columns.append(Column(k, col_type, primary_key=pk, index=index))
            elif v.type_ is str or issubclass(v.type_, ConstrainedStr):
                columns.append(
                    Column(
                        k, String(v.field_info.max_length), primary_key=pk, index=index
                    )
                )
            elif v.type_ is int:
                columns.append(Column(k, Integer, primary_key=pk, index=index))
            elif v.type_ is float:
                columns.append(Column(k, Float, primary_key=pk, index=index))
            elif v.type_ is dict:
                columns.append(Column(k, JSON, primary_key=pk, index=index))
            elif v.type_ is list:
                columns.append(Column(k, JSON, primary_key=pk, index=index))
        return tuple(columns)

    def _tablename_from_model(self, model: Any) -> str:
        for tablename, v in self._schema.items():
            if v.model == model:
                return tablename
        raise ValueError("Given model is not a table.")
