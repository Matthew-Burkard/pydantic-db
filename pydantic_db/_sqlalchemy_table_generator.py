"""Module providing SQLAlchemyTableGenerator."""
import uuid
from typing import Any

from pydantic import BaseModel, ConstrainedStr
from sqlalchemy import Column, Float, ForeignKey, Integer, JSON, MetaData, String, Table
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncEngine

from pydantic_db._model_type import ModelType

metadata = MetaData()


class SQLAlchemyTableGenerator:
    """Generate SQL Alchemy tables from pydantic models."""

    def __init__(self, engine: AsyncEngine, schema: dict[str, ModelType]) -> None:
        self._engine = engine
        self._tables: dict[ModelType, Table] = {}
        self._schema = schema

    async def init(self) -> None:
        """Generate SQL Alchemy tables."""
        for tablename, model in self._schema.items():
            self._generate_table(tablename, model)
        async with self._engine.begin() as conn:
            # TODO Remove drop_all
            await conn.run_sync(metadata.drop_all)
            await conn.run_sync(metadata.create_all)

    def _generate_table(self, tablename: str, pydantic_model: BaseModel) -> None:
        self._tables[pydantic_model] = Table(
            tablename, metadata, *self._get_columns(pydantic_model)
        )

    def _get_columns(
        self, pydantic_model: BaseModel
    ) -> tuple[Column[Any] | Column, ...]:
        columns = []
        for k, v in pydantic_model.__fields__.items():
            pk = v.field_info.extra.get("pk") or False
            if issubclass(v.type_, BaseModel):
                if v.type_ in self._schema.values():
                    foreign_table = self._tablename_from_model(v.type_)
                    columns.append(
                        Column(f"{k}_id", ForeignKey(f"{foreign_table}.id"))
                    )
                else:
                    columns.append(Column(k, JSON))
            elif v.type_ is uuid.UUID:
                col_type = (
                    postgresql.UUID if self._engine.name == "postgres" else String(36)
                )
                columns.append(Column(k, col_type, primary_key=pk))
            elif v.type_ is str or issubclass(v.type_, ConstrainedStr):
                columns.append(
                    Column(k, String(v.field_info.max_length), primary_key=pk)
                )
            elif v.type_ is int:
                columns.append(Column(k, Integer, primary_key=pk))
            elif v.type_ is float:
                columns.append(Column(k, Float, primary_key=pk))
            elif v.type_ is dict:
                columns.append(Column(k, JSON, primary_key=pk))
            elif v.type_ is list:
                columns.append(Column(k, JSON, primary_key=pk))
        return tuple(columns)

    def _tablename_from_model(self, model: Any) -> str:
        for tablename, v in self._schema.items():
            if v == model:
                return tablename
