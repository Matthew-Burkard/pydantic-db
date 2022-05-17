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

from pydantic_db._table import PyDBTableMeta, RelationType
from pydantic_db._util import get_joining_tablename, tablename_from_model


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
                "nullable": not v.required,
            }
            if issubclass(v.type_, BaseModel):
                # Get foreign table name from schema.
                if back_reference := table_data.back_references.get(k):
                    foreign_table = tablename_from_model(v.type_, self._schema)
                    if (
                        table_data.relationships[k].relation_type
                        != RelationType.MANY_TO_MANY
                    ):
                        # This field is not a column.
                        continue
                    if self._mtm.get(table_data.name) == k:
                        # This mtm has already been made.
                        continue
                    # Create joining table.
                    self._mtm[foreign_table] = back_reference
                    mtm_tablename = get_joining_tablename(
                        table=table_data.name,
                        column=k,
                        other_table=foreign_table,
                        other_column=back_reference,
                    )
                    Table(
                        mtm_tablename,
                        self._metadata,
                        *self._get_mtm_columns(table_data.name, foreign_table),
                    )
                if v.type_ in [it.model for it in self._schema.values()]:
                    foreign_table = tablename_from_model(v.type_, self._schema)
                    foreign_data = self._schema[foreign_table]
                    columns.append(
                        Column(
                            f"{k}_id",
                            ForeignKey(f"{foreign_table}.{foreign_data.pk}"),
                            **kwargs,
                        )
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

    def _get_mtm_columns(self, table_a: str, table_b: str) -> list[Column]:
        col_type = postgresql.UUID if self._engine.name == "postgres" else String(36)
        table_a_pk = self._schema[table_a].pk
        table_b_pk = self._schema[table_b].pk
        table_a_col_name = f"{table_a}_id"
        table_b_col_name = f"{table_b}_id"
        if table_a == table_b:
            table_a_col_name = f"{table_a}_a_id"
            table_b_col_name = f"{table_b}_b_id"
        columns = [
            Column("id", col_type, primary_key=True),
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
