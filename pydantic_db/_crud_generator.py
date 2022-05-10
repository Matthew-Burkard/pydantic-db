"""Generate Python CRUD methods for a model."""
import asyncio
import json
from typing import Any, Callable, Generic, Iterable, Type
from uuid import UUID

import pydantic
from pydantic import BaseModel
from pydantic.generics import GenericModel
from pypika import Field, Order, Query, Table  # type: ignore
from pypika.queries import QueryBuilder  # type: ignore
from sqlalchemy import text  # type: ignore
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession  # type: ignore
from sqlalchemy.orm import sessionmaker  # type: ignore

from pydantic_db._table import PyDBTableMeta
from pydantic_db._types import ModelType


class Result(GenericModel, Generic[ModelType]):
    """Search result object."""

    offset: int
    limit: int
    data: list[ModelType]


class CRUDMethods(GenericModel, Generic[ModelType]):
    """Holds CRUD methods for a model."""

    find_one: Callable[[UUID], ModelType]
    find_many: Callable[
        [dict[str, Any] | None, list[str] | None, Order, int, int, int],
        list[Result[ModelType]],
    ]
    insert: Callable[[ModelType], ModelType]
    update: Callable[[ModelType], ModelType]
    upsert: Callable[[ModelType], ModelType]
    delete: Callable[[UUID], bool]


class CRUDGenerator(Generic[ModelType]):
    """Provides DB CRUD methods for a model type."""

    def __init__(
        self,
        tablename: str,
        engine: AsyncEngine,
        schema: dict[str, PyDBTableMeta],
    ) -> None:
        """Provides DB CRUD methods for a model type.

        :param tablename: Name of the corresponding database table.
        :param engine: A SQL Alchemy async engine.
        :param schema: Map of tablename to table information objects.
        """
        self._tablename = tablename
        self._table = Table(tablename)
        self._engine = engine
        self._schema = schema
        self._field_to_column: dict[Any, str] = {}

    async def find_one(self, pk: Any, depth: int = 0) -> ModelType | None:
        """Get one record.

        :param pk: Primary key of the record to get.
        :param depth: ORM fetch depth.
        :return: A model representing the record if it exists else None.
        """
        pydb_table = self._schema[self._tablename]
        query, columns = self._build_joins(
            Query.from_(self._table),
            pydb_table,
            depth,
            self._columns(pydb_table),
        )
        query = query.where(self._table.id == self._py_type_to_sql(pk)).select(*columns)
        result = await self._execute(query)
        try:
            # noinspection PyProtectedMember
            return self._model_from_row_mapping(next(result)._mapping)
        except StopIteration:
            return None

    async def find_many(
        self,
        where: dict[str, Any] | None = None,
        order_by: list[str] | None = None,
        order: Order = Order.asc,
        limit: int = 0,
        offset: int = 0,
        depth: int = 0,
    ) -> Result[ModelType]:
        """Get many records.

        :param where: Dictionary of column name to desired value.
        :param order_by: Columns to order by.
        :param order: Order results by ascending or descending.
        :param limit: Number of records to return.
        :param offset: Number of records to offset by.
        :param depth: Depth of relations to populate.
        :return: A list of models representing table records.
        """
        where = where or {}
        order_by = order_by or []
        pydb_table = self._schema[self._tablename]
        query, columns = self._build_joins(
            Query.from_(self._table),
            pydb_table,
            depth,
            self._columns(pydb_table),
        )
        for field, value in where.items():
            query = query.where(self._table.field(field) == value)
        query = query.orderby(*order_by, order=order).select(*columns)
        if limit:
            query = query.limit(limit)
        if offset:
            query = query.offset(offset)
        result = await self._execute(query)
        # noinspection PyProtectedMember
        return Result(
            offset=offset,
            limit=limit,
            data=[self._model_from_row_mapping(row._mapping) for row in result],
        )

    async def insert(self, model_instance: ModelType) -> ModelType:
        """Insert a record.

        :param model_instance: Instance to save as database record.
        :return: Inserted model.
        """
        inserts = self._get_inserts(model_instance)
        await self._execute_many((text(s) for s in inserts))
        return model_instance

    async def update(self, model_instance: ModelType) -> ModelType:  # TODO
        """Update a record.

        :param model_instance: Model representing record to update.
        :return: The updated model.
        """
        statement = text("")
        await self._execute(statement)
        return model_instance

    async def upsert(self, model_instance: ModelType) -> ModelType:
        """Insert a record if it does not exist, else update it.

        :param model_instance: Model representing record to insert or
            update.
        :return: The inserted or updated model.
        """
        if model := await self.find_one(
            model_instance.__dict__[self._schema[self._tablename].pk]
        ):
            if model == model_instance:
                return model
            return await self.update(model_instance)
        return await self.insert(model_instance)

    async def delete(self, pk: Any) -> bool:
        """Delete a record."""
        await self._execute(
            Query.from_(self._table)
            .where(self._table.field(self._schema[self._tablename].pk) == pk)
            .delete()
        )
        return True

    async def _execute(self, query: QueryBuilder) -> Any:
        async_session = sessionmaker(
            self._engine, expire_on_commit=False, class_=AsyncSession
        )
        async with async_session() as session:
            async with session.begin():
                result = await session.execute(text(str(query)))
            await session.commit()
        await self._engine.dispose()
        return result

    async def _execute_many(
        self, statements: Iterable[str]
    ) -> tuple[
        BaseException | Any,
        BaseException | Any,
        BaseException | Any,
        BaseException | Any,
        BaseException | Any,
    ]:
        async_session = sessionmaker(
            self._engine, expire_on_commit=False, class_=AsyncSession
        )
        async with async_session() as session:
            async with session.begin():
                results = await asyncio.gather(
                    *(session.execute(s) for s in statements)
                )
            await session.commit()
        await self._engine.dispose()
        return results

    def _columns(self, pydb_table: PyDBTableMeta) -> list[Field]:
        return [
            self._table.field(c).as_(f"{self._tablename}//{c}")
            for c in pydb_table.columns
        ]

    def _build_joins(
        self,
        query: QueryBuilder,
        table: PyDBTableMeta,
        depth: int,
        columns: list[Field],
        table_tree: str | None = None,
    ) -> tuple[QueryBuilder, list[Field]]:
        if depth and (relationships := self._schema[table.name].relationships):
            depth -= 1
            table_tree = table_tree or table.name
            pypika_table: Table = Table(table.name)
            if table.name != table_tree:
                pypika_table = pypika_table.as_(table_tree)
            # For each related table, add join to query.
            for field_name, tablename in relationships.items():
                relation_name = f"{table_tree}/{field_name.removesuffix('_id')}"
                rel_table = Table(tablename).as_(relation_name)
                query = query.left_join(rel_table).on(
                    pypika_table.field(field_name)
                    == rel_table.field(self._schema[tablename].pk)
                )
                columns.extend(
                    [
                        rel_table.field(c).as_(f"{relation_name}//{c}")
                        for c in self._schema[tablename].columns
                    ]
                )
                # Add joins of relations of this table to query.
                query, new_cols = self._build_joins(
                    query, self._schema[tablename], depth, columns, relation_name
                )
                columns.extend([c for c in new_cols if c not in columns])
        return query, columns

    def _get_inserts(
        self, model_instance: ModelType, inserts: list[str] | None = None
    ) -> list[str]:
        inserts = inserts or []
        schema_info = self._schema[self._tablename_from_model_instance(model_instance)]
        columns = [c for c in schema_info.columns]
        values = [
            self._py_type_to_sql(
                model_instance.__dict__[
                    c.removesuffix("_id") if c in schema_info.relationships else c
                ]
            )
            for c in columns
        ]
        for k, v in type(model_instance).__fields__.items():
            if k in schema_info.relationships:
                inserts = self._get_inserts(model_instance.__dict__[k]) + inserts
        inserts.append(str(Query.into(self._table).columns(*columns).insert(*values)))
        return inserts

    def _get_upserts(self) -> list[str]:
        return [""] or [str(self)]  # TODO

    def _model_from_row_mapping(
        self,
        row_mapping: dict[str, Any],
        model_type: Type[ModelType] | None = None,
        table_tree: str | None = None,
    ) -> ModelType:
        model_type = model_type or self._schema[self._tablename].model
        table_tree = table_tree or self._tablename
        py_type = {}
        schema_info = self._schema[self._tablename_from_model(model_type)]
        for column, value in row_mapping.items():
            if not column.startswith(f"{table_tree}//"):
                # This must be a column somewhere else in the tree.
                continue
            column_name = column.removeprefix(f"{table_tree}//")
            if column_name in schema_info.relationships:
                if value is None:
                    # No further depth has been found.
                    continue
                foreign_table = self._schema[schema_info.relationships[column_name]]
                py_type[column_name.removesuffix("_id")] = self._model_from_row_mapping(
                    row_mapping={
                        k.removeprefix(f"{table_tree}/"): v
                        for k, v in row_mapping.items()
                        if not k.startswith(f"{table_tree}//")
                    },
                    model_type=foreign_table.model,
                    table_tree=column_name.removesuffix("_id"),
                )
            else:
                py_type[column_name] = self._sql_type_to_py(
                    model_type, column_name, value
                )
        return model_type(**py_type)  # type: ignore

    def _tablename_from_model_instance(self, model: BaseModel) -> str:
        # noinspection PyTypeHints
        return [k for k, v in self._schema.items() if isinstance(model, v.model)][0]

    def _tablename_from_model(self, model: Type[ModelType]) -> str:
        return [k for k, v in self._schema.items() if v.model == model][0]

    def _py_type_to_sql(self, value: Any) -> Any:
        if self._engine.name != "postgres" and isinstance(value, UUID):
            return str(value)
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        if isinstance(value, BaseModel) and type(value) in [
            it.model for it in self._schema.values()
        ]:
            tablename = self._tablename_from_model_instance(value)
            return self._py_type_to_sql(value.__dict__[self._schema[tablename].pk])
        if isinstance(value, BaseModel):
            return value.json()
        return value

    @staticmethod
    def _sql_type_to_py(model: Type[ModelType], column: str, value: Any) -> Any:
        if value is None:
            return None
        if model.__fields__[column].type_ in [dict, list]:
            return json.loads(value)
        if issubclass(model.__fields__[column].type_, pydantic.BaseModel):
            return model.__fields__[column].type_(**json.loads(value))
        return model.__fields__[column].type_(value)
