"""Handle table interactions for a model."""
import asyncio
import json
import re
from types import NoneType
from typing import Any, Generic, get_args, Type

import pydantic
from pydantic.generics import GenericModel
from pypika import Field, Order, Query, Table  # type: ignore
from pypika.queries import QueryBuilder  # type: ignore
from sqlalchemy import text  # type: ignore
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession  # type: ignore
from sqlalchemy.orm import sessionmaker  # type: ignore

import pydantic_db._util as util
from pydantic_db._models import PyDBTableMeta, Relationship, TableMap
from pydantic_db._types import ModelType
from ._crud.field_query_builder import FieldQueryBuilder
from ._crud.model_query_builder import ModelQueryBuilder


class Result(GenericModel, Generic[ModelType]):
    """Search result object."""

    offset: int
    limit: int
    data: list[ModelType]


class TableManager(Generic[ModelType]):
    """Provides DB CRUD methods and table information for a model."""

    def __init__(
        self,
        table_data: PyDBTableMeta,
        table_map: TableMap,
        engine: AsyncEngine,
    ) -> None:
        """Provides DB CRUD methods for a model type.

        :param table_data: Corresponding database table meta data.
        :param table_map: Map of tablenames and models.
        :param engine: A SQL Alchemy async engine.
        """
        self._engine = engine
        self._table_map = table_map
        self._table_data = table_data
        self.tablename = table_data.tablename

    async def find_one(self, pk: Any, depth: int = 0) -> ModelType | None:
        """Get one record.

        :param pk: Primary key of the record to get.
        :param depth: ORM fetch depth.
        :return: A model representing the record if it exists else None.
        """
        return await self._find_one(self.tablename, pk, depth)

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
        query = self._get_find_many_query(
            self.tablename, where, order_by, order, limit, offset, depth
        )
        result = await self._execute_query(query)
        # noinspection PyProtectedMember
        return Result(
            offset=offset,
            limit=limit,
            data=[self._model_from_row_mapping(row._mapping) for row in result],
        )

    async def insert(self, model_instance: ModelType) -> ModelType:
        """Insert a record.

        If there is depth, a record will be inserted for the model and
        each model in its model tree down to the provided depth.

        :param model_instance: Instance to save as database record.
        :return: Inserted model.
        """
        await self._execute_query(
            ModelQueryBuilder(model_instance, self._table_map).get_insert_query()
        )
        return model_instance

    async def update(self, model_instance: ModelType) -> ModelType:
        """Update a record.

        :param model_instance: Model representing record to update.
        :return: The updated model.
        """
        await self._execute_query(
            ModelQueryBuilder(model_instance, self._table_map).get_update_queries()
        )
        return model_instance

    async def upsert(self, model_instance: ModelType) -> ModelType:
        """Insert a record if it does not exist, else update it.

        :param model_instance: Model representing record to insert or
            update.
        :return: The inserted or updated model.
        """
        await self._execute_query(
            ModelQueryBuilder(model_instance, self._table_map).get_upsert_query()
        )
        return model_instance

    async def delete(self, pk: Any) -> bool:
        """Delete a record."""
        await self._execute_query(
            FieldQueryBuilder(self._table_data).get_delete_query(pk)
        )
        return True

    async def _find_one(
        self, tablename: str, pk: Any, depth: int = 0
    ) -> ModelType | None:
        table_data = self._table_map.name_to_data[tablename]
        table = Table(tablename)
        query, columns = self._build_joins(
            Query.from_(table),
            table_data,
            depth,
            self._columns(table_data, depth),
        )
        query = query.where(
            table.field(table_data.pk) == util.py_type_to_sql(self._table_map, pk)
        ).select(*columns)
        result = await self._execute_query(query)
        try:
            # noinspection PyProtectedMember
            model_instance = self._model_from_row_mapping(
                next(result)._mapping, tablename=tablename
            )
            model_instance = await self._populate_many_relations(
                table_data, model_instance, depth
            )
            return model_instance
        except StopIteration:
            return None

    async def _populate_many_relations(
        self, table_data: PyDBTableMeta, model_instance: ModelType, depth: int
    ) -> ModelType:
        if depth <= 0:
            return model_instance
        depth -= 1
        for column, relation in table_data.relationships.items():
            if not relation.back_references:
                continue
            pk = model_instance.__dict__[table_data.pk]
            models = await self._find_many_relation(table_data, pk, relation, depth)
            models = await asyncio.gather(
                *[
                    self._populate_many_relations(
                        self._table_map.name_to_data[relation.foreign_table],
                        model,
                        depth,
                    )
                    for model in models
                ]
            )
            model_instance.__setattr__(column, models)
        # If depth is exhausted back out to here to skip needless loops.
        if depth <= 0:
            return model_instance
        # For each field, populate the many relationships of that field.
        for tablename, data in self._table_map.name_to_data.items():
            for column in table_data.model.__fields__:
                if type(model := model_instance.__dict__.get(column)) == data.model:
                    model = await self._populate_many_relations(data, model, depth)
                    model_instance.__setattr__(column, model)
        return model_instance

    async def _find_many_relation(
        self, table_data: PyDBTableMeta, pk: Any, relation: Relationship, depth: int
    ) -> list[ModelType] | None:
        table = Table(table_data.tablename)
        foreign_table = Table(relation.foreign_table)
        foreign_table_data = self._table_map.name_to_data[relation.foreign_table]
        many_result = await self._find_otm(
            table_data, foreign_table_data, relation, table, foreign_table, pk, depth
        )
        # noinspection PyProtectedMember
        return [
            self._model_from_row_mapping(
                row._mapping, tablename=foreign_table_data.tablename
            )
            for row in many_result
        ]

    async def _find_otm(
        self,
        table_data: PyDBTableMeta,
        foreign_table_data: PyDBTableMeta,
        relation: Relationship,
        table: Table,
        foreign_table: Table,
        pk: Any,
        depth: int,
    ) -> Any:
        query = (
            Query.from_(table)
            .left_join(foreign_table)
            .on(
                foreign_table.field(relation.back_references)
                == table.field(table_data.pk)
            )
            .where(table.field(table_data.pk) == pk)
            .select(foreign_table.field(foreign_table_data.pk))
        )
        result = await self._execute_query(query)
        many_query = self._get_find_many_query(
            foreign_table_data.tablename, depth=depth
        ).where(
            foreign_table.field(foreign_table_data.pk).isin([it[0] for it in result])
        )
        return await self._execute_query(many_query)

    def _get_find_many_query(
        self,
        tablename: str,
        where: dict[str, Any] | None = None,
        order_by: list[str] | None = None,
        order: Order = Order.asc,
        limit: int = 0,
        offset: int = 0,
        depth: int = 0,
    ) -> QueryBuilder:
        table = Table(tablename)
        where = where or {}
        order_by = order_by or []
        pydb_table = self._table_map.name_to_data[tablename]
        query, columns = self._build_joins(
            Query.from_(table),
            pydb_table,
            depth,
            self._columns(pydb_table, depth),
        )
        for field, value in where.items():
            query = query.where(table.field(field) == value)
        query = query.orderby(*order_by, order=order).select(*columns)
        if limit:
            query = query.limit(limit)
        if offset:
            query = query.offset(offset)
        return query

    async def _execute_query(self, query: QueryBuilder) -> Any:
        async_session = sessionmaker(
            self._engine, expire_on_commit=False, class_=AsyncSession
        )
        async with async_session() as session:
            async with session.begin():
                result = await session.execute(text(str(query)))
            await session.commit()
        await self._engine.dispose()
        return result

    def _build_joins(
        self,
        query: QueryBuilder,
        table_data: PyDBTableMeta,
        depth: int,
        columns: list[Field],
        table_tree: str | None = None,
    ) -> tuple[QueryBuilder, list[Field]]:
        if depth <= 0:
            return query, columns
        if not (
            relationships := self._table_map.name_to_data[
                table_data.tablename
            ].relationships
        ):
            return query, columns
        depth -= 1
        table_tree = table_tree or table_data.tablename
        pypika_table: Table = Table(table_data.tablename)
        if table_data.tablename != table_tree:
            pypika_table = pypika_table.as_(table_tree)
        # For each related table, add join to query.
        for field_name, relation in relationships.items():
            if relation.back_references is not None:
                continue
            relation_name = f"{table_tree}/{field_name}"
            rel_table = Table(relation.foreign_table).as_(relation_name)
            query = query.left_join(rel_table).on(
                pypika_table.field(field_name)
                == rel_table.field(
                    self._table_map.name_to_data[relation.foreign_table].pk
                )
            )
            columns.extend(
                [
                    rel_table.field(c).as_(f"{relation_name}//{depth}//{c}")
                    for c in self._table_map.name_to_data[
                        relation.foreign_table
                    ].columns
                ]
            )
            # Add joins of relations of this table to query.
            query, new_cols = self._build_joins(
                query,
                self._table_map.name_to_data[relation.foreign_table],
                depth,
                columns,
                relation_name,
            )
            columns.extend([c for c in new_cols if c not in columns])
        return query, columns

    def _model_from_row_mapping(
        self,
        row_mapping: dict[str, Any],
        model_type: Type[ModelType] | None = None,
        table_tree: str | None = None,
        tablename: str | None = None,
    ) -> ModelType:
        tablename = tablename or self.tablename
        model_type = model_type or self._table_map.name_to_data[tablename].model
        table_tree = table_tree or tablename
        py_type = {}
        table_data = self._table_map.name_to_data[
            util.tablename_from_model(model_type, self._table_map)
        ]
        for column, value in row_mapping.items():
            if not column.startswith(f"{table_tree}//"):
                # This must be a column somewhere else in the tree.
                continue
            groups = re.match(rf"{re.escape(table_tree)}//(\d+)//(.*)", column)
            depth = int(groups.group(1))
            column_name = groups.group(2)
            if column_name in table_data.relationships:
                if value is None:
                    # No further depth has been found.
                    continue
                foreign_table = self._table_map.name_to_data[
                    table_data.relationships[column_name].foreign_table
                ]
                if depth <= 0:
                    py_type[column_name] = self._sql_type_to_py(
                        model_type, column_name, row_mapping[column]
                    )
                else:
                    py_type[column_name] = self._model_from_row_mapping(
                        row_mapping={
                            k.removeprefix(f"{table_tree}/"): v
                            for k, v in row_mapping.items()
                            if not k.startswith(f"{table_tree}//")
                        },
                        model_type=foreign_table.model,
                        table_tree=column_name,
                    )
            else:
                py_type[column_name] = self._sql_type_to_py(
                    model_type, column_name, value
                )
        return model_type.construct(**py_type)

    @staticmethod
    def _columns(table_data: PyDBTableMeta, depth: int) -> list[Field]:
        table = Table(table_data.tablename)
        return [
            table.field(c).as_(f"{table_data.tablename}//{depth}//{c}")
            for c in table_data.columns
        ]

    @staticmethod
    def _sql_type_to_py(model_type: Type[ModelType], column: str, value: Any) -> Any:
        if value is None:
            return None
        if model_type.__fields__[column].type_ in [dict, list]:
            return json.loads(value)
        if get_args(model_type.__fields__[column].type_):
            type_ = None
            for arg in get_args(model_type.__fields__[column].type_):
                if arg is NoneType:
                    continue
                type_ = arg
            if type_:
                return type_(value)
        if issubclass(model_type.__fields__[column].type_, pydantic.BaseModel):
            return model_type.__fields__[column].type_(**json.loads(value))
        return model_type.__fields__[column].type_(value)
