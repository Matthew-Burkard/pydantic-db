"""Generate Python CRUD methods for a model."""
import json
from typing import Any, Callable, Generic, Type
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
from pydantic_db._util import tablename_from_model


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
        self._engine = engine
        self._schema = schema
        self._field_to_column: dict[Any, str] = {}

    async def find_one(self, pk: Any, depth: int = 0) -> ModelType | None:
        """Get one record.

        :param pk: Primary key of the record to get.
        :param depth: ORM fetch depth.
        :return: A model representing the record if it exists else None.
        """
        return await self._find_one(self._tablename, pk, depth)

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
        table = Table(self._tablename)
        where = where or {}
        order_by = order_by or []
        pydb_table = self._schema[self._tablename]
        query, columns = self._build_joins(
            Query.from_(table),
            pydb_table,
            depth,
            self._columns(pydb_table),
        )
        for field, value in where.items():
            query = query.where(table.field(field) == value)
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

    async def insert(
        self, model_instance: ModelType, upsert_relations: bool = True
    ) -> ModelType:
        """Insert a record.

        :param model_instance: Instance to save as database record.
        :param upsert_relations: Upsert related table data if True.
        :return: Inserted model.
        """
        return await self._insert(model_instance, self._tablename, upsert_relations)

    async def update(
        self, model_instance: ModelType, upsert_relations: bool = True
    ) -> ModelType:
        """Update a record.

        :param model_instance: Model representing record to update.
        :param upsert_relations: Upsert related table data if True.
        :return: The updated model.
        """
        return await self._update(model_instance, self._tablename, upsert_relations)

    async def upsert(
        self, model_instance: ModelType, upsert_relations: bool = True
    ) -> ModelType:
        """Insert a record if it does not exist, else update it.

        :param model_instance: Model representing record to insert or
            update.
        :param upsert_relations: Upsert related table data if True.
        :return: The inserted or updated model.
        """
        return await self._upsert(model_instance, self._tablename, upsert_relations)

    async def delete(self, pk: Any) -> bool:
        """Delete a record."""
        table = Table(self._tablename)
        await self._execute(
            Query.from_(table)
            .where(table.field(self._schema[self._tablename].pk) == pk)
            .delete()
        )
        return True

    async def _insert(
        self, model_instance: ModelType, tablename: str, upsert_relations
    ):
        # noinspection DuplicatedCode
        table_data = self._schema[tablename]
        table = Table(tablename)
        if upsert_relations:
            await self._upsert_relations(model_instance, table_data)
        # Insert record.
        values = [
            self._py_type_to_sql(
                model_instance.__dict__[
                    c.removesuffix("_id") if c in table_data.relationships else c
                ]
            )
            for c in table_data.columns
        ]
        await self._execute(
            Query.into(table).columns(*table_data.columns).insert(*values)
        )
        return model_instance

    async def _update(
        self, model_instance: ModelType, tablename: str, upsert_relations: bool = True
    ) -> ModelType:
        # noinspection DuplicatedCode
        table_data = self._schema[tablename]
        table = Table(tablename)
        if upsert_relations:
            await self._upsert_relations(model_instance, table_data)
        # Update record.
        values = [
            self._py_type_to_sql(
                model_instance.__dict__[
                    c.removesuffix("_id") if c in table_data.relationships else c
                ]
            )
            for c in table_data.columns
        ]
        query = Query.update(table)
        for i, column in enumerate(table_data.columns):
            query = query.set(column, values[i])
        pk = model_instance.__dict__[table_data.pk]
        query = query.where(table.field(table_data.pk) == self._py_type_to_sql(pk))
        await self._execute(query)
        return model_instance

    async def _upsert(
        self, model_instance: ModelType, tablename: str, upsert_relations: bool
    ) -> ModelType:
        if model := await self._find_one(
            tablename, model_instance.__dict__[self._schema[tablename].pk]
        ):
            if model == model_instance:
                return model
            return await self.update(model_instance)
        return await self._insert(model_instance, tablename, upsert_relations)

    async def _find_one(
        self, tablename: str, pk: Any, depth: int = 0
    ) -> ModelType | None:
        table_data = self._schema[tablename]
        table = Table(tablename)
        query, columns = self._build_joins(
            Query.from_(table),
            table_data,
            depth,
            self._columns(table_data, tablename),
        )
        query = query.where(
            table.field(table_data.pk) == self._py_type_to_sql(pk)
        ).select(*columns)
        result = await self._execute(query)
        try:
            # noinspection PyProtectedMember
            return self._model_from_row_mapping(
                next(result)._mapping, tablename=tablename
            )
        except StopIteration:
            return None

    async def _upsert_relations(
        self, model_instance: ModelType, table_data: PyDBTableMeta
    ):
        # Upsert relationships.
        for rel in table_data.relationships:
            if rel_model := model_instance.__dict__.get(rel.removesuffix("_id")):
                tablename = tablename_from_model(type(rel_model), self._schema)
                await self._upsert(rel_model, tablename, True)

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

    def _columns(
        self, pydb_table: PyDBTableMeta, tablename: str | None = None
    ) -> list[Field]:
        tablename = tablename or self._tablename
        table = Table(tablename)
        return [table.field(c).as_(f"{tablename}//{c}") for c in pydb_table.columns]

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
        if not (relationships := self._schema[table_data.name].relationships):
            return query, columns
        depth -= 1
        table_tree = table_tree or table_data.name
        pypika_table: Table = Table(table_data.name)
        if table_data.name != table_tree:
            pypika_table = pypika_table.as_(table_tree)
        # For each related table, add join to query.
        for field_name, relation in relationships.items():
            if relation.back_references is not None:
                continue
            relation_name = f"{table_tree}/{field_name.removesuffix('_id')}"
            rel_table = Table(relation.foreign_table).as_(relation_name)
            query = query.left_join(rel_table).on(
                pypika_table.field(field_name)
                == rel_table.field(self._schema[relation.foreign_table].pk)
            )
            columns.extend(
                [
                    rel_table.field(c).as_(f"{relation_name}//{c}")
                    for c in self._schema[relation.foreign_table].columns
                ]
            )
            # Add joins of relations of this table to query.
            query, new_cols = self._build_joins(
                query,
                self._schema[relation.foreign_table],
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
        tablename = tablename or self._tablename
        model_type = model_type or self._schema[tablename].model
        table_tree = table_tree or tablename
        py_type = {}
        schema_info = self._schema[tablename_from_model(model_type, self._schema)]
        for column, value in row_mapping.items():
            if not column.startswith(f"{table_tree}//"):
                # This must be a column somewhere else in the tree.
                continue
            column_name = column.removeprefix(f"{table_tree}//")
            if column_name in schema_info.relationships:
                if value is None:
                    # No further depth has been found.
                    continue
                foreign_table = self._schema[
                    schema_info.relationships[column_name].foreign_table
                ]
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
        return model_type(**py_type)

    def _tablename_from_model_instance(self, model: BaseModel) -> str:
        # noinspection PyTypeHints
        return [k for k, v in self._schema.items() if isinstance(model, v.model)][0]

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
