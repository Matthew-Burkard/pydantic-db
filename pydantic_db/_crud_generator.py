"""Generate Python CRUD methods for a model."""
import asyncio
import json
import uuid
from typing import Any, Callable, Generic, Iterable
from uuid import UUID

from pydantic.generics import GenericModel
from pypika import Field, Query, Table  # type: ignore
from pypika.queries import QueryBuilder  # type: ignore
from sqlalchemy import text  # type: ignore
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession  # type: ignore
from sqlalchemy.orm import sessionmaker  # type: ignore

from pydantic_db._table import PyDBTableMeta
from pydantic_db.models import BaseModel, ModelType


class Result(GenericModel, Generic[ModelType]):
    """Search result object."""

    offset: int
    limit: int
    data: list[ModelType]


class CRUDMethods(GenericModel, Generic[ModelType]):
    """Holds CRUD methods for a model."""

    find_one: Callable[[UUID], ModelType]
    find_many: Callable[
        [dict[str, Any] | None, list[str] | None, int, int, int | None],
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
        pydantic_model: ModelType,
        tablename: str,
        engine: AsyncEngine,
        models: dict[str, ModelType],
        schema: dict[str, PyDBTableMeta],
    ) -> None:
        """Provides DB CRUD methods for a model type.

        :param pydantic_model: Model to create CRUD methods for.
        :param tablename: Name of the corresponding database table.
        :param engine: A SQL Alchemy async engine.
        :param models: List of all models.
        :param schema: Map of tablename to table information objects.
        """
        self._pydantic_model = pydantic_model
        self._tablename = tablename
        self._table = Table(tablename)
        self._engine = engine
        self._models = models
        self._schema = schema
        self._field_to_column: dict[Any, str] = {}

    async def find_one(self, pk: uuid.UUID, depth: int = 0) -> ModelType | None:
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
            [self._table.field(c) for c in pydb_table.columns],
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
        limit: int = 0,
        offset: int = 0,
        depth: int = 0,
    ) -> Result[ModelType]:
        """Get many records.

        :param where: Dictionary of column name to desired value.
        :param order_by: Columns to order by.
        :param limit: Number of records to return.
        :param offset: Number of records to offset by.
        :param depth: Depth of relations to populate.
        :return: A list of models representing table records.
        """
        pydb_table = self._schema[self._tablename]
        query, columns = self._build_joins(
            Query.from_(self._table),
            pydb_table,
            depth,
            [self._table.field(c) for c in pydb_table.columns],
        )
        query.limit(limit)
        query = query.where(*()).select(*columns)
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
        print(inserts)  # TODO Delete me.
        await self._execute_many((text(s) for s in inserts))
        return model_instance

    async def update(self, model_instance: ModelType) -> ModelType:
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
        if model := await self.find_one(model_instance.id):
            if model == model_instance:
                return model
            return await self.update(model_instance)
        return await self.insert(model_instance)

    async def delete(self, pk: uuid.UUID) -> bool:
        """Delete a record."""
        statement = text("")
        await self._execute(statement)
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

    def _build_joins(
        self,
        query: QueryBuilder,
        table: PyDBTableMeta,
        depth: int,
        columns: list[Field],
        table_tree: str | None = None,
    ) -> tuple[QueryBuilder, list]:
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
                    pypika_table.field(field_name) == rel_table.id
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

    def _py_type_to_sql(self, value: Any) -> Any:
        if self._engine.name != "postgres" and isinstance(value, uuid.UUID):
            return str(value)
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        if isinstance(value, BaseModel):
            return self._py_type_to_sql(value.id)
        return value

    def _sql_type_to_py(self, model: BaseModel, column: str, row_mapping: dict) -> Any:
        if row_mapping[column] is None:
            return None
        # Include row_mapping so the columns of child tables are present
        #  and may be used for child model serialization.
        schema_info = self._schema[self._tablename_from_model(model)]
        if column in schema_info.relationships:
            # TODO Manipulate row_mapping data into proper form.
            print(row_mapping)
        return model.__fields__[column].type_(row_mapping[column])

    def _model_from_row_mapping(self, row_mapping: dict[str, Any]) -> ModelType:
        py_type = {}
        # noinspection PyProtectedMember
        for column, value in row_mapping.items():
            py_type[column] = self._sql_type_to_py(
                self._pydantic_model, column, row_mapping
            )
        # noinspection PyCallingNonCallable
        return self._pydantic_model(**py_type)

    def _tablename_from_model_instance(self, model: BaseModel) -> str:
        # noinspection PyTypeHints
        return [k for k, v in self._models.items() if isinstance(model, v)][0]

    def _tablename_from_model(self, model: BaseModel) -> str:
        return [k for k, v in self._models.items() if v == model][0]
