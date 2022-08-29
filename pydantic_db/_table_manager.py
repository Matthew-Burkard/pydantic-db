"""Handle table interactions for a model."""
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
from pydantic_db._models import PyDBTableMeta, TableMap
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
        self.columns = table_data.columns

    async def find_one(self, pk: Any, depth: int = 0) -> ModelType | None:
        """Get one record.

        :param pk: Primary key of the record to get.
        :param depth: ORM fetch depth.
        :return: A model representing the record if it exists else None.
        """
        result = await self._execute_query(
            FieldQueryBuilder(self._table_data, self._table_map).get_find_one_query(
                pk, depth
            )
        )
        try:
            # noinspection PyProtectedMember
            model_instance = self._model_from_row_mapping(next(result)._mapping)
            return model_instance
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
        result = await self._execute_query(
            FieldQueryBuilder(self._table_data, self._table_map).get_find_many_query(
                where, order_by, order, limit, offset, depth
            )
        )
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

    async def delete(self, pk: Any) -> None:
        """Delete a record.

        :param pk: Primary key of the record to delete.
        """
        await self._execute_query(
            FieldQueryBuilder(self._table_data).get_delete_query(pk)
        )

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
