"""Handle table interactions for a model."""
from typing import Any, Generic

from pydantic.generics import GenericModel
from pypika import Field, Order, Query, Table  # type: ignore
from pypika.queries import QueryBuilder  # type: ignore
from sqlalchemy import text  # type: ignore
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession  # type: ignore
from sqlalchemy.orm import sessionmaker  # type: ignore

from pydantic_db._models import PyDBTableMeta, TableMap
from pydantic_db._types import ModelType
from ._crud.field_query_builder import FieldQueryBuilder
from ._crud.model_query_builder import ModelQueryBuilder
from ._crud.result_deserializer import ResultSetDeserializer


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
        return ResultSetDeserializer[ModelType | None](
            table_data=self._table_data,
            table_map=self._table_map,
            result_set=result,
            is_array=False,
            depth=depth,
        ).deserialize()

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
        deserialized_data = ResultSetDeserializer[ModelType | None](
            table_data=self._table_data,
            table_map=self._table_map,
            result_set=result,
            is_array=True,
            depth=depth,
        ).deserialize()
        # noinspection PyProtectedMember
        return Result(
            offset=offset,
            limit=limit,
            data=deserialized_data,
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
            FieldQueryBuilder(self._table_data, self._table_map).get_delete_query(pk)
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
