"""Generate Python CRUD methods for a model."""
import uuid
from typing import Any, Callable, Generic
from uuid import UUID

from pydantic.generics import GenericModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.orm import sessionmaker

from pydantic_db._model_type import ModelType


class Result(GenericModel, Generic[ModelType]):
    """Search result object."""

    offset: int
    limit: int
    data: list[ModelType]


class CRUDMethods(GenericModel, Generic[ModelType]):
    """Holds CRUD methods for a model."""

    find_one: Callable[[UUID], ModelType]
    find_many: Callable[
        [
            dict[str, Any] | None,
            list[str] | None,
            int,
            int,
            list[Any] | None,
            int | None,
        ],
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
        schema: dict[str, ModelType],
    ) -> None:
        self._pydantic_model = pydantic_model
        self._tablename = tablename
        self._engine = engine
        self._relations: dict[str, "CRUDGenerator"] = {}
        self._field_to_column: dict[Any, str] = {}
        self._schema = schema

    async def find_one(
        self, pk: uuid.UUID, exclude: list[Any] | None = None
    ) -> ModelType | None:
        """Get one record.

        :param pk: Primary key of the record to get.
        :param exclude: Columns to exclude from search.
        :return: A model representing the record if it exists else None.
        """
        statement = text("")
        result = await self._execute(statement)
        return self._model_from_row(result)

    async def find_many(
        self,
        where: dict[str, Any] | None = None,
        order_by: list[str] | None = None,
        limit: int = 0,
        offset: int = 0,
        exclude: list[Any] | None = None,
        depth: int | None = None,
    ) -> Result[ModelType]:
        """Get many records.

        :param where: Dictionary of column name to desired value.
        :param order_by: Columns to order by.
        :param limit: Number of records to return.
        :param offset: Number of records to offset by.
        :param exclude: Columns to exclude.
        :param depth: Depth of relations to populate.
        :return: A list of models representing table records.
        """
        exclude = exclude or []
        statement = text("")
        rows = await self._execute(statement)
        return Result(
            offset=offset,
            limit=limit,
            data=[self._model_from_row(row) for row in rows],
        )

    async def insert(self, model_instance: ModelType) -> ModelType:
        """Insert a record.

        :param model_instance: Instance to save as database record.
        :return: Inserted model.
        """
        statement = text("")
        await self._execute(statement)
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

    def get_crud_methods(self) -> CRUDMethods:
        """Get stand-alone CRUD methods for the model."""
        pass

    async def _execute(self, statement: str) -> Any:
        async_session = sessionmaker(
            self._engine, expire_on_commit=False, class_=AsyncSession
        )
        async with async_session() as session:
            async with session.begin():
                result = await session.execute(statement)
            await session.commit()
        await self._engine.dispose()
        return result

    def _pk(self, pk: uuid.UUID) -> uuid.UUID | str:
        if self._engine.name != "postgres" and isinstance(pk, uuid.UUID):
            return str(pk)
        return pk

    def _model_instance_data(self, model_instance: ModelType) -> dict[str, Any]:
        data = model_instance.dict()
        if self._engine.name != "postgres":
            for k, v in data.items():
                if isinstance(v, uuid.UUID):
                    data[k] = str(v)
                if v in self._schema.values():
                    data[k] = self._pk(v.id)
        return data

    def _model_from_row(self, data: Any) -> ModelType:
        # noinspection PyCallingNonCallable
        return self._pydantic_model(**{k: data[i] for i, k in "TODO"})  # TODO
