"""Module providing PyDB and Column classes."""
import uuid
from typing import Any, Callable, Generic, Type, TypeVar

import caseswitcher
from pydantic import BaseModel, ConstrainedStr
from pydantic.generics import GenericModel
from sqlalchemy import (
    Column,
    Float,
    ForeignKey,
    Integer,
    JSON,
    MetaData,
    String,
    Table,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession  # type: ignore
from sqlalchemy.orm import declarative_base, sessionmaker  # type: ignore
from sqlalchemy.sql import Select

ModelType = TypeVar("ModelType", bound=BaseModel)
Base = declarative_base()
metadata = MetaData()


class Result(GenericModel, Generic[ModelType]):
    """Search result object."""

    offset: int
    limit: int
    data: list[ModelType]


class _PyDB(Generic[ModelType]):
    """Provides DB CRUD methods for a model type."""

    def __init__(
        self,
        pydb: "PyDB",
        pydantic_model: ModelType,
        tablename: str,
        engine: AsyncEngine,
    ) -> None:
        self._pydb = pydb
        self._pydantic_model = pydantic_model
        self._tablename = tablename
        self._engine = engine
        self.table: Table = self._generate_db_model()

    async def find_one(self, pk: uuid.UUID | int) -> ModelType:
        """Get one record."""
        async_session = sessionmaker(
            self._engine, expire_on_commit=False, class_=AsyncSession
        )
        async with async_session() as session:
            query = self.table.select().where(self.table.c.id == self._pk(pk))
            rows = await session.execute(query)
            result = next(rows)
            return self._model_from_db(result, query)

    async def find_many(
        self,
        where: dict[str, Any] | None = None,
        order_by: list[str] | None = None,
        limit: int = 0,
        offset: int = 0,
    ) -> Result[ModelType]:
        """Get many records."""
        async_session = sessionmaker(
            self._engine, expire_on_commit=False, class_=AsyncSession
        )
        async with async_session() as session:
            order = (self.table.c.get(col) for col in order_by) if order_by else ()
            where = (
                (self.table.c.get(k) == v for k, v in where.items())
                if where
                else (True,)  # type: ignore
            )
            query = (
                self.table.select()
                .where(*where)
                .offset(offset)
                .limit(limit or None)
                .order_by(*order)
            )
            rows = await session.execute(query)
        return Result(
            offset=offset,
            limit=limit,
            data=[self._model_from_db(row, query) for row in rows],
        )

    async def insert(self, model_instance: ModelType) -> ModelType:
        """Insert a record."""
        async_session = sessionmaker(
            self._engine, expire_on_commit=False, class_=AsyncSession
        )
        async with async_session() as session:
            async with session.begin():
                await session.execute(
                    self.table.insert().values(
                        **self._model_instance_data(model_instance)
                    )
                )
            await session.commit()
        await self._engine.dispose()
        return model_instance

    async def update(self, model_instance: ModelType) -> ModelType:
        """Update a record."""
        async_session = sessionmaker(
            self._engine, expire_on_commit=False, class_=AsyncSession
        )
        async with async_session() as session:
            async with session.begin():
                await session.execute(
                    self.table.update()
                    .where(self.table.c.id == self._pk(model_instance.id))
                    .values(**self._model_instance_data(model_instance))
                )
            await session.commit()
        await self._engine.dispose()
        return model_instance

    async def upsert(self, model_instance: ModelType) -> ModelType:
        """Insert or update a record."""

    async def delete(self, pk: uuid.UUID | int) -> bool:
        """Delete a record."""

    def _pk(self, pk: UUID | int) -> UUID | int:
        if self._engine.name != "postgres" and isinstance(pk, uuid.UUID):
            return str(pk)  # type: ignore
        return pk

    def _model_instance_data(self, model_instance: ModelType) -> dict[str, Any]:
        data = model_instance.dict()
        if self._engine.name != "postgres":
            for k, v in data.items():
                if isinstance(v, uuid.UUID):
                    data[k] = str(v)
        return data

    def _model_from_db(self, data: Any, query: Select) -> ModelType:
        # noinspection PyCallingNonCallable
        return self._pydantic_model(
            **{k: data[i] for i, k in enumerate(query.columns.keys())}
        )

    def _generate_db_model(self) -> Table:
        return Table(self._tablename, metadata, *self._get_columns())

    def _get_columns(self) -> tuple[Column[Any] | Column, ...]:
        columns = []
        for k, v in self._pydantic_model.__fields__.items():
            pk = v.field_info.extra.get("pk") or False
            if issubclass(v.type_, BaseModel):
                foreign_table = self._pydb.get(v.type_)
                columns.append(
                    Column(k, ForeignKey(f"{foreign_table.table.name}.id"))
                    if foreign_table
                    else Column(k, JSON)
                )
            elif v.type_ is uuid.UUID:
                col_type = UUID if self._engine.name == "postgres" else String(36)
                columns.append(Column(k, col_type, primary_key=pk))  # type: ignore
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


class PyDB:
    """Class to use pydantic models as ORM models."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._tables: dict[Type[ModelType], _PyDB[ModelType]] = {}  # type: ignore
        self._metadata = MetaData()
        self._engine = engine

    def __getitem__(self, item: Type[ModelType]) -> _PyDB[ModelType]:
        return self._tables[item]

    def get(self, model: Type[ModelType]) -> _PyDB[ModelType] | None:
        """Get table or None."""
        return self._tables.get(model)

    def table(
        self, tablename: str | None = None
    ) -> Callable[[Type[ModelType]], Type[ModelType]]:
        """Make the decorated model a database table."""

        def _wrapper(cls: Type[ModelType]) -> Type[ModelType]:
            self._tables[cls] = _PyDB(
                self,
                cls,
                tablename or caseswitcher.to_snake(cls.__name__),
                self._engine,
            )
            return cls

        return _wrapper

    async def generate_schemas(self) -> None:
        """Generate database tables from PyDB models."""
        async with self._engine.begin() as conn:
            # TODO Remove drop_all
            await conn.run_sync(metadata.drop_all)
            await conn.run_sync(metadata.create_all)


class PyDBColumn(BaseModel):
    """A pydantic-db table column."""

    pk: bool = False
    indexed: bool = False
    foreign_key: ModelType | None = None  # type: ignore
    pydantic_only: bool = False
