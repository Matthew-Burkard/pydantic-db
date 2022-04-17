"""Module providing PyDB and Column classes."""
import uuid
from typing import Any, Callable, Generic, Type, TypeVar

import caseswitcher
from pydantic import BaseModel, ConstrainedStr
from sqlalchemy import Column, Float, Integer, JSON, MetaData, select, String, Table
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession  # type: ignore
from sqlalchemy.orm import declarative_base, sessionmaker  # type: ignore

ModelType = TypeVar("ModelType", bound=BaseModel)
DBModelType = TypeVar("DBModelType", bound=Table)
Base = declarative_base()


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
        self.table: Type[DBModelType] = self._generate_db_model()  # type: ignore

    async def find_one(self, pk: uuid.UUID | int) -> ModelType:
        """Get one record."""
        async_session = sessionmaker(
            self._engine, expire_on_commit=False, class_=AsyncSession
        )
        async with async_session() as session:
            stmt = select(self.table).where(self.table.id == pk)
            result = next(await session.execute(stmt))[0]
            # noinspection PyCallingNonCallable
            return self._pydantic_model(
                **{k: result.__dict__[k] for k in self._pydantic_model.__fields__}
            )

    # TODO Take filter object and return paginated result.
    async def find_many(self) -> list[ModelType]:
        """Get many records."""

    async def insert(self, model_instance: ModelType) -> ModelType:
        """Insert a record."""
        async_session = sessionmaker(
            self._engine, expire_on_commit=False, class_=AsyncSession
        )
        async with async_session() as session:
            async with session.begin():
                session.add(self.table(**model_instance.dict()))
            await session.commit()
        await self._engine.dispose()
        return model_instance

    async def update(self, model_instance: ModelType) -> ModelType:
        """Update a record."""

    async def upsert(self, model_instance: ModelType) -> ModelType:
        """Insert or update a record."""

    async def delete(self, pk: uuid.UUID | int) -> bool:
        """Delete a record."""

    def _generate_db_model(self) -> Type[DBModelType]:
        # noinspection PyTypeChecker
        return type(
            self._pydantic_model.__name__,  # type: ignore
            (Base,),
            self._get_fields(),
        )

    def _get_fields(self) -> dict[str, Any]:
        fields = {"__tablename__": self._tablename}
        for k, v in self._pydantic_model.__fields__.items():
            pk = v.field_info.extra.get("pk") or False
            if issubclass(v.type_, BaseModel):
                foreign_table = self._pydb.get(v.type_)
                fields[k] = Column if foreign_table else Column(JSON)
            elif v.type_ is uuid.UUID:
                # TODO String if not postgres.
                fields[k] = Column(UUID, primary_key=pk)
            elif v.type_ is str or issubclass(v.type_, ConstrainedStr):
                fields[k] = Column(String(v.field_info.max_length), primary_key=pk)
            elif v.type_ is int:
                fields[k] = Column(Integer, primary_key=pk)
            elif v.type_ is float:
                fields[k] = Column(Float, primary_key=pk)
            elif v.type_ is dict:
                fields[k] = Column(JSON, primary_key=pk)
            elif v.type_ is list:
                fields[k] = Column(JSON, primary_key=pk)
        return fields


class PyDB:
    """Class to use pydantic models as ORM models."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._tables: dict[ModelType, _PyDB[ModelType]] = {}  # type: ignore
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
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)


class PyDBColumn(BaseModel):
    """A pydantic-db table column."""

    pk: bool = False
    indexed: bool = False
    foreign_key: ModelType | None = None  # type: ignore
    pydantic_only: bool = False
