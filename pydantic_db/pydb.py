"""Module providing PyDB and Column classes."""
import uuid
from typing import Any, Callable, Generic, Type, TypeVar

import caseswitcher
from pydantic import BaseModel
from sqlalchemy import Column, Float, Integer, JSON, String, Table
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base

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
    ) -> None:
        self._pydb = pydb
        self._pydantic_model = pydantic_model
        self._tablename = tablename
        self.table: Type[DBModelType] = self._generate_db_model()  # type: ignore

    async def find_one(self, pk: uuid.UUID) -> ModelType:
        """Get one record."""

    async def find_many(
        self,
        where: dict[str, Any] | None = None,
        like: dict[str, Any] | None = None,
    ) -> list[ModelType]:
        """Get many records."""

    async def insert(self, model_instance: ModelType) -> ModelType:
        """Insert a record."""

    async def update(self, model_instance: ModelType) -> ModelType:
        """Update a record."""

    async def upsert(self, model_instance: ModelType) -> ModelType:
        """Insert or update a record."""

    async def delete(self, pk: uuid.UUID) -> bool:
        """Delete a record."""

    def _generate_db_model(self) -> Type[DBModelType]:
        # noinspection PyTypeChecker
        return type(
            self._pydantic_model.__name__,  # type: ignore
            (Base,),
            self._get_fields(),
        )  # type: ignore

    def _get_fields(self) -> dict[str, Any]:
        columns = {}
        for k, v in self._pydantic_model.__fields__.items():
            pk = v.field_info.extra.get("pk") or False
            if issubclass(v.type_, BaseModel):
                foreign_table = self._pydb.get(v.type_)
                columns[k] = Column if foreign_table else Column(JSON)
            elif v.type_ is uuid.UUID:
                # TODO String if not postgres.
                columns[k] = Column(UUID, primary_key=pk)
            elif v.type_ is str:
                columns[k] = Column(String(v.field_info.max_length), primary_key=pk)
            elif v.type_ is int:
                columns[k] = Column(Integer, primary_key=pk)
            elif v.type_ is float:
                columns[k] = Column(Float, primary_key=pk)
            elif v.type_ is dict:
                columns[k] = Column(JSON, primary_key=pk)
            elif v.type_ is list:
                columns[k] = Column(JSON, primary_key=pk)
        return columns


class PyDB:
    """Class to use pydantic models as ORM models."""

    def __init__(self) -> None:
        self._tables: dict[ModelType, _PyDB[ModelType]] = {}  # type: ignore

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
            )
            return cls

        return _wrapper

    async def generate_schemas(self) -> None:
        """Generate database tables from PyDB models."""
        pass


class PyDBColumn(BaseModel):
    """A pydantic-db table column."""

    pk: bool = False
    indexed: bool = False
    foreign_key: ModelType | None = None  # type: ignore
    pydantic_only: bool = False
