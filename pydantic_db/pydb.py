"""Module providing PyDB and Column classes."""
import uuid
from typing import Any, Callable, Generic, Type, TypeVar
from uuid import UUID

import caseswitcher
from pydantic import BaseModel

# noinspection PyPackageRequirements
from tortoise import fields, Model

ModelType = TypeVar("ModelType", bound=BaseModel)
DBModelType = TypeVar("DBModelType", bound=Model)


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

    async def find_one(self, pk: UUID) -> ModelType:
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

    async def delete(self, pk: UUID) -> bool:
        """Delete a record."""

    def _generate_db_model(self) -> Type[DBModelType]:
        # noinspection PyTypeChecker
        return type(
            self._pydantic_model.__name__,  # type: ignore
            (Model,),
            self._get_fields(),
        )  # type: ignore

    def _get_fields(self) -> dict[str, Any]:
        columns = {}
        for k, v in self._pydantic_model.__fields__.items():
            pk = v.field_info.extra.get("pk") or False
            if issubclass(v.type_, BaseModel):
                foreign_table = self._pydb.get(v.type_)
                columns[k] = (
                    fields.ForeignKeyField(
                        f"models.{foreign_table._tablename}", foreign_table._tablename
                    )
                    if foreign_table
                    else fields.JSONField(pk=pk)
                )
            elif v.type_ is uuid.UUID:
                columns[k] = fields.UUIDField(pk=pk)
            elif v.type_ is str:
                columns[k] = fields.CharField(pk=pk, max_length=v.field_info.max_length)
            elif v.type_ is int:
                columns[k] = fields.IntField(pk=pk)
            elif v.type_ is float:
                columns[k] = fields.FloatField(pk=pk)
            elif v.type_ is dict:
                columns[k] = fields.JSONField(pk=pk)
            elif v.type_ is list:
                columns[k] = fields.JSONField(pk=pk)  # TODO Many to many?
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


class Column(BaseModel):
    """A pydantic-db table column."""

    pk: bool = False
    indexed: bool = False
    foreign_key: ModelType | None = None  # type: ignore
    pydantic_only: bool = False
