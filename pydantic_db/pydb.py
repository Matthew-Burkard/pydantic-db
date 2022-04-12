"""Module providing PyDB and Column classes."""
import uuid
from typing import Any, Callable, Generic, Type, TypeVar
from uuid import UUID

import caseswitcher
from pydantic import BaseModel
# noinspection PyPackageRequirements
from tortoise import fields, Model
# noinspection PyPackageRequirements
from tortoise.fields import Field

PydanticModelType = TypeVar("PydanticModelType", bound=BaseModel)
TortoiseModelType = TypeVar("TortoiseModelType", bound=Model)


class _PyDB(Generic[PydanticModelType]):
    """Provides DB CRUD methods for a model type."""

    def __init__(
        self,
        pydb: "PyDB",
        pydantic_model: PydanticModelType,
        tablename: str,
    ) -> None:
        self._pydb = pydb
        self._pydantic_model = pydantic_model
        self._tablename = tablename
        self.table = self._generate_tortoise_model()

    async def find_one(self, pk: UUID) -> PydanticModelType:
        """Get one record."""

    async def find_many(
        self,
        where: dict[str, Any] | None = None,
        like: dict[str, Any] | None = None,
    ) -> list[PydanticModelType]:
        """Get many records."""

    async def insert(self, model_instance: PydanticModelType) -> PydanticModelType:
        """Insert a record."""

    async def update(self, model_instance: PydanticModelType) -> PydanticModelType:
        """Update a record."""

    async def upsert(self, model_instance: PydanticModelType) -> PydanticModelType:
        """Insert or update a record."""

    async def delete(self, pk: UUID) -> bool:
        """Delete a record."""

    def _generate_tortoise_model(self) -> Type[TortoiseModelType]:
        # noinspection PyTypeChecker
        return type(
            self._pydantic_model.__name__,
            (Model,),
            **self._get_tortoise_fields(),
        )  # type: ignore

    def _get_tortoise_fields(self) -> dict[str, Field]:
        columns = {}
        for k, v in self._pydantic_model.__fields__.items():
            pk = v.field_info.extra.get("pk")
            if issubclass(v.type_, BaseModel):
                foreign_table = self._pydb.get(v.type_)
                columns[k] = (
                    fields.ForeignKeyField(
                        f"models.{foreign_table._tablename}", foreign_table._tablename
                    )
                    if foreign_table
                    else fields.JSONField(pk=pk)
                )
            columns[k] = {
                uuid.UUID: fields.UUIDField(pk=pk),
                str: fields.CharField(
                    pk=pk, max_length=v.field_info.extra.get("max_length")
                ),
                int: fields.IntField(pk=pk),
                float: fields.FloatField(pk=pk),
                dict: fields.JSONField(pk=pk),
                list: fields.JSONField(pk=pk),  # TODO Many to many?
            }[v.type_]
        return columns


class PyDB:
    """Class to use pydantic models as ORM models."""

    def __init__(self) -> None:
        self._tables: dict[PydanticModelType, _PyDB[PydanticModelType]] = {}  # type: ignore

    def __getitem__(self, item: Type[PydanticModelType]) -> _PyDB[PydanticModelType]:
        return self._tables[item]

    def get(self, model: Type[PydanticModelType]) -> _PyDB[PydanticModelType] | None:
        """Get table or None."""
        return self._tables.get(model)

    def table(
        self, tablename: str | None = None
    ) -> Callable[[Type[PydanticModelType]], Type[PydanticModelType]]:
        """Make the decorated model a database table."""

        def _wrapper(cls: Type[PydanticModelType]) -> Type[PydanticModelType]:
            self._tables[cls] = _PyDB(
                self,
                cls,
                tablename or caseswitcher.to_snake(cls.__name__),
            )
            return cls

        return _wrapper


class Column(BaseModel):
    """A pydantic-db table column."""

    primary_key: bool = False
    indexed: bool = False
    foreign_key: PydanticModelType | None = None
    pydantic_only: bool = False
