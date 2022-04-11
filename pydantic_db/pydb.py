"""Main."""
import uuid
from typing import Any, Callable, Generic, Type, TypeVar
from uuid import UUID

import caseswitcher
import sqlalchemy
from pydantic import BaseModel
from sqlalchemy.dialects import postgresql

ModelType = TypeVar("ModelType", bound=BaseModel)


class _PyDB(Generic[ModelType]):
    """Provides DB CRUD methods for a model type."""

    def __init__(
        self,
        pydb: "PyDB",
        pydantic_model: ModelType,
        tablename: str,
        metadata: sqlalchemy.MetaData,
    ) -> None:
        self._pydb = pydb
        self._pydantic_model = pydantic_model
        self._tablename = tablename
        self._metadata = metadata
        self._sqlalchemy_model = self._generate_sqlalchemy_model()

    async def find_one(self, pk: UUID) -> ModelType:
        """Get one record."""

    async def find_many(
        self,
        where: dict[str, Any] | None = None,
        like: dict[str, Any] | None = None,
    ) -> list[ModelType]:
        """Get many records from an example model."""

    async def insert(self, model_instance: ModelType) -> ModelType:
        """Insert a record."""

    async def update(self, model_instance: ModelType) -> ModelType:
        """Update a record."""

    async def upsert(self, model_instance: ModelType) -> ModelType:
        """Insert or update a record."""

    async def delete(self, pk: UUID) -> bool:
        """Delete a record."""

    def _generate_sqlalchemy_model(self) -> sqlalchemy.Table:
        for k, v in self._pydantic_model.__fields__.items():
            print(k, v)
        return sqlalchemy.Table(
            self._tablename,
            self._metadata,
            *(
                sqlalchemy.Column(
                    k,
                    self._get_sqlalchemy_type(v.type_),
                    primary_key=v.field_info.extra["primary_key"],
                    nullable=v.required,
                )
                for k, v in self._pydantic_model.__fields__.items()
            ),
        )

    def _get_sqlalchemy_type(
        self, field_type: Any, max_length: int | None = None
    ) -> Any:
        if issubclass(field_type, BaseModel):
            foreign_table = self._pydb.get(field_type)
            # TODO Get primary key column.
            return (
                sqlalchemy.ForeignKey(f"{foreign_table._tablename}.id")
                if foreign_table
                else sqlalchemy.JSON
            )
        return {
            uuid.UUID: postgresql.UUID(as_uuid=True),
            str: sqlalchemy.String(max_length),
            int: sqlalchemy.Integer,
            float: sqlalchemy.Float,
            dict: sqlalchemy.JSON,
            list: sqlalchemy.JSON,  # TODO Many to many?
        }[field_type]


class PyDB:
    """Class to use pydantic models as ORM models."""

    def __init__(self, metadata: sqlalchemy.MetaData) -> None:
        self._tables: dict[ModelType, _PyDB[ModelType]] = {}  # type: ignore
        self._metadata = metadata

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
                self._metadata,
            )
            return cls

        return _wrapper


class Column(BaseModel):
    """A pydantic-db table column."""

    primary_key: bool = False
    indexed: bool = False
    foreign_key: ModelType | None = None
    pydantic_only: bool = False
