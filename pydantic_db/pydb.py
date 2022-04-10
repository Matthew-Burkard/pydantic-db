"""Main."""
from typing import Any, Callable, Generic, Optional, Type, TypeVar
from uuid import UUID

import caseswitcher
from pydantic import BaseModel, fields
from pydantic.fields import Undefined
from pydantic.typing import NoArgAnyCallable

ModelType = TypeVar("ModelType", bound=BaseModel)


class _PyDB(Generic[ModelType]):
    """Provides DB CRUD methods for a model type."""

    def __init__(self, pydantic_model: ModelType, tablename: str) -> None:
        self._pydantic_model = pydantic_model
        self._tablename = tablename

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


class PyDB:
    """Class to use pydantic models as ORM models."""

    def __init__(self) -> None:
        self._tables: dict[ModelType, _PyDB[ModelType]] = {}  # type: ignore

    def __getitem__(self, item: Type[ModelType]) -> _PyDB[ModelType]:
        return self._tables[item]

    def table(
        self, tablename: str | None = None
    ) -> Callable[[Type[ModelType]], Type[ModelType]]:
        """Make the decorated model a database table."""

        def _wrapper(cls: Type[ModelType]) -> Type[ModelType]:
            self._tables[cls] = _PyDB(
                cls, tablename or caseswitcher.to_snake(cls.__name__)
            )
            return cls

        return _wrapper


# noinspection PyPep8Naming
def Field(
    default: Any = Undefined,
    *,
    default_factory: Optional[NoArgAnyCallable] = None,
    alias: str = None,
    title: str = None,
    description: str = None,
    exclude: Any = None,
    include: Any = None,
    const: bool = None,
    gt: float = None,
    ge: float = None,
    lt: float = None,
    le: float = None,
    multiple_of: float = None,
    max_digits: int = None,
    decimal_places: int = None,
    min_items: int = None,
    max_items: int = None,
    unique_items: bool = None,
    min_length: int = None,
    max_length: int = None,
    allow_mutation: bool = True,
    regex: str = None,
    discriminator: str = None,
    repr_: bool = True,
    # PyDB fields.
    primary_key: bool = False,
    required: bool = False,
    foreign_key: ModelType | None = None,
    pydantic_only: bool = False,
    **extra: Any,
) -> Any:
    """PyDB field wrapping a pydantic field."""
    return fields.Field(
        default,
        default_factory=default_factory,
        alias=alias,
        title=title,
        description=description,
        exclude=exclude,
        include=include,
        const=const,
        gt=gt,
        ge=ge,
        lt=lt,
        le=le,
        multiple_of=multiple_of,
        max_digits=max_digits,
        decimal_places=decimal_places,
        min_items=min_items,
        max_items=max_items,
        unique_items=unique_items,
        min_length=min_length,
        max_length=max_length,
        allow_mutation=allow_mutation,
        regex=regex,
        discriminator=discriminator,
        repr=repr_,
        **extra,
        primary_key=primary_key,
        required=required,
        foreign_key=foreign_key,
        pydantic_only=pydantic_only,
    )
