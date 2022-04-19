"""Module providing PyDB and Column classes."""
import uuid
from typing import Any, Callable, Generic, Optional, Type, TypeVar

import caseswitcher
import pydantic
from pydantic import BaseModel, ConstrainedStr
from pydantic.fields import Undefined
from pydantic.generics import GenericModel
from pydantic.typing import NoArgAnyCallable
from sqlalchemy import (  # type: ignore
    Column,
    Float,
    ForeignKey,
    Integer,
    JSON,
    MetaData,
    String,
    Table,
)
from sqlalchemy.dialects.postgresql import UUID  # type: ignore
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession  # type: ignore
from sqlalchemy.orm import declarative_base, sessionmaker  # type: ignore
from sqlalchemy.sql import Select  # type: ignore

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
            result = self._model_from_db(next(await session.execute(query)), query)
        await self._engine.dispose()
        return result

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
        await self._engine.dispose()
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
                    .where(
                        self.table.c.id == self._pk(model_instance.id)  # type: ignore
                    )
                    .values(**self._model_instance_data(model_instance))
                )
            await session.commit()
        await self._engine.dispose()
        return model_instance

    async def upsert(self, model_instance: ModelType) -> ModelType:
        """Insert or update a record."""

    async def delete(self, pk: uuid.UUID | int) -> bool:
        """Delete a record."""

    def _pk(self, pk: uuid.UUID | int) -> uuid.UUID | int | str:
        if self._engine.name != "postgres" and isinstance(pk, uuid.UUID):
            return str(pk)
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
        return self._pydantic_model(  # type: ignore
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
                columns.append(Column(k, col_type, primary_key=pk))
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


# noinspection PyPep8Naming
def Field(
    default: Any = Undefined,
    *,
    pk: bool = False,
    indexed: bool = False,
    back_populates: str | None = None,
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
    **extra: Any,
) -> Any:
    """
    Wrapper for pydantic field with added PyDB args.

    Used to provide extra information about a field, either for the
    model schema or complex validation. Some arguments apply only to
    number fields (``int``, ``float``, ``Decimal``) and some apply only
    to ``str``.

    :param default: since this is replacing the field’s default, its
        first argument is used to set the default, use ellipsis
        (``...``) to indicate the field is required.
    :param pk: determines whether this column is a primary key.
    :param indexed: determines whether this column is indexed.
    :param back_populates: table name and column this field relates to
        in the format ``table.column``.
    :param default_factory: callable that will be called when a default
        value is needed for this field If both `default` and
        `default_factory` are set, an error is raised.
    :param alias: the public name of the field.
    :param title: can be any string, used in the schema.
    :param description: can be any string, used in the schema.
    :param exclude: exclude this field while dumping.
        Takes same values as the ``include`` and ``exclude`` arguments
        on the ``.dict`` method.
    :param include: include this field while dumping.
        Takes same values as the ``include`` and ``exclude`` arguments
        on the ``.dict`` method.
    :param const: this field is required and *must* take its default
        value.
    :param gt: only applies to numbers, requires the field to be
        "greater than". The schema will have an ``exclusiveMinimum``
        validation keyword
    :param ge: only applies to numbers, requires the field to be
        "greater than or equal to". The schema will have a ``minimum``
        validation keyword.
    :param lt: only applies to numbers, requires the field to be
        "less than". The schema will have an ``exclusiveMaximum``
        validation keyword.
    :param le: only applies to numbers, requires the field to be
        "less than or equal to". The schema will have a ``maximum``
        validation keyword.
    :param multiple_of: only applies to numbers, requires the field to
        be "a multiple of". The schema will have a ``multipleOf``
        validation keyword.
    :param max_digits: only applies to Decimals, requires the field to
        have a maximum number of digits within the decimal. It does not
        include a zero before the decimal point or trailing decimal
        zeroes.
    :param decimal_places: only applies to Decimals, requires the field
        to have at most a number of decimal places allowed. It does not
        include trailing decimal zeroes.
    :param min_items: only applies to lists, requires the field to have
        a minimum number of elements. The schema will have a
        ``minItems`` validation keyword.
    :param max_items: only applies to lists, requires the field to have
        a maximum number of elements. The schema will have a
        ``maxItems`` validation keyword.
    :param max_items: only applies to lists, requires the field not to
        have duplicated elements. The schema will have a ``uniqueItems``
        validation keyword.
    :param unique_items: determines whether list items must be unique.
    :param min_length: only applies to strings, requires the field to
        have a minimum length. The schema will have a ``maximum``
        validation keyword.
    :param max_length: only applies to strings, requires the field to
        have a maximum length. The schema will have a ``maxLength``
        validation keyword.
    :param allow_mutation: a boolean which defaults to True. When False,
        the field raises a TypeError if the field is assigned on an
        instance. The BaseModel Config must set validate_assignment to
        True.
    :param regex: only applies to strings, requires the field match
        against a regular expression pattern string. The schema will
        have a ``pattern`` validation keyword.
    :param discriminator: only useful with a (discriminated a.k.a. tagged)
        `Union` of sub models with a common field. The `discriminator`
        is the name of this common field to shorten validation and
        improve generated schema.
    :param repr_: show this field in the representation.
    :param extra: any additional keyword arguments will be added as is
        to the schema.
    """
    return pydantic.Field(
        default,
        pk=pk,
        indexed=indexed,
        back_populates=back_populates,
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
    )
