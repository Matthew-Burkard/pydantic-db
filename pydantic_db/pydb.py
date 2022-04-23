"""Module providing PyDB and Column classes."""
from typing import Any, Callable, Optional, Type

import caseswitcher
import pydantic
from pydantic.fields import Undefined
from pydantic.typing import NoArgAnyCallable
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import AsyncEngine

from pydantic_db._crud_generator import CRUDGenerator
from pydantic_db._model_type import ModelType
from pydantic_db._sqlalchemy_table_generator import SQLAlchemyTableGenerator


class PyDB:
    """Class to use pydantic models as ORM models."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._crud_generators: dict[Type[ModelType], CRUDGenerator[ModelType]] = {}
        self._schema: dict[str, Type[ModelType]] = {}
        self._metadata = MetaData()
        self._engine = engine

    def __getitem__(self, item: Type[ModelType]) -> CRUDGenerator[ModelType]:
        return self._crud_generators[item]

    def get(self, model: Type[ModelType]) -> CRUDGenerator[ModelType] | None:
        """Get table or None."""
        return self._crud_generators.get(model)

    def table(
        self, tablename: str | None = None
    ) -> Callable[[Type[ModelType]], Type[ModelType]]:
        """Make the decorated model a database table."""

        def _wrapper(cls: Type[ModelType]) -> Type[ModelType]:
            self._schema[tablename or caseswitcher.to_snake(cls.__name__)] = cls
            return cls

        return _wrapper

    async def init(self) -> None:
        """Generate database tables from PyDB models."""
        for tablename, model in self._schema.items():
            self._crud_generators[model] = CRUDGenerator(
                model,
                tablename or caseswitcher.to_snake(model.__name__),
                self._engine,
                self._schema,
            )
        await SQLAlchemyTableGenerator(self._engine, self._schema).init()


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
