"""Provides ModelType TypeVar used throughout lib."""
from typing import Any, TypeVar
from uuid import UUID, uuid4

import pydantic
from pydantic.fields import Undefined
from pydantic.typing import NoArgAnyCallable


class BaseModel(pydantic.BaseModel):
    """Base class extending the pydantic BaseModel.

    ``BaseModel`` has one field called ``id``, it's a primary_key and
    defaults to a UUID4.
    """

    id: UUID = pydantic.Field(default_factory=uuid4, pk=True)


# noinspection PyPep8Naming
def Field(
    default: Any = Undefined,
    *,
    indexed: bool = False,
    default_factory: NoArgAnyCallable | None = None,
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

    :param default: Since this is replacing the field’s default, its
        first argument is used to set the default, use ellipsis
        (``...``) to indicate the field is required.
    :param indexed: Determines whether this column is indexed.
    :param default_factory: Callable that will be called when a default
        value is needed for this field If both `default` and
        `default_factory` are set, an error is raised.
    :param alias: The public name of the field.
    :param title: Can be any string, used in the schema.
    :param description: Can be any string, used in the schema.
    :param exclude: Exclude this field while dumping.
        Takes same values as the ``include`` and ``exclude`` arguments
        on the ``.dict`` method.
    :param include: Include this field while dumping.
        Takes same values as the ``include`` and ``exclude`` arguments
        on the ``.dict`` method.
    :param const: This field is required and *must* take its default
        value.
    :param gt: Only applies to numbers, requires the field to be
        "greater than". The schema will have an ``exclusiveMinimum``
        validation keyword
    :param ge: Only applies to numbers, requires the field to be
        "greater than or equal to". The schema will have a ``minimum``
        validation keyword.
    :param lt: Only applies to numbers, requires the field to be
        "less than". The schema will have an ``exclusiveMaximum``
        validation keyword.
    :param le: Only applies to numbers, requires the field to be
        "less than or equal to". The schema will have a ``maximum``
        validation keyword.
    :param multiple_of: Only applies to numbers, requires the field to
        be "a multiple of". The schema will have a ``multipleOf``
        validation keyword.
    :param max_digits: Only applies to Decimals, requires the field to
        have a maximum number of digits within the decimal. It does not
        include a zero before the decimal point or trailing decimal
        zeroes.
    :param decimal_places: Only applies to Decimals, requires the field
        to have at most a number of decimal places allowed. It does not
        include trailing decimal zeroes.
    :param min_items: Only applies to lists, requires the field to have
        a minimum number of elements. The schema will have a
        ``minItems`` validation keyword.
    :param max_items: Only applies to lists, requires the field to have
        a maximum number of elements. The schema will have a
        ``maxItems`` validation keyword.
    :param unique_items: Only applies to lists, requires the field not to
        have duplicated elements. The schema will have a ``uniqueItems``
        validation keyword.
    :param min_length: Only applies to strings, requires the field to
        have a minimum length. The schema will have a ``maximum``
        validation keyword.
    :param max_length: Only applies to strings, requires the field to
        have a maximum length. The schema will have a ``maxLength``
        validation keyword.
    :param allow_mutation: A boolean which defaults to True. When False,
        the field raises a TypeError if the field is assigned on an
        instance. The BaseModel Config must set validate_assignment to
        True.
    :param regex: Only applies to strings, requires the field match
        against a regular expression pattern string. The schema will
        have a ``pattern`` validation keyword.
    :param discriminator: Only useful with a (discriminated a.k.a. tagged)
        `Union` of sub models with a common field. The `discriminator`
        is the name of this common field to shorten validation and
        improve generated schema.
    :param repr_: Show this field in the representation.
    :param extra: Any additional keyword arguments will be added as is
        to the schema.
    """
    return pydantic.Field(
        default,
        indexed=indexed,
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


ModelType = TypeVar("ModelType", bound=BaseModel)
