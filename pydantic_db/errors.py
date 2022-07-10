"""PyDB Errors."""
from typing import Type


class ConfigurationError(Exception):
    """Raised for mal-configured database models"""

    def __init__(self, msg: str):
        super(ConfigurationError, self).__init__(msg)


class UndefinedBackReferenceError(ConfigurationError):
    """Raised when a back reference is missing."""

    def __init__(self, table_a: str, table_b: str, field: str) -> None:
        super(UndefinedBackReferenceError, self).__init__(
            f'Many relation defined on "{table_a}.{field}" to table {table_b}" must be'
            f' "back-referenced from table "{table_a}"'
        )


class MismatchingBackReferenceError(ConfigurationError):
    """Raised when a back reference is typed incorrectly."""

    def __init__(
        self, table_a: str, table_b: str, field: str, back_reference: str
    ) -> None:
        super(MismatchingBackReferenceError, self).__init__(
            f'Many relation defined on "{table_a}.{field}" to'
            f' {table_b}.{back_reference}" must use the same model type back-referenced'
            f' from table "{table_a}"'
        )


class MustUnionForeignKeyError(ConfigurationError):
    """Raised when a relation field doesn't allow for just foreign key."""

    def __init__(
        self, table_a: str, table_b: str, field: str, model_b: Type, pk_type: Type
    ) -> None:
        super(MustUnionForeignKeyError, self).__init__(
            f'Relation defined on "{table_a}.{field}" to "{table_b}" must be a union'
            f' type of "Model | model_pk_type" e.g. "{model_b.__name__} | {pk_type}"'
        )


class TypeConversionError(ConfigurationError):
    """Raised when a Python type fails to convert to SQL."""

    def __init__(self, py_type: Type) -> None:
        super(TypeConversionError, self).__init__(
            f"Failed to convert type {py_type} to SQL."
        )
