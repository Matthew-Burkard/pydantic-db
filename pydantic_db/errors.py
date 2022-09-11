"""PyDB Errors."""
from typing import Type


class ConfigurationError(Exception):
    """Raised for mal-configured database models."""

    def __init__(self, msg: str):
        """Init ConfigurationError.

        :param msg: Error message.
        """
        super(ConfigurationError, self).__init__(msg)


class UndefinedBackReferenceError(ConfigurationError):
    """Raised when a back reference is missing."""

    def __init__(self, table_a: str, table_b: str, field: str) -> None:
        """Init UndefinedBackReferenceError.

        :param table_a: Table with back reference.
        :param table_b: Back-referenced table.
        :param field: `table_a` field missing a back reference.
        """
        super(UndefinedBackReferenceError, self).__init__(
            f'Many relation defined on "{table_a}.{field}" to table {table_b}" must be'
            f' "back-referenced from table "{table_a}"'
        )


class MismatchingBackReferenceError(ConfigurationError):
    """Raised when a back reference is typed incorrectly."""

    def __init__(
        self, table_a: str, table_b: str, field: str, back_reference: str
    ) -> None:
        """Init MismatchingBackReferenceError.

        :param table_a: Table defining many relation.
        :param table_b: Back-referenced table.
        :param field: `table_a` field.
        :param back_reference: Back-referenced field.
        """
        super(MismatchingBackReferenceError, self).__init__(
            f'Many relation defined on "{table_a}.{field}" to'
            f' "{table_b}.{back_reference}" must use the same model type'
            f" back-referenced."
        )


class MustUnionForeignKeyError(ConfigurationError):
    """Raised when a relation field doesn't allow for just foreign key."""

    def __init__(
        self, table_a: str, table_b: str, field: str, model_b: Type, pk_type: Type
    ) -> None:
        """Init MustUnionForeignKeyError.

        :param table_a: Table with foreign key.
        :param table_b: Table referenced in foreign key.
        :param field: `table_a` foreign key field.
        :param model_b: Model foreign key relates to.
        :param pk_type: Type of `table_b` primary key.
        """
        super(MustUnionForeignKeyError, self).__init__(
            f'Relation defined on "{table_a}.{field}" to "{table_b}" must be a union'
            f' type of "Model | model_pk_type" e.g. "{model_b.__name__} | {pk_type}"'
        )


class TypeConversionError(ConfigurationError):
    """Raised when a Python type fails to convert to SQL."""

    def __init__(self, py_type: Type) -> None:
        """Init TypeConversionError.

        :param py_type: Python type that failed to convert to SQL.
        """
        super(TypeConversionError, self).__init__(
            f"Failed to convert type {py_type} to SQL."
        )
