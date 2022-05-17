"""PyDB Errors."""


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
