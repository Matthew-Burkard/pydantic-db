"""Module providing PyDB and Column classes."""
from typing import Callable, Type

import caseswitcher
from sqlalchemy import MetaData  # type: ignore
from sqlalchemy.ext.asyncio import AsyncEngine  # type: ignore

from pydantic_db._crud_generator import CRUDGenerator
from pydantic_db._sqlalchemy_table_generator import SQLAlchemyTableGenerator
from pydantic_db.models import ModelType


class PyDB:
    """Class to use pydantic models as ORM models."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._crud_generators: dict[  # type: ignore
            Type[ModelType], CRUDGenerator[ModelType]
        ] = {}
        self._schema: dict[str, Type[ModelType]] = {}  # type: ignore
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
        await SQLAlchemyTableGenerator(
            self._engine, self._schema
        ).init()  # type: ignore
