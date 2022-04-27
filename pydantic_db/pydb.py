"""Module providing PyDB and Column classes."""
from typing import Callable, Type

import caseswitcher
from sqlalchemy import MetaData  # type: ignore
from sqlalchemy.ext.asyncio import AsyncEngine  # type: ignore

from pydantic_db._crud_generator import CRUDGenerator
from pydantic_db._sqlalchemy_table_generator import SQLAlchemyTableGenerator
from pydantic_db._table import PyDBTable
from pydantic_db.models import ModelType


class PyDB:
    """Class to use pydantic models as ORM models."""

    def __init__(self, engine: AsyncEngine) -> None:
        """Register models as ORM models and create schemas.

        :param engine: A SQL Alchemy async engine.
        """
        self._crud_generators: dict[  # type: ignore
            Type[ModelType], CRUDGenerator[ModelType]
        ] = {}
        self._models: dict[str, Type[ModelType]] = {}  # type: ignore
        self._schema: dict[str, PyDBTable] = {}
        self._metadata = MetaData()
        self._engine = engine

    def __getitem__(self, item: Type[ModelType]) -> CRUDGenerator[ModelType]:
        return self._crud_generators[item]

    def get(self, model: Type[ModelType]) -> CRUDGenerator[ModelType] | None:
        """Get CRUD generator or None.

        :param model: Model to get generator for.
        """
        return self._crud_generators.get(model)

    def table(
        self, tablename: str | None = None
    ) -> Callable[[Type[ModelType]], Type[ModelType]]:
        """Make the decorated model a database table.

        :param tablename: The database table name.
        :return: The decorated class.
        """

        def _wrapper(cls: Type[ModelType]) -> Type[ModelType]:
            self._models[tablename or caseswitcher.to_snake(cls.__name__)] = cls
            return cls

        return _wrapper

    async def init(self) -> None:
        """Generate database tables from PyDB models."""
        for tablename, model in self._models.items():
            # noinspection PyTypeChecker
            self._crud_generators[model] = CRUDGenerator(
                model,
                tablename or caseswitcher.to_snake(model.__name__),
                self._engine,
                self._models,  # type: ignore
                self._schema,
            )
        await SQLAlchemyTableGenerator(
            self._engine, self._models
        ).init()  # type: ignore
        self._populate_schema()

    def _populate_schema(self) -> None:
        for tablename, model in self._models.items():
            self._schema[tablename] = self._get_table(tablename, model)

    def _get_table(self, tablename: str, model: ModelType) -> PyDBTable:
        columns = []
        relationships = {}
        for k, v in model.__fields__.items():
            if v.type_ in self._models.values():
                name = [n for n in self._models if self._models[n] == v.type_][0]
                columns.append(f"{k}_id")
                relationships[f"{k}_id"] = name
            else:
                columns.append(k)
        return PyDBTable(name=tablename, columns=columns, relationships=relationships)
