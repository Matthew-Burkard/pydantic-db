"""Module providing PyDB and Column classes."""
from typing import Callable, Type

import caseswitcher
from sqlalchemy import MetaData  # type: ignore
from sqlalchemy.ext.asyncio import AsyncEngine  # type: ignore

from pydantic_db._crud_generator import CRUDGenerator
from pydantic_db._table_generator import SQLAlchemyTableGenerator
from pydantic_db._table import PyDBTableMeta
from pydantic_db._types import ModelType


class PyDB:
    """Class to use pydantic models as ORM models."""

    def __init__(self, engine: AsyncEngine) -> None:
        """Register models as ORM models and create schemas.

        :param engine: A SQL Alchemy async engine.
        """
        self._crud_generators: dict[  # type: ignore
            Type[ModelType], CRUDGenerator[ModelType]
        ] = {}
        self._schema: dict[str, PyDBTableMeta] = {}
        self._engine = engine

    def __getitem__(self, item: Type[ModelType]) -> CRUDGenerator[ModelType]:
        return self._crud_generators[item]

    def get(self, model: Type[ModelType]) -> CRUDGenerator[ModelType] | None:
        """Get CRUD generator or None.

        :param model: Model to get generator for.
        """
        return self._crud_generators.get(model)

    def table(
        self, tablename: str | None = None, *, pk: str, indexed: list[str] | None = None
    ) -> Callable[[Type[ModelType]], Type[ModelType]]:
        """Make the decorated model a database table.

        :param tablename: The database table name.
        :param pk: Field name of table primary key.
        :param indexed: Field names to index.
        :return: The decorated class.
        """

        def _wrapper(cls: Type[ModelType]) -> Type[ModelType]:
            tablename_ = tablename or caseswitcher.to_snake(cls.__name__)
            self._schema[tablename_] = PyDBTableMeta(
                name=tablename_,
                model=cls,
                pk=pk,
                indexed=indexed or [],
                columns=[],
                relationships={},
            )
            return cls

        return _wrapper

    async def init(self) -> None:
        """Generate database tables from PyDB models."""
        self._populate_columns_and_relationships()
        for tablename, table_data in self._schema.items():
            # noinspection PyTypeChecker
            self._crud_generators[table_data.model] = CRUDGenerator(
                table_data.model,
                tablename,
                self._engine,
                self._schema,
            )
        await SQLAlchemyTableGenerator(self._engine, self._schema).init()

    def _populate_columns_and_relationships(self) -> None:
        for tablename, table_data in self._schema.items():
            columns = []
            relationships = {}
            for k, v in table_data.model.__fields__.items():
                if v.type_ in [it.model for it in self._schema.values()]:
                    name = [
                        it.name for it in self._schema.values() if it.model == v.type_
                    ][0]
                    columns.append(f"{k}_id")
                    relationships[f"{k}_id"] = name
                else:
                    columns.append(k)
            table_data.columns = columns
            table_data.relationships = relationships
