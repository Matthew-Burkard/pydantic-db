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
        self._crud_generators: dict[Type, CRUDGenerator] = {}
        self._schema: dict[str, PyDBTableMeta] = {}
        self._engine = engine
        self.metadata: MetaData | None = None

    def __getitem__(self, item: Type[ModelType]) -> CRUDGenerator[ModelType]:
        return self._crud_generators[item]

    def table(
        self,
        tablename: str | None = None,
        *,
        pk: str,
        indexed: list[str] | None = None,
        unique: list[str] | None = None,
        unique_constraints: list[list[str]] | None = None,
    ) -> Callable[[Type[ModelType]], Type[ModelType]]:
        """Make the decorated model a database table.

        :param tablename: The database table name.
        :param pk: Field name of table primary key.
        :param indexed: Names of fields to index.
        :param unique: Names of fields that must be unique.
        :param unique_constraints: Fields that must be unique together.
        :return: The decorated class.
        """

        def _wrapper(cls: Type[ModelType]) -> Type[ModelType]:
            tablename_ = tablename or caseswitcher.to_snake(cls.__name__)
            self._schema[tablename_] = PyDBTableMeta(
                name=tablename_,
                model=cls,
                pk=pk,
                indexed=indexed or [],
                unique=unique or [],
                unique_constraints=unique_constraints or [],
                columns=[],
                relationships={},
            )
            return cls

        return _wrapper

    async def init(self) -> None:
        """Generate database tables from PyDB models."""
        self._populate_columns_and_relationships()
        self.metadata = MetaData()
        for tablename, table_data in self._schema.items():
            # noinspection PyTypeChecker
            self._crud_generators[table_data.model] = CRUDGenerator(
                tablename,
                self._engine,
                self._schema,
            )
        await SQLAlchemyTableGenerator(self._engine, self.metadata, self._schema).init()

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
