"""Module providing PyDB and Column classes."""
from types import UnionType
from typing import Callable, ForwardRef, get_args, get_origin, Type

import caseswitcher
from pydantic.fields import ModelField
from sqlalchemy import MetaData  # type: ignore
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine  # type: ignore

from pydantic_db._models import (
    PyDBTableMeta,
    Relationship,
    TableMap,
)
from pydantic_db._table_generator import DBTableGenerator
from pydantic_db._table_manager import TableManager
from pydantic_db._types import ModelType
from pydantic_db.errors import (
    MismatchingBackReferenceError,
    MustUnionForeignKeyError,
    UndefinedBackReferenceError,
)


class PyDB:
    """Class to use pydantic models as ORM models."""

    def __init__(self, connection_str: str) -> None:
        """DB interface for registering models and CRUD operations.

        :param connection_str: Connection string for SQLAlchemy async
            engine.
        """
        self._metadata: MetaData | None = None
        self._crud_generators: dict[Type, TableManager] = {}
        self._engine = create_async_engine(connection_str)
        self._table_map: TableMap = TableMap()

    def __getitem__(self, item: Type[ModelType]) -> TableManager[ModelType]:
        return self._crud_generators[item]

    def table(
        self,
        tablename: str | None = None,
        *,
        pk: str,
        indexed: list[str] | None = None,
        unique: list[str] | None = None,
        unique_constraints: list[list[str]] | None = None,
        back_references: dict[str, str] | None = None,
    ) -> Callable[[Type[ModelType]], Type[ModelType]]:
        """Register a model as a database table.

        :param tablename: The database table name.
        :param pk: Field name of table primary key.
        :param indexed: Names of fields to index.
        :param unique: Names of fields that must be unique.
        :param unique_constraints: Fields that must be unique together.
        :param back_references: Dict of field names to back-referenced
            field names.
        :return: The decorated model.
        """

        def _wrapper(cls: Type[ModelType]) -> Type[ModelType]:
            tablename_ = tablename or caseswitcher.to_snake(cls.__name__)
            cls_back_references = back_references or {}
            table_metadata = PyDBTableMeta[ModelType](
                model=cls,
                tablename=tablename_,
                pk=pk,
                indexed=indexed or [],
                unique=unique or [],
                unique_constraints=unique_constraints or [],
                columns=[
                    field
                    for field in cls.__fields__
                    if field not in cls_back_references
                ],
                relationships={},
                back_references=cls_back_references,
            )
            self._table_map.model_to_data[cls] = table_metadata
            self._table_map.name_to_data[tablename_] = table_metadata
            return cls

        return _wrapper

    async def init(self) -> None:
        """Generate database tables from PyDB models."""
        # Populate relation information.
        for table_data in self._table_map.name_to_data.values():
            rels = self._get_relationships(table_data)
            table_data.relationships = rels
        # Now that relation information is populated generate tables.
        self._metadata = MetaData()
        for table_data in self._table_map.name_to_data.values():
            self._crud_generators[table_data.model] = TableManager(
                table_data,
                self._table_map,
                self._engine,
            )
        await DBTableGenerator(self._engine, self._metadata, self._table_map).init()
        async with self._engine.begin() as conn:
            await conn.run_sync(self._metadata.create_all)

    def _get_relationships(self, table_data: PyDBTableMeta) -> dict[str, Relationship]:
        relationships = {}
        for field_name, field in table_data.model.__fields__.items():
            related_table = self._get_related_table(field)
            if related_table is None:
                continue
            back_reference = table_data.back_references.get(field_name)
            if back_reference:
                relationships[field_name] = self._get_many_relationship(
                    field_name, back_reference, table_data, related_table
                )
                continue
            # If this is a list of another table, it's missing back reference.
            if get_origin(field.outer_type_) == list or field.type_ == ForwardRef(
                f"list[{table_data.model.__name__}]"
            ):
                raise UndefinedBackReferenceError(
                    table_data.tablename, related_table.tablename, field_name
                )
            args = get_args(field.type_)
            correct_type = (
                related_table.model.__fields__[related_table.pk].type_ in args
            )
            origin = get_origin(field.type_)
            if not args or not origin == UnionType or not correct_type:
                raise MustUnionForeignKeyError(
                    table_data.tablename,
                    related_table.tablename,
                    field_name,
                    related_table.model,
                    related_table.model.__fields__[related_table.pk].type_.__name__,
                )
            relationships[field_name] = Relationship(
                foreign_table=related_table.tablename
            )
        return relationships

    def _get_related_table(self, field: ModelField) -> PyDBTableMeta | None:
        related_table: PyDBTableMeta | None = None
        # Try to get foreign model from union.
        if args := get_args(field.type_):
            for arg in args:
                try:
                    related_table = self._table_map.model_to_data.get(arg)
                except TypeError:
                    break
                if related_table is not None:
                    break
        # Try to get foreign table from type.
        return related_table or self._table_map.model_to_data.get(field.type_)

    @staticmethod
    def _get_many_relationship(
        field_name: str,
        back_reference: str,
        table_data: PyDBTableMeta,
        related_table: PyDBTableMeta,
    ) -> Relationship:
        back_referenced_field = related_table.model.__fields__.get(back_reference)
        # Check if back-reference is present but mismatched in type.
        if (
            table_data.model not in get_args(back_referenced_field.type_)
            and table_data.model != back_referenced_field.type_
        ):
            raise MismatchingBackReferenceError(
                table_data.tablename,
                related_table.tablename,
                field_name,
                back_reference,
            )
        # Is the back referenced field also a list?
        return Relationship(
            foreign_table=related_table.tablename, back_references=back_reference
        )
