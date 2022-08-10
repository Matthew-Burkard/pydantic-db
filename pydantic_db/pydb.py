"""Module providing PyDB and Column classes."""
from types import UnionType
from typing import Callable, ForwardRef, get_args, get_origin, Type

import caseswitcher
from pydantic import Field
from sqlalchemy import MetaData  # type: ignore
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine  # type: ignore

from pydantic_db._crud_generator import CRUDGenerator
from pydantic_db._models import (
    MTMData,
    PyDBTableMeta,
    Relationship,
    RelationType,
    TableMap,
)
from pydantic_db._table_generator import DBTableGenerator
from pydantic_db._types import ModelType
from pydantic_db._util import get_joining_tablename
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
        self._crud_generators: dict[Type, CRUDGenerator] = {}
        self._engine = create_async_engine(connection_str)
        self._table_map: TableMap = TableMap()

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
            table_metadata = PyDBTableMeta(
                model=cls,
                tablename=tablename_,
                pk=pk,
                indexed=indexed or [],
                unique=unique or [],
                unique_constraints=unique_constraints or [],
                columns=[],
                relationships={},
                back_references=back_references or {},
            )
            self._table_map.model_to_data[cls] = table_metadata
            self._table_map.name_to_data[tablename_] = table_metadata
            return cls

        return _wrapper

    async def init(self) -> None:
        """Generate database tables from PyDB models."""
        # Populate relation information.
        for tablename, table_data in self._table_map.name_to_data.items():
            cols, rels = self._get_columns_and_relationships(tablename, table_data)
            table_data.columns = cols
            table_data.relationships = rels
        # Now that relation information is populated generate tables.
        self._metadata = MetaData()
        for tablename, table_data in self._table_map.name_to_data.items():
            # noinspection PyTypeChecker
            self._crud_generators[table_data.model] = CRUDGenerator(
                tablename,
                self._engine,
                self._table_map,
            )
        await DBTableGenerator(self._engine, self._metadata, self._table_map).init()
        async with self._engine.begin() as conn:
            await conn.run_sync(self._metadata.drop_all)

    def _get_columns_and_relationships(
        self, tablename: str, table_data: PyDBTableMeta
    ) -> tuple[list[str], dict[str, Relationship]]:
        columns = []
        relationships = {}
        for field_name, field in table_data.model.__fields__.items():
            related_table = self._get_related_table(field)
            if related_table is None:
                columns.append(field_name)
                continue
            # Check if back-reference is present but mismatched in type.
            back_reference = table_data.back_references.get(field_name)
            back_referenced_field = related_table.model.__fields__.get(back_reference)
            if (
                back_reference
                and table_data.model not in get_args(back_referenced_field.type_)
                and table_data.model != back_referenced_field.type_
            ):
                raise MismatchingBackReferenceError(
                    tablename, related_table.tablename, field_name, back_reference
                )
            # If this is not a list of another table, add foreign key.
            if get_origin(field.outer_type_) != list and field.type_ != ForwardRef(
                f"list[{table_data.model.__name__}]"
            ):
                args = get_args(field.type_)
                correct_type = (
                    related_table.model.__fields__[related_table.pk].type_ in args
                )
                origin = get_origin(field.type_)
                if not args or not origin == UnionType or not correct_type:
                    raise MustUnionForeignKeyError(
                        tablename,
                        related_table.tablename,
                        field_name,
                        related_table.model,
                        related_table.model.__fields__[related_table.pk].type_.__name__,
                    )
                columns.append(field_name)
                relationships[field_name] = Relationship(
                    foreign_table=related_table.tablename,
                    relationship_type=RelationType.ONE_TO_MANY,
                )
                continue
            # MTM Must have a back-reference.
            if not back_reference:
                raise UndefinedBackReferenceError(
                    tablename, related_table.tablename, field_name
                )
            # Is the back referenced field also a list?
            is_mtm = get_origin(back_referenced_field.outer_type_) == list
            relation_type = RelationType.ONE_TO_MANY
            mtm_tablename = None
            if is_mtm:
                relation_type = RelationType.MANY_TO_MANY
                # Get mtm tablename or make one.
                if rel := related_table.relationships.get(back_reference):
                    mtm_tablename = rel.mtm_data.tablename
                else:
                    mtm_tablename = get_joining_tablename(
                        table_data.tablename,
                        field_name,
                        related_table.tablename,
                        back_reference,
                    )
            relationships[field_name] = Relationship(
                foreign_table=related_table.tablename,
                relationship_type=relation_type,
                back_references=back_reference,
                mtm_data=MTMData(tablename=mtm_tablename),
            )
        return columns, relationships

    def _get_related_table(self, field: Field) -> PyDBTableMeta:
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
        related_table = related_table or self._table_map.model_to_data.get(field.type_)
        return related_table
