"""Module providing PyDB and Column classes."""
from typing import Callable, ForwardRef, get_origin, Type

import caseswitcher
from pydantic import BaseModel
from sqlalchemy import MetaData  # type: ignore
from sqlalchemy.ext.asyncio import AsyncEngine  # type: ignore

from pydantic_db._crud_generator import CRUDGenerator
from pydantic_db._table import PyDBTableMeta, Relation, RelationType
from pydantic_db._table_generator import SQLAlchemyTableGenerator
from pydantic_db._types import ModelType
from pydantic_db._util import get_joining_tablename
from pydantic_db.errors import ConfigurationError


class PyDB:
    """Class to use pydantic models as ORM models."""

    def __init__(self, engine: AsyncEngine) -> None:
        """Register models as ORM models and create schemas.

        :param engine: A SQL Alchemy async engine.
        """
        self.metadata: MetaData | None = None
        self._crud_generators: dict[Type, CRUDGenerator] = {}
        self._schema: dict[str, PyDBTableMeta] = {}
        self._model_to_metadata: dict[Type[BaseModel], PyDBTableMeta] = {}
        self._engine = engine

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
        """Make the decorated model a database table.

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
            metadata: PyDBTableMeta = PyDBTableMeta(
                name=tablename_,
                model=cls,
                pk=pk,
                indexed=indexed or [],
                unique=unique or [],
                unique_constraints=unique_constraints or [],
                columns=[],
                relationships={},
                back_references=back_references or {},
            )
            self._schema[tablename_] = metadata
            self._model_to_metadata[cls] = metadata
            return cls

        return _wrapper

    async def init(self) -> None:
        """Generate database tables from PyDB models."""
        # Populate relation information.
        for tablename, table_data in self._schema.items():
            self._populate_columns_and_relationships(tablename, table_data)
        # Now that relation information is populated generate tables.
        self.metadata = MetaData()
        for tablename, table_data in self._schema.items():
            # noinspection PyTypeChecker
            self._crud_generators[table_data.model] = CRUDGenerator(
                tablename,
                self._engine,
                self._schema,
            )
        await SQLAlchemyTableGenerator(self._engine, self.metadata, self._schema).init()

    def _populate_columns_and_relationships(
        self, tablename: str, table_data: PyDBTableMeta
    ) -> None:
        columns = []
        relationships = {}
        for field_name, field_info in table_data.model.__fields__.items():
            if not (related_table := self._model_to_metadata.get(field_info.type_)):
                columns.append(field_name)
                continue
            origin = get_origin(field_info.outer_type_)
            # If this is not a list of another table, add foreign key.
            if origin != list and field_info.outer_type_ != ForwardRef(
                f"list[{table_data.model.__name__}]"
            ):
                columns.append(f"{field_name}_id")
                relationships[f"{field_name}_id"] = Relation(
                    foreign_table=related_table.name,
                    relation_type=RelationType.ONE_TO_MANY,
                )
                continue
            back_reference = table_data.back_references.get(field_name)
            if not back_reference:
                raise self._get_configuration_error(
                    tablename, related_table.name, field_name
                )
            back_referenced_field = related_table.model.__fields__[back_reference]
            # Is the back referenced field also a list?
            is_mtm = get_origin(back_referenced_field.outer_type_) == list
            relation_type = RelationType.ONE_TO_MANY
            mtm_tablename = None
            if is_mtm:
                relation_type = RelationType.MANY_TO_MANY
                # Get mtm tablename or make one.
                if rel := related_table.relationships.get(back_reference):
                    mtm_tablename = rel.mtm_table
                else:
                    mtm_tablename = get_joining_tablename(
                        table=table_data.name,
                        column=field_name,
                        other_table=related_table.name,
                        other_column=back_reference,
                    )
            relationships[field_name] = Relation(
                foreign_table=related_table.name,
                relation_type=relation_type,
                back_references=field_name,
                mtm_table=mtm_tablename,
            )
        table_data.columns = columns
        table_data.relationships = relationships

    @staticmethod
    def _get_configuration_error(
        table_a: str, table_b: str, field: str
    ) -> ConfigurationError:
        return ConfigurationError(
            f'Many relation defined from field "{field}" on table "{table_a}" to table'
            f' "{table_b}" must be back-referenced from table "{table_a}"'
        )
