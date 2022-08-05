"""Module for building queries."""
import json
from typing import Any
from uuid import UUID

from pydantic import BaseModel
from pypika import Query, Table

from pydantic_db._models import TableMap
from pydantic_db._types import ModelType
from pydantic_db._util import tablename_from_model_instance


class PyDBQueryBuilder:
    """Build SQL queries for model CRUD operations."""

    def __init__(
        self,
        model: ModelType,
        table_map: TableMap,
        depth: int = 1,
        processed_models: list[ModelType] | None = None,
    ) -> None:
        """Build SQL queries for model CRUD operations.

        :param model: Model to build query for.
        :param table_map: Map of tablenames and models.
        :param depth: Depth of query to build.
        :param processed_models: Models which a query has already been
            made for.
        """
        self._depth = depth
        self._model = model
        self._query = Query
        self._table_map = table_map
        self._processed_models = processed_models

    def get_insert_queries(self) -> list[Query]:
        """Get queries to insert model tree."""
        return self._get_inserts_or_upserts(is_upsert=False)

    def get_upsert_queries(self) -> list[Query]:
        """Get queries to upsert model tree."""
        return self._get_inserts_or_upserts(is_upsert=True)

    def get_find_one_query(self, populate_back_references: bool = False) -> Query:
        """pass"""

    def get_find_many_query(self, populate_back_references: bool = False) -> Query:
        """pass"""

    def get_update_queries(self) -> list[Query]:
        """pass"""

    def get_patch_queries(self) -> list[Query]:
        """pass"""

    def get_delete_queries(self) -> list[Query]:
        """pass"""

    def _get_inserts_or_upserts(self, is_upsert: bool) -> list[Query]:
        if self._model in self._processed_models:
            return []
        table_data = self._table_map.model_to_data[self._model]
        values = [
            self._py_type_to_sql(self._model.__dict__[c]) for c in table_data.columns
        ]
        self._query = (
            Query.into(Table(self._table_map.model_to_data[self._model].tablename))
            .columns(*table_data.columns)
            .insert(*values)
        )
        if is_upsert:
            self._query = self._query.on_duplicate_key_update(*values)
        queries = [self._query]
        if self._depth > 0:
            for col, rel in table_data.relationships.items():
                queries.extend(
                    PyDBQueryBuilder(
                        model=self._model.__dict__[col],
                        table_map=self._table_map,
                        depth=self._depth - 1,
                        processed_models=self._processed_models + [self._model],
                    ).get_upsert_queries()
                )
        return queries

    def _py_type_to_sql(self, value: Any) -> Any:
        if isinstance(value, UUID):
            return str(value)
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        if isinstance(value, BaseModel) and type(value) in [
            self._table_map.model_to_data
        ]:
            tablename = tablename_from_model_instance(value, self._table_map)
            return self._py_type_to_sql(
                value.__dict__[self._table_map.name_to_data[tablename].pk]
            )
        if isinstance(value, BaseModel):
            return value.json()
        return value
