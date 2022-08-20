"""Module for building queries from models."""

from pypika import PostgreSQLQuery, Query, Table  # type: ignore
from pypika.dialects import PostgreSQLQueryBuilder  # type: ignore
from pypika.queries import QueryBuilder  # type: ignore

import pydantic_db._util as util
from pydantic_db._models import TableMap
from pydantic_db._types import ModelType


class ModelQueryBuilder:
    """Build SQL queries from a model instance."""

    def __init__(
        self,
        model: ModelType,
        table_map: TableMap,
        processed_models: list[ModelType] | None = None,
        query: Query | PostgreSQLQuery | None = None,
    ) -> None:
        """Build SQL queries for CRUD operations from a model.

        :param model: Model to build query for.
        :param table_map: Map of tablenames and models.
        :param processed_models: Models which a query has already been
            made for.
        """
        self._model = model
        # PostgreSQLQuery works for SQLite and PostgreSQL.
        self._query: QueryBuilder | PostgreSQLQueryBuilder | Query | PostgreSQLQuery = (
            query or PostgreSQLQuery
        )
        self._table_map = table_map
        self._processed_models = processed_models or []
        self._table_data = self._table_map.model_to_data[type(self._model)]
        self._table = Table(self._table_data.tablename)

    def get_insert_query(self) -> QueryBuilder | PostgreSQLQueryBuilder:
        """Get queries to insert model tree."""
        return self._get_inserts_or_upserts(is_upsert=False)

    def get_upsert_query(self) -> QueryBuilder | PostgreSQLQueryBuilder:
        """Get queries to upsert model tree."""
        return self._get_inserts_or_upserts(is_upsert=True)

    def get_update_queries(self) -> QueryBuilder | PostgreSQLQueryBuilder:
        """Get queries to update model tree."""
        self._query = self._query.update(self._table)
        for column, value in self._get_columns_and_values().items():
            self._query = self._query.set(column, value)
        self._query = self._query.where(
            self._table.field(self._table_data.pk)
            == self._model.__dict__[self._table_data.pk]
        )
        return self._query

    def get_patch_queries(self) -> list[QueryBuilder | PostgreSQLQueryBuilder]:
        """pass"""

    def _get_inserts_or_upserts(
        self, is_upsert: bool
    ) -> QueryBuilder | PostgreSQLQueryBuilder:
        col_to_value = self._get_columns_and_values()
        self._query = (
            self._query.into(self._table)
            .columns(*self._table_data.columns)
            .insert(*col_to_value.values())
        )
        if is_upsert:
            if isinstance(self._query, PostgreSQLQueryBuilder):
                self._query = self._query.on_conflict(self._table_data.pk)
                for column, value in col_to_value.items():
                    self._query = self._query.do_update(
                        self._table.field(column), value
                    )
        return self._query

    def _get_columns_and_values(self):
        return {
            column: util.py_type_to_sql(self._table_map, self._model.__dict__[column])
            for column in self._table_data.columns
        }
