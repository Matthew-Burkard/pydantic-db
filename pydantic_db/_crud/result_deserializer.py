"""Deserialize a result set into Python models."""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field
from sqlalchemy.engine import CursorResult, Row  # type: ignore

from pydantic_db._models import PyDBTableMeta, TableMap
from pydantic_db._types import ModelType

DeserializedType = TypeVar("DeserializedType")


class ResultSchema(BaseModel):
    """Model to describe the schema of a model result."""

    table_data: PyDBTableMeta | None = None
    is_array: bool
    references: dict[str, ResultSchema] = Field(default_factory=lambda: {})
    models: list[ModelType] = Field(default_factory=lambda: [])


class ResultSetDeserializer(Generic[DeserializedType]):
    """Generate Python models from a table map and result set."""

    def __init__(
        self,
        table_data: PyDBTableMeta,
        table_map: TableMap,
        result_set: CursorResult,
        is_array: bool,
        depth: int,
    ) -> None:
        """Generate Python models from a table map and result set.

        :param table_data: Table data for the returned model type.
        :param table_map: Map of tablenames and models.
        :param result_set: SQL Alchemy cursor result.
        :param is_array: Deserialize as a model or a list of models?
        :param depth: Model tree depth.
        """
        self._table_data = table_data
        self._table_map = table_map
        self._result_set = result_set
        self._is_array = is_array
        self._depth = depth
        self._result_schema = ResultSchema(
            is_array=is_array,
            references={
                table_data.tablename: self._get_result_schema(
                    table_data, depth, is_array
                )
            },
        )
        self._columns = [it[0] for it in self._result_set.cursor.description]
        self._return_dict: dict[str, Any] = {}

    def deserialize(self) -> DeserializedType:
        """Deserialize the result set into Python models."""
        for row in self._result_set:
            row_schema = {}
            for column_idx, column_tree in enumerate(self._columns):
                # `node` is the currently acted on level of depth in return.
                node = self._return_dict
                # `schema` describes acted on level of depth.
                schema = self._result_schema
                column_tree, column = column_tree.split("\\")
                current_tree = ""
                for branch in column_tree.split("/"):
                    current_tree += f"/{branch}"
                    # Update schema position.
                    schema = schema.references[branch]
                    # Update last pk if this column is a pk.
                    if (
                        column == schema.table_data.pk
                        and current_tree == f"/{column_tree}"
                    ):
                        row_schema[current_tree] = row[column_idx]
                    # If this branch in schema is absent from result set.
                    if row_schema[current_tree] is None:
                        break
                    # Initialize this object if it is None.
                    if node.get(branch) is None:
                        node[branch] = {}
                    if (
                        schema.is_array
                        and node[branch].get(row_schema[current_tree]) is None
                    ):
                        node[branch][row_schema[current_tree]] = {}
                    # Set node to this level.
                    if schema.is_array:
                        node = node[branch][row_schema[current_tree]]
                    else:
                        node = node[branch]
                # If we did not break.
                else:
                    # Set value.
                    if column:
                        node[column] = row[column_idx]

        if self._result_schema.is_array:
            return [
                self._table_data.model(**record)
                for record in self._flatten_result(
                    self._return_dict, self._result_schema
                )[self._table_data.tablename]
            ]
        return self._table_data.model(
            **self._flatten_result(self._return_dict, self._result_schema)[
                self._table_data.tablename
            ]
        )

    def _flatten_result(
        self, node: dict[Any, Any], schema: ResultSchema
    ) -> dict[str, Any] | None:
        for key, val in node.items():
            if key in schema.references:
                ref_schema = schema.references[key]
                if ref_schema.is_array:
                    node[key] = [
                        self._flatten_result(v, ref_schema) for v in node[key].values()
                    ]
                else:
                    node[key] = self._flatten_result(node[key], ref_schema)
        return node

    def _get_result_schema(
        self,
        table_data: PyDBTableMeta,
        depth: int,
        is_array: bool,
    ) -> ResultSchema | None:
        if depth < 0:
            return None
        result_schema = ResultSchema(
            table_data=table_data,
            is_array=is_array,
            references={
                column: schema
                for column, rel in table_data.relationships.items()
                if (
                    schema := self._get_result_schema(
                        table_data=self._table_map.name_to_data[rel.foreign_table],
                        depth=depth - 1,
                        is_array=rel.back_references is not None,
                    )
                )
                is not None
            },
        )
        return result_schema
