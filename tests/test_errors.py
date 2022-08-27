"""Test PyDB errors."""
from __future__ import annotations

import asyncio
import unittest
from typing import Callable
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel, Field
from sqlalchemy import MetaData

from pydantic_db.errors import (
    MismatchingBackReferenceError,
    MustUnionForeignKeyError,
    TypeConversionError,
    UndefinedBackReferenceError,
)
from pydantic_db.pydb import PyDB

connection_str = "sqlite+aiosqlite:///db.sqlite3"
ubr_db = PyDB(connection_str)
mbr_db = PyDB(connection_str)
muf_missing_union_db = PyDB(connection_str)
muf_wrong_pk_type_db = PyDB(connection_str)
type_conversion_error_db = PyDB(connection_str)


@ubr_db.table(pk="id")
class UndefinedBackreference(BaseModel):
    """Missing explicit back-reference to raise exception."""

    id: UUID = Field(default_factory=uuid4)
    self_ref: list[UndefinedBackreference | UUID] | None


@mbr_db.table(pk="id", back_references={"other": "other"})
class MismatchedBackreferenceA(BaseModel):
    """Type of back-reference for "other" is not this model."""

    id: UUID = Field(default_factory=uuid4)
    other: list[MismatchedBackreferenceB] | None


@mbr_db.table(pk="id", back_references={"other": "other"})
class MismatchedBackreferenceB(BaseModel):
    """Type of back-reference for "other" is this model."""

    id: UUID = Field(default_factory=uuid4)
    other: list[MismatchedBackreferenceB] | None


@muf_missing_union_db.table(pk="id")
class A(BaseModel):
    """A table."""

    id: UUID = Field(default_factory=uuid4)


@muf_missing_union_db.table(pk="id")
class B(BaseModel):
    """Another table."""

    id: UUID = Field(default_factory=uuid4)
    a: A


@muf_wrong_pk_type_db.table(pk="id")
class E(BaseModel):
    """Another table."""

    id: UUID = Field(default_factory=uuid4)


@muf_wrong_pk_type_db.table(pk="id")
class F(BaseModel):
    """Another table."""

    id: UUID = Field(default_factory=uuid4)
    e: E | int


@type_conversion_error_db.table(pk="id")
class G(BaseModel):
    """Another table."""

    id: UUID = Field(default_factory=uuid4)
    e: Callable[[], int]


MismatchedBackreferenceA.update_forward_refs()
MismatchedBackreferenceB.update_forward_refs()
UndefinedBackreference.update_forward_refs()


class PyDBManyRelationsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        """Setup clean sqlite database."""

        async def _init(db: PyDB) -> None:
            metadata = MetaData()
            async with db._engine.begin() as conn:
                await conn.run_sync(metadata.drop_all)

        asyncio.run(_init(ubr_db))
        asyncio.run(_init(mbr_db))
        asyncio.run(_init(muf_missing_union_db))
        asyncio.run(_init(muf_wrong_pk_type_db))
        asyncio.run(_init(type_conversion_error_db))

    @staticmethod
    async def test_undefined_back_reference() -> None:
        with pytest.raises(UndefinedBackReferenceError) as e:
            await ubr_db.init()
        assert e.value.args[0] == (
            'Many relation defined on "undefined_backreference.self_ref" to table '
            'undefined_backreference" must be "back-referenced from table '
            '"undefined_backreference"'
        )

    @staticmethod
    async def test_mismatched_back_reference() -> None:
        with pytest.raises(MismatchingBackReferenceError) as e:
            await mbr_db.init()
        assert (
            e.value.args[0]
            == 'Many relation defined on "mismatched_backreference_a.other" to'
            ' "mismatched_backreference_b.other" must use the same model type'
            ' back-referenced.'
        )

    @staticmethod
    async def test_missing_foreign_key_union() -> None:
        with pytest.raises(MustUnionForeignKeyError) as e:
            await muf_missing_union_db.init()
        assert (
            e.value.args[0]
            == 'Relation defined on "b.a" to "a" must be a union type of "Model |'
            ' model_pk_type" e.g. "A | UUID"'
        )

    @staticmethod
    async def test_missing_wrong_pk_type() -> None:
        with pytest.raises(MustUnionForeignKeyError) as e:
            await muf_wrong_pk_type_db.init()
        assert (
            e.value.args[0]
            == 'Relation defined on "f.e" to "e" must be a union type of "Model |'
            ' model_pk_type" e.g. "E | UUID"'
        )

    @staticmethod
    async def test_conversion_type_error() -> None:
        with pytest.raises(TypeConversionError) as e:
            await type_conversion_error_db.init()
        assert (
            e.value.args[0] == "Failed to convert type typing.Callable[[], int] to SQL."
        )
