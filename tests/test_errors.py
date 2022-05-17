"""Test PyDB errors."""
from __future__ import annotations

import asyncio
import unittest
from uuid import UUID, uuid4

from pydantic import BaseModel, Field
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import create_async_engine

from pydantic_db.errors import (
    MismatchingBackReferenceError,
    UndefinedBackReferenceError,
)
from pydantic_db.pydb import PyDB

engine = create_async_engine("sqlite+aiosqlite:///db.sqlite3")
ubr_db = PyDB(engine)
mbr_db = PyDB(engine)


@ubr_db.table(pk="id")
class UndefinedBackreference(BaseModel):
    """Missing explicit back-reference to raise exception."""

    id: UUID = Field(default_factory=uuid4)
    self_ref: list[UndefinedBackreference] | None


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


MismatchedBackreferenceA.update_forward_refs()
MismatchedBackreferenceB.update_forward_refs()
UndefinedBackreference.update_forward_refs()


class PyDBManyRelationsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        """Setup clean sqlite database."""

        async def _init() -> None:
            metadata = MetaData()
            async with engine.begin() as conn:
                await conn.run_sync(metadata.drop_all)

        asyncio.run(_init())

    async def test_undefined_back_reference(self) -> None:
        correct_error = False
        try:
            await ubr_db.init()
        except UndefinedBackReferenceError:
            correct_error = True
        self.assertTrue(correct_error)

    async def test_mismatched_back_reference(self) -> None:
        correct_error = False
        try:
            await mbr_db.init()
        except MismatchingBackReferenceError:
            correct_error = True
        self.assertTrue(correct_error)
