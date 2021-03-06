"""PyDB tests for many-to-many relationships."""
from __future__ import annotations

import asyncio
import unittest
from uuid import UUID, uuid4

from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import create_async_engine

from pydantic_db.pydb import PyDB

engine = create_async_engine("sqlite+aiosqlite:///mtm_db.sqlite3")
db = PyDB(engine)


@db.table(pk="id", back_references={"many": "many", "many_two": "many_two"})
class ManyToManyA(BaseModel):
    """Has many-to-many relationship with ManyToManyB."""

    id: UUID = Field(default_factory=uuid4)
    many: list[ManyToManyB] | None = None
    many_two: list[ManyToManyB] | None = None


@db.table(pk="id", back_references={"many": "many", "many_two": "many_two"})
class ManyToManyB(BaseModel):
    """Has many-to-many relationship with ManyToManyA."""

    id: UUID = Field(default_factory=uuid4)
    many: list[ManyToManyA]
    many_two: list[ManyToManyA] | None = None


@db.table(pk="id", back_references={"many": "many", "many_two": "many_two"})
class ManyToSelf(BaseModel):
    """Has many-to-many relationship with self."""

    id: UUID = Field(default_factory=uuid4)
    many: list[ManyToSelf] | None = None
    many_two: list[ManyToSelf] | None = None


ManyToManyA.update_forward_refs()
ManyToManyB.update_forward_refs()
ManyToSelf.update_forward_refs()


class PyDBManyRelationsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        """Setup clean sqlite database."""

        async def _init() -> None:
            await db.init()
            async with engine.begin() as conn:
                await conn.run_sync(db.metadata.drop_all)
                await conn.run_sync(db.metadata.create_all)

        asyncio.run(_init())

    async def test_many_to_many_insert_and_get(self) -> None:
        many_a = [ManyToManyA(), ManyToManyA()]
        for many in many_a:
            await db[ManyToManyA].insert(many)
        many_b = ManyToManyB(many=many_a)
        await db[ManyToManyB].insert(many_b)
        find_b = await db[ManyToManyB].find_one(many_b.id, depth=1)
        self.assertDictEqual(many_b.dict(), find_b.dict())
        find_a = await db[ManyToManyA].find_one(many_a[0].id, depth=2)
        self.assertDictEqual(find_a.many[0].dict(), find_b.dict())
