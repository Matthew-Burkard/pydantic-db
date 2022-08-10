"""PyDB tests for one-to-many relationships."""
from __future__ import annotations

import asyncio
import unittest
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from pydantic_db.pydb import PyDB

connection_str = "sqlite+aiosqlite:///db.sqlite3"
db = PyDB(connection_str)


@db.table(pk="id", back_references={"many_a": "one_a", "many_b": "one_b"})
class One(BaseModel):
    """One will have many "Many"."""

    id: UUID = Field(default_factory=uuid4)
    many_a: list[Many] | None = None
    many_b: list[Many] | None = None


@db.table(pk="id")
class Many(BaseModel):
    """Has a "One" parent and "Many" siblings."""

    id: UUID = Field(default_factory=uuid4)
    one_a: One | UUID
    one_b: One | UUID | None = None


One.update_forward_refs()
Many.update_forward_refs()


class PyDBManyRelationsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        """Setup clean sqlite database."""

        async def _init() -> None:
            async with db._engine.begin() as conn:
                await db.init()
                await conn.run_sync(db._metadata.drop_all)
                await conn.run_sync(db._metadata.create_all)

        asyncio.run(_init())

    async def test_one_to_many_insert_and_get(self) -> None:
        one_a = One()
        one_b = One()
        many_a = [Many(one_a=one_a), Many(one_a=one_a)]
        many_b = [
            Many(one_a=one_a, one_b=one_b),
            Many(one_a=one_a, one_b=one_b),
            Many(one_a=one_a, one_b=one_b),
        ]
        for many in many_a + many_b:
            await db[Many].insert(many, depth=2)
        find_one_a = await db[One].find_one(one_a.id, depth=2)
        many_a_plus_b = many_a + many_b
        many_a_plus_b.sort(key=lambda x: x.id)
        find_one_a.many_a.sort(key=lambda x: x.id)
        self.assertListEqual(many_a_plus_b, find_one_a.many_a)
        self.assertListEqual([], find_one_a.many_b)
        find_one_b = await db[One].find_one(one_b.id, depth=2)
        many_b.sort(key=lambda x: x.id)
        find_one_b.many_b.sort(key=lambda x: x.id)
        self.assertListEqual(many_b, find_one_b.many_b)
        self.assertListEqual([], find_one_b.many_a)
        many_a_idx_zero = await db[Many].find_one(many_a[0].id, depth=3)
        self.assertDictEqual(find_one_a.dict(), many_a_idx_zero.one_a.dict())

    async def test_one_to_many_update(self) -> None:
        pass
