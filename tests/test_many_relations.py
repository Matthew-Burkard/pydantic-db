"""PyDB tests."""
from __future__ import annotations

import asyncio
import unittest
from uuid import UUID, uuid4

from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import create_async_engine

from pydantic_db.pydb import PyDB

engine = create_async_engine("sqlite+aiosqlite:///db.sqlite3")
db = PyDB(engine)


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
    one_a: One
    one_b: One


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
    """Has many-to-many relationship with ManyToManyA."""

    id: UUID = Field(default_factory=uuid4)
    many: list[ManyToSelf] | None = None
    many_two: list[ManyToSelf] | None = None


One.update_forward_refs()
Many.update_forward_refs()
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

    async def test_one_to_many_insert_and_get(self) -> None:
        one_a = One()
        one_b = One()
        many_a = [Many(one_a=one_a, one_b=one_b), Many(one_a=one_a, one_b=one_b)]
        many_b = [
            Many(one_a=one_a, one_b=one_b),
            Many(one_a=one_a, one_b=one_b),
            Many(one_a=one_a, one_b=one_b),
        ]
        for many in many_a + many_b:
            await db[Many].insert(many)
        find_one_a = await db[One].find_one(one_a.id, depth=2)
        # print(find_one_a)
        self.assertListEqual(many_a, find_one_a.many_a)
        self.assertListEqual(many_b, find_one_a.many_b)
        many_a_idx_zero = await db[Many].find_one(many_a[0].pk, depth=3)
        self.assertDictEqual(find_one_a.dict(), many_a_idx_zero.one_a.dict())

    async def test_one_to_many_update(self) -> None:
        pass

    # async def test_many_to_many_insert_and_get(self) -> None:
    #     many_a = [ManyToManyA(), ManyToManyA()]
    #     for many in many_a:
    #         await db[ManyToManyA].insert(many)
    #     many_b = ManyToManyB(many=many_a)
    #     await db[ManyToManyB].insert(many_b)
    #     find_b = await db[ManyToManyB].find_one(many_b.id, depth=2)
    #     self.assertDictEqual(many_b.dict(), find_b.dict())
    #     find_a = await db[ManyToManyA].find_one(many_a[0].id, depth=3)
    #     self.assertDictEqual(find_a.many[0].dict(), find_b.dict())
