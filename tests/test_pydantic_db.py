"""PyDB tests."""
from __future__ import annotations

import asyncio
import unittest
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field
from pypika import Order

from pydantic_db.pydb import PyDB

connection_str = "sqlite+aiosqlite:///db.sqlite3"
db = PyDB(connection_str)


class Vector3(BaseModel):
    """3 floating point numbers."""

    x: float = 1.0
    y: float = 1.0
    z: float = 1.0


@db.table(
    "flavors",
    pk="id",
    indexed=["strength"],
    unique_constraints=[["name", "strength"]],
)
class Flavor(BaseModel):
    """A coffee flavor."""

    id: UUID = Field(default_factory=uuid4)
    name: str = Field(..., max_length=63)
    strength: int | None = None
    coffee: Coffee | UUID | None = None


@db.table(pk="id")
class Coffee(BaseModel):
    """Drink it in the morning."""

    id: UUID = Field(default_factory=uuid4)
    primary_flavor: Flavor | UUID
    secondary_flavor: Flavor | UUID | None
    sweetener: str
    cream: float
    place: dict
    ice: list
    size: Vector3
    attributes: dict[str, Any] | None = None


@db.table(pk="id")
class PlainTable(BaseModel):
    """Drink it in the morning."""

    id: UUID = Field(default_factory=uuid4)


Flavor.update_forward_refs()


class PyDBTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        """Setup clean sqlite database."""

        async def _init() -> None:
            async with db._engine.begin() as conn:
                await db.init()
                await conn.run_sync(db._metadata.drop_all)
                await conn.run_sync(db._metadata.create_all)

        asyncio.run(_init())

    async def test_find_nothing(self) -> None:
        self.assertEqual(None, (await db[Flavor].find_one(uuid4())))
        self.assertEqual(None, (await db[Coffee].find_one(uuid4(), depth=3)))

    async def test_no_relation_insert_and_fine_one(self) -> None:
        # Insert record.
        record = PlainTable()
        find = await db[PlainTable].insert(record)
        # Find new record and compare.
        self.assertDictEqual(
            find.dict(), (await db[PlainTable].find_one(find.id, 1)).dict()
        )

    async def test_insert_and_find_one(self) -> None:
        # Insert record.
        flavor = Flavor(name="mocha")
        mocha = await db[Flavor].insert(flavor)
        # Find new record and compare.
        self.assertDictEqual(mocha.dict(), (await db[Flavor].find_one(mocha.id)).dict())

    async def test_find_many(self) -> None:
        # Insert 3 records.
        mocha1 = await db[Flavor].insert(Flavor(name="mocha"))
        mocha2 = await db[Flavor].insert(Flavor(name="mocha"))
        caramel = await db[Flavor].insert(Flavor(name="caramel"))
        # Find two records with filter.
        mochas = await db[Flavor].find_many(where={"name": "mocha"})
        self.assertListEqual([mocha1, mocha2], mochas.data)
        flavors = await db[Flavor].find_many()
        self.assertListEqual([mocha1, mocha2, caramel], flavors.data)

    async def test_find_many_order(self) -> None:
        # Insert 3 records.
        mocha1 = await db[Flavor].insert(Flavor(name="mocha", strength=3))
        mocha2 = await db[Flavor].insert(Flavor(name="mocha", strength=2))
        caramel = await db[Flavor].insert(Flavor(name="caramel"))
        flavors = await db[Flavor].find_many(
            order_by=["name", "strength"], order=Order.desc
        )
        self.assertListEqual([mocha1, mocha2, caramel], flavors.data)

    async def test_find_many_pagination(self) -> None:
        # Insert 4 records.
        mocha1 = await db[Flavor].insert(Flavor(name="mocha"))
        mocha2 = await db[Flavor].insert(Flavor(name="mocha"))
        vanilla = await db[Flavor].insert(Flavor(name="vanilla"))
        caramel = await db[Flavor].insert(Flavor(name="caramel"))
        flavors_page_1 = await db[Flavor].find_many(limit=2)
        self.assertListEqual([mocha1, mocha2], flavors_page_1.data)
        flavors_page_2 = await db[Flavor].find_many(limit=2, offset=2)
        self.assertListEqual([vanilla, caramel], flavors_page_2.data)

    async def test_update(self) -> None:
        # Insert record.
        flavor = await db[Flavor].insert(Flavor(name="mocha"))
        # Update record.
        flavor.name = "caramel"
        await db[Flavor].update(flavor)
        # Find the updated record.
        self.assertEqual(flavor.name, (await db[Flavor].find_one(flavor.id)).name)

    async def test_upsert(self) -> None:
        # Upsert record as insert.
        flavor = await db[Flavor].upsert(Flavor(name="vanilla"))
        await db[Flavor].upsert(flavor)
        # Find all "vanilla" record.
        flavors = await db[Flavor].find_many(where={"id": flavor.id})
        self.assertEqual(1, len(flavors.data))
        # Upsert as update.
        flavor.name = "caramel"
        await db[Flavor].upsert(flavor)
        # Find one record.
        flavors = await db[Flavor].find_many(where={"id": flavor.id})
        self.assertEqual(1, len(flavors.data))
        self.assertDictEqual(flavor.dict(), flavors.data[0].dict())

    async def test_delete(self) -> None:
        # Insert record.
        caramel = Flavor(name="caramel")
        await db[Flavor].insert(caramel)
        # Delete record.
        await db[Flavor].delete(caramel.id)
        # Find one record.
        self.assertIsNone(await db[Flavor].find_one(caramel.id))

    async def test_insert_and_find_orm(self) -> None:
        mocha = Flavor(name="mocha")
        vanilla = Flavor(name="vanilla")
        await db[Flavor].insert(mocha)
        await db[Flavor].insert(vanilla)
        coffee = Coffee(
            primary_flavor=mocha,
            secondary_flavor=vanilla,
            sweetener="none",
            cream=0,
            place={"sum": 1},
            ice=["cubes"],
            size=Vector3(),
        )
        await db[Coffee].insert(coffee)
        # Find record and compare.
        coffee_dict = coffee.dict()
        find_coffee = await db[Coffee].find_one(coffee.id, depth=1)
        self.assertDictEqual(coffee_dict, find_coffee.dict())
        coffee_dict["primary_flavor"] = coffee_dict["primary_flavor"]["id"]
        coffee_dict["secondary_flavor"] = coffee_dict["secondary_flavor"]["id"]
        self.assertDictEqual(coffee_dict, (await db[Coffee].find_one(coffee.id)).dict())
