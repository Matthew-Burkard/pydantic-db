"""PyDB tests."""
from __future__ import annotations

import asyncio
import unittest
from uuid import uuid4

from pypika import Order
from sqlalchemy.ext.asyncio import create_async_engine

from pydantic_db.models import BaseModel, Field
from pydantic_db.pydb import PyDB

engine = create_async_engine("sqlite+aiosqlite:///db.sqlite3")
db = PyDB(engine)


@db.table()
class Flavor(BaseModel):
    """A coffee flavor."""

    name: str = Field(max_length=63)
    strength: int | None = None
    coffee: Coffee | None = None


@db.table()
class Coffee(BaseModel):
    """Drink it in the morning."""

    primary_flavor: Flavor
    secondary_flavor: Flavor
    sweetener: str
    cream: float
    place: dict
    ice: list


Flavor.update_forward_refs()


class PyDBTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        """Setup clean sqlite database."""
        asyncio.get_event_loop().run_until_complete(db.init())

    async def test_table_creation(self) -> None:
        # TODO Get all tables.
        self.assertEqual(["coffee", "flavor"], ["coffee", "flavor"])

    async def test_find_nothing(self) -> None:
        self.assertEqual(None, (await db[Flavor].find_one(uuid4())))
        self.assertEqual(None, (await db[Coffee].find_one(uuid4(), depth=3)))

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

    # async def test_update(self) -> None:
    #     # Insert record.
    #     flavor = await db[Flavor].insert(Flavor(name="mocha"))
    #     # Update record.
    #     flavor.name = "caramel"
    #     await db[Flavor].update(flavor)
    #     # Find the updated record.
    #     self.assertEqual(flavor.name, (await db[Flavor].find_one(flavor.id)).name)
    #
    # async def test_upsert(self) -> None:
    #     # Insert record.
    #     flavor = await db[Flavor].insert(Flavor(name="vanilla"))
    #     # upsert record.
    #     await db[Flavor].upsert(flavor)
    #     # Find all "vanilla" record.
    #     vanillas = await db[Flavor].find_many(where={"id": flavor.id})
    #     self.assertEqual(1, len(vanillas.data))
    #     # Upsert as update.
    #     flavor.name = "caramel"
    #     await db[Flavor].upsert(flavor)
    #     # Find one record.
    #     vanillas = await db[Flavor].find_many(where={"id": flavor.id})
    #     self.assertEqual(1, len(vanillas.data))
    #     self.assertEqual("caramel", len(vanillas.data))

    async def test_delete(self) -> None:
        # Insert record.
        caramel = Flavor(name="caramel")
        await db[Flavor].insert(caramel)
        # Delete record.
        await db[Flavor].delete(caramel.id)
        # Find one record.
        self.assertIsNone(await db[Flavor].find_one(caramel.id))

    async def test_insert_and_find_orm(self) -> None:
        # Insert record.
        mocha = Flavor(name="mocha")
        await db[Flavor].insert(mocha)
        vanilla = Flavor(name="vanilla")
        await db[Flavor].insert(vanilla)
        coffee = Coffee(
            primary_flavor=mocha,
            secondary_flavor=vanilla,
            sweetener="none",
            cream=0,
            place={"sum": 1},
            ice=["cubes"],
        )
        await db[Coffee].insert(coffee)
        # Find new record and compare.
        self.assertDictEqual(
            coffee.dict(), (await db[Coffee].find_one(coffee.id, depth=1)).dict()
        )
