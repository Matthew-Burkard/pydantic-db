"""PyDB tests."""
import unittest
from uuid import UUID, uuid4

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import create_async_engine

from pydantic_db.pydb import Field, PyDB

engine = create_async_engine("sqlite+aiosqlite:///db.sqlite3")
db = PyDB(engine)


@db.table()
class Flavor(BaseModel):
    """A coffee flavor."""

    id: UUID = Field(default_factory=uuid4, pk=True)
    name: str = Field(max_length=63)


@db.table()
class Coffee(BaseModel):
    """Drink it in the morning."""

    id: UUID = Field(default_factory=uuid4, pk=True)
    flavor: Flavor
    sweetener: str
    cream: float
    size: int
    place: dict
    ice: list


class PyDBTests(unittest.IsolatedAsyncioTestCase):
    async def test_create_tables(self) -> None:
        await db.generate_schemas()
        self.assertEqual(["coffee", "flavor"], ["coffee", "flavor"])

    async def test_find_nothing(self) -> None:
        self.assertEqual(None, (await db[Flavor].find_one(uuid4())))

    async def test_insert_and_find_one(self) -> None:
        # Insert record.
        await db.generate_schemas()
        flavor = Flavor(name="mocha")
        mocha = await db[Flavor].insert(flavor)
        # Find new record and compare.
        self.assertEqual("mocha", (await db[Flavor].find_one(mocha.id)).name)

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

    async def test_update(self) -> None:
        # Insert record.
        flavor = await db[Flavor].insert(Flavor(name="mocha"))
        # Update record.
        flavor.name = "caramel"
        await db[Flavor].update(flavor)
        # Find the updated record.
        self.assertEqual(flavor.name, (await db[Flavor].find_one(flavor.id)).name)

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
        # TODO Insert record.
        # TODO Delete record.
        # TODO Find one record.
        self.assertEqual(None, None)


class ORMPyDBTests(unittest.IsolatedAsyncioTestCase):
    pass
    # async def test_insert_and_find_one(self) -> None:
    #     # Insert record.
    #     await db.generate_schemas()
    #     flavor = Flavor(name="mocha")
    #     await db[Flavor].insert(flavor)
    #     coffee = Coffee(flavor=flavor)
    #     await db[Coffee].insert(coffee)
    #     # Find new record and compare.
    #     self.assertDictEqual(
    #         coffee.dict(), (await db[Coffee].find_one(coffee.id)).dict()
    #     )
