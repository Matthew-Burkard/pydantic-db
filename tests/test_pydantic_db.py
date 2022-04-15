"""PyDB tests."""
import unittest
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from pydantic_db.pydb import PyDBColumn, PyDB

# noinspection PyPackageRequirements

db = PyDB()
db_url = "sqlite://db.sqlite3"


@db.table()
class Flavor(BaseModel):
    """A coffee flavor."""

    id: UUID = Field(default_factory=uuid4, **PyDBColumn(pk=True).dict())
    name: str = Field(max_length=63)


@db.table()
class Coffee(BaseModel):
    """Drink it in the morning."""

    id: UUID = Field(default_factory=uuid4, **PyDBColumn(pk=True).dict())
    flavor: Flavor


class PyDBTests(unittest.TestCase):
    async def test_create_tables(self) -> None:
        await db.generate_schemas()
        self.assertEqual(["coffee", "flavor"], ["coffee", "flavor"])

    async def test_find_one(self) -> None:
        # Insert record.
        flavor = Flavor(name="mocha")
        mocha = await db[Flavor].insert(flavor)
        # Find one record and compare.
        self.assertEqual("mocha", (await db[Flavor].find_one(mocha.id)).name)

    async def test_find_many(self) -> None:
        # Insert 3 records.
        mocha1 = await db[Flavor].insert(Flavor(name="mocha"))
        mocha2 = await db[Flavor].insert(Flavor(name="mocha"))
        caramel = await db[Flavor].insert(Flavor(name="caramel"))
        # Find two records with filter.
        mochas = await db[Flavor].find_many(where={"name": "mocha"})
        self.assertListEqual([mocha1, mocha2], mochas)
        flavors = await db[Flavor].find_many()
        self.assertListEqual([mocha1, mocha2, caramel], flavors)

    def test_insert(self) -> None:
        # TODO Insert record.
        # TODO Find one record.
        self.assertEqual("", "")

    def test_update(self) -> None:
        # TODO Insert record.
        # TODO update record.
        # TODO Find one record.
        self.assertEqual("", "")

    def test_upsert(self) -> None:
        # TODO Insert record.
        # TODO upsert record.
        # TODO Find one record.
        self.assertEqual("", "")
        # TODO upsert new record.
        # TODO Find one record.
        self.assertEqual("", "")

    def test_delete(self) -> None:
        # TODO Insert record.
        # TODO Delete record.
        # TODO Find one record.
        self.assertEqual(None, None)


class ORMPyDBTests(unittest.TestCase):
    pass
