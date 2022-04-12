"""PyDB tests."""
import asyncio
import unittest
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field
# noinspection PyPackageRequirements
from tortoise import Tortoise

from pydantic_db.pydb import Column, PyDB

db = PyDB()
db_url = "sqlite://db.sqlite3"


@db.table()
class Flavor(BaseModel):
    """A coffee flavor."""

    id: UUID = Field(default_factory=uuid4, **Column(pk=True).dict())
    name: str = Field(max_length=63)


@db.table()
class Coffee(BaseModel):
    """Drink it in the morning."""

    id: UUID = Field(default_factory=uuid4, **Column(pk=True).dict())
    flavor: Flavor


def await_(coroutine: Any) -> Any:
    """Get event loop, run a coroutine, and return the result."""
    return asyncio.get_event_loop().run_until_complete(coroutine())


class PyDBTests(unittest.TestCase):
    def test_create_tables(self) -> None:
        await_(Tortoise.init(db_url=db_url, modules={"models": ["app.models"]}))
        await_(Tortoise.generate_schemas())
        self.assertEqual(["coffee", "flavor"], [])

    def test_find_one(self) -> None:
        # Insert record.
        flavor = Flavor(name="mocha")
        mocha = await_(db[Flavor].insert(flavor))
        self.assertEqual("mocha", db[Flavor].find_one(mocha.id))

    def test_find_many(self) -> None:
        # TODO Insert 3 records.
        # TODO Find two records with filter.
        self.assertEqual("", "")
        # TODO Find all records.
        self.assertEqual("", "")

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
