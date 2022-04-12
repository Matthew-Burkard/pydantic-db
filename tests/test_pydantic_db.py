"""PyDB tests."""
import unittest
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from pydantic_db.pydb import Column, PyDB

db = PyDB()


@db.table()
class Flavor(BaseModel):
    """Drink it in the morning."""

    id: UUID = Field(default_factory=uuid4, **Column(primary_key=True).dict())
    name: str = Field(max_length=63)


@db.table()
class Coffee(BaseModel):
    """Drink it in the morning."""

    id: UUID = Field(default_factory=uuid4, **Column(primary_key=True).dict())
    flavor: Flavor


class PyDBTests(unittest.TestCase):
    def test_create_tables(self) -> None:
        self.assertEqual(["coffee", "flavor"], [])

    def test_find_one(self) -> None:
        # TODO Insert record.
        # TODO Find one record.
        self.assertEqual("", "")

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
