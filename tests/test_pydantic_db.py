"""PyDB tests."""
import asyncio
from uuid import UUID, uuid4

import sqlalchemy
from pydantic import BaseModel, Field

from pydantic_db.pydb import Column, PyDB

metadata = sqlalchemy.MetaData()
engine = sqlalchemy.create_engine("sqlite://", echo=True, future=True)
db = PyDB(metadata)


@db.table()
class Coffee(BaseModel):
    """Drink it in the morning."""

    id: UUID = Field(default_factory=uuid4, **Column(primary_key=True).dict())
    name: str = Field(max_length=63)


async def test() -> None:
    # Insert
    coffee = Coffee(name="mocha")
    coffee = await db[Coffee].insert(coffee)

    # Find one
    coffee = await db[Coffee].find_one(coffee.id)

    # Find many
    await db[Coffee].find_many()
    await db[Coffee].find_many(where={"name": "mocha"})

    # Update
    coffee.name = "caramel"
    coffee = await db[Coffee].update(coffee)

    # Upsert
    coffee.name = "vanilla"
    coffee = await db[Coffee].upsert(coffee)

    # Delete
    await db[Coffee].delete(coffee.id)


if __name__ == "__main__":
    asyncio.run(test())
