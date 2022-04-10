"""PyDB tests."""
import asyncio
from uuid import UUID, uuid4

from pydantic import BaseModel

from pydantic_db.pydb import field, PyDB

db = PyDB()


@db.table
class Coffee(BaseModel):
    """Drink it in the morning."""

    id: UUID = field(primary_key=True, default_factory=uuid4)
    name: str


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
