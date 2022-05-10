# Pydantic DB

Pydantic DB is an asynchronous ORM that uses pydantic models to represent database
tables.

## Getting started

Pydantic DB uses SQL Alchemy tp run queries.
To start, create a SQL Alchemy async engine and pass it to a `PyDB` object.

```python
from sqlalchemy.ext.asyncio import create_async_engine
from pydantic_db.pydb import PyDB

engine = create_async_engine("sqlite+aiosqlite:///db.sqlite3")
db = PyDB(engine)
```

To create tables decorate a pydantic model with the `db.table` decorator, passing db info to the decorator call.

```python
@db.table(pk="id", indexed=["name"])
class Flavor(BaseModel):
    """A coffee flavor."""

    id: UUID = Field(default_factory=uuid4)
    name: str = Field(max_length=63)
```

## Full Example

```python
"""Pydantic DB Demo."""
import asyncio
from uuid import UUID, uuid4

from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import create_async_engine

from pydantic_db.pydb import PyDB

engine = create_async_engine("sqlite+aiosqlite:///db.sqlite3")
db = PyDB(engine)


@db.table(pk="id", indexed=["name"])
class Flavor(BaseModel):
    """A coffee flavor."""

    id: UUID = Field(default_factory=uuid4)
    name: str = Field(max_length=63)


@db.table(pk="id")
class Coffee(BaseModel):
    """Drink it in the morning."""

    id: UUID = Field(default_factory=uuid4)
    sweetener: str | None = Field(max_length=63)
    sweetener_count: int | None = None
    flavor: Flavor


async def demo() -> None:
    """Demo CRUD operations."""
    # Init
    await db.init()

    # Insert
    flavor = Flavor(name="mocha")
    await db[Flavor].insert(flavor)
    coffee = Coffee(sweetener=None, flavor=flavor)
    await db[Coffee].insert(coffee)

    # Find one
    mocha = await db[Flavor].find_one(flavor.id)
    print(mocha.name)
    # Find one with depth.
    find_coffee = await db[Coffee].find_one(coffee.id, depth=1)
    print(find_coffee.flavor.name)

    # Find many
    await db[Flavor].find_many()  # Find all.
    # Get paginated results.
    await db[Flavor].find_many(
        where={"name": "mocha"}, order_by=["id", "name"], limit=2, offset=2
    )

    # Update
    flavor.name = "caramel"
    flavor = await db[Flavor].update(flavor)

    # Upsert
    flavor.name = "vanilla"
    flavor = await db[Flavor].upsert(flavor)

    # Delete
    await db[Flavor].delete(flavor.id)


if __name__ == "__main__":
    asyncio.run(demo())
```
