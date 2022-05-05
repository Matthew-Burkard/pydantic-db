"""Module providing PyDBTable."""
from pydantic import BaseModel


class PyDBTable(BaseModel):
    """Class to store table information."""

    name: str
    columns: list[str]
    relationships: dict[str, str]
