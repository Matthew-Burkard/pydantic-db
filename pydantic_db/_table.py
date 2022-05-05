"""Module providing PyDBTable."""
from pydantic import BaseModel


class PyDBTableMeta(BaseModel):
    """Class to store table information."""

    name: str
    columns: list[str]
    relationships: dict[str, str]
