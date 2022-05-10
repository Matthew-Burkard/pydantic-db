"""Provides ModelType TypeVar used throughout lib."""
from typing import TypeVar

from pydantic import BaseModel

ModelType = TypeVar("ModelType", bound=BaseModel)
