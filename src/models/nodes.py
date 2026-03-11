from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class FileNode(BaseModel):
	"""Represents a source file entity in the code intelligence graph."""

	path: str = Field(..., description="Repository-relative or absolute file path")
	language: str | None = Field(default=None, description="Detected programming language")

	@property
	def extension(self) -> str:
		return Path(self.path).suffix.lower()


class FunctionNode(BaseModel):
	"""Represents a function discovered in a module."""

	name: str
	is_public: bool = True
	line: int = 0
	column: int = 0


class ClassNode(BaseModel):
	"""Represents a class definition with inheritance details."""

	name: str
	bases: list[str] = Field(default_factory=list)
	line: int = 0
	column: int = 0


class ImportNode(BaseModel):
	"""Represents an import edge from one module to another."""

	module: str | None = None
	names: list[str] = Field(default_factory=list)
	is_from_import: bool = False
	is_relative: bool = False
	level: int = 0
	resolved_path: str | None = None
	line: int = 0
	column: int = 0


class ModuleNode(FileNode):
	"""Top-level analysis artifact for a Python module."""

	functions: list[FunctionNode] = Field(default_factory=list)
	classes: list[ClassNode] = Field(default_factory=list)
	imports: list[ImportNode] = Field(default_factory=list)
	metadata: dict[str, Any] = Field(default_factory=dict)

