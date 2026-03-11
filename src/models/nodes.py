from pathlib import Path
from typing import Any
from typing import Literal
from enum import Enum

from pydantic import BaseModel, Field


class FileNode(BaseModel):
	"""Represents a source file entity in the code intelligence graph."""

	path: str = Field(..., description="Repository-relative or absolute file path")
	language: str | None = Field(default=None, description="Detected programming language")

	@property
	def extension(self) -> str:
		return Path(self.path).suffix.lower()


class EdgeType(str, Enum):
	IMPORTS = "IMPORTS"
	PRODUCES = "PRODUCES"
	CONSUMES = "CONSUMES"


class EdgeNode(BaseModel):
	source: str
	target: str
	edge_type: EdgeType
	metadata: dict[str, Any] = Field(default_factory=dict)


class FunctionNode(BaseModel):
	"""Represents a function discovered in a module."""

	name: str
	is_public: bool = True
	line: int = 0
	column: int = 0


class TransformationNode(BaseModel):
	name: str
	operation: str
	inputs: list[str] = Field(default_factory=list)
	outputs: list[str] = Field(default_factory=list)
	metadata: dict[str, Any] = Field(default_factory=dict)


class DatasetNode(BaseModel):
	uri: str
	dataset_type: str = "external"
	metadata: dict[str, Any] = Field(default_factory=dict)


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


class DataLineageEdge(BaseModel):
	"""Represents a data movement edge discovered within a module."""

	source_uri: str | None = None
	sink_uri: str | None = None
	operation_type: Literal["READ", "WRITE", "TRANSFORM", "ORCHESTRATION"]
	is_dynamic: bool = False


class ModuleNode(FileNode):
	"""Top-level analysis artifact for a Python module."""

	functions: list[FunctionNode] = Field(default_factory=list)
	classes: list[ClassNode] = Field(default_factory=list)
	imports: list[ImportNode] = Field(default_factory=list)
	transformations: list[TransformationNode] = Field(default_factory=list)
	lineage: list[DataLineageEdge] = Field(default_factory=list)
	metadata: dict[str, Any] = Field(default_factory=dict)

