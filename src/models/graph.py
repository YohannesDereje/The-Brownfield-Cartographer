from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from src.models.nodes import DatasetNode, EdgeNode, ModuleNode


class StructuralGraphArtifact(BaseModel):
	nodes: list[ModuleNode] = Field(default_factory=list)
	edges: list[EdgeNode] = Field(default_factory=list)
	metadata: dict[str, Any] = Field(default_factory=dict)


class LineageGraphArtifact(BaseModel):
	modules: list[ModuleNode] = Field(default_factory=list)
	datasets: list[DatasetNode] = Field(default_factory=list)
	edges: list[EdgeNode] = Field(default_factory=list)
	metadata: dict[str, Any] = Field(default_factory=dict)


__all__ = ["StructuralGraphArtifact", "LineageGraphArtifact"]
