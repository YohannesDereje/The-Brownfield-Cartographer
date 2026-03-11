from __future__ import annotations

from pydantic import BaseModel, Field

from src.models.nodes import DataLineageEdge, EdgeNode, EdgeType


class ImportEdge(BaseModel):
	source: str
	target: str
	edge_type: EdgeType = EdgeType.IMPORTS
	metadata: dict[str, str] = Field(default_factory=dict)


class ProduceEdge(BaseModel):
	source: str
	target: str
	edge_type: EdgeType = EdgeType.PRODUCES
	metadata: dict[str, str] = Field(default_factory=dict)


class ConsumeEdge(BaseModel):
	source: str
	target: str
	edge_type: EdgeType = EdgeType.CONSUMES
	metadata: dict[str, str] = Field(default_factory=dict)


__all__ = [
	"EdgeType",
	"EdgeNode",
	"DataLineageEdge",
	"ImportEdge",
	"ProduceEdge",
	"ConsumeEdge",
]
