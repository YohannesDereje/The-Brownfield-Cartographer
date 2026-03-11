from __future__ import annotations

import json
from pathlib import Path

import networkx as nx


class KnowledgeGraph:
	"""Central graph store for merged structural and lineage data."""

	def __init__(self) -> None:
		self.graph = nx.DiGraph()

	def merge_graphs(self, structural_graph: nx.DiGraph, lineage_graph: nx.DiGraph) -> nx.DiGraph:
		self.graph = nx.compose(structural_graph, lineage_graph)
		return self.graph

	def save_json(self, output_path: str | Path) -> str:
		target = Path(output_path)
		target.parent.mkdir(parents=True, exist_ok=True)
		payload = nx.node_link_data(self.graph)
		with target.open("w", encoding="utf-8") as file:
			json.dump(payload, file, indent=2, default=str)
		return str(target)

	@staticmethod
	def load_json(input_path: str | Path) -> nx.DiGraph:
		target = Path(input_path)
		with target.open("r", encoding="utf-8") as file:
			payload = json.load(file)
		graph = nx.node_link_graph(payload)
		return graph if isinstance(graph, nx.DiGraph) else nx.DiGraph(graph)


__all__ = ["KnowledgeGraph"]
