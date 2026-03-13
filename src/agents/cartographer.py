from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from loguru import logger
import networkx as nx

from src.models.nodes import ModuleNode


class Cartographer:
    """Builds and analyzes a directed dependency graph of modules."""

    def __init__(self, graph: nx.DiGraph | None = None) -> None:
        self.graph = graph if graph is not None else nx.DiGraph()

    def build_graph(
        self,
        modules: list[ModuleNode],
        velocity_map: dict[str, int] | None = None,
    ) -> None:
        """Build graph nodes and dependency edges from analyzed modules."""
        self.graph.clear()

        normalized_velocity_map = {
            self._normalize_path(path): count
            for path, count in (velocity_map or {}).items()
        }

        module_paths = {self._normalize_path(module.path) for module in modules}

        for module in modules:
            module_path = self._normalize_path(module.path)
            node_payload = self._model_to_dict(module)
            node_payload["git_velocity"] = normalized_velocity_map.get(module_path, 0)
            self.graph.add_node(module_path, **node_payload)

        for module in modules:
            source_path = self._normalize_path(module.path)
            for import_node in module.imports:
                if not import_node.resolved_path:
                    continue

                target_path = self._normalize_path(import_node.resolved_path)
                if target_path not in module_paths:
                    logger.debug(
                        "Skipping edge {} -> {} because target module is outside current scope.",
                        source_path,
                        target_path,
                    )
                    continue

                self.graph.add_edge(source_path, target_path)

    def compute_architectural_metrics(self) -> dict[str, Any]:
        """Compute PageRank and strongly connected components for the graph."""
        if self.graph.number_of_nodes() == 0:
            return {"pagerank": {}, "strongly_connected_components": []}

        try:
            pagerank_scores = nx.pagerank(self.graph)
        except Exception as exc:
            logger.warning("Failed to compute PageRank: {}", exc)
            pagerank_scores = {node: 0.0 for node in self.graph.nodes}

        for node_path, score in pagerank_scores.items():
            self.graph.nodes[node_path]["pagerank_score"] = score

        components = [
            sorted(component)
            for component in nx.strongly_connected_components(self.graph)
            if len(component) > 1
        ]

        return {
            "pagerank": pagerank_scores,
            "strongly_connected_components": components,
        }

    def get_hubs(self, top_n: int = 10) -> list[str]:
        """Return top modules ranked by PageRank score."""
        ranked = sorted(
            self.graph.nodes(data=True),
            key=lambda item: item[1].get("pagerank_score", 0.0),
            reverse=True,
        )
        return [module_path for module_path, _ in ranked[:top_n]]

    def save_graph(
        self,
        output_dir: str = ".cartography",
        filename: str = "module_graph.json",
    ) -> str:
        """Persist the current graph to disk in node-link JSON format."""
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)

        output_path = directory / filename
        data = nx.node_link_data(self.graph)

        with output_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)

        logger.info(
            "Saved graph to {} (nodes={}, edges={})",
            output_path,
            self.graph.number_of_nodes(),
            self.graph.number_of_edges(),
        )
        return str(output_path)

    @staticmethod
    def load_graph(file_path: str) -> nx.DiGraph:
        """Load a graph from a node-link JSON file."""
        path = Path(file_path)
        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            graph = nx.node_link_graph(data)
            if isinstance(graph, nx.DiGraph):
                return graph

            logger.warning(
                "Loaded graph at {} is not DiGraph; converting to DiGraph.",
                path,
            )
            return nx.DiGraph(graph)
        except (FileNotFoundError, json.JSONDecodeError, OSError, TypeError, ValueError) as exc:
            logger.error("Failed to load graph from {}: {}", path, exc)
            return nx.DiGraph()

    def _model_to_dict(self, module: ModuleNode) -> dict[str, Any]:
        if hasattr(module, "model_dump"):
            return module.model_dump()
        return module.dict()

    def _normalize_path(self, path: str) -> str:
        return path.replace("\\", "/")


__all__ = ["Cartographer"]
