from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from loguru import logger
import networkx as nx

from src.agents.cartographer import Cartographer
from src.agents.hydrologist import Hydrologist
from src.agents.surveyor import SurveyorAgent
from src.analyzers.tree_sitter_analyzer import LanguageRouter, TreeSitterAnalyzer
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.nodes import ModuleNode


def _save_lineage_graph(graph: nx.DiGraph, output_dir: Path) -> str:
	store = KnowledgeGraph()
	store.graph = graph
	return store.save_json(output_dir / "lineage_graph.json")


def _save_summary(output_dir: Path, summary: dict[str, Any]) -> str:
	output_dir.mkdir(parents=True, exist_ok=True)
	path = output_dir / "analysis_summary.json"
	with path.open("w", encoding="utf-8") as handle:
		json.dump(summary, handle, indent=2, default=str)
	return str(path)


def run_interim_pipeline(repo_path: str) -> dict[str, Any]:
	"""Run Surveyor -> Hydrologist pipeline and persist interim submission artifacts."""
	logger.info("Starting interim analysis of {}", repo_path)

	repository = Path(repo_path).resolve()
	output_dir = repository / ".cartography"
	router = LanguageRouter()
	analyzer = TreeSitterAnalyzer(router=router)
	surveyor = SurveyorAgent(analyzer=analyzer)
	cartographer = Cartographer()
	hydrologist = Hydrologist()

	try:
		source_files = surveyor.scan_directory(repository)
		logger.info("Found {} files. Starting AST parsing...", len(source_files))

		modules = [surveyor.analyze_module(str(file_path)) for file_path in source_files]
		velocity_map = surveyor.get_git_velocity(str(repository), [str(path) for path in source_files])

		cartographer.build_graph(modules, velocity_map=velocity_map)
		metrics = cartographer.compute_architectural_metrics()
		module_graph_path = cartographer.save_graph(output_dir=str(output_dir), filename="module_graph.json")

		hydrologist.hydrate_repository_lineage(modules)
		lineage_graph = hydrologist.build_global_graph(modules)
		lineage_graph_path = _save_lineage_graph(lineage_graph, output_dir)

		structural_graph = surveyor.build_import_graph(modules)
		summary = {
			"repo": str(repository),
			"file_count": len(source_files),
			"module_count": len(modules),
			"structural_node_count": structural_graph.number_of_nodes(),
			"structural_edge_count": structural_graph.number_of_edges(),
			"lineage_node_count": lineage_graph.number_of_nodes(),
			"lineage_edge_count": lineage_graph.number_of_edges(),
			"architectural_hubs": surveyor.compute_architectural_hubs(structural_graph),
			"circular_dependencies": surveyor.detect_circular_dependencies(structural_graph),
			"pagerank": metrics.get("pagerank", {}),
			"strongly_connected_components": metrics.get("strongly_connected_components", []),
			"git_velocity": velocity_map,
		}
		summary_path = _save_summary(output_dir, summary)

		logger.info("Saved structural graph to {}", module_graph_path)
		logger.info("Saved lineage graph to {}", lineage_graph_path)
		return {
			"modules": modules,
			"module_graph_path": module_graph_path,
			"lineage_graph_path": lineage_graph_path,
			"summary_path": summary_path,
		}
	except Exception as exc:
		logger.exception("Interim pipeline failed for {}: {}", repository, exc)
		return {
			"modules": [],
			"module_graph_path": None,
			"lineage_graph_path": None,
			"summary_path": None,
		}


def run_surveyor_pipeline(repo_path: str) -> list[ModuleNode]:
	"""Backward-compatible wrapper that returns analyzed modules only."""
	result = run_interim_pipeline(repo_path)
	return result.get("modules", [])


__all__ = ["run_interim_pipeline", "run_surveyor_pipeline"]
