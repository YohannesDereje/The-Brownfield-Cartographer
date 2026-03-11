from __future__ import annotations

from pathlib import Path

from loguru import logger

from src.agents.cartographer import Cartographer
from src.agents.surveyor import SurveyorAgent
from src.analyzers.tree_sitter_analyzer import LanguageRouter, TreeSitterAnalyzer
from src.models.nodes import ModuleNode
from src.utils.git_utils import get_git_velocity


def run_surveyor_pipeline(repo_path: str) -> list[ModuleNode]:
	"""Run Phase 1 structural analysis and graph construction for a repository."""
	logger.info("Starting structural analysis of {}", repo_path)

	repository = Path(repo_path).resolve()
	router = LanguageRouter()
	analyzer = TreeSitterAnalyzer(router=router)
	surveyor = SurveyorAgent(analyzer=analyzer)
	cartographer = Cartographer()

	try:
		source_files = surveyor.scan_directory(repository)
		logger.info("Found {} files. Starting AST parsing...", len(source_files))

		modules = [surveyor.analyze_module(str(file_path)) for file_path in source_files]

		velocity_map = get_git_velocity(str(repository))

		cartographer.build_graph(modules, velocity_map=velocity_map)
		cartographer.compute_architectural_metrics()
		output_path = cartographer.save_graph(
			output_dir=str(repository / ".cartography"),
			filename="module_graph.json",
		)

		logger.info("Phase 1 Complete. Saved graph to .cartography/module_graph.json")
		logger.debug("Graph persisted to {}", output_path)
		return modules
	except Exception as exc:
		logger.exception("Surveyor pipeline failed for {}: {}", repository, exc)
		return []


__all__ = ["run_surveyor_pipeline"]
