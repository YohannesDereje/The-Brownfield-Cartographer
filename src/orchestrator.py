from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
from time import time
from typing import Any

from loguru import logger
import networkx as nx

from src.agents.archivist import Archivist
from src.agents.cartographer import Cartographer
from src.agents.hydrologist import Hydrologist
from src.agents.semanticist import Semanticist
from src.agents.surveyor import SurveyorAgent
from src.analyzers.tree_sitter_analyzer import LanguageRouter, TreeSitterAnalyzer
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.nodes import ModuleNode
from src.utils.hashing import FileManifest, sha256_file
from src.utils.tracer import CartographyTracer, InferenceMethod


def _lineage_phase(module: ModuleNode) -> tuple[int, str]:
	suffix = Path(module.path).suffix.lower()
	if suffix in {".yml", ".yaml"}:
		return (0, module.path)
	if suffix == ".sql":
		return (1, module.path)
	return (2, module.path)


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


def _load_cached_modules(module_graph_path: Path) -> dict[str, ModuleNode]:
	if not module_graph_path.exists():
		return {}
	try:
		payload = json.loads(module_graph_path.read_text(encoding="utf-8"))
	except (json.JSONDecodeError, OSError, TypeError, ValueError):
		return {}

	results: dict[str, ModuleNode] = {}
	allowed_keys = {
		"path",
		"language",
		"functions",
		"classes",
		"imports",
		"transformations",
		"lineage",
		"metadata",
	}
	for node_payload in payload.get("nodes", []):
		if not isinstance(node_payload, dict):
			continue
		if "path" not in node_payload:
			continue
		filtered = {key: value for key, value in node_payload.items() if key in allowed_keys}
		try:
			module = ModuleNode(**filtered)
		except Exception:
			continue
		results[str(Path(module.path).resolve())] = module
	return results


def _prune_removed_modules(graph: nx.DiGraph, valid_module_paths: set[str]) -> None:
	for node_id, attrs in list(graph.nodes(data=True)):
		node_path = attrs.get("path")
		if isinstance(node_path, str) and str(Path(node_path).resolve()) not in valid_module_paths:
			graph.remove_node(node_id)


def _cache_hit_trace(tracer: CartographyTracer, file_path: str) -> None:
	tracer.log_action(
		agent_name="system",
		action_type="cache_hit",
		evidence_source=f"{file_path}:1",
		confidence_level=1.0,
		inference_method=InferenceMethod.STATIC_ANALYSIS,
	)


def _clone_remote_repository(repo_url: str) -> Path:
	"""Clone a remote repository URL into a temporary workspace folder."""
	temp_base = Path(tempfile.gettempdir()) / "cartography_clone"
	temp_base.mkdir(parents=True, exist_ok=True)
	safe_name = repo_url.rstrip("/").split("/")[-1].removesuffix(".git") or "repo"
	clone_dir = temp_base / f"{safe_name}_{int(time())}"

	logger.info("Remote repository detected; cloning {} into {}", repo_url, clone_dir)
	result = subprocess.run(
		["git", "clone", "--depth", "1", repo_url, str(clone_dir)],
		capture_output=True,
		text=True,
		check=False,
	)
	if result.returncode != 0:
		stderr = (result.stderr or "").strip()
		raise RuntimeError(f"git clone failed for {repo_url}: {stderr or 'unknown git error'}")

	logger.info("Remote clone ready at {}", clone_dir)
	return clone_dir


def _resolve_repository_path(repo_path: str, surveyor: SurveyorAgent) -> tuple[Path, Path | None, str]:
	input_type = surveyor.detect_input_type(repo_path)
	if input_type == "remote":
		clone_dir = _clone_remote_repository(repo_path)
		return clone_dir.resolve(), clone_dir.resolve(), "remote"

	repository = Path(repo_path).expanduser().resolve()
	return repository, None, "local"


def _slugify_repo_name(value: str) -> str:
	text = value.strip().rstrip("/")
	text = text.removesuffix(".git")
	text = text.split("/")[-1] if "/" in text or "\\" in text else text
	slug = re.sub(r"[^A-Za-z0-9._-]+", "-", text).strip("-._")
	return (slug or "repo").lower()


def _analysis_output_dir(repo_path: str, repository: Path, input_type: str) -> Path:
	if input_type == "remote":
		repo_name = _slugify_repo_name(repo_path)
	else:
		repo_name = _slugify_repo_name(repository.name)
	output_dir = Path.cwd().resolve() / ".cartography" / repo_name
	output_dir.mkdir(parents=True, exist_ok=True)
	return output_dir


def _force_remove_directory(path: Path) -> None:
	def _onerror(func, target, exc_info):
		try:
			os.chmod(target, 0o700)
		except Exception:
			pass
		func(target)

	shutil.rmtree(path, onerror=_onerror)


def run_interim_pipeline(repo_path: str, cleanup_remote_clone: bool = True) -> dict[str, Any]:
	"""Run Surveyor -> Hydrologist pipeline and persist interim submission artifacts."""
	logger.info("Starting interim analysis of {}", repo_path)

	router = LanguageRouter()
	analyzer = TreeSitterAnalyzer(router=router)
	input_probe = SurveyorAgent(analyzer=analyzer)
	repository, temp_clone_path, input_type = _resolve_repository_path(repo_path, input_probe)
	output_dir = _analysis_output_dir(repo_path, repository, input_type)
	logger.info("Artifacts for this run will be written to {}", output_dir)
	module_graph_file = output_dir / "module_graph.json"
	lineage_graph_file = output_dir / "lineage_graph.json"
	surveyor = SurveyorAgent(analyzer=analyzer, trace_path=output_dir / "cartography_trace.jsonl")
	cartographer = Cartographer()
	hydrologist = Hydrologist(trace_path=output_dir / "cartography_trace.jsonl", repo_root=repository)
	semanticist = Semanticist(trace_path=output_dir / "cartography_trace.jsonl")
	archivist = Archivist(output_dir=output_dir)
	tracer = CartographyTracer(output_dir / "cartography_trace.jsonl")
	manifest = FileManifest(output_dir / "file_manifest.json")

	try:
		source_files = surveyor.scan_directory(repository)
		logger.info("Found {} files. Starting AST parsing...", len(source_files))

		valid_paths = {str(file_path.resolve()) for file_path in source_files}
		cached_modules = _load_cached_modules(module_graph_file)
		manifest.prune(valid_paths)

		structural_graph = Cartographer.load_graph(str(module_graph_file)) if module_graph_file.exists() else nx.DiGraph()
		lineage_graph = nx.DiGraph()
		_prune_removed_modules(structural_graph, valid_paths)

		modules_by_path: dict[str, ModuleNode] = {}
		changed_paths: set[str] = set()
		for file_path in source_files:
			resolved_path = str(file_path.resolve())
			current_hash = sha256_file(file_path)
			cached_module = cached_modules.get(resolved_path)
			if not manifest.has_changed(file_path, current_hash) and cached_module is not None:
				modules_by_path[resolved_path] = cached_module
				_cache_hit_trace(tracer, resolved_path)
				manifest.update(file_path, current_hash)
				continue

			module = surveyor.analyze_module(str(file_path))
			modules_by_path[resolved_path] = module
			changed_paths.add(resolved_path)
			manifest.update(file_path, current_hash)

		modules = [modules_by_path[path] for path in sorted(modules_by_path.keys())]
		velocity_map = surveyor.get_git_velocity(str(repository), [str(path) for path in source_files])

		cartographer.graph = structural_graph if structural_graph.number_of_nodes() > 0 else nx.DiGraph()
		for module in modules:
			if str(Path(module.path).resolve()) not in changed_paths and cartographer.graph.has_node(module.path.replace("\\", "/")):
				continue
			surveyor.upsert_module_dependencies(cartographer.graph, module, modules)
			module_node_path = module.path.replace("\\", "/")
			cartographer.graph.nodes[module_node_path]["git_velocity"] = velocity_map.get(str(Path(module.path).resolve()), 0)
			cartographer.graph.nodes[module_node_path]["metadata"] = module.metadata
			cartographer.graph.nodes[module_node_path]["functions"] = [item.model_dump() for item in module.functions]
			cartographer.graph.nodes[module_node_path]["imports"] = [item.model_dump() for item in module.imports]
			cartographer.graph.nodes[module_node_path]["classes"] = [item.model_dump() for item in module.classes]
			cartographer.graph.nodes[module_node_path]["transformations"] = [item.model_dump() for item in module.transformations]
			cartographer.graph.nodes[module_node_path]["lineage"] = [item.model_dump() for item in module.lineage]

		for module in modules:
			resolved_path = str(Path(module.path).resolve())
			if resolved_path in changed_paths:
				semanticist.generate_purpose_statement(module)
				continue
			cached_module = cached_modules.get(resolved_path)
			if cached_module is not None:
				cached_semanticist = cached_module.metadata.get("semanticist")
				if isinstance(cached_semanticist, dict):
					module.metadata.setdefault("semanticist", {}).update(cached_semanticist)

		clusters = semanticist.identify_domain_clusters(modules)
		semanticist.assign_modules_to_clusters(modules, clusters)
		outliers = [
			module for module in modules
			if module.metadata.get("semanticist", {}).get("is_architectural_outlier", False)
		]
		day_one_brief = semanticist.generate_day_one_brief(clusters, outliers)

		metrics = cartographer.compute_architectural_metrics()
		module_graph_path = cartographer.save_graph(output_dir=str(output_dir), filename="module_graph.json")

		for module in sorted(modules, key=_lineage_phase):
			hydrologist.hydrate_repository_lineage([module])
			hydrologist.upsert_module_lineage(lineage_graph, module, modules)

		for module in modules:
			module_node_path = module.path.replace("\\", "/")
			if not cartographer.graph.has_node(module_node_path):
				continue
			cartographer.graph.nodes[module_node_path]["metadata"] = module.metadata
			cartographer.graph.nodes[module_node_path]["functions"] = [item.model_dump() for item in module.functions]
			cartographer.graph.nodes[module_node_path]["imports"] = [item.model_dump() for item in module.imports]
			cartographer.graph.nodes[module_node_path]["classes"] = [item.model_dump() for item in module.classes]
			cartographer.graph.nodes[module_node_path]["transformations"] = [item.model_dump() for item in module.transformations]
			cartographer.graph.nodes[module_node_path]["lineage"] = [item.model_dump() for item in module.lineage]
		boundary_nodes = hydrologist.identify_system_boundary_nodes(modules)
		lineage_graph_path = _save_lineage_graph(lineage_graph, output_dir)

		structural_graph = surveyor.build_import_graph(modules)
		summary = {
			"repo": str(repo_path if input_type == "remote" else repository),
			"analyzed_repo_path": str(repository),
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
			"delta_mode": True,
		}
		summary_path = _save_summary(output_dir, summary)
		manifest.save()

		semanticist_data = {
			"day_one_brief": day_one_brief,
			"clusters": clusters,
			"drift_audit": archivist._build_drift_audit(modules),
		}
		hydrologist_data = {"boundaries": boundary_nodes}
		day_one_questions = semanticist.answer_day_one_questions(
			surveyor_data=summary,
			hydrologist_data=hydrologist_data,
			nodes=modules,
		)
		semanticist_data["day_one_questions"] = day_one_questions
		target_analysis_dir = output_dir
		codebase_report_path = archivist.generate_CODEBASE_md(
			surveyor_data=summary,
			hydrologist_data=hydrologist_data,
			semanticist_data=semanticist_data,
			nodes=modules,
			target_dir=target_analysis_dir,
		)
		onboarding_brief_path = archivist.generate_onboarding_brief_md(
			repo_root=repository,
			surveyor_data=summary,
			hydrologist_data=hydrologist_data,
			semanticist_data=semanticist_data,
			nodes=modules,
			target_dir=target_analysis_dir,
		)

		logger.info("Saved structural graph to {}", module_graph_path)
		logger.info("Saved lineage graph to {}", lineage_graph_path)

		manifest_path = str(manifest.manifest_path)
		logger.info(
			"Artifact set ready. CODEBASE.md: {} | onboarding_brief.md: {}",
			Path(str(codebase_report_path)).resolve(),
			Path(str(onboarding_brief_path)).resolve(),
		)

		return {
			"modules": modules,
			"module_graph_path": module_graph_path,
			"lineage_graph_path": lineage_graph_path,
			"summary_path": summary_path,
			"codebase_report_path": str(codebase_report_path),
			"onboarding_brief_path": str(onboarding_brief_path),
			"input_type": input_type,
			"cloned_repo_path": str(temp_clone_path) if temp_clone_path is not None else None,
			"cleanup_remote_clone": cleanup_remote_clone,
			"manifest_path": manifest_path,
		}
	except Exception as exc:
		logger.exception("Interim pipeline failed for {}: {}", repository, exc)
		return {
			"modules": [],
			"module_graph_path": None,
			"lineage_graph_path": None,
			"summary_path": None,
		}
	finally:
		if temp_clone_path is not None:
			if cleanup_remote_clone:
				try:
					_force_remove_directory(temp_clone_path)
					logger.info("Temporary clone deleted: {}", temp_clone_path)
				except Exception as cleanup_exc:
					logger.warning("Failed to delete temporary clone {}: {}", temp_clone_path, cleanup_exc)
			else:
				logger.info(
					"Temporary clone retained at {}. Clear manually when no longer needed after Archivist artifacts are consumed.",
					temp_clone_path,
				)


def run_surveyor_pipeline(repo_path: str) -> list[ModuleNode]:
	"""Backward-compatible wrapper that returns analyzed modules only."""
	result = run_interim_pipeline(repo_path)
	return result.get("modules", [])


__all__ = ["run_interim_pipeline", "run_surveyor_pipeline"]
