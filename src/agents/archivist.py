from __future__ import annotations

import json
import os
from pathlib import Path
import re
from typing import Any

from loguru import logger

from src.models.nodes import ModuleNode


class Archivist:
	"""Compiles cartography outputs into a readable codebase report."""

	def __init__(self, output_dir: str | Path = ".cartography") -> None:
		self.output_dir = Path(output_dir)
		self.output_dir.mkdir(parents=True, exist_ok=True)
		self.report_writer_model = os.getenv("ARCHIVIST_REPORT_WRITER_MODEL", "openai/gpt-4o-mini")

	def write_cartography_report(
		self,
		day_one_brief: dict[str, Any] | None = None,
		clusters: dict[str, str] | None = None,
		nodes: list[ModuleNode] | None = None,
		output_path: str | Path | None = None,
		surveyor_data: dict[str, Any] | None = None,
		hydrologist_data: dict[str, Any] | None = None,
		semanticist_data: dict[str, Any] | None = None,
		target_dir: str | Path | None = None,
	) -> Path:
		"""Write the final cartography report to .cartography/CODEBASE.md.

		This supports both the older semantic-only call shape and the full integrated
		Surveyor/Hydrologist/Semanticist payload used by the definitive Phase 4 report.
		"""
		return self.generate_CODEBASE_md(
			surveyor_data=surveyor_data,
			hydrologist_data=hydrologist_data,
			semanticist_data=semanticist_data
			or self._legacy_semanticist_payload(day_one_brief or {}, clusters or {}, nodes or []),
			nodes=nodes or [],
			target_dir=target_dir,
			output_path=output_path,
		)

	def generate_CODEBASE_md(
		self,
		*,
		surveyor_data: dict[str, Any] | None = None,
		hydrologist_data: dict[str, Any] | None = None,
		semanticist_data: dict[str, Any] | None = None,
		nodes: list[ModuleNode] | None = None,
		target_dir: str | Path | None = None,
		output_path: str | Path | None = None,
	) -> Path:
		"""Compile the definitive CODEBASE.md living context artifact."""
		target_path = Path(output_path) if output_path is not None else self.output_dir / "CODEBASE.md"
		target_path.parent.mkdir(parents=True, exist_ok=True)
		analysis_dir = self._resolve_analysis_dir(target_dir)

		resolved_nodes = nodes or self._load_nodes_from_analysis_dir(analysis_dir)
		resolved_surveyor_data = surveyor_data or self._load_summary_from_analysis_dir(analysis_dir)
		resolved_hydrologist_data = hydrologist_data or {}
		resolved_semanticist_data = semanticist_data or {}
		repo_root_for_paths = self._resolve_repo_root_for_paths(resolved_surveyor_data, resolved_nodes)

		report = self._render_codebase_report(
			surveyor_data=resolved_surveyor_data,
			hydrologist_data=resolved_hydrologist_data,
			semanticist_data=resolved_semanticist_data,
			nodes=resolved_nodes,
			repo_root=repo_root_for_paths,
		)
		if not self._is_valid_markdown_report(report, "# Codebase Context"):
			logger.error("Refusing to write empty CODEBASE.md report for analysis dir {}", analysis_dir)
			return target_path
		target_path.write_text(report, encoding="utf-8")
		return target_path

	def generate_onboarding_brief_md(
		self,
		*,
		repo_root: str | Path,
		surveyor_data: dict[str, Any] | None = None,
		hydrologist_data: dict[str, Any] | None = None,
		semanticist_data: dict[str, Any] | None = None,
		nodes: list[ModuleNode] | None = None,
		target_dir: str | Path | None = None,
		output_path: str | Path | None = None,
	) -> Path:
		"""Generate .cartography/onboarding_brief.md for new engineers."""
		target_path = Path(output_path) if output_path is not None else self.output_dir / "onboarding_brief.md"
		target_path.parent.mkdir(parents=True, exist_ok=True)

		resolved_repo_root = Path(repo_root).resolve()
		analysis_dir = self._resolve_analysis_dir(target_dir)
		resolved_surveyor_data = surveyor_data or self._load_summary_from_analysis_dir(analysis_dir)
		resolved_hydrologist_data = hydrologist_data or {}
		resolved_semanticist_data = semanticist_data or {}
		resolved_nodes = nodes or self._load_nodes_from_analysis_dir(analysis_dir)
		questions = self._resolve_fde_day_one_questions(
			surveyor_data=resolved_surveyor_data,
			hydrologist_data=resolved_hydrologist_data,
			semanticist_data=resolved_semanticist_data,
			nodes=resolved_nodes,
			repo_root=resolved_repo_root,
		)

		repo_name = resolved_repo_root.name or "repo"

		lines = [
			"# Onboarding Brief",
			"",
			f"Generated for: {repo_name}",
			"",
		]
		for entry in questions:
			lines.extend(
				[
					f"## {entry['question']}",
					"",
					entry["answer"],
					"",
					"Evidence:",
				]
			)
			evidence = entry.get("evidence", [])
			if evidence:
				for citation in evidence:
					lines.append(f"- {self._normalize_path_text(str(citation), resolved_repo_root)}")
			else:
				lines.append("- No direct evidence provided.")
			lines.append("")

		report = "\n".join(lines).rstrip() + "\n"
		if not self._is_valid_markdown_report(report, "# Onboarding Brief"):
			logger.error("Refusing to write empty onboarding_brief.md report for analysis dir {}", analysis_dir)
			return target_path

		target_path.write_text(report, encoding="utf-8")
		return target_path

	def generate_onboarding_brief(
		self,
		*,
		repo_root: str | Path,
		surveyor_data: dict[str, Any] | None = None,
		hydrologist_data: dict[str, Any] | None = None,
		semanticist_data: dict[str, Any] | None = None,
		nodes: list[ModuleNode] | None = None,
		target_dir: str | Path | None = None,
		output_path: str | Path | None = None,
	) -> Path:
		"""Backward-compatible onboarding brief entrypoint with the five-question synthesis."""
		return self.generate_onboarding_brief_md(
			repo_root=repo_root,
			surveyor_data=surveyor_data,
			hydrologist_data=hydrologist_data,
			semanticist_data=semanticist_data,
			nodes=nodes,
			target_dir=target_dir,
			output_path=output_path,
		)

	def _resolve_fde_day_one_questions(
		self,
		*,
		surveyor_data: dict[str, Any],
		hydrologist_data: dict[str, Any],
		semanticist_data: dict[str, Any],
		nodes: list[ModuleNode],
		repo_root: Path,
	) -> list[dict[str, Any]]:
		required_questions = [
			"What is the primary data ingestion path?",
			"What are the 3-5 most critical output datasets/endpoints?",
			"What is the blast radius if the most critical module fails?",
			"Where is the business logic concentrated vs. distributed?",
			"What has changed most frequently in the last 90 days (git velocity map)?",
		]

		day_one_questions = semanticist_data.get("day_one_questions")
		if isinstance(day_one_questions, dict):
			rows = day_one_questions.get("questions")
			if isinstance(rows, list):
				indexed: dict[str, dict[str, Any]] = {}
				for row in rows:
					if not isinstance(row, dict):
						continue
					question = str(row.get("question") or "").strip()
					if question in required_questions:
						normalized_answer = self._normalize_path_text(
							str(row.get("answer") or "No answer generated.").strip() or "No answer generated.",
							repo_root,
						)
						normalized_evidence = [
							self._normalize_path_text(str(item), repo_root)
							for item in row.get("evidence", [])
							if str(item).strip()
						] if isinstance(row.get("evidence"), list) else []
						indexed[question] = {
							"question": question,
							"answer": normalized_answer,
							"evidence": normalized_evidence,
						}
				if len(indexed) == len(required_questions):
					return [indexed[question] for question in required_questions]

		sources = []
		sinks = []
		boundaries = hydrologist_data.get("boundaries") if isinstance(hydrologist_data, dict) else None
		if isinstance(boundaries, dict):
			sources = [self._normalize_path_text(str(item), repo_root) for item in boundaries.get("ultimate_sources", []) if str(item).strip()]
			sinks = [self._normalize_path_text(str(item), repo_root) for item in boundaries.get("ultimate_sinks", []) if str(item).strip()]

		pagerank = surveyor_data.get("pagerank", {}) if isinstance(surveyor_data, dict) else {}
		velocity = surveyor_data.get("git_velocity", {}) if isinstance(surveyor_data, dict) else {}
		top_module = None
		if isinstance(pagerank, dict) and pagerank:
			top_module = sorted(((str(path), float(score)) for path, score in pagerank.items()), key=lambda row: row[1], reverse=True)[0][0]
			top_module = self._normalize_path_text(top_module, repo_root)

		cluster_counts: dict[str, int] = {}
		for module_node in nodes:
			cluster = module_node.metadata.get("semanticist", {}).get("domain_cluster")
			if not isinstance(cluster, str) or not cluster.strip():
				cluster = "unassigned"
			cluster_counts[cluster] = cluster_counts.get(cluster, 0) + 1
		cluster_lines = [f"{name}: {count} modules" for name, count in sorted(cluster_counts.items(), key=lambda row: row[1], reverse=True)[:4]]

		velocity_lines = []
		if isinstance(velocity, dict):
			velocity_lines = [
				f"{self._normalize_path_text(str(path), repo_root)} ({int(count)} changes)"
				for path, count in sorted(((str(path), int(count)) for path, count in velocity.items()), key=lambda row: row[1], reverse=True)[:5]
			]

		return [
			{
				"question": required_questions[0],
				"answer": f"The ingestion path appears to begin from {sources[0]}." if sources else "No primary ingestion source was detected.",
				"evidence": sources[:3] or ["lineage_sources=none"],
			},
			{
				"question": required_questions[1],
				"answer": ", ".join(sinks[:5]) if sinks else "No explicit output datasets/endpoints were detected.",
				"evidence": sinks[:5] or ["lineage_sinks=none"],
			},
			{
				"question": required_questions[2],
				"answer": f"Failure of {top_module} likely impacts dependent modules and downstream sinks." if top_module else "Blast radius cannot be estimated from current PageRank evidence.",
				"evidence": [f"pagerank_top={top_module}"] if top_module else ["pagerank=none"],
			},
			{
				"question": required_questions[3],
				"answer": "; ".join(cluster_lines) if cluster_lines else "Business logic concentration could not be inferred.",
				"evidence": cluster_lines or ["cluster_distribution=none"],
			},
			{
				"question": required_questions[4],
				"answer": "; ".join(velocity_lines) if velocity_lines else "No git velocity evidence was available.",
				"evidence": velocity_lines or ["git_velocity=none"],
			},
		]

	def _resolve_analysis_dir(self, target_dir: str | Path | None) -> Path:
		if target_dir is None:
			return self.output_dir
		candidate = Path(target_dir).resolve()
		if candidate.name == ".cartography":
			return candidate
		return candidate / ".cartography"

	def _load_summary_from_analysis_dir(self, analysis_dir: Path) -> dict[str, Any]:
		path = analysis_dir / "analysis_summary.json"
		if not path.exists():
			return {}
		try:
			payload = json.loads(path.read_text(encoding="utf-8"))
			return payload if isinstance(payload, dict) else {}
		except Exception:
			return {}

	def _load_nodes_from_analysis_dir(self, analysis_dir: Path) -> list[ModuleNode]:
		path = analysis_dir / "module_graph.json"
		if not path.exists():
			return []
		try:
			payload = json.loads(path.read_text(encoding="utf-8"))
		except Exception:
			return []

		results: list[ModuleNode] = []
		for node_payload in payload.get("nodes", []):
			if not isinstance(node_payload, dict):
				continue
			if "path" not in node_payload:
				continue
			try:
				results.append(
					ModuleNode(
						path=str(node_payload.get("path")),
						language=str(node_payload.get("language") or "unknown"),
						metadata=node_payload.get("metadata") or {},
					)
				)
			except Exception:
				continue
		return results

	def _is_valid_markdown_report(self, content: str, header: str) -> bool:
		if not content or not content.strip():
			return False
		lines = [line.strip() for line in content.splitlines() if line.strip()]
		if not lines:
			return False
		if len(lines) == 1 and lines[0] == header:
			return False
		if len(lines) <= 3 and lines[0] == header:
			return False
		return True

	def _legacy_semanticist_payload(
		self,
		day_one_brief: dict[str, Any],
		clusters: dict[str, str],
		nodes: list[ModuleNode],
	) -> dict[str, Any]:
		return {
			"day_one_brief": day_one_brief,
			"clusters": clusters,
			"drift_audit": self._build_drift_audit(nodes),
		}

	def _build_domain_heatmap(self, clusters: dict[str, str], nodes: list[ModuleNode]) -> dict[str, dict[str, Any]]:
		heatmap: dict[str, dict[str, Any]] = {
			name: {"definition": definition, "modules": []}
			for name, definition in clusters.items()
		}
		for module_node in nodes:
			semanticist_metadata = module_node.metadata.get("semanticist", {})
			cluster_name = semanticist_metadata.get("domain_cluster")
			if isinstance(cluster_name, str) and cluster_name in heatmap:
				heatmap[cluster_name]["modules"].append(module_node.path)
		return heatmap

	def _build_drift_audit(self, nodes: list[ModuleNode]) -> list[dict[str, str]]:
		drift_audit: list[dict[str, str]] = []
		for module_node in nodes:
			semanticist_metadata = module_node.metadata.get("semanticist", {})
			if not semanticist_metadata.get("documentation_drift", False):
				continue
			drift_audit.append(
				{
					"path": module_node.path,
					"purpose_statement": str(semanticist_metadata.get("purpose_statement") or "").strip(),
					"drift_reason": str(semanticist_metadata.get("drift_reason") or "").strip(),
				}
			)
		return drift_audit

	def _render_report(
		self,
		day_one_brief: dict[str, Any],
		heatmap: dict[str, dict[str, Any]],
		drift_audit: list[dict[str, str]],
	) -> str:
		brief_lines = [
			"# Codebase Cartography",
			"",
			"## Day-One Brief",
			"",
			f"**Primary Business Mission**: {day_one_brief.get('primary_business_mission', 'N/A')}",
			"",
			"**Critical Path Clusters**:",
		]
		for cluster_name in day_one_brief.get("critical_path_clusters", []):
			brief_lines.append(f"- {cluster_name}")
		if not day_one_brief.get("critical_path_clusters"):
			brief_lines.append("- None identified")

		brief_lines.extend(["", "**Top Technical Risks**:"])
		for risk in day_one_brief.get("top_technical_risks", []):
			brief_lines.append(f"- {risk}")
		if not day_one_brief.get("top_technical_risks"):
			brief_lines.append("- None identified")

		brief_lines.extend(
			[
				"",
				f"**Mental Model**: {day_one_brief.get('mental_model', 'N/A')}",
				"",
				"## Domain Heatmap",
				"",
			]
		)

		if not heatmap:
			brief_lines.append("No domain clusters were available.")
		else:
			for cluster_name, cluster_data in heatmap.items():
				brief_lines.append(f"### {cluster_name}")
				brief_lines.append("")
				brief_lines.append(cluster_data["definition"])
				brief_lines.append("")
				modules = cluster_data.get("modules", [])
				if modules:
					for module_path in modules:
						brief_lines.append(f"- {module_path}")
				else:
					brief_lines.append("- No modules assigned")
				brief_lines.append("")

		brief_lines.extend(["## Drift Audit", ""])
		if not drift_audit:
			brief_lines.append("No documentation drift was identified.")
		else:
			for item in drift_audit:
				brief_lines.append(f"- {item['path']}: {item['drift_reason']}")
				if item["purpose_statement"]:
					brief_lines.append(f"  Purpose: {item['purpose_statement']}")

		return "\n".join(brief_lines).rstrip() + "\n"

	def _render_codebase_report(
		self,
		*,
		surveyor_data: dict[str, Any],
		hydrologist_data: dict[str, Any],
		semanticist_data: dict[str, Any],
		nodes: list[ModuleNode],
		repo_root: Path | None,
	) -> str:
		day_one_brief = semanticist_data.get("day_one_brief", {})
		mental_model = str(day_one_brief.get("mental_model") or "").strip()
		architecture_overview = mental_model or "No data available."

		critical_path = self._top_pagerank_modules(surveyor_data, limit=5)
		boundary_nodes = self._extract_boundary_nodes(hydrologist_data)
		known_debt = self._build_known_debt(surveyor_data, semanticist_data, nodes)
		high_velocity_files = self._top_high_velocity_files(surveyor_data, limit=5)
		purpose_index = self._build_module_purpose_index(nodes)

		lines = [
			"# Codebase Context",
			"",
			"## Architecture Overview",
			"",
			architecture_overview,
			"",
			"## Critical Path",
			"",
		]

		if critical_path:
			for module_path, score in critical_path:
				lines.append(f"- {self._relative_or_original(module_path, repo_root)} ({score:.6f})")
		else:
			lines.append("No data available.")

		lines.extend(["", "## Data Sources & Sinks", ""])
		sources = boundary_nodes.get("ultimate_sources", [])
		sinks = boundary_nodes.get("ultimate_sinks", [])
		if not sources and not sinks:
			lines.append("No data available.")
		else:
			lines.append("**Sources**")
			if sources:
				for source in sources:
					lines.append(f"- {self._relative_or_original(source, repo_root)}")
			else:
				lines.append("- No data available.")
			lines.append("")
			lines.append("**Sinks**")
			if sinks:
				for sink in sinks:
					lines.append(f"- {self._relative_or_original(sink, repo_root)}")
			else:
				lines.append("- No data available.")

		lines.extend(["", "## Known Debt", ""])
		if known_debt:
			for debt_item in known_debt:
				lines.append(f"- {self._normalize_path_text(debt_item, repo_root)}")
		else:
			lines.append("No data available.")

		lines.extend(["", "## High-Velocity Files", ""])
		if high_velocity_files:
			for file_path, velocity in high_velocity_files:
				lines.append(f"- {self._relative_or_original(file_path, repo_root)} ({velocity} changes)")
		else:
			lines.append("No data available.")

		lines.extend(["", "## Module Purpose Index", "", "| Module | Purpose |", "| --- | --- |"])
		if purpose_index:
			for module_path, purpose in purpose_index:
				lines.append(f"| {self._relative_or_original(module_path, repo_root)} | {purpose} |")
		else:
			lines.append("| No data available | No data available |")

		return "\n".join(lines).rstrip() + "\n"

	def _top_pagerank_modules(self, surveyor_data: dict[str, Any], limit: int) -> list[tuple[str, float]]:
		pagerank = surveyor_data.get("pagerank", {})
		if not isinstance(pagerank, dict):
			return []
		ranked = sorted(
			((str(path), float(score)) for path, score in pagerank.items()),
			key=lambda item: item[1],
			reverse=True,
		)
		return ranked[:limit]

	def _extract_boundary_nodes(self, hydrologist_data: dict[str, Any]) -> dict[str, list[str]]:
		for key in ("boundaries", "boundary_nodes", "system_boundary_nodes"):
			value = hydrologist_data.get(key)
			if isinstance(value, dict):
				return {
					"ultimate_sources": list(value.get("ultimate_sources", []) or []),
					"ultimate_sinks": list(value.get("ultimate_sinks", []) or []),
				}
		return {
			"ultimate_sources": list(hydrologist_data.get("ultimate_sources", []) or []),
			"ultimate_sinks": list(hydrologist_data.get("ultimate_sinks", []) or []),
		}

	def _build_known_debt(
		self,
		surveyor_data: dict[str, Any],
		semanticist_data: dict[str, Any],
		nodes: list[ModuleNode],
	) -> list[str]:
		debt_items: list[str] = []

		circular_dependencies = surveyor_data.get("circular_dependencies", [])
		if isinstance(circular_dependencies, list):
			for component in circular_dependencies:
				if isinstance(component, list) and component:
					debt_items.append(f"Circular dependency: {' -> '.join(str(item) for item in component)}")

		drift_audit = semanticist_data.get("drift_audit")
		if not isinstance(drift_audit, list):
			drift_audit = self._build_drift_audit(nodes)
		for item in drift_audit:
			if not isinstance(item, dict):
				continue
			path = str(item.get("path") or "unknown")
			reason = str(item.get("drift_reason") or "Documentation drift detected.")
			debt_items.append(f"Documentation drift: {path} - {reason}")

		return debt_items

	def _top_high_velocity_files(self, surveyor_data: dict[str, Any], limit: int) -> list[tuple[str, int]]:
		git_velocity = surveyor_data.get("git_velocity", {})
		if not isinstance(git_velocity, dict):
			return []
		ranked = sorted(
			((str(path), int(count)) for path, count in git_velocity.items()),
			key=lambda item: item[1],
			reverse=True,
		)
		return ranked[:limit]

	def _build_module_purpose_index(self, nodes: list[ModuleNode]) -> list[tuple[str, str]]:
		rows: list[tuple[str, str]] = []
		for module_node in nodes:
			semanticist_metadata = module_node.metadata.get("semanticist", {})
			purpose_statement = str(semanticist_metadata.get("purpose_statement") or "").strip()
			if not purpose_statement:
				purpose_statement = "No data available."
			rows.append((module_node.path, purpose_statement.replace("|", "\\|")))
		return rows

	def _resolve_repo_root_for_paths(self, surveyor_data: dict[str, Any], nodes: list[ModuleNode]) -> Path | None:
		candidate = surveyor_data.get("analyzed_repo_path") if isinstance(surveyor_data, dict) else None
		if isinstance(candidate, str) and candidate.strip():
			path = Path(candidate)
			if path.exists():
				return path.resolve()

		module_paths: list[str] = []
		for module_node in nodes:
			if not isinstance(module_node.path, str) or not module_node.path.strip():
				continue
			module_paths.append(module_node.path)

		if module_paths:
			try:
				common_root = os.path.commonpath(module_paths)
				if common_root:
					return Path(common_root).resolve()
			except Exception:
				pass

		for module_path in module_paths:
			try:
				return Path(module_path).resolve().parent
			except Exception:
				continue
		return None

	def _relative_or_original(self, value: str, repo_root: Path | None) -> str:
		if repo_root is None:
			return str(value).replace("\\", "/")
		return self._normalize_path_text(str(value), repo_root)

	def _normalize_path_text(self, text: str, repo_root: Path | None) -> str:
		if repo_root is None:
			return text.replace("\\", "/")

		normalized = text.replace("\\", "/")
		root = repo_root.resolve().as_posix().rstrip("/")
		if root:
			normalized = re.sub(re.escape(root) + r"/", "", normalized, flags=re.IGNORECASE)

		path_token_pattern = re.compile(r"([A-Za-z]:/[A-Za-z0-9_./ -]+|/[A-Za-z0-9_./ -]+)")

		def _replace(match: re.Match[str]) -> str:
			token = match.group(1)
			try:
				return self._to_workspace_relative(token, repo_root)
			except Exception:
				return token

		return path_token_pattern.sub(_replace, normalized)

	def _build_executive_summary(
		self,
		*,
		repo_root: Path,
		surveyor_data: dict[str, Any],
		nodes: list[ModuleNode],
		architecture_overview: str,
	) -> str:
		repo_name = repo_root.name or "the analyzed repository"
		module_count = int(surveyor_data.get("module_count", len(nodes))) if surveyor_data else len(nodes)
		critical_modules = [self._to_workspace_relative(path, repo_root) for path, _ in self._top_pagerank_modules(surveyor_data, limit=3)]
		critical_modules_text = ", ".join(critical_modules) if critical_modules else "its core modules"
		overview = architecture_overview.strip() or "The repository structure highlights its primary architectural boundaries and hotspots."
		return (
			f"{repo_name} is the analyzed codebase, with {module_count} source files contributing to its current architecture map. "
			f"{overview} "
			f"The most central modules in this run are {critical_modules_text}, which are strong starting points for new engineers."
		)

	def _build_entry_points(self, nodes: list[ModuleNode], repo_root: Path) -> list[str]:
		entry_points: set[str] = set()

		for module_node in nodes:
			path = module_node.path.replace("\\", "/").lower()
			if path.endswith("/src/cli.py") or path.endswith("/cli.py"):
				entry_points.add(self._to_workspace_relative(module_node.path, repo_root))
			if path.endswith("/__main__.py"):
				entry_points.add(self._to_workspace_relative(module_node.path, repo_root))
			if path.endswith("/setup.py"):
				entry_points.add(self._to_workspace_relative(module_node.path, repo_root))
			if path.endswith("/manage.py"):
				entry_points.add(self._to_workspace_relative(module_node.path, repo_root))

		for candidate in ("src/orchestrator.py", "src/cli.py", "cli.py", "setup.py", "pyproject.toml"):
			if (repo_root / candidate).exists():
				entry_points.add(candidate)

		ordered = sorted(entry_points)
		return ordered

	def _load_tech_stack(self, repo_root: Path) -> list[str]:
		libraries: list[str] = []
		for requirements_path in sorted(repo_root.glob("requirements*.txt")):
			for raw_line in requirements_path.read_text(encoding="utf-8").splitlines():
				line = raw_line.strip()
				if not line or line.startswith("#"):
					continue
				base = line.split(";", 1)[0].strip()
				base = base.split("==", 1)[0].strip()
				base = base.split(">=", 1)[0].strip()
				base = base.split("<=", 1)[0].strip()
				if base:
					libraries.append(base)

		pyproject_path = repo_root / "pyproject.toml"
		if pyproject_path.exists():
			for line in pyproject_path.read_text(encoding="utf-8").splitlines():
				match = re.search(r'"([A-Za-z0-9_.-]+)(?:[<>=~!].*)?"', line)
				if match:
					libraries.append(match.group(1))

		setup_path = repo_root / "setup.py"
		if setup_path.exists():
			for match in re.finditer(r'"([A-Za-z0-9_.-]+)(?:[<>=~!].*)?"|\'([A-Za-z0-9_.-]+)(?:[<>=~!].*)?\'', setup_path.read_text(encoding="utf-8", errors="replace")):
				library = match.group(1) or match.group(2)
				if library and library.lower() not in {"python", "requests"}:
					libraries.append(library)

		seen: set[str] = set()
		deduped: list[str] = []
		for library in libraries:
			key = library.lower()
			if key in seen:
				continue
			seen.add(key)
			deduped.append(library)
		return deduped

	def _load_architecture_overview_from_analysis_dir(self, analysis_dir: Path) -> str:
		codebase_path = analysis_dir / "CODEBASE.md"
		if not codebase_path.exists():
			return ""
		text = codebase_path.read_text(encoding="utf-8", errors="replace")
		lines = text.splitlines()
		capture = False
		collected: list[str] = []
		for line in lines:
			if line.strip() == "## Architecture Overview":
				capture = True
				continue
			if capture and line.startswith("## "):
				break
			if capture and line.strip():
				collected.append(line.strip())
		return " ".join(collected).strip()

	def _to_workspace_relative(self, path: str, repo_root: Path) -> str:
		try:
			resolved = Path(path).resolve()
			relative = resolved.relative_to(repo_root.resolve())
			return relative.as_posix()
		except Exception:
			normalized = path.replace("\\", "/")
			repo_token = repo_root.resolve().as_posix().lower().rstrip("/")
			normalized_lower = normalized.lower()
			if normalized_lower.startswith(f"{repo_token}/"):
				return normalized[len(repo_token) + 1 :]
			return normalized
