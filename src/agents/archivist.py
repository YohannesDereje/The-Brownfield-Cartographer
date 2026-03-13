from __future__ import annotations

from pathlib import Path
from typing import Any

from src.models.nodes import ModuleNode


class Archivist:
	"""Compiles cartography outputs into a readable codebase report."""

	def __init__(self, output_dir: str | Path = ".cartography") -> None:
		self.output_dir = Path(output_dir)
		self.output_dir.mkdir(parents=True, exist_ok=True)

	def write_cartography_report(
		self,
		day_one_brief: dict[str, Any],
		clusters: dict[str, str],
		nodes: list[ModuleNode],
		output_path: str | Path | None = None,
	) -> Path:
		"""Write the final cartography report to .cartography/CODEBASE.md."""
		target_path = Path(output_path) if output_path is not None else self.output_dir / "CODEBASE.md"
		target_path.parent.mkdir(parents=True, exist_ok=True)

		heatmap = self._build_domain_heatmap(clusters, nodes)
		drift_audit = self._build_drift_audit(nodes)
		report = self._render_report(day_one_brief, heatmap, drift_audit)
		target_path.write_text(report, encoding="utf-8")
		return target_path

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
