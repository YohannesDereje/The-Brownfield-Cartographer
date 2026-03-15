from __future__ import annotations

from difflib import SequenceMatcher
from dataclasses import dataclass
import os
from pathlib import Path
import json
import re
from typing import Any, Literal, TypedDict

import networkx as nx
from langgraph.graph import END, StateGraph

try:
	from openai import OpenAI
except Exception:  # pragma: no cover - optional dependency
	OpenAI = None


class NavigatorState(TypedDict, total=False):
	query: str
	system_prompt: str
	selected_tool: str
	tool_input: str
	observation: dict[str, Any]
	final_response: str


@dataclass
class ToolObservation:
	tool_name: str
	summary: str
	claims: list[str]
	citations: list[str]
	raw: dict[str, Any]

	def to_state_payload(self) -> dict[str, Any]:
		return {
			"tool_name": self.tool_name,
			"summary": self.summary,
			"claims": self.claims,
			"citations": self.citations,
			"raw": self.raw,
		}


class Navigator:
	"""Artifact-backed interactive GPS for the codebase using a LangGraph ReAct loop."""

	SYSTEM_PROMPT = (
		"You are Navigator, a codebase GPS for engineers. Every claim must cite evidence. "
		"Use citations in square brackets with file-or-artifact evidence such as [src/module.py:1] or "
		"[.cartography/<repo>/lineage_graph.json:1]. If evidence is incomplete, say so explicitly and cite the artifact you inspected."
	)
	PLANNER_MODEL = "qwen/qwen3.5-9b"

	def __init__(
		self,
		cartography_dir: str | Path = ".cartography",
		*,
		repo_name: str | None = None,
		artifact_path: str | Path | None = None,
	) -> None:
		self.cartography_root = Path(cartography_dir)
		if artifact_path is not None:
			resolved_artifact_dir = Path(artifact_path)
		elif repo_name:
			resolved_artifact_dir = self.cartography_root / repo_name
		else:
			resolved_artifact_dir = self.cartography_root

		self.artifact_dir = resolved_artifact_dir
		self.cartography_dir = self.artifact_dir
		self.workspace_root = self.cartography_root.resolve().parent
		self.codebase_path = self.artifact_dir / "CODEBASE.md"
		self.analysis_summary_path = self.artifact_dir / "analysis_summary.json"
		self.module_graph_path = self.artifact_dir / "module_graph.json"
		self.lineage_graph_path = self.artifact_dir / "lineage_graph.json"
		self.trace_log_path = self.artifact_dir / "cartography_trace.jsonl"
		self.onboarding_brief_path = self.artifact_dir / "onboarding_brief.md"
		self.artifact_rel_prefix = self._resolve_artifact_rel_prefix()
		self.openrouter_base_url = "https://openrouter.ai/api/v1"
		self.openrouter_referer = os.getenv("OPENROUTER_HTTP_REFERER", "https://example.com")
		self.openrouter_title = os.getenv("OPENROUTER_APP_TITLE", "The Brownfield Cartographer")
		self.planner_model = os.getenv("NAVIGATOR_PLANNER_MODEL", self.PLANNER_MODEL)
		self.openrouter_key = self._load_openrouter_api_key()
		self._openai_client = None
		if OpenAI is not None and self.openrouter_key:
			try:
				self._openai_client = OpenAI(
					api_key=self.openrouter_key,
					base_url=self.openrouter_base_url,
					default_headers={
						"HTTP-Referer": self.openrouter_referer,
						"X-Title": self.openrouter_title,
					},
				)
			except Exception:
				self._openai_client = None
		self._cache: dict[str, Any] = {}
		self.tools: dict[str, Any] = {
			"find_implementation": self.find_implementation,
			"trace_lineage": self.trace_lineage,
			"blast_radius": self.blast_radius,
			"explain_module": self.explain_module,
			"module_overview": self.module_overview,
		}
		self.graph = self._build_graph()

	def answer(self, query: str) -> str:
		state: NavigatorState = {
			"query": query,
			"system_prompt": self.SYSTEM_PROMPT,
		}
		result = self.graph.invoke(state)
		return result.get("final_response", "No response generated.")

	def find_implementation(self, query: str) -> ToolObservation:
		purpose_index = self._load_module_purpose_index()
		query_tokens = self._tokenize(query)
		ranked: list[tuple[float, str, str, str]] = []
		for module_path, purpose, citation in purpose_index:
			tokens = self._tokenize(f"{module_path} {purpose}")
			overlap = len(query_tokens & tokens)
			contains = 1 if query.lower() in f"{module_path} {purpose}".lower() else 0
			score = overlap + contains
			if score > 0:
				ranked.append((float(score), module_path, purpose, citation))

		ranked.sort(key=lambda item: item[0], reverse=True)
		top_matches = ranked[:3]
		if not top_matches:
			return ToolObservation(
				tool_name="find_implementation",
				summary="No semantically similar implementation was found in the Module Purpose Index.",
				claims=[
					f"No matching module purpose was found after scanning the Module Purpose Index [{self._artifact_citation('CODEBASE.md:1')}]."
				],
				citations=[self._artifact_citation("CODEBASE.md:1")],
				raw={"matches": []},
			)

		claims = [
			f"{module_path} appears relevant because its purpose is '{purpose}' [{citation}]."
			for _, module_path, purpose, citation in top_matches
		]
		return ToolObservation(
			tool_name="find_implementation",
			summary="Found the closest implementation matches from the Module Purpose Index.",
			claims=claims,
			citations=[citation for _, _, _, citation in top_matches],
			raw={
				"matches": [
					{"module_path": module_path, "purpose": purpose, "citation": citation}
					for _, module_path, purpose, citation in top_matches
				]
			},
		)

	def trace_lineage(self, data_element: str) -> ToolObservation:
		graph = self._load_lineage_graph()
		resolved_node, suggestions = self._resolve_lineage_node(graph, data_element)
		if not resolved_node:
			suggestion_text = self._format_list(suggestions) if suggestions else "no close candidates"
			return ToolObservation(
				tool_name="trace_lineage",
				summary="No exact lineage node matched the requested data element. Suggested closest candidates are included.",
				claims=[
					f"No exact lineage evidence matched '{data_element}' after scanning [{self._artifact_citation('lineage_graph.json:1')}].",
					f"Closest lineage candidates: {suggestion_text} [{self._artifact_citation('lineage_graph.json:1')}].",
				],
				citations=[self._artifact_citation("lineage_graph.json:1")],
				raw={"matches": [], "suggestions": suggestions},
			)

		node_id = resolved_node
		upstream = sorted(self._display_lineage_label(graph, ancestor) for ancestor in nx.ancestors(graph, node_id))
		downstream = sorted(self._display_lineage_label(graph, descendant) for descendant in nx.descendants(graph, node_id))
		evidence = self._best_trace_citation(data_element) or self._artifact_citation("lineage_graph.json:1")
		claims = [
			f"Based on static lineage analysis, '{self._display_lineage_label(graph, node_id)}' has upstream flow from {self._format_list(upstream)} [{evidence}].",
			f"Based on static lineage analysis, '{self._display_lineage_label(graph, node_id)}' flows downstream to {self._format_list(downstream)} [{evidence}].",
		]
		return ToolObservation(
			tool_name="trace_lineage",
			summary="Traced upstream and downstream lineage for the requested data element.",
			claims=claims,
			citations=[evidence],
			raw={"node": node_id, "upstream": upstream, "downstream": downstream},
		)

	def blast_radius(self, module_path: str) -> ToolObservation:
		module_graph = self._load_module_graph()
		lineage_graph = self._load_lineage_graph()
		analysis_summary = self._load_analysis_summary()
		matched_node, suggestions = self._resolve_module_node(module_path, module_graph)
		lineage_node, lineage_suggestions = self._resolve_lineage_node(lineage_graph, module_path)
		if matched_node is None and lineage_node is None:
			suggestion_text = self._format_list(suggestions) if suggestions else "no close candidates"
			if lineage_suggestions:
				suggestion_text = self._format_list(list(dict.fromkeys(suggestions + lineage_suggestions)))
			return ToolObservation(
				tool_name="blast_radius",
				summary="No exact structural graph module matched the request. Suggested closest module nodes are included.",
				claims=[
					f"No exact structural graph module matched '{module_path}' after scanning [{self._artifact_citation('module_graph.json:1')}].",
					f"Closest module candidates: {suggestion_text} [{self._artifact_citation('module_graph.json:1')}].",
				],
				citations=[self._artifact_citation("module_graph.json:1")],
				raw={"query": module_path, "suggestions": list(dict.fromkeys(suggestions + lineage_suggestions))},
			)

		claims: list[str] = []
		citations: list[str] = []
		raw: dict[str, Any] = {"query": module_path}
		structural_in_edges: list[str] = []

		if matched_node is not None:
			node_attrs = module_graph.nodes.get(matched_node, {})
			pagerank = float(node_attrs.get("pagerank_score", analysis_summary.get("pagerank", {}).get(matched_node, 0.0)))
			out_edges = sorted(str(target) for _, target in module_graph.out_edges(matched_node))
			in_edges = sorted(str(source) for source, _ in module_graph.in_edges(matched_node))
			structural_in_edges = in_edges
			citation = f"{matched_node}:1"
			claims.extend(
				[
					f"The module has a PageRank score of {pagerank:.6f} based on the structural graph [{citation}].",
					f"Its structural out-edges point to {self._format_list(out_edges)} [{citation}].",
				]
			)
			citations.append(citation)
			raw.update({"module_path": matched_node, "pagerank": pagerank, "in_edges": in_edges, "out_edges": out_edges})

		if lineage_node is not None:
			lineage_downstream = sorted(
				self._display_lineage_label(lineage_graph, descendant)
				for descendant in nx.descendants(lineage_graph, lineage_node)
			)
			lineage_upstream = sorted(
				self._display_lineage_label(lineage_graph, ancestor)
				for ancestor in nx.ancestors(lineage_graph, lineage_node)
			)
			lineage_citation = self._artifact_citation("lineage_graph.json:1")
			claims.extend(
				[
					f"Affected Components (lineage downstream): {self._format_list(lineage_downstream)} [{lineage_citation}].",
					f"Upstream dependencies feeding this node: {self._format_list(lineage_upstream)} [{lineage_citation}].",
				]
			)
			citations.append(lineage_citation)
			raw.update({"lineage_node": lineage_node, "lineage_downstream": lineage_downstream, "lineage_upstream": lineage_upstream})

		if structural_in_edges:
			if lineage_node is None:
				claims.append(
					f"Affected Components (structural dependents): {self._format_list(structural_in_edges)} [{citations[0]}]."
				)
			else:
				claims.append(
					f"Structural dependents (import graph): {self._format_list(structural_in_edges)} [{citations[0]}]."
				)

		return ToolObservation(
			tool_name="blast_radius",
			summary="Computed blast radius from structural graph evidence and lineage downstream dependencies.",
			claims=claims,
			citations=sorted(set(citations)),
			raw=raw,
		)

	def explain_module(self, module_path: str) -> ToolObservation:
		module_graph = self._load_module_graph()
		matched_node = self._find_module_node(module_path, module_graph)
		if matched_node is None:
			return self._codebase_fallback_observation(
				tool_name="explain_module",
				query=module_path,
				prefix="No exact module matched for purpose lookup.",
			)

		node_attrs = module_graph.nodes.get(matched_node, {})
		semanticist_data = (node_attrs.get("metadata", {}) or {}).get("semanticist", {})
		resolved_path = str(node_attrs.get("path") or matched_node)
		cluster, cluster_citation = self._find_cluster_for_module(resolved_path)
		citation = f"{matched_node}:1"

		purpose_statement = str(semanticist_data.get("purpose_statement", "")).strip() or "No purpose statement available."
		drift_reason = str(semanticist_data.get("drift_reason", "")).strip()
		documentation_drift = bool(semanticist_data.get("documentation_drift", False))
		domain_cluster = str(semanticist_data.get("domain_cluster") or cluster or "Unknown Cluster")
		domain_guess = ""
		if domain_cluster == "Unknown Cluster":
			domain_guess = self._guess_domain_from_path(resolved_path)

		claims = [
			f"{resolved_path} has purpose: {purpose_statement} [{citation}].",
			f"{resolved_path} is assigned to cluster '{domain_cluster}' [{cluster_citation}].",
		]
		if domain_guess:
			claims.append(f"No explicit cluster assignment was found; likely domain is '{domain_guess}' based on file path [{citation}].")
		if documentation_drift and drift_reason:
			claims.append(f"{resolved_path} has documentation drift: {drift_reason} [{citation}].")
		else:
			claims.append(f"No documentation drift reason is recorded for {resolved_path} [{citation}].")

		return ToolObservation(
			tool_name="explain_module",
			summary="Retrieved module purpose and semantic metadata from cartography artifacts.",
			claims=claims,
			citations=[citation, cluster_citation],
			raw={
				"module_path": resolved_path,
				"purpose": purpose_statement,
				"cluster": domain_cluster,
				"drift_reason": drift_reason,
			},
		)

	def module_overview(self, module_path: str) -> ToolObservation:
		explanation = self.explain_module(module_path)
		blast = self.blast_radius(module_path)
		claims = list(dict.fromkeys(explanation.claims + blast.claims))
		citations = sorted(set(explanation.citations + blast.citations))
		return ToolObservation(
			tool_name="module_overview",
			summary="Combined module purpose analysis and blast-radius assessment.",
			claims=claims,
			citations=citations,
			raw={"explain_module": explanation.raw, "blast_radius": blast.raw},
		)

	def _build_graph(self):
		graph = StateGraph(NavigatorState)
		graph.add_node("choose_tool", self._choose_tool_node)
		graph.add_node("execute_tool", self._execute_tool_node)
		graph.add_node("finalize", self._finalize_node)
		graph.set_entry_point("choose_tool")
		graph.add_edge("choose_tool", "execute_tool")
		graph.add_edge("execute_tool", "finalize")
		graph.add_edge("finalize", END)
		return graph.compile()

	def _choose_tool_node(self, state: NavigatorState) -> NavigatorState:
		query = state.get("query", "")
		planned_tool, planned_input = self._plan_tool_with_qwen(query)
		if planned_tool in self.tools and planned_input.strip():
			return {"selected_tool": planned_tool, "tool_input": planned_input.strip()}

		intents = self._infer_intents(query)
		if intents["lineage"]:
			selected_tool = "trace_lineage"
		elif intents["blast"] and intents["purpose"]:
			selected_tool = "module_overview"
		elif intents["blast"]:
			selected_tool = "blast_radius"
		elif intents["purpose"]:
			selected_tool = "explain_module"
		else:
			selected_tool = "find_implementation"
		return {"selected_tool": selected_tool, "tool_input": self._extract_tool_input(selected_tool, query)}

	def _plan_tool_with_qwen(self, query: str) -> tuple[str | None, str]:
		if not query.strip():
			return None, ""

		context = self._build_planner_context()
		prompt = (
			"You are a Navigator planner. Select exactly one tool and provide precise tool_input.\n"
			"Tools:\n"
			"- find_implementation: discover likely implementation files for business logic\n"
			"- trace_lineage: trace upstream/downstream data lineage for a model or dataset\n"
			"- blast_radius: estimate impact dependencies for a module/file/model\n"
			"- explain_module: explain one module purpose and semantic metadata\n"
			"- module_overview: combine explain_module + blast_radius\n\n"
			"Rules:\n"
			"1) Prefer trace_lineage for lineage/upstream/downstream/flow questions.\n"
			"2) Prefer blast_radius for impact/dependencies/radius questions.\n"
			"3) If user asks both purpose and impact, use module_overview.\n"
			"4) tool_input must be the best concrete node/file/model key from available nodes and paths.\n"
			"5) Return JSON only with keys: selected_tool, tool_input.\n\n"
			f"Question: {query}\n\n"
			"Context:\n"
			f"{context}"
		)

		response_text = self._call_planner_model(prompt)
		if not response_text:
			return None, ""

		payload = self._parse_planner_json(response_text)
		selected_tool = str(payload.get("selected_tool", "")).strip()
		tool_input = str(payload.get("tool_input", "")).strip() or query.strip()
		if selected_tool not in self.tools:
			return None, ""
		return selected_tool, tool_input

	def _execute_tool_node(self, state: NavigatorState) -> NavigatorState:
		tool_name = state.get("selected_tool", "find_implementation")
		tool_input = state.get("tool_input", state.get("query", ""))
		observation = self.tools[tool_name](tool_input)
		return {"observation": observation.to_state_payload()}

	def _finalize_node(self, state: NavigatorState) -> NavigatorState:
		observation = state.get("observation", {})
		claims = [
			self._normalize_text_paths(self._normalize_text_citations(str(claim)))
			for claim in observation.get("claims", [])
		]
		summary = self._normalize_text_paths(
			self._normalize_text_citations(str(observation.get("summary", "No summary available.")))
		)
		response_lines = [summary, ""]
		response_lines.extend(f"- {claim}" for claim in claims)
		return {"final_response": "\n".join(response_lines).strip()}

	def _extract_tool_input(self, tool_name: str, query: str) -> str:
		cleaned = query.strip()
		if tool_name in {"explain_module", "blast_radius", "module_overview"}:
			file_candidate = self._extract_file_candidate(cleaned)
			return file_candidate or cleaned
		if tool_name == "trace_lineage":
			lowered = cleaned.lower()
			for marker in ("for ", "of ", "trace "):
				index = lowered.rfind(marker)
				if index >= 0:
					candidate = cleaned[index + len(marker) :].strip(" ?.")
					if candidate:
						return candidate
		return query.strip()

	def _infer_intents(self, query: str) -> dict[str, bool]:
		lowered = query.lower()
		return {
			"lineage": any(token in lowered for token in ("lineage", "upstream", "downstream", "flow", "source", "sink")),
			"blast": any(
				token in lowered
				for token in (
					"blast",
					"impact",
					"dependent",
					"radius",
					"pagerank",
					"out edge",
					"out-edge",
					"out_edges",
					"delete",
					"deletion",
					"remove",
					"removed",
				)
			),
			"purpose": any(token in lowered for token in ("explain", "purpose", "what does", "why does", "module")),
		}

	def _extract_file_candidate(self, query: str) -> str | None:
		normalized = query.strip().strip("'\"")
		path_like_match = re.search(r"([A-Za-z0-9_./\\-]+\.[A-Za-z0-9_]+)", normalized)
		if path_like_match:
			return path_like_match.group(1).strip(" ,;:.?!")

		words = re.findall(r"[A-Za-z0-9_./\\-]+", normalized)
		for word in words:
			clean_word = word.strip(" ,;:.?!").lower()
			if clean_word in {
				"explain",
				"purpose",
				"blast",
				"radius",
				"impact",
				"module",
				"its",
				"and",
				"show",
				"me",
				"the",
				"of",
				"for",
				"what",
				"happens",
				"if",
				"i",
				"delete",
				"deletion",
				"remove",
				"removed",
			}:
				continue
			if "/" in clean_word or "\\" in clean_word:
				return clean_word
			if clean_word.endswith((".py", ".sql", ".yaml", ".yml", ".md", ".json")):
				return clean_word
			if clean_word.isidentifier() and len(clean_word) > 2:
				return clean_word
		return None

	def _load_analysis_summary(self) -> dict[str, Any]:
		return self._load_json_file("analysis_summary", self.analysis_summary_path)

	def _load_module_graph(self) -> nx.DiGraph:
		if "module_graph" not in self._cache:
			data = self._load_json_file("module_graph_raw", self.module_graph_path)
			self._cache["module_graph"] = nx.node_link_graph(data) if data else nx.DiGraph()
		return self._cache["module_graph"]

	def _load_lineage_graph(self) -> nx.DiGraph:
		if "lineage_graph" not in self._cache:
			data = self._load_json_file("lineage_graph_raw", self.lineage_graph_path)
			self._cache["lineage_graph"] = nx.node_link_graph(data) if data else nx.DiGraph()
		return self._cache["lineage_graph"]

	def _load_trace_records(self) -> list[dict[str, Any]]:
		if "trace_records" in self._cache:
			return self._cache["trace_records"]
		if not self.trace_log_path.exists():
			self._cache["trace_records"] = []
			return []
		records: list[dict[str, Any]] = []
		for line in self.trace_log_path.read_text(encoding="utf-8").splitlines():
			if not line.strip():
				continue
			try:
				records.append(json.loads(line))
			except json.JSONDecodeError:
				continue
		self._cache["trace_records"] = records
		return records

	def _load_codebase_text(self) -> str:
		if "codebase_text" in self._cache:
			return self._cache["codebase_text"]
		text = self.codebase_path.read_text(encoding="utf-8") if self.codebase_path.exists() else ""
		self._cache["codebase_text"] = text
		return text

	def _load_onboarding_summary(self, max_lines: int = 40) -> str:
		if "onboarding_summary" in self._cache:
			return self._cache["onboarding_summary"]

		if not self.onboarding_brief_path.exists():
			summary = "No onboarding brief available."
			self._cache["onboarding_summary"] = summary
			return summary

		lines = [line.strip() for line in self.onboarding_brief_path.read_text(encoding="utf-8").splitlines() if line.strip()]
		summary = "\n".join(lines[:max_lines]) if lines else "No onboarding brief available."
		self._cache["onboarding_summary"] = summary
		return summary

	def _get_module_node_paths(self) -> list[str]:
		if "module_node_paths" in self._cache:
			return self._cache["module_node_paths"]

		graph = self._load_module_graph()
		paths: list[str] = []
		for node_id, attrs in graph.nodes(data=True):
			value = str(attrs.get("path") or node_id).replace("\\", "/")
			if value:
				paths.append(value)
		paths = sorted(dict.fromkeys(paths))
		self._cache["module_node_paths"] = paths
		return paths

	def _get_lineage_node_paths(self) -> list[str]:
		if "lineage_node_paths" in self._cache:
			return self._cache["lineage_node_paths"]

		graph = self._load_lineage_graph()
		paths: list[str] = []
		for node_id, attrs in graph.nodes(data=True):
			for value in (
				str(node_id),
				str(attrs.get("path", "")),
				str(attrs.get("raw_uri", "")),
				str(attrs.get("canonical_uri", "")),
			):
				normalized = value.replace("\\", "/").strip()
				if normalized:
					paths.append(normalized)
		paths = sorted(dict.fromkeys(paths))
		self._cache["lineage_node_paths"] = paths
		return paths

	def _build_planner_context(self) -> str:
		onboarding = self._load_onboarding_summary()
		module_nodes = self._get_module_node_paths()
		lineage_nodes = self._get_lineage_node_paths()

		module_block = "\n".join(f"- {item}" for item in module_nodes) or "- (none)"
		lineage_block = "\n".join(f"- {item}" for item in lineage_nodes) or "- (none)"

		return (
			"Onboarding Brief Summary:\n"
			f"{onboarding}\n\n"
			"Available ModuleNode paths (module_graph):\n"
			f"{module_block}\n\n"
			"Available lineage nodes (lineage_graph):\n"
			f"{lineage_block}"
		)

	def _load_module_purpose_index(self) -> list[tuple[str, str, str]]:
		if "module_purpose_index" in self._cache:
			return self._cache["module_purpose_index"]
		results: list[tuple[str, str, str]] = []
		text = self._load_codebase_text()
		in_table = False
		for line in text.splitlines():
			if line.strip() == "## Module Purpose Index":
				in_table = True
				continue
			if in_table and line.startswith("## "):
				break
			if in_table and line.startswith("| ") and not line.startswith("| ---"):
				parts = [part.strip() for part in line.strip("|").split("|")]
				if len(parts) >= 2 and parts[0] != "Module":
					results.append((parts[0], parts[1], f"{parts[0]}:1"))

		if not results:
			for record in self._load_trace_records():
				if record.get("agent_name") != "semanticist" or record.get("action_type") != "purpose_statement_generated":
					continue
				results.append((
					record.get("evidence_source", "unknown:1").rsplit(":", 1)[0],
					str(record.get("purpose_statement", "")).strip(),
					str(record.get("evidence_source", "unknown:1")),
				))

		self._cache["module_purpose_index"] = results
		return results

	def _find_lineage_nodes(self, graph: nx.DiGraph, data_element: str) -> list[str]:
		needle = data_element.lower()
		matches: list[str] = []
		for node_id, attrs in graph.nodes(data=True):
			candidates = [
				str(node_id),
				str(attrs.get("path", "")),
				str(attrs.get("raw_uri", "")),
				str(attrs.get("canonical_uri", "")),
			]
			if any(needle in candidate.lower() for candidate in candidates if candidate):
				matches.append(str(node_id))
		return matches

	def _resolve_lineage_node(self, graph: nx.DiGraph, query: str) -> tuple[str | None, list[str]]:
		inventory = self._get_lineage_node_paths()
		resolved_hint = self._planner_resolve_node_hint(query, inventory, domain="lineage")
		search_term = resolved_hint or query

		candidate_nodes = self._find_lineage_nodes(graph, search_term)
		if candidate_nodes:
			preferred = sorted(
				candidate_nodes,
				key=lambda node_id: self._path_preference(self._display_lineage_label(graph, node_id)),
				reverse=True,
			)
			return preferred[0], self._closest_matches(search_term, inventory, top_n=3)

		suggestions = self._closest_matches(query, inventory, top_n=3)
		return None, suggestions

	def _display_lineage_label(self, graph: nx.DiGraph, node_id: str) -> str:
		attrs = graph.nodes.get(node_id, {})
		return str(attrs.get("path") or attrs.get("raw_uri") or attrs.get("canonical_uri") or node_id)

	def _best_trace_citation(self, text: str) -> str | None:
		needle = text.lower()
		for record in reversed(self._load_trace_records()):
			evidence_source = str(record.get("evidence_source", ""))
			payload = json.dumps(record, ensure_ascii=False).lower()
			if needle in payload:
				return evidence_source or None
		return None

	def _find_module_node(self, module_path: str, graph: nx.DiGraph) -> str | None:
		candidate = (self._extract_file_candidate(module_path) or module_path).replace("\\", "/").lower().strip()
		if not candidate:
			return None

		node_rows: list[tuple[str, str, str, str]] = []
		for node_id, attrs in graph.nodes(data=True):
			path = str(attrs.get("path") or node_id).replace("\\", "/")
			lower_path = path.lower()
			basename = lower_path.rsplit("/", 1)[-1]
			stem = basename.rsplit(".", 1)[0]
			node_rows.append((str(node_id), lower_path, basename, stem))

		exact_matches: list[str] = []
		for node_id, lower_path, basename, stem in node_rows:
			if candidate == lower_path or candidate == basename or candidate == stem:
				exact_matches.append(node_id)
		if exact_matches:
			return sorted(exact_matches, key=self._path_preference, reverse=True)[0]

		contains_matches: list[str] = []
		for node_id, lower_path, basename, stem in node_rows:
			if lower_path.endswith(candidate) or candidate in lower_path or candidate == stem or candidate == basename:
				contains_matches.append(node_id)
		if contains_matches:
			return sorted(contains_matches, key=self._path_preference, reverse=True)[0]

		best_match: tuple[float, str] | None = None
		for node_id, lower_path, basename, stem in node_rows:
			score = max(
				SequenceMatcher(a=candidate, b=lower_path).ratio(),
				SequenceMatcher(a=candidate, b=basename).ratio(),
				SequenceMatcher(a=candidate, b=stem).ratio(),
			)
			score += 0.01 * self._path_preference(node_id)
			if best_match is None or score > best_match[0]:
				best_match = (score, node_id)

		if best_match and best_match[0] >= 0.55:
			return best_match[1]
		return None

	def _resolve_module_node(self, module_path: str, graph: nx.DiGraph) -> tuple[str | None, list[str]]:
		module_nodes = self._get_module_node_paths()
		lineage_nodes = self._get_lineage_node_paths()
		global_inventory = sorted(dict.fromkeys(module_nodes + lineage_nodes))
		resolved_hint = self._planner_resolve_node_hint(module_path, global_inventory, domain="module")
		search_term = resolved_hint or module_path

		matched = self._find_module_node(search_term, graph)
		if matched:
			return matched, self._closest_matches(search_term, module_nodes, top_n=3)

		suggestions = self._closest_matches(module_path, module_nodes, top_n=3)
		return None, suggestions

	def _planner_resolve_node_hint(self, user_text: str, candidates: list[str], *, domain: Literal["module", "lineage"]) -> str | None:
		if not user_text.strip() or not candidates:
			return None

		prompt = (
			"Map the user text to exactly one best candidate from the available nodes.\n"
			f"Domain: {domain}\n"
			"If no candidate is reasonable, return no_match.\n"
			"Return JSON only with keys: match, confidence where confidence is 0..1.\n\n"
			f"User text: {user_text}\n\n"
			"Candidates:\n"
			+ "\n".join(f"- {value}" for value in candidates)
		)

		response_text = self._call_planner_model(prompt)
		if not response_text:
			return None

		payload = self._parse_planner_json(response_text)
		match_value = str(payload.get("match", "")).strip()
		if not match_value or match_value.lower() in {"no_match", "none", "null"}:
			return None
		if match_value in candidates:
			return match_value

		fuzzy = self._closest_matches(match_value, candidates, top_n=1)
		return fuzzy[0] if fuzzy else None

	def _closest_matches(self, query: str, candidates: list[str], top_n: int = 3) -> list[str]:
		needle = (self._extract_file_candidate(query) or query).replace("\\", "/").lower().strip()
		if not needle:
			return []

		ranked: list[tuple[float, str]] = []
		for candidate in candidates:
			normalized = candidate.replace("\\", "/").lower().strip()
			basename = normalized.rsplit("/", 1)[-1]
			stem = basename.rsplit(".", 1)[0]
			overlap = len(self._tokenize(needle) & self._tokenize(normalized))
			ratio = max(
				SequenceMatcher(a=needle, b=normalized).ratio(),
				SequenceMatcher(a=needle, b=basename).ratio(),
				SequenceMatcher(a=needle, b=stem).ratio(),
			)
			score = ratio + (0.05 * overlap) + (0.01 * self._path_preference(candidate))
			ranked.append((score, candidate))

		ranked.sort(key=lambda item: item[0], reverse=True)
		results: list[str] = []
		for _, candidate in ranked:
			if candidate not in results:
				results.append(candidate)
			if len(results) >= top_n:
				break
		return results

	def _call_planner_model(self, prompt: str) -> str | None:
		if self._openai_client is None:
			return None
		try:
			response = self._openai_client.chat.completions.create(
				model=self.planner_model,
				messages=[
					{"role": "system", "content": self.SYSTEM_PROMPT},
					{"role": "user", "content": prompt},
				],
			)
			return str(response.choices[0].message.content or "")
		except Exception:
			return None

	def _parse_planner_json(self, text: str) -> dict[str, Any]:
		cleaned = text.strip()
		if cleaned.startswith("```"):
			cleaned = re.sub(r"^```(?:json)?", "", cleaned, flags=re.IGNORECASE).strip()
			cleaned = re.sub(r"```$", "", cleaned).strip()

		try:
			parsed = json.loads(cleaned)
			return parsed if isinstance(parsed, dict) else {}
		except Exception:
			match = re.search(r"\{[\s\S]*\}", cleaned)
			if not match:
				return {}
			try:
				parsed = json.loads(match.group(0))
				return parsed if isinstance(parsed, dict) else {}
			except Exception:
				return {}

	def _load_openrouter_api_key(self, env_file_path: str | Path = ".env") -> str | None:
		env_key = os.getenv("OPENROUTER_API_KEY")
		if env_key:
			return env_key.strip()

		env_file = Path(env_file_path)
		if not env_file.exists():
			return None

		for raw_line in env_file.read_text(encoding="utf-8").splitlines():
			line = raw_line.strip()
			if not line or line.startswith("#") or "=" not in line:
				continue
			key, value = line.split("=", 1)
			if key.strip() == "OPENROUTER_API_KEY":
				return value.strip().strip('"').strip("'")
		return None

	def _codebase_fallback_observation(self, *, tool_name: str, query: str, prefix: str) -> ToolObservation:
		matches = self._search_codebase_lines(query)
		if matches:
			claims = [
				f"General codebase evidence: {line} [{self._artifact_citation('CODEBASE.md:1')}]."
				for line in matches
			]
			summary = f"{prefix} Returned closest CODEBASE context instead."
		else:
			claims = [
				f"No direct module match and no close CODEBASE evidence for '{query}' after scanning [{self._artifact_citation('CODEBASE.md:1')}]."
			]
			summary = f"{prefix} No close CODEBASE evidence found."
		return ToolObservation(
			tool_name=tool_name,
			summary=summary,
			claims=claims,
			citations=[self._artifact_citation("CODEBASE.md:1")],
			raw={"query": query, "matches": matches},
		)

	def _search_codebase_lines(self, query: str, max_hits: int = 5) -> list[str]:
		text = self._load_codebase_text()
		if not text:
			return []
		tokens = [
			token
			for token in re.findall(r"[a-z0-9_]+", query.lower())
			if token not in {"explain", "purpose", "blast", "radius", "and", "its", "the", "show", "me", "of", "for", "module"}
		]
		if not tokens:
			return []

		hits: list[str] = []
		for line in text.splitlines():
			stripped = line.strip()
			if len(stripped) < 4:
				continue
			lowered = stripped.lower()
			if any(token in lowered for token in tokens):
				hits.append(stripped)
				if len(hits) >= max_hits:
					break
		return hits

	def _guess_domain_from_path(self, module_path: str) -> str:
		path = module_path.replace("\\", "/").lower()
		if "/agents/" in path:
			return "Agent Orchestration"
		if "/analyzers/" in path:
			return "Static Analysis"
		if "/models/" in path:
			return "Domain Modeling"
		if "/graph/" in path:
			return "Graph Infrastructure"
		if "/utils/" in path:
			return "Shared Utilities"
		if "orchestrator" in path:
			return "Pipeline Orchestration"
		if "/tests/" in path or path.startswith("test_") or path.endswith("_test.py"):
			return "Testing"
		return "General Application Logic"

	def _normalize_text_citations(self, text: str) -> str:
		def _replace(match: re.Match[str]) -> str:
			raw = match.group(1)
			return f"[{self._normalize_citation(raw)}]"

		return re.sub(r"\[([^\[\]]+)\]", _replace, text)

	def _normalize_text_paths(self, text: str) -> str:
		normalized = text.replace("\\", "/")
		root = str(self.workspace_root).replace("\\", "/").rstrip("/")
		if not root:
			return normalized
		pattern = re.compile(re.escape(root) + r"/", re.IGNORECASE)
		return pattern.sub("", normalized)

	def _normalize_citation(self, citation: str) -> str:
		raw = citation.strip().replace("\\", "/")
		if not raw:
			return raw

		line_suffix = ""
		line_match = re.search(r":(\d+)$", raw)
		if line_match:
			line_suffix = f":{line_match.group(1)}"
			raw = raw[: line_match.start()]

		if raw.startswith(self.artifact_rel_prefix + "/"):
			return f"{raw}{line_suffix}"

		if raw.startswith(".cartography/"):
			return f"{raw}{line_suffix}"

		root = str(self.workspace_root).replace("\\", "/").rstrip("/")
		raw_lower = raw.lower()
		root_lower = root.lower()
		if raw_lower.startswith(f"{root_lower}/"):
			relative = raw[len(root) + 1 :]
			return f"{relative}{line_suffix}"

		anchor = f"/{self.workspace_root.name.lower()}/"
		anchor_index = raw_lower.find(anchor)
		if anchor_index >= 0:
			relative = raw[anchor_index + len(anchor) :]
			return f"{relative}{line_suffix}"

		return f"{raw}{line_suffix}"

	def _get_module_semantic_info(self, module_path: str) -> dict[str, str] | None:
		needle = module_path.replace("\\", "/").lower()
		purpose = None
		purpose_citation = self._artifact_citation("CODEBASE.md:1")
		for path, statement, citation in self._load_module_purpose_index():
			if path.replace("\\", "/").lower().endswith(needle) or needle in path.replace("\\", "/").lower():
				purpose = statement
				purpose_citation = citation
				break

		cluster, cluster_citation = self._find_cluster_for_module(module_path)
		drift_reason, drift_citation = self._find_drift_for_module(module_path)
		if purpose is None and cluster is None and drift_reason is None:
			return None
		return {
			"module_path": module_path,
			"purpose": purpose or "No purpose statement available.",
			"purpose_citation": purpose_citation,
			"cluster": cluster or "Unknown Cluster",
			"cluster_citation": cluster_citation,
			"drift_reason": drift_reason or "",
			"drift_citation": drift_citation,
		}

	def _find_cluster_for_module(self, module_path: str) -> tuple[str | None, str]:
		text = self._load_codebase_text()
		current_cluster: str | None = None
		needle = module_path.replace("\\", "/").lower()
		for line in text.splitlines():
			if line.startswith("### "):
				current_cluster = line.removeprefix("### ").strip()
				continue
			if current_cluster and line.startswith("- "):
				candidate = line.removeprefix("- ").strip()
				candidate_normalized = candidate.replace("\\", "/").lower()
				if candidate_normalized.endswith(needle) or needle in candidate_normalized:
					return current_cluster, self._artifact_citation("CODEBASE.md:1")
		return None, self._artifact_citation("CODEBASE.md:1")

	def _find_drift_for_module(self, module_path: str) -> tuple[str | None, str]:
		needle = module_path.replace("\\", "/").lower()
		for record in reversed(self._load_trace_records()):
			if record.get("action_type") != "documentation_drift_detected":
				continue
			evidence = str(record.get("evidence_source", ""))
			if needle in evidence.replace("\\", "/").lower():
				return str(record.get("drift_reason", "")).strip(), evidence

		text = self._load_codebase_text()
		for line in text.splitlines():
			if line.startswith("- ") and ":" in line and needle in line.replace("\\", "/").lower():
				return line.split(":", 1)[1].strip(), self._artifact_citation("CODEBASE.md:1")
		return None, self._artifact_citation("CODEBASE.md:1")

	def _resolve_artifact_rel_prefix(self) -> str:
		root = self.cartography_root.resolve()
		artifact = self.artifact_dir.resolve()
		try:
			relative = artifact.relative_to(root)
		except ValueError:
			return ".cartography"

		relative_text = str(relative).replace("\\", "/").strip("/")
		if not relative_text or relative_text == ".":
			return ".cartography"
		return f".cartography/{relative_text}"

	def _artifact_citation(self, suffix: str) -> str:
		normalized_suffix = suffix.strip().lstrip("/")
		return f"{self.artifact_rel_prefix}/{normalized_suffix}"

	def _load_json_file(self, cache_key: str, path: Path) -> Any:
		if cache_key in self._cache:
			return self._cache[cache_key]
		if not path.exists():
			self._cache[cache_key] = {}
			return self._cache[cache_key]
		self._cache[cache_key] = json.loads(path.read_text(encoding="utf-8"))
		return self._cache[cache_key]

	def _tokenize(self, text: str) -> set[str]:
		return set(re.findall(r"[a-z0-9_]+", text.lower()))

	def _path_preference(self, path_value: str) -> int:
		normalized = str(path_value).replace("\\", "/").lower()
		if normalized.endswith(".sql"):
			return 4
		if normalized.endswith(".py"):
			return 3
		if normalized.endswith((".yml", ".yaml")):
			return 2
		if normalized.endswith(".csv"):
			return 1
		return 0

	def _format_list(self, values: list[str]) -> str:
		if not values:
			return "no recorded nodes"
		return ", ".join(values)


__all__ = ["Navigator"]

