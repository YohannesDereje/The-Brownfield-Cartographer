from __future__ import annotations

from pathlib import Path
import re
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import Any

from loguru import logger
import networkx as nx
from sqlglot import exp, parse_one
from tree_sitter import Language, Node, Parser, Query, QueryCursor
import tree_sitter_python
import yaml

from src.analyzers.dag_config_parser import DAGConfigParser, infer_dbt_resource_name, normalize_dbt_resource_name
from src.analyzers.sql_lineage import SQLLineageAnalyzer as DbtSQLLineageAnalyzer
from src.models.nodes import DataLineageEdge, ModuleNode
from src.utils.tracer import CartographyTracer, InferenceMethod


class PythonDataFlowAnalyzer:
	"""Detects coarse-grained Python data flow operations from AST call nodes."""

	READ_FUNCTIONS = {
		"read_csv",
		"read_sql",
		"read_parquet",
		"load",
		"open",
	}
	WRITE_FUNCTIONS = {
		"to_csv",
		"to_parquet",
		"to_sql",
		"save",
	}
	TRANSFORM_FUNCTIONS = set()
	CALL_QUERY = """
	(call
	  function: [
	    (identifier) @call.name
	    (attribute
	      attribute: (identifier) @call.name)
	  ]
	  arguments: (argument_list) @call.args
	) @call.node
	"""

	def __init__(self) -> None:
		self.language = Language(tree_sitter_python.language())
		self.parser = Parser()
		set_language = getattr(self.parser, "set_language", None)
		if callable(set_language):
			set_language(self.language)
		else:
			self.parser.language = self.language
		self.query = Query(self.language, self.CALL_QUERY)

	def analyze_python_lineage(self, file_content: str, module_path: str) -> list[DataLineageEdge]:
		"""Scan Python source for read/write data movement operations."""
		try:
			tree = self.parser.parse(file_content.encode("utf-8"))
		except Exception as exc:
			logger.exception("Failed to parse Python lineage for {}: {}", module_path, exc)
			return []

		captures = self._run_captures(tree.root_node)
		call_records: dict[tuple[int, int], dict[str, Node]] = {}
		for capture_name, node in captures:
			key = (node.start_byte, node.end_byte)
			if capture_name == "call.node":
				call_records.setdefault(key, {})[capture_name] = node
				continue

			call_node = self._find_ancestor(node, "call")
			if call_node is None:
				continue

			call_key = (call_node.start_byte, call_node.end_byte)
			call_records.setdefault(call_key, {})["call.node"] = call_node
			call_records[call_key][capture_name] = node

		lineage_edges: list[DataLineageEdge] = []
		for record in call_records.values():
			name_node = record.get("call.name")
			args_node = record.get("call.args")
			if name_node is None or args_node is None:
				continue

			function_name = self._node_text(name_node, file_content)
			operation_type = self._classify_operation(function_name)
			if operation_type is None:
				continue

			first_arg = self._extract_first_argument(args_node)
			if first_arg is None:
				continue

			uri_value, is_dynamic = self._extract_argument_value(first_arg, file_content)
			edge = self._build_lineage_edge(operation_type, uri_value, is_dynamic)
			lineage_edges.append(edge)
			logger.info("Detected {} operation in {}", operation_type, module_path)

		return lineage_edges

	def _classify_operation(self, function_name: str) -> str | None:
		if function_name in self.READ_FUNCTIONS:
			return "READ"
		if function_name in self.WRITE_FUNCTIONS:
			return "WRITE"
		if function_name in self.TRANSFORM_FUNCTIONS:
			return "TRANSFORM"
		return None

	def _extract_first_argument(self, args_node: Node) -> Node | None:
		for child in args_node.named_children:
			if child.type == "keyword_argument":
				continue
			return child
		return None

	def _extract_argument_value(self, argument_node: Node, source: str) -> tuple[str, bool]:
		if argument_node.type == "string":
			return self._extract_string_literal(argument_node, source), False

		if argument_node.type in {"identifier", "attribute"}:
			return self._node_text(argument_node, source), True

		return self._node_text(argument_node, source), True

	def _extract_string_literal(self, node: Node, source: str) -> str:
		text = self._node_text(node, source)
		if len(text) >= 2 and text[0] in {'"', "'"} and text[-1] == text[0]:
			return text[1:-1]

		# Handles prefixed literals such as r"..." or f'...'
		quote_index = min((idx for idx, char in enumerate(text) if char in {'"', "'"}), default=-1)
		if quote_index >= 0 and text[-1] in {'"', "'"}:
			return text[quote_index + 1 : -1]

		return text

	def _build_lineage_edge(self, operation_type: str, uri_value: str, is_dynamic: bool) -> DataLineageEdge:
		if operation_type == "READ":
			return DataLineageEdge(
				source_uri=uri_value,
				sink_uri=None,
				operation_type=operation_type,
				is_dynamic=is_dynamic,
			)

		if operation_type == "WRITE":
			return DataLineageEdge(
				source_uri=None,
				sink_uri=uri_value,
				operation_type=operation_type,
				is_dynamic=is_dynamic,
			)

		return DataLineageEdge(
			source_uri=uri_value,
			sink_uri=uri_value,
			operation_type=operation_type,
			is_dynamic=is_dynamic,
		)

	def _run_captures(self, root: Node) -> list[tuple[str, Node]]:
		try:
			cursor = QueryCursor(self.query)
			captures = cursor.captures(root)
			results: list[tuple[str, Node]] = []
			for capture_name, nodes in captures.items():
				for node in nodes:
					results.append((capture_name, node))
			return results
		except Exception:
			try:
				raw = self.query.captures(root)
				return [(capture_name, node) for node, capture_name in raw]
			except Exception as exc:
				logger.exception("Failed to execute lineage query captures: {}", exc)
				return []

	def _find_ancestor(self, node: Node, node_type: str) -> Node | None:
		current = node
		while current is not None:
			if current.type == node_type:
				return current
			current = current.parent
		return None

	def _node_text(self, node: Node, source: str) -> str:
		return source.encode("utf-8")[node.start_byte : node.end_byte].decode("utf-8", errors="replace")


class DAGConfigAnalyzer:
	"""Parses config and orchestration logic to discover cross-file lineage edges."""

	_AIRFLOW_SHIFT_PATTERN = re.compile(r"\b([A-Za-z_][\w]*)\b\s*>>\s*\b([A-Za-z_][\w]*)\b")
	_AIRFLOW_SET_DOWNSTREAM_PATTERN = re.compile(
		r"\b([A-Za-z_][\w]*)\b\.set_downstream\(\s*([A-Za-z_][\w]*)\s*\)"
	)
	_BASH_TASK_PATTERN = re.compile(
		r"\b([A-Za-z_][\w]*)\s*=\s*\w*Operator\(.*?bash_command\s*=\s*['\"][^'\"]*?([\w\-\/\.]+\.py)[^'\"]*['\"]",
		re.DOTALL,
	)

	def analyze_yaml_config(self, yaml_content: str, file_path: str) -> list[DataLineageEdge]:
		"""Extract dbt source/seed -> model lineage from schema/source YAML files."""
		if not file_path.lower().endswith(("sources.yml", "schema.yml", ".yaml", ".yml")):
			return []

		yaml_payload = yaml_content.replace("\t", "  ")

		try:
			parsed = yaml.safe_load(yaml_payload) or {}
		except Exception as exc:
			logger.exception("Failed to parse YAML config {}: {}", file_path, exc)
			return []

		models = parsed.get("models", []) if isinstance(parsed, dict) else []
		sources = parsed.get("sources", []) if isinstance(parsed, dict) else []
		seeds = parsed.get("seeds", []) if isinstance(parsed, dict) else []

		declared_source_tables = self._collect_declared_sources(sources)
		declared_seed_names = self._collect_declared_seeds(seeds)

		edges: list[DataLineageEdge] = []
		for model in models:
			if not isinstance(model, dict):
				continue
			model_name = model.get("name")
			if not model_name:
				continue

			for source_name in self._extract_meta_refs(model, keys=("sources", "depends_on_sources", "upstream_sources")):
				edges.append(
					DataLineageEdge(
						source_uri=source_name,
						sink_uri=str(model_name),
						operation_type="TRANSFORM",
						is_dynamic=False,
					)
				)

			for seed_name in self._extract_meta_refs(model, keys=("seeds", "depends_on_seeds", "upstream_seeds")):
				edges.append(
					DataLineageEdge(
						source_uri=seed_name,
						sink_uri=str(model_name),
						operation_type="TRANSFORM",
						is_dynamic=False,
					)
				)

			# Infer relationships from tests like relationships/to and accepted_values where possible.
			for inferred_source in self._extract_model_test_relationships(model):
				edges.append(
					DataLineageEdge(
						source_uri=inferred_source,
						sink_uri=str(model_name),
						operation_type="TRANSFORM",
						is_dynamic=False,
					)
				)

		# Add broad source/seed declarations as standalone producer entities.
		for source_uri in sorted(declared_source_tables):
			edges.append(
				DataLineageEdge(
					source_uri=source_uri,
					sink_uri=None,
					operation_type="TRANSFORM",
					is_dynamic=False,
				)
			)

		for seed_uri in sorted(declared_seed_names):
			edges.append(
				DataLineageEdge(
					source_uri=seed_uri,
					sink_uri=None,
					operation_type="TRANSFORM",
					is_dynamic=False,
				)
			)

		return self._dedupe_edges(edges)

	def analyze_dag_logic(self, file_content: str, file_path: str) -> list[DataLineageEdge]:
		"""Extract orchestration dependencies from Airflow-style DAG expressions."""
		task_to_script = self._extract_task_script_mapping(file_content)
		edges: list[DataLineageEdge] = []

		for upstream, downstream in self._AIRFLOW_SHIFT_PATTERN.findall(file_content):
			source_uri = task_to_script.get(upstream, upstream)
			sink_uri = task_to_script.get(downstream, downstream)
			edges.append(
				DataLineageEdge(
					source_uri=source_uri,
					sink_uri=sink_uri,
					operation_type="ORCHESTRATION",
					is_dynamic=source_uri == upstream or sink_uri == downstream,
				)
			)

		for upstream, downstream in self._AIRFLOW_SET_DOWNSTREAM_PATTERN.findall(file_content):
			source_uri = task_to_script.get(upstream, upstream)
			sink_uri = task_to_script.get(downstream, downstream)
			edges.append(
				DataLineageEdge(
					source_uri=source_uri,
					sink_uri=sink_uri,
					operation_type="ORCHESTRATION",
					is_dynamic=source_uri == upstream or sink_uri == downstream,
				)
			)

		logger.debug("Detected {} orchestration edges in {}", len(edges), file_path)
		return self._dedupe_edges(edges)

	def _extract_task_script_mapping(self, file_content: str) -> dict[str, str]:
		mapping: dict[str, str] = {}
		for task_var, script_path in self._BASH_TASK_PATTERN.findall(file_content):
			mapping[task_var] = script_path
		return mapping

	def _collect_declared_sources(self, sources: list[Any]) -> set[str]:
		results: set[str] = set()
		for source in sources:
			if not isinstance(source, dict):
				continue
			source_name = source.get("name")
			for table in source.get("tables", []) or []:
				if not isinstance(table, dict):
					continue
				table_name = table.get("name")
				if source_name and table_name:
					results.add(f"{source_name}.{table_name}")
				elif table_name:
					results.add(str(table_name))
		return results

	def _collect_declared_seeds(self, seeds: list[Any]) -> set[str]:
		results: set[str] = set()
		for seed in seeds:
			if not isinstance(seed, dict):
				continue
			seed_name = seed.get("name")
			if seed_name:
				results.add(str(seed_name))
		return results

	def _extract_meta_refs(self, model: dict[str, Any], keys: tuple[str, ...]) -> list[str]:
		meta = model.get("meta")
		if not isinstance(meta, dict):
			return []

		refs: list[str] = []
		for key in keys:
			value = meta.get(key)
			if isinstance(value, list):
				refs.extend(str(item) for item in value if item)
			elif isinstance(value, str) and value:
				refs.append(value)
		return refs

	def _extract_model_test_relationships(self, model: dict[str, Any]) -> list[str]:
		relationships: list[str] = []
		for column in model.get("columns", []) or []:
			if not isinstance(column, dict):
				continue
			for test in column.get("tests", []) or []:
				if not isinstance(test, dict):
					continue
				rel = test.get("relationships")
				if not isinstance(rel, dict):
					continue
				to_target = rel.get("to")
				if to_target:
					relationships.append(str(to_target))
		return relationships

	def _dedupe_edges(self, edges: list[DataLineageEdge]) -> list[DataLineageEdge]:
		seen: set[tuple[str | None, str | None, str, bool]] = set()
		unique: list[DataLineageEdge] = []
		for edge in edges:
			key = (edge.source_uri, edge.sink_uri, edge.operation_type, edge.is_dynamic)
			if key in seen:
				continue
			seen.add(key)
			unique.append(edge)
		return unique


class Hydrologist:
	"""Master lineage engine that hydrates modules, builds graph, and computes blast radius."""

	def __init__(self, trace_path: str | Path = ".cartography/cartography_trace.jsonl", repo_root: str | Path | None = None) -> None:
		self.python_analyzer = PythonDataFlowAnalyzer()
		self.sql_analyzer = DbtSQLLineageAnalyzer()
		self.dag_config_analyzer = DAGConfigParser()
		self.tracer = CartographyTracer(trace_path)
		self.repo_root = Path(repo_root).resolve() if repo_root is not None else None

	def hydrate_repository_lineage(self, nodes: list[ModuleNode]) -> list[ModuleNode]:
		"""Populate node.lineage by dispatching to language/config/orchestration analyzers."""
		for node in nodes:
			file_path = Path(node.path)
			suffix = file_path.suffix.lower()
			try:
				content = file_path.read_text(encoding="utf-8", errors="replace")
			except Exception as exc:
				logger.exception("Failed to read file for lineage hydration {}: {}", file_path, exc)
				node.lineage = []
				continue

			lineage_edges: list[DataLineageEdge] = []
			if suffix == ".py":
				lineage_edges.extend(self.python_analyzer.analyze_python_lineage(content, str(file_path)))
				lineage_edges.extend(self.dag_config_analyzer.analyze_dag_logic(content, str(file_path)))
			elif suffix == ".sql":
				lineage_edges.extend(self.sql_analyzer.analyze_sql_lineage(content, str(file_path)))
			elif suffix in {".yml", ".yaml"}:
				lineage_edges.extend(self.dag_config_analyzer.parse_dbt_schema(content, str(file_path)))

			node.lineage = lineage_edges

		return nodes

	def build_global_graph(self, nodes: list[ModuleNode]) -> nx.DiGraph:
		"""Build a lineage graph connecting modules and canonicalized external data entities."""
		graph = nx.DiGraph()

		module_paths = {self._normalize_path(node.path): node for node in nodes}
		module_canonical_index = self._build_module_canonical_index(module_paths.keys())

		for module_path, module in module_paths.items():
			graph.add_node(module_path, kind="module", path=module_path, language=module.language)

		for module_path, module in module_paths.items():
			for lineage_edge in module.lineage:
				op_type = lineage_edge.operation_type
				source_node = self._resolve_graph_node(lineage_edge.source_uri, module_canonical_index, graph)
				sink_node = self._resolve_graph_node(lineage_edge.sink_uri, module_canonical_index, graph)

				if op_type == "READ":
					if source_node:
						graph.add_edge(source_node, module_path, operation_type=op_type)
					continue

				if op_type == "WRITE":
					if sink_node:
						graph.add_edge(module_path, sink_node, operation_type=op_type)
					continue

				if op_type == "ORCHESTRATION":
					if source_node and sink_node:
						graph.add_edge(source_node, sink_node, operation_type=op_type)
					continue

				# TRANSFORM and any future operation types.
				if source_node:
					graph.add_edge(source_node, module_path, operation_type=op_type)
				if sink_node:
					graph.add_edge(module_path, sink_node, operation_type=op_type)

		return graph

	def upsert_module_lineage(
		self,
		graph: nx.DiGraph,
		module: ModuleNode,
		nodes: list[ModuleNode],
	) -> nx.DiGraph:
		"""Update one module's lineage edges while preserving the rest of the lineage graph."""
		module_paths = {self._normalize_path(node.path): node for node in nodes}
		module_canonical_index = self._build_module_canonical_index(module_paths.keys())

		module_path = self._normalize_path(module.path)
		if graph.has_node(module_path):
			edges_to_remove = []
			for source_node, target_node, edge_data in graph.in_edges(module_path, data=True):
				if edge_data.get("emitter") == module_path:
					edges_to_remove.append((source_node, target_node))
			for source_node, target_node, edge_data in graph.out_edges(module_path, data=True):
				if edge_data.get("emitter") == module_path:
					edges_to_remove.append((source_node, target_node))
			if edges_to_remove:
				graph.remove_edges_from(edges_to_remove)
		graph.add_node(module_path, kind="module", path=module_path, language=module.language)

		for lineage_edge in module.lineage:
			op_type = lineage_edge.operation_type
			source_node = self._resolve_graph_node(lineage_edge.source_uri, module_canonical_index, graph)
			sink_node = self._resolve_graph_node(lineage_edge.sink_uri, module_canonical_index, graph)

			if op_type == "READ" and source_node:
				graph.add_edge(source_node, module_path, operation_type=op_type, is_dynamic=lineage_edge.is_dynamic, emitter=module_path)
			elif op_type == "WRITE" and sink_node:
				graph.add_edge(module_path, sink_node, operation_type=op_type, is_dynamic=lineage_edge.is_dynamic, emitter=module_path)
			elif source_node and sink_node:
				graph.add_edge(source_node, sink_node, operation_type=op_type, is_dynamic=lineage_edge.is_dynamic, emitter=module_path)

		self._prune_orphan_external_nodes(graph)
		return graph

	def get_blast_radius(self, target_path: str, nodes: list[ModuleNode]) -> set[str]:
		"""Return downstream file paths impacted by a change in target_path."""
		graph = self.build_global_graph(nodes)
		normalized_target = self._normalize_path(target_path)
		start_nodes = self._find_start_nodes(graph, normalized_target)

		if not start_nodes:
			return set()

		downstream: set[str] = set()
		for start_node in start_nodes:
			for descendant in nx.descendants(graph, start_node):
				node_data = graph.nodes.get(descendant, {})
				if node_data.get("kind") != "module":
					continue
				if descendant == normalized_target:
					continue
				downstream.add(descendant)

		return downstream

	def identify_system_boundary_nodes(self, nodes: list[ModuleNode]) -> dict[str, list[str]]:
		"""Identify entry-point sources and terminal sinks in the lineage graph."""
		graph = self.build_global_graph(nodes)
		if graph.number_of_nodes() == 0:
			return {"ultimate_sources": [], "ultimate_sinks": []}

		sources: list[str] = []
		sinks: list[str] = []

		for node_id in graph.nodes:
			if graph.in_degree(node_id) == 0:
				source_label = self._display_node_label(graph, node_id)
				sources.append(source_label)
				self.tracer.log_action(
					agent_name="hydrologist",
					action_type="data_source_identified",
					evidence_source=self._boundary_evidence_source(graph, node_id),
					confidence_level=1.0,
					inference_method=InferenceMethod.STATIC_ANALYSIS,
					node_label=source_label,
				)
			if graph.out_degree(node_id) == 0:
				sink_label = self._display_node_label(graph, node_id)
				sinks.append(sink_label)
				self.tracer.log_action(
					agent_name="hydrologist",
					action_type="data_sink_identified",
					evidence_source=self._boundary_evidence_source(graph, node_id),
					confidence_level=1.0,
					inference_method=InferenceMethod.STATIC_ANALYSIS,
					node_label=sink_label,
				)

		return {
			"ultimate_sources": sorted(set(sources)),
			"ultimate_sinks": sorted(set(sinks)),
		}

	def generate_lineage_summary(self, nodes: list[ModuleNode]) -> str:
		"""Generate a Markdown lineage summary with sources, sinks, and critical path."""
		graph = self.build_global_graph(nodes)
		boundaries = self.identify_system_boundary_nodes(nodes)

		if graph.number_of_nodes() == 0:
			return "# Lineage Summary\n\nNo lineage graph data available."

		critical_nodes = self._compute_critical_nodes(graph, top_n=3)

		lines: list[str] = ["# Lineage Summary", ""]
		lines.append("## Primary Sources")
		if boundaries["ultimate_sources"]:
			for source in boundaries["ultimate_sources"]:
				lines.append(f"- {source}")
		else:
			lines.append("- None identified")

		lines.append("")
		lines.append("## Final Sinks")
		if boundaries["ultimate_sinks"]:
			for sink in boundaries["ultimate_sinks"]:
				lines.append(f"- {sink}")
		else:
			lines.append("- None identified")

		lines.append("")
		lines.append("## Critical Path")
		if critical_nodes:
			for node_label, score in critical_nodes:
				lines.append(f"- {node_label} (score={score:.4f})")
		else:
			lines.append("- None identified")

		return "\n".join(lines)

	def _find_start_nodes(self, graph: nx.DiGraph, normalized_target: str) -> set[str]:
		starts: set[str] = set()
		if normalized_target in graph:
			starts.add(normalized_target)

		canonical_target = self._canonical_uri(normalized_target)
		uri_node = self._uri_node_id(canonical_target)
		if uri_node in graph:
			starts.add(uri_node)

		for node_id, attrs in graph.nodes(data=True):
			if attrs.get("kind") != "module":
				continue
			if self._canonical_uri(node_id) == canonical_target:
				starts.add(node_id)

		return starts

	def _resolve_graph_node(
		self,
		uri: str | None,
		module_canonical_index: dict[str, str],
		graph: nx.DiGraph,
	) -> str | None:
		if not uri:
			return None

		normalized_uri = self._normalize_path(uri)
		if normalized_uri in module_canonical_index.values():
			return normalized_uri

		canonical_uri = self._canonical_uri(uri)
		if canonical_uri in module_canonical_index:
			return module_canonical_index[canonical_uri]

		node_id = self._uri_node_id(canonical_uri)
		if node_id not in graph:
			graph.add_node(node_id, kind="external", canonical_uri=canonical_uri, raw_uri=uri)
		return node_id

	def _uri_node_id(self, canonical_uri: str) -> str:
		return f"uri::{canonical_uri}"

	def _canonical_uri(self, value: str) -> str:
		normalized = self._normalize_path(value).strip().strip("\"'")
		if not normalized:
			return ""

		dbt_name = normalize_dbt_resource_name(normalized)
		if dbt_name:
			return dbt_name

		token = normalized.rsplit("/", 1)[-1]
		lower_token = token.lower()

		file_suffixes = (".csv", ".sql", ".py", ".parquet", ".json")
		if lower_token.endswith(file_suffixes):
			return Path(token).stem.lower()

		if "." in token:
			# Handle schema-prefixed entities like raw.orders -> orders.
			return token.rsplit(".", 1)[-1].lower()

		return token.lower()

	def _normalize_path(self, path_value: str) -> str:
		path_text = str(path_value).replace("\\", "/")
		if not path_text:
			return path_text
		candidate = Path(path_text)
		if self.repo_root is not None:
			try:
				resolved = candidate.resolve(strict=False)
				relative = resolved.relative_to(self.repo_root)
				return str(relative).replace("\\", "/")
			except Exception:
				pass
		return path_text

	def _build_module_canonical_index(self, normalized_paths: Any) -> dict[str, str]:
		index: dict[str, str] = {}
		for normalized_path in normalized_paths:
			canonical = self._canonical_uri(str(normalized_path))
			if not canonical:
				continue
			existing = index.get(canonical)
			if existing is None or self._path_preference(str(normalized_path)) > self._path_preference(existing):
				index[canonical] = str(normalized_path)
		return index

	def _path_preference(self, path_value: str) -> int:
		suffix = Path(path_value).suffix.lower()
		if suffix == ".sql":
			return 4
		if suffix == ".py":
			return 3
		if suffix in {".yml", ".yaml"}:
			return 2
		if suffix == ".csv":
			return 1
		return 0

	def _display_node_label(self, graph: nx.DiGraph, node_id: str) -> str:
		attrs = graph.nodes.get(node_id, {})
		if attrs.get("kind") == "module":
			return attrs.get("path", node_id)
		if attrs.get("kind") == "external":
			return attrs.get("raw_uri") or attrs.get("canonical_uri") or node_id
		return node_id

	def _boundary_evidence_source(self, graph: nx.DiGraph, node_id: str) -> str:
		label = self._display_node_label(graph, node_id)
		return f"{label}:1"

	def _prune_orphan_external_nodes(self, graph: nx.DiGraph) -> None:
		for node_id, attrs in list(graph.nodes(data=True)):
			if attrs.get("kind") != "external":
				continue
			if graph.degree(node_id) == 0:
				graph.remove_node(node_id)

	def _compute_critical_nodes(self, graph: nx.DiGraph, top_n: int = 3) -> list[tuple[str, float]]:
		if graph.number_of_nodes() == 0:
			return []

		module_nodes = [
			node_id for node_id, attrs in graph.nodes(data=True) if attrs.get("kind") == "module"
		]
		if not module_nodes:
			return []

		try:
			centrality = nx.betweenness_centrality(graph)
		except Exception as exc:
			logger.warning("Failed to compute betweenness centrality: {}", exc)
			centrality = {}

		scored: list[tuple[str, float]] = []
		for node_id in module_nodes:
			score = centrality.get(node_id)
			if score is None or score == 0:
				score = float(graph.out_degree(node_id))
			scored.append((self._display_node_label(graph, node_id), float(score)))

		scored.sort(key=lambda item: item[1], reverse=True)
		return scored[:top_n]


def run_python_lineage_smoke_test() -> list[DataLineageEdge]:
	"""Temporary verification helper for Step 1 hydrologist development."""
	sample_python = """
import pandas as pd

df = pd.read_csv('data/input.csv')
with open(args.input) as handle:
	payload = handle.read()
df.to_parquet(output_path)
"""
	analyzer = PythonDataFlowAnalyzer()
	return analyzer.analyze_python_lineage(sample_python, module_path=str(Path("sample.py")))


def run_sql_lineage_smoke_test() -> list[DataLineageEdge]:
	"""Temporary verification helper for Step 2 hydrologist development."""
	sample_sql = """
	create table analytics.orders_enriched as
	select o.order_id, c.customer_id
	from {{ ref('stg_orders') }} as o
	join {{ ref('stg_customers') }} as c
		on o.customer_id = c.customer_id
	"""
	analyzer = DbtSQLLineageAnalyzer()
	return analyzer.analyze_sql_lineage(sample_sql, file_path="models/marts/orders_enriched.sql", dialect="duckdb")


def run_config_lineage_smoke_test() -> list[DataLineageEdge]:
	"""Temporary verification helper for Step 3 DAG/config lineage development."""
	sample_yaml = dedent(
		"""
		version: 2
		models:
		  - name: fct_orders
		    meta:
		      sources: [raw.orders]
		      seeds: [seed_exchange_rates]
		"""
	)

	sample_dag = dedent(
		"""
		load_orders = BashOperator(task_id='load_orders', bash_command='python scripts/load_orders.py')
		transform_orders = BashOperator(task_id='transform_orders', bash_command='python scripts/transform_orders.py')
		load_orders >> transform_orders
		"""
	)

	analyzer = DAGConfigAnalyzer()
	yaml_edges = analyzer.analyze_yaml_config(sample_yaml, file_path="models/schema.yml")
	dag_edges = analyzer.analyze_dag_logic(sample_dag, file_path="dags/orders_dag.py")
	return [*yaml_edges, *dag_edges]


def run_full_hydrologist_smoke_test() -> set[str]:
	"""Temporary end-to-end verification helper for Step 4 hydrologist development."""
	with TemporaryDirectory() as temp_dir:
		repo_root = Path(temp_dir)
		csv_path = repo_root / "raw_orders.csv"
		py_path = repo_root / "ingest_orders.py"
		sql_path = repo_root / "orders_model.sql"

		csv_path.write_text("order_id,amount\n1,100\n", encoding="utf-8")
		py_path.write_text(
			dedent(
				"""
				import pandas as pd

				df = pd.read_csv('raw_orders.csv')
				"""
			),
			encoding="utf-8",
		)
		sql_path.write_text(
			dedent(
				"""
				create table mart.orders_final as
				select *
				from raw_orders
				"""
			),
			encoding="utf-8",
		)

		nodes = [
			ModuleNode(path=str(csv_path), language="csv"),
			ModuleNode(path=str(py_path), language="python"),
			ModuleNode(path=str(sql_path), language="sql"),
		]

		hydrologist = Hydrologist()
		hydrologist.hydrate_repository_lineage(nodes)
		blast_radius = hydrologist.get_blast_radius(str(csv_path), nodes)
		print("Blast Radius:", sorted(blast_radius))
		return blast_radius


__all__ = [
	"Hydrologist",
	"PythonDataFlowAnalyzer",
	"SQLLineageAnalyzer",
	"DAGConfigAnalyzer",
]
