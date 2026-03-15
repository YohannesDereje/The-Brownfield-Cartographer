from __future__ import annotations

from pathlib import Path
import re

from loguru import logger
from sqlglot import exp, parse_one

from src.analyzers.dag_config_parser import infer_dbt_resource_name, normalize_dbt_resource_name
from src.models.nodes import DataLineageEdge


class SQLLineageAnalyzer:
	"""Extract dbt-aware SQL lineage edges with explicit ref() handling."""

	_DBT_REF_PATTERN = re.compile(r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}", re.IGNORECASE)
	_DBT_SOURCE_PATTERN = re.compile(
		r"\{\{\s*source\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}",
		re.IGNORECASE,
	)
	_DBT_CONFIG_BLOCK_PATTERN = re.compile(r"\{\{\s*config\([\s\S]*?\)\s*\}\}", re.IGNORECASE)

	def analyze_sql_lineage(self, sql_content: str, file_path: str, dialect: str = "duckdb") -> list[DataLineageEdge]:
		"""Return logical model-to-model edges for dbt SQL files."""
		sink_uri = infer_dbt_resource_name(file_path)
		if not sink_uri:
			logger.debug("No logical sink inferred for SQL file {}", file_path)
			return []

		preprocessed_sql = self._preprocess_dbt_sql(sql_content)
		source_uris = self._extract_ref_sources(sql_content)

		try:
			expression = parse_one(preprocessed_sql, read=dialect)
		except Exception as exc:
			logger.warning("Failed to parse SQL lineage with sqlglot for {}: {}", file_path, exc)
			expression = None

		if expression is not None:
			for source_uri in self._extract_sources(expression, sink_uri):
				if source_uri not in source_uris:
					source_uris.append(source_uri)

		return [
			DataLineageEdge(
				source_uri=source_uri,
				sink_uri=sink_uri,
				operation_type="TRANSFORM",
				is_dynamic=False,
			)
			for source_uri in source_uris
			if source_uri and source_uri != sink_uri
		]

	def _preprocess_dbt_sql(self, sql_content: str) -> str:
		without_config = self._DBT_CONFIG_BLOCK_PATTERN.sub("", sql_content)
		with_refs = self._DBT_REF_PATTERN.sub(lambda match: match.group(1), without_config)
		return self._DBT_SOURCE_PATTERN.sub(lambda match: f"{match.group(1)}.{match.group(2)}", with_refs)

	def _extract_ref_sources(self, sql_content: str) -> list[str]:
		results: list[str] = []
		for match in self._DBT_REF_PATTERN.findall(sql_content):
			name = normalize_dbt_resource_name(match)
			if name and name not in results:
				results.append(name)
		for source_name, table_name in self._DBT_SOURCE_PATTERN.findall(sql_content):
			combined = normalize_dbt_resource_name(f"{source_name}.{table_name}")
			if combined and combined not in results:
				results.append(combined)
		return results

	def _extract_sources(self, expression: exp.Expression, sink_uri: str | None) -> list[str]:
		source_names: list[str] = []
		seen: set[str] = set()

		for table in expression.find_all(exp.Table):
			table_name = self._table_name(table)
			normalized_name = normalize_dbt_resource_name(table_name)
			if not normalized_name:
				continue
			if sink_uri is not None and normalized_name == sink_uri:
				continue
			if normalized_name in seen:
				continue
			seen.add(normalized_name)
			source_names.append(normalized_name)

		return source_names

	def _table_name(self, node: exp.Expression | None) -> str | None:
		if node is None:
			return None

		if isinstance(node, exp.Schema):
			node = node.this

		if isinstance(node, exp.Table):
			parts = [part for part in [node.catalog, node.db, node.name] if part]
			return ".".join(parts)

		if isinstance(node, exp.Identifier):
			return node.name

		name = getattr(node, "name", None)
		if isinstance(name, str) and name:
			return name

		return node.sql() if hasattr(node, "sql") else None


__all__ = ["SQLLineageAnalyzer"]
