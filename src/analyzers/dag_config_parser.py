from __future__ import annotations

from pathlib import Path
import re
from typing import Any

from loguru import logger
import yaml

from src.models.nodes import DataLineageEdge


_DBT_REF_CALL_PATTERN = re.compile(r"ref\(\s*['\"]([^'\"]+)['\"]\s*\)", re.IGNORECASE)
_DBT_SOURCE_CALL_PATTERN = re.compile(
    r"source\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)",
    re.IGNORECASE,
)


def normalize_dbt_resource_name(value: str | None) -> str:
    text = str(value or "").strip().strip("\"'")
    if not text:
        return ""

    text = text.replace("\\", "/")
    ref_match = _DBT_REF_CALL_PATTERN.search(text)
    if ref_match:
        text = ref_match.group(1)
    else:
        source_match = _DBT_SOURCE_CALL_PATTERN.search(text)
        if source_match:
            text = f"{source_match.group(1)}.{source_match.group(2)}"

    if text.startswith("uri::"):
        text = text[5:]

    leaf = text.rsplit("/", 1)[-1]
    leaf_lower = leaf.lower()
    if leaf_lower.endswith((".sql", ".yml", ".yaml", ".py", ".csv", ".json", ".parquet")):
        leaf = Path(leaf).stem

    return leaf.strip().lower()


def infer_dbt_resource_name(file_path: str) -> str | None:
    path = Path(str(file_path).replace("\\", "/"))
    stem = path.stem.strip().lower()
    if not stem:
        return None
    if stem in {"schema", "sources", "src", "packages", "package-lock", "dbt_project", "profiles", "__sources"}:
        return None
    return normalize_dbt_resource_name(stem)


class DAGConfigParser:
    """Parses lightweight DAG/config relationships from Airflow and dbt YAML."""

    _SHIFT_PATTERN = re.compile(r"\b([A-Za-z_][\w]*)\b\s*>>\s*\b([A-Za-z_][\w]*)\b")
    _SET_DOWNSTREAM_PATTERN = re.compile(
        r"\b([A-Za-z_][\w]*)\b\.set_downstream\(\s*([A-Za-z_][\w]*)\s*\)"
    )
    _BASH_TASK_PATTERN = re.compile(
        r"\b([A-Za-z_][\w]*)\s*=\s*\w*Operator\(.*?bash_command\s*=\s*['\"][^'\"]*?([\w\-\/\.]+\.py)[^'\"]*['\"]",
        re.DOTALL,
    )

    def parse_airflow_dependencies(self, file_content: str, file_path: str) -> list[DataLineageEdge]:
        edges: list[DataLineageEdge] = []
        for src, dst in self._SHIFT_PATTERN.findall(file_content):
            edges.append(
                DataLineageEdge(
                    source_uri=src,
                    sink_uri=dst,
                    operation_type="ORCHESTRATION",
                    is_dynamic=True,
                )
            )
        for src, dst in self._SET_DOWNSTREAM_PATTERN.findall(file_content):
            edges.append(
                DataLineageEdge(
                    source_uri=src,
                    sink_uri=dst,
                    operation_type="ORCHESTRATION",
                    is_dynamic=True,
                )
            )
        logger.debug("Parsed {} airflow edges from {}", len(edges), file_path)
        return edges

    def parse_dbt_schema(self, yaml_content: str, file_path: str) -> list[DataLineageEdge]:
        if not file_path.lower().endswith((".yml", ".yaml")):
            return []

        try:
            payload = yaml.safe_load(yaml_content.replace("\t", "  ")) or {}
        except Exception as exc:
            logger.exception("Failed to parse dbt schema YAML {}: {}", file_path, exc)
            return []

        edges: list[DataLineageEdge] = []
        if not isinstance(payload, dict):
            return edges

        models = payload.get("models", [])
        for model in models if isinstance(models, list) else []:
            if not isinstance(model, dict):
                continue
            model_name = normalize_dbt_resource_name(model.get("name"))
            if not model_name:
                continue
            meta = model.get("meta") if isinstance(model.get("meta"), dict) else {}
            for key in ("sources", "depends_on_sources", "upstream_sources", "seeds", "depends_on_seeds"):
                value = meta.get(key)
                if isinstance(value, list):
                    for ref in value:
                        normalized_ref = normalize_dbt_resource_name(str(ref))
                        if not normalized_ref:
                            continue
                        edges.append(
                            DataLineageEdge(
                                source_uri=normalized_ref,
                                sink_uri=str(model_name),
                                operation_type="TRANSFORM",
                                is_dynamic=False,
                            )
                        )

            for ref in self._extract_model_test_relationships(model):
                normalized_ref = normalize_dbt_resource_name(ref)
                if not normalized_ref:
                    continue
                edges.append(
                    DataLineageEdge(
                        source_uri=normalized_ref,
                        sink_uri=str(model_name),
                        operation_type="TRANSFORM",
                        is_dynamic=False,
                    )
                )

        logger.debug("Parsed {} dbt config edges from {}", len(edges), file_path)
        return edges

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

    def _extract_task_script_mapping(self, file_content: str) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for task_var, script_path in self._BASH_TASK_PATTERN.findall(file_content):
            mapping[task_var] = script_path
        return mapping

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

    def analyze_dag_logic(self, file_content: str, file_path: str) -> list[DataLineageEdge]:
        edges: list[DataLineageEdge] = []
        task_to_script = self._extract_task_script_mapping(file_content)

        for upstream, downstream in self._SHIFT_PATTERN.findall(file_content):
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

        for upstream, downstream in self._SET_DOWNSTREAM_PATTERN.findall(file_content):
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

        logger.debug("Parsed {} orchestration edges from {}", len(edges), file_path)
        return self._dedupe_edges(edges)


__all__ = ["DAGConfigParser", "infer_dbt_resource_name", "normalize_dbt_resource_name"]
