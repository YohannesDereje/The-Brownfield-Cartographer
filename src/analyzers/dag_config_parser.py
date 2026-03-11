from __future__ import annotations

import re
from typing import Any

from loguru import logger
import yaml

from src.models.nodes import DataLineageEdge


class DAGConfigParser:
    """Parses lightweight DAG/config relationships from Airflow and dbt YAML."""

    _SHIFT_PATTERN = re.compile(r"\b([A-Za-z_][\w]*)\b\s*>>\s*\b([A-Za-z_][\w]*)\b")
    _SET_DOWNSTREAM_PATTERN = re.compile(
        r"\b([A-Za-z_][\w]*)\b\.set_downstream\(\s*([A-Za-z_][\w]*)\s*\)"
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
        if not file_path.lower().endswith(("schema.yml", "schema.yaml", "sources.yml", "sources.yaml")):
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
            model_name = model.get("name")
            if not model_name:
                continue
            meta = model.get("meta") if isinstance(model.get("meta"), dict) else {}
            for key in ("sources", "depends_on_sources", "upstream_sources", "seeds", "depends_on_seeds"):
                value = meta.get(key)
                if isinstance(value, list):
                    for ref in value:
                        edges.append(
                            DataLineageEdge(
                                source_uri=str(ref),
                                sink_uri=str(model_name),
                                operation_type="TRANSFORM",
                                is_dynamic=False,
                            )
                        )

        logger.debug("Parsed {} dbt config edges from {}", len(edges), file_path)
        return edges


__all__ = ["DAGConfigParser"]
