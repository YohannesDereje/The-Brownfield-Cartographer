from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
import json
from pathlib import Path
from typing import Any


class InferenceMethod(str, Enum):
	STATIC_ANALYSIS = "STATIC_ANALYSIS"
	LLM_INFERENCE = "LLM_INFERENCE"


class CartographyTracer:
	"""Writes a unified JSONL audit trail for architectural claims."""

	def __init__(self, trace_path: str | Path = ".cartography/cartography_trace.jsonl") -> None:
		self.trace_path = Path(trace_path)
		self.trace_path.parent.mkdir(parents=True, exist_ok=True)

	def log_action(
		self,
		*,
		agent_name: str,
		action_type: str,
		evidence_source: str,
		confidence_level: float,
		inference_method: InferenceMethod | str,
		**extra: Any,
	) -> None:
		record: dict[str, Any] = {
			"timestamp": datetime.now(timezone.utc).isoformat(),
			"agent_name": agent_name,
			"action_type": action_type,
			"evidence_source": evidence_source or "unknown:1",
			"confidence_level": self._normalize_confidence(confidence_level),
			"inference_method": self._normalize_inference_method(inference_method),
		}
		if extra:
			record.update(extra)

		with self.trace_path.open("a", encoding="utf-8") as handle:
			handle.write(json.dumps(record, ensure_ascii=False) + "\n")

	def _normalize_confidence(self, confidence_level: float) -> float:
		try:
			value = float(confidence_level)
		except (TypeError, ValueError):
			value = 0.0
		return max(0.0, min(1.0, value))

	def _normalize_inference_method(self, inference_method: InferenceMethod | str) -> str:
		if isinstance(inference_method, InferenceMethod):
			return inference_method.value
		return str(inference_method)


__all__ = ["CartographyTracer", "InferenceMethod"]
