from __future__ import annotations

import ast
from datetime import datetime, timezone
from enum import Enum
import json
import os
from pathlib import Path
import re
from typing import Any

from loguru import logger

from src.models.nodes import ModuleNode

try:
	from openai import OpenAI
except Exception:  # pragma: no cover - optional dependency
	OpenAI = None


class ModelTier(str, Enum):
	BULK = "bulk"
	SYNTHESIS = "synthesis"
	EMBEDDING = "embedding"


class ContextWindowBudget:
	"""Tracks token usage and estimated cost savings for free-model routing."""

	def __init__(
		self,
		model_context_windows: dict[str, int] | None = None,
		gpt4_reference_price_per_1k_tokens: float = 0.03,
	) -> None:
		self.model_context_windows = model_context_windows or {
			"stepfun/step-3.5-flash:free": 256_000,
			"meta-llama/llama-3.3-70b-instruct:free": 131_072,
			"nvidia/llama-nemotron-embed-v1-1b-v2:free": 8_192,
		}
		self.gpt4_reference_price_per_1k_tokens = gpt4_reference_price_per_1k_tokens
		self.total_tokens_processed: int = 0
		self.estimated_cost_saved: float = 0.0
		self.ledger: list[dict[str, Any]] = []

	def estimate_tokens(self, text: str) -> int:
		"""Estimate token count using a 4-characters-per-token approximation."""
		if not text:
			return 0
		return max(1, int(len(text) / 4))

	def exceeds_context_window(self, content: str, model: str) -> bool:
		"""Return True if content likely exceeds the model's context window."""
		window = self.model_context_windows.get(model)
		if window is None:
			return False
		return self.estimate_tokens(content) > window

	def record_usage(self, *, model: str, tokens: int, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
		"""Record token usage and estimate savings vs a paid GPT-4 baseline."""
		estimated_paid_cost = (tokens / 1000.0) * self.gpt4_reference_price_per_1k_tokens
		self.total_tokens_processed += tokens
		self.estimated_cost_saved += estimated_paid_cost

		entry: dict[str, Any] = {
			"timestamp": datetime.now(timezone.utc).isoformat(),
			"model": model,
			"tokens_used": tokens,
			"estimated_paid_cost": estimated_paid_cost,
			"running_estimated_cost_saved": self.estimated_cost_saved,
		}
		if metadata:
			entry["metadata"] = metadata
		self.ledger.append(entry)
		return entry


class Semanticist:
	"""Governance, budgeting, and connectivity layer for semantic analysis."""

	def __init__(
		self,
		trace_path: str | Path = ".cartography/cartography_trace.jsonl",
		bulk_model: str = "stepfun/step-3.5-flash:free",
		synthesis_model: str = "meta-llama/llama-3.3-70b-instruct:free",
		embedding_model: str = "nvidia/llama-nemotron-embed-v1-1b-v2:free",
		context_budget: ContextWindowBudget | None = None,
		http_referer: str = "https://example.com",
	) -> None:
		self.trace_path = Path(trace_path)
		self.trace_path.parent.mkdir(parents=True, exist_ok=True)
		self.context_budget = context_budget or ContextWindowBudget()
		self.model_tiers: dict[ModelTier, str] = {
			ModelTier.BULK: bulk_model,
			ModelTier.SYNTHESIS: synthesis_model,
			ModelTier.EMBEDDING: embedding_model,
		}
		self.http_referer = http_referer
		self.app_title = "The Brownfield Cartographer"
		self.base_url = "https://openrouter.ai/api/v1"
		self._openai_client = None
		self.openrouter_key = self._load_openrouter_api_key()
		if OpenAI is not None:
			try:
				if self.openrouter_key:
					self._openai_client = OpenAI(
						api_key=self.openrouter_key,
						base_url=self.base_url,
						default_headers={
							"HTTP-Referer": self.http_referer,
							"X-Title": self.app_title,
						},
					)
				else:
					logger.warning("OPENROUTER_API_KEY not found; Semanticist running in simulated mode.")
			except Exception as exc:
				logger.warning("OpenRouter client initialization failed; running in simulated mode: {}", exc)

	def _load_openrouter_api_key(self, env_file_path: str | Path = ".env") -> str | None:
		"""Load OPENROUTER_API_KEY from environment first, then from .env file."""
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

	def get_model_for_tier(self, tier: ModelTier | str) -> str:
		resolved_tier = ModelTier(tier)
		return self.model_tiers[resolved_tier]

	def _call_llm(
		self,
		prompt: str,
		tier: ModelTier | str = ModelTier.BULK,
		*,
		metadata: dict[str, Any] | None = None,
	) -> dict[str, Any]:
		"""Call LLM tier with pre-call budget update and trace logging."""
		resolved_tier = ModelTier(tier)
		model = self.get_model_for_tier(resolved_tier)
		estimated_tokens = self.context_budget.estimate_tokens(prompt)
		context_exceeded = self.context_budget.exceeds_context_window(prompt, model)

		usage_entry = self.context_budget.record_usage(
			model=model,
			tokens=estimated_tokens,
			metadata={"tier": resolved_tier.value, **(metadata or {})},
		)

		result: dict[str, Any] = {
			"tier": resolved_tier.value,
			"model": model,
			"tokens_used": estimated_tokens,
			"context_exceeded": context_exceeded,
			"usage": usage_entry,
			"status": "skipped",
			"response": None,
		}

		if context_exceeded:
			result["status"] = "context_exceeded"
			self._write_trace("semanticist.llm_call", result)
			logger.warning("Semanticist context window exceeded for model {}", model)
			return result

		if self._openai_client is None:
			result["status"] = "simulated"
			result["response"] = "LLM client unavailable; governance-only simulation completed."
			self._write_trace("semanticist.llm_call", result)
			return result

		try:
			response = self._openai_client.chat.completions.create(
				model=model,
				messages=[{"role": "user", "content": prompt}],
			)
			response_text = response.choices[0].message.content
			result["status"] = "ok"
			result["response"] = response_text
		except Exception as exc:
			result["status"] = "error"
			result["response"] = str(exc)
			logger.exception("Semanticist LLM call failed")

		self._write_trace("semanticist.llm_call", result)
		return result

	def annotate_module_with_llm_run(
		self,
		module_node: ModuleNode,
		*,
		tier: ModelTier | str = ModelTier.BULK,
		prompt: str | None = None,
	) -> ModuleNode:
		"""Attach governance metadata from an LLM run to a module node.

		This method intentionally does not implement purpose extraction logic.
		"""
		resolved_tier = ModelTier(tier)
		llm_prompt = prompt or (
			"Governance connectivity check for module "
			f"{module_node.path}. Do not perform semantic extraction."
		)

		llm_run = self._call_llm(
			llm_prompt,
			resolved_tier,
			metadata={"module_path": module_node.path},
		)

		module_node.metadata.setdefault("semanticist", {})
		module_node.metadata["semanticist"].update(
			{
				"llm_run": {
					"tier": llm_run["tier"],
					"model": llm_run["model"],
					"status": llm_run["status"],
					"tokens_used": llm_run["tokens_used"],
					"context_exceeded": llm_run["context_exceeded"],
					"estimated_paid_cost": llm_run["usage"]["estimated_paid_cost"],
				},
				"budget_totals": {
					"total_tokens_processed": self.context_budget.total_tokens_processed,
					"estimated_cost_saved": self.context_budget.estimated_cost_saved,
				},
			}
		)
		return module_node

	def _write_trace(self, action: str, payload: dict[str, Any]) -> None:
		tokens_used = int(payload.get("tokens_used", 0))
		model = str(payload.get("model", "unknown"))
		record = {
			"timestamp": datetime.now(timezone.utc).isoformat(),
			"agent": "semanticist",
			"model": model,
			"tokens_used": tokens_used,
			"agent_action": action,
			"payload": payload,
		}
		with self.trace_path.open("a", encoding="utf-8") as handle:
			handle.write(json.dumps(record, ensure_ascii=False) + "\n")

	def generate_purpose_statement(self, module_node: ModuleNode) -> ModuleNode:
		"""Generate a business-purpose summary and doc drift assessment for a module."""
		semanticist_metadata = module_node.metadata.setdefault("semanticist", {})
		source_text, read_status = self._load_module_source(module_node)
		if read_status != "ok" or not source_text.strip():
			semanticist_metadata.update(
				{
					"status": "no_implementation_evidence",
					"purpose_statement": "No implementation evidence found",
					"documentation_drift": False,
					"drift_reason": "No implementation evidence found.",
				}
			)
			return module_node

		existing_docstring = self._extract_existing_docstring(module_node, source_text)
		prompt = self._build_purpose_prompt(source_text, existing_docstring)
		model = self.get_model_for_tier(ModelTier.BULK)
		if self.context_budget.exceeds_context_window(prompt, model):
			semanticist_metadata.update(
				{
					"status": "context_exceeded",
					"purpose_statement": "No implementation evidence found",
					"documentation_drift": False,
					"drift_reason": "Module source exceeds the bulk model context window.",
				}
			)
			return module_node

		llm_run = self._call_llm(
			prompt,
			ModelTier.BULK,
			metadata={"module_path": module_node.path, "action": "generate_purpose_statement"},
		)
		parsed = self._parse_purpose_response(llm_run.get("response"))

		semanticist_metadata.update(
			{
				"status": llm_run["status"],
				"purpose_statement": parsed["purpose_statement"],
				"documentation_drift": parsed["documentation_drift"],
				"drift_reason": parsed["drift_reason"],
				"llm_run": {
					"tier": llm_run["tier"],
					"model": llm_run["model"],
					"status": llm_run["status"],
					"tokens_used": llm_run["tokens_used"],
					"context_exceeded": llm_run["context_exceeded"],
					"estimated_paid_cost": llm_run["usage"]["estimated_paid_cost"],
				},
				"budget_totals": {
					"total_tokens_processed": self.context_budget.total_tokens_processed,
					"estimated_cost_saved": self.context_budget.estimated_cost_saved,
				},
			}
		)
		return module_node

	def analyze_repository_semantics(self, nodes: list[ModuleNode]) -> list[ModuleNode]:
		"""Apply semantic purpose extraction across repository module nodes."""
		analyzed_nodes: list[ModuleNode] = []
		for module_node in nodes:
			analyzed_nodes.append(self.generate_purpose_statement(module_node))
		return analyzed_nodes

	def identify_domain_clusters(self, nodes: list[ModuleNode]) -> dict[str, str]:
		"""Group module purposes into a small set of domain clusters using the synthesis model."""
		purposes = self._collect_purpose_statements(nodes)
		if not purposes:
			return {}

		prompt = self._build_domain_cluster_prompt(purposes)
		llm_run = self._call_llm(
			prompt,
			ModelTier.SYNTHESIS,
			metadata={"action": "identify_domain_clusters", "purpose_count": len(purposes)},
		)
		clusters = self._parse_domain_clusters(llm_run.get("response"))
		self._write_trace(
			"semanticist.domain_clusters",
			{
				"model": llm_run["model"],
				"tokens_used": llm_run["tokens_used"],
				"cluster_count": len(clusters),
				"status": llm_run["status"],
				"clusters": clusters,
			},
		)
		return clusters

	def assign_modules_to_clusters(self, nodes: list[ModuleNode], clusters: dict[str, str]) -> list[ModuleNode]:
		"""Assign each module to the best-fit domain cluster and flag architectural outliers."""
		if not clusters:
			for module_node in nodes:
				semanticist_metadata = module_node.metadata.setdefault("semanticist", {})
				semanticist_metadata["domain_cluster"] = None
				semanticist_metadata["is_architectural_outlier"] = True
				semanticist_metadata["outlier_reason"] = "No domain clusters were available for assignment."
			return nodes

		cluster_catalog = self._format_cluster_catalog(clusters)
		for module_node in nodes:
			semanticist_metadata = module_node.metadata.setdefault("semanticist", {})
			purpose_statement = str(semanticist_metadata.get("purpose_statement") or "").strip()
			if not purpose_statement or purpose_statement == "No implementation evidence found":
				semanticist_metadata["domain_cluster"] = None
				semanticist_metadata["is_architectural_outlier"] = True
				semanticist_metadata["outlier_reason"] = "No purpose statement was available for clustering."
				continue

			prompt = self._build_cluster_assignment_prompt(module_node, purpose_statement, cluster_catalog)
			llm_run = self._call_llm(
				prompt,
				ModelTier.BULK,
				metadata={"action": "assign_modules_to_clusters", "module_path": module_node.path},
			)
			assignment = self._parse_cluster_assignment(llm_run.get("response"), clusters)
			semanticist_metadata["domain_cluster"] = assignment["domain_cluster"]
			semanticist_metadata["is_architectural_outlier"] = assignment["is_architectural_outlier"]
			semanticist_metadata["outlier_reason"] = assignment["outlier_reason"]
			semanticist_metadata["cluster_assignment_confidence"] = assignment["confidence"]
			semanticist_metadata.setdefault("llm_run", {})
			semanticist_metadata["llm_run"]["cluster_assignment_model"] = llm_run["model"]
			semanticist_metadata["llm_run"]["cluster_assignment_status"] = llm_run["status"]
		return nodes

	def detect_architectural_outliers(self, nodes: list[ModuleNode]) -> list[ModuleNode]:
		"""Normalize outlier flags so orchestrators can call this stage explicitly."""
		for module_node in nodes:
			semanticist_metadata = module_node.metadata.setdefault("semanticist", {})
			semanticist_metadata["is_architectural_outlier"] = bool(
				semanticist_metadata.get("is_architectural_outlier", False)
			)
		return nodes

	def generate_day_one_brief(self, clusters: dict[str, str], outliers: list[ModuleNode]) -> dict[str, Any]:
		"""Generate a structured Day-One onboarding brief from cluster and outlier context."""
		if not clusters and not outliers:
			return {
				"status": "no_architecture_evidence",
				"primary_business_mission": "No architecture evidence found.",
				"critical_path_clusters": [],
				"top_technical_risks": [],
				"mental_model": "No architecture evidence found.",
			}

		prompt = self._build_day_one_brief_prompt(clusters, outliers)
		llm_run = self._call_llm(
			prompt,
			ModelTier.SYNTHESIS,
			metadata={
				"action": "generate_day_one_brief",
				"cluster_count": len(clusters),
				"outlier_count": len(outliers),
			},
		)
		brief = self._parse_day_one_brief(llm_run.get("response"))
		brief.update(
			{
				"status": llm_run["status"],
				"llm_run": {
					"tier": llm_run["tier"],
					"model": llm_run["model"],
					"status": llm_run["status"],
					"tokens_used": llm_run["tokens_used"],
					"context_exceeded": llm_run["context_exceeded"],
					"estimated_paid_cost": llm_run["usage"]["estimated_paid_cost"],
				},
				"budget_totals": {
					"total_tokens_processed": self.context_budget.total_tokens_processed,
					"estimated_cost_saved": self.context_budget.estimated_cost_saved,
				},
			}
		)
		self._write_trace(
			"semanticist.day_one_brief",
			{
				"model": llm_run["model"],
				"tokens_used": llm_run["tokens_used"],
				"status": llm_run["status"],
				"critical_path_clusters": brief.get("critical_path_clusters", []),
			},
		)
		return brief

	def _load_module_source(self, module_node: ModuleNode) -> tuple[str, str]:
		metadata = module_node.metadata or {}
		for key in ("source_code", "source", "content"):
			value = metadata.get(key)
			if isinstance(value, str):
				return value, "ok"

		try:
			path = Path(module_node.path)
			if not path.exists() or not path.is_file():
				return "", "unreadable"
			return path.read_text(encoding="utf-8", errors="replace"), "ok"
		except Exception as exc:
			logger.warning("Failed to read module source for {}: {}", module_node.path, exc)
			return "", "unreadable"

	def _extract_existing_docstring(self, module_node: ModuleNode, source_text: str) -> str | None:
		metadata = module_node.metadata or {}
		for key in ("docstring", "module_docstring"):
			value = metadata.get(key)
			if isinstance(value, str) and value.strip():
				return value.strip()

		if (module_node.language or "").lower() != "python" and Path(module_node.path).suffix.lower() != ".py":
			return None

		try:
			parsed = ast.parse(source_text)
			docstring = ast.get_docstring(parsed)
			return docstring.strip() if docstring else None
		except Exception:
			return None

	def _build_purpose_prompt(self, source_text: str, existing_docstring: str | None) -> str:
		docstring_text = existing_docstring or "<missing>"
		return (
			"You are analyzing a source module to describe its business purpose.\n"
			"Read the raw implementation and summarize what this module contributes to the system.\n"
			"Do not describe how it is implemented. Avoid naming specific libraries, APIs, loops, or syntax.\n"
			"Write the purpose statement in 2-3 concise sentences.\n"
			"Also compare the implementation against the existing module docstring.\n"
			"If the docstring is misleading, missing, stale, or contradictory, set documentation_drift to true and explain why in one sentence.\n"
			"Return JSON only with keys: purpose_statement, documentation_drift, drift_reason.\n\n"
			f"Existing docstring:\n{docstring_text}\n\n"
			f"Raw source code:\n```\n{source_text}\n```"
		)

	def _collect_purpose_statements(self, nodes: list[ModuleNode]) -> list[dict[str, str]]:
		purposes: list[dict[str, str]] = []
		for module_node in nodes:
			semanticist_metadata = module_node.metadata.get("semanticist", {})
			purpose_statement = str(semanticist_metadata.get("purpose_statement") or "").strip()
			if not purpose_statement or purpose_statement == "No implementation evidence found":
				continue
			purposes.append({"path": module_node.path, "purpose_statement": purpose_statement})
		return purposes

	def _build_domain_cluster_prompt(self, purposes: list[dict[str, str]]) -> str:
		purpose_lines = [
			f"- {item['path']}: {item['purpose_statement']}"
			for item in purposes
		]
		return (
			"You are synthesizing a software architecture heatmap from module purpose statements.\n"
			"Propose 4 to 7 domain clusters that best organize the system into coherent business domains.\n"
			"Each cluster must have a short name and a one-sentence definition.\n"
			"Return JSON only in this shape: {\"clusters\": [{\"name\": \"...\", \"definition\": \"...\"}]}.\n\n"
			"Purpose statements:\n"
			+ "\n".join(purpose_lines)
		)

	def _format_cluster_catalog(self, clusters: dict[str, str]) -> str:
		return "\n".join(f"- {name}: {definition}" for name, definition in clusters.items())

	def _build_cluster_assignment_prompt(
		self,
		module_node: ModuleNode,
		purpose_statement: str,
		cluster_catalog: str,
	) -> str:
		return (
			"You are assigning a software module to the best-fit architectural domain cluster.\n"
			"Choose the single best cluster based on the module's business purpose.\n"
			"If the module spans too many domains or cannot be placed confidently, mark it as an architectural outlier.\n"
			"Return JSON only with keys: domain_cluster, confidence, is_architectural_outlier, outlier_reason.\n\n"
			f"Module path: {module_node.path}\n"
			f"Purpose statement: {purpose_statement}\n\n"
			f"Available clusters:\n{cluster_catalog}\n"
		)

	def _build_day_one_brief_prompt(self, clusters: dict[str, str], outliers: list[ModuleNode]) -> str:
		cluster_lines = [f"- {name}: {definition}" for name, definition in clusters.items()]
		outlier_lines = self._summarize_outliers(outliers)
		return (
			"You are writing a Day-One Onboarding Brief for a software engineer joining this codebase.\n"
			"Use the domain cluster definitions and architectural outlier summaries to answer these four questions:\n"
			"1. What is the primary business mission of this codebase?\n"
			"2. Which 3 clusters represent the Critical Path?\n"
			"3. Identify the top 3 technical risks based on Documentation Drift and Outliers.\n"
			"4. Provide a 1-sentence Mental Model for the system architecture.\n"
			"Return JSON only with keys: primary_business_mission, critical_path_clusters, top_technical_risks, mental_model.\n"
			"critical_path_clusters must be a list of exactly 3 cluster names when possible.\n"
			"top_technical_risks must be a list of 3 concise sentences when possible.\n\n"
			"Domain clusters:\n"
			+ "\n".join(cluster_lines or ["- None available"])
			+ "\n\nArchitectural outliers and drift signals:\n"
			+ "\n".join(outlier_lines or ["- None identified"])
		)

	def _summarize_outliers(self, outliers: list[ModuleNode]) -> list[str]:
		results: list[str] = []
		for module_node in outliers:
			semanticist_metadata = module_node.metadata.get("semanticist", {})
			domain_cluster = semanticist_metadata.get("domain_cluster") or "unassigned"
			drift = bool(semanticist_metadata.get("documentation_drift", False))
			drift_reason = str(semanticist_metadata.get("drift_reason") or "No drift reason recorded.").strip()
			outlier_reason = str(semanticist_metadata.get("outlier_reason") or "No outlier reason recorded.").strip()
			purpose_statement = str(semanticist_metadata.get("purpose_statement") or "No purpose statement recorded.").strip()
			results.append(
				f"- {module_node.path}: cluster={domain_cluster}; purpose={purpose_statement}; "
				f"documentation_drift={drift}; drift_reason={drift_reason}; outlier_reason={outlier_reason}"
			)
		return results

	def _parse_domain_clusters(self, response_text: Any) -> dict[str, str]:
		payload = self._extract_json_payload(response_text)
		if not isinstance(payload, dict):
			return {}

		clusters = payload.get("clusters")
		if not isinstance(clusters, list):
			return {}

		results: dict[str, str] = {}
		for item in clusters:
			if not isinstance(item, dict):
				continue
			name = str(item.get("name") or "").strip()
			definition = str(item.get("definition") or "").strip()
			if name and definition:
				results[name] = definition
		return results

	def _parse_cluster_assignment(self, response_text: Any, clusters: dict[str, str]) -> dict[str, Any]:
		default = {
			"domain_cluster": None,
			"confidence": "low",
			"is_architectural_outlier": True,
			"outlier_reason": "The module could not be confidently assigned to a domain cluster.",
		}
		payload = self._extract_json_payload(response_text)
		if not isinstance(payload, dict):
			return default

		domain_cluster = payload.get("domain_cluster")
		confidence = str(payload.get("confidence") or default["confidence"]).strip().lower()
		is_architectural_outlier = bool(payload.get("is_architectural_outlier", False))
		outlier_reason = str(payload.get("outlier_reason") or "").strip()

		if not isinstance(domain_cluster, str) or domain_cluster not in clusters:
			domain_cluster = None
			is_architectural_outlier = True

		if confidence not in {"high", "medium", "low"}:
			confidence = default["confidence"]

		if domain_cluster is None and not outlier_reason:
			outlier_reason = default["outlier_reason"]
		if confidence == "low":
			is_architectural_outlier = True
		if not outlier_reason and is_architectural_outlier:
			outlier_reason = default["outlier_reason"]

		return {
			"domain_cluster": domain_cluster,
			"confidence": confidence,
			"is_architectural_outlier": is_architectural_outlier,
			"outlier_reason": outlier_reason,
		}

	def _parse_day_one_brief(self, response_text: Any) -> dict[str, Any]:
		default = {
			"primary_business_mission": "Unable to determine the codebase mission from available evidence.",
			"critical_path_clusters": [],
			"top_technical_risks": [],
			"mental_model": "The architecture could not be synthesized from available evidence.",
		}
		payload = self._extract_json_payload(response_text)
		if not isinstance(payload, dict):
			return default

		mission = str(payload.get("primary_business_mission") or default["primary_business_mission"]).strip()
		mental_model = str(payload.get("mental_model") or default["mental_model"]).strip()

		critical_path_clusters = payload.get("critical_path_clusters")
		if not isinstance(critical_path_clusters, list):
			critical_path_clusters = []
		critical_path_clusters = [str(item).strip() for item in critical_path_clusters if str(item).strip()]

		top_technical_risks = payload.get("top_technical_risks")
		if not isinstance(top_technical_risks, list):
			top_technical_risks = []
		top_technical_risks = [str(item).strip() for item in top_technical_risks if str(item).strip()]

		return {
			"primary_business_mission": mission,
			"critical_path_clusters": critical_path_clusters[:3],
			"top_technical_risks": top_technical_risks[:3],
			"mental_model": mental_model,
		}

	def _extract_json_payload(self, response_text: Any) -> Any:
		if not isinstance(response_text, str) or not response_text.strip():
			return None

		payload = response_text.strip()
		try:
			return json.loads(payload)
		except json.JSONDecodeError:
			pass

		for match in re.finditer(r"```(?:json)?\s*(\{.*?\})\s*```", payload, flags=re.DOTALL | re.IGNORECASE):
			candidate = match.group(1).strip()
			try:
				return json.loads(candidate)
			except json.JSONDecodeError:
				continue

		decoder = json.JSONDecoder()
		for start_index, char in enumerate(payload):
			if char != "{":
				continue
			try:
				candidate, _ = decoder.raw_decode(payload[start_index:])
				if isinstance(candidate, (dict, list)):
					return candidate
			except json.JSONDecodeError:
				continue

		return None

	def _parse_purpose_response(self, response_text: Any) -> dict[str, Any]:
		default = {
			"purpose_statement": "No implementation evidence found",
			"documentation_drift": False,
			"drift_reason": "No drift assessment available.",
		}
		if not isinstance(response_text, str) or not response_text.strip():
			return default

		data = self._extract_json_payload(response_text)
		if not isinstance(data, dict):
			payload = response_text.strip()
			return {
				"purpose_statement": payload,
				"documentation_drift": False,
				"drift_reason": "LLM response was not structured JSON.",
			}

		purpose_statement = str(data.get("purpose_statement") or default["purpose_statement"]).strip()
		drift_reason = str(data.get("drift_reason") or default["drift_reason"]).strip()
		documentation_drift = bool(data.get("documentation_drift", False))
		return {
			"purpose_statement": purpose_statement or default["purpose_statement"],
			"documentation_drift": documentation_drift,
			"drift_reason": drift_reason or default["drift_reason"],
		}

