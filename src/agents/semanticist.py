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
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_fixed

from src.models.nodes import ModuleNode
from src.utils.tracer import CartographyTracer, InferenceMethod

try:
	from openai import OpenAI
except Exception:  # pragma: no cover - optional dependency
	OpenAI = None


def _is_rate_limit_error(exc: BaseException) -> bool:
	status_code = getattr(exc, "status_code", None)
	if status_code == 429:
		return True

	response = getattr(exc, "response", None)
	response_status = getattr(response, "status_code", None)
	if response_status == 429:
		return True

	message = str(exc).lower()
	return "rate limit" in message or "429" in message or "temporarily rate-limited" in message


def _safe_error_message(exc: BaseException) -> str:
	response = getattr(exc, "response", None)
	if response is not None:
		response_json_getter = getattr(response, "json", None)
		if callable(response_json_getter):
			try:
				payload = response_json_getter()
				if isinstance(payload, dict):
					message = payload.get("error", {}).get("message", "Unknown Error")
					if message:
						return str(message)
			except Exception:
				pass
		response_text = getattr(response, "text", None)
		if isinstance(response_text, str) and response_text.strip():
			return response_text.strip()

	args = getattr(exc, "args", ())
	if args:
		first_arg = args[0]
		if isinstance(first_arg, dict):
			message = first_arg.get("error", {}).get("message", "Unknown Error")
			return str(message)

	return str(exc) or "Unknown Error"


def _log_retry_before_sleep(retry_state: Any) -> None:
	exc = retry_state.outcome.exception() if retry_state.outcome else None
	err_message = _safe_error_message(exc) if isinstance(exc, BaseException) else "Unknown Error"
	wait_seconds = getattr(retry_state.next_action, "sleep", None)
	if wait_seconds is None:
		wait_seconds = 0
	logger.warning("Retrying Semanticist LLM call in {} seconds due to rate-limit error: {}", wait_seconds, err_message)


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
			"deepseek/deepseek-chat-v3.1": 128_000,
			"qwen/qwen3.5-9b": 128_000,
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
		bulk_model: str | None = None,
		synthesis_model: str | None = None,
		embedding_model: str = "nvidia/llama-nemotron-embed-v1-1b-v2:free",
		context_budget: ContextWindowBudget | None = None,
		http_referer: str = "https://example.com",
	) -> None:
		self.trace_path = Path(trace_path)
		self.trace_path.parent.mkdir(parents=True, exist_ok=True)
		self.tracer = CartographyTracer(self.trace_path)
		self.context_budget = context_budget or ContextWindowBudget()
		resolved_bulk_model = bulk_model or os.getenv("SEMANTICIST_BULK_MODEL", "deepseek/deepseek-chat-v3.1")
		resolved_synthesis_model = synthesis_model or os.getenv("SEMANTICIST_ARCHITECT_MODEL", "deepseek/deepseek-chat-v3.1")
		self.model_tiers: dict[ModelTier, str] = {
			ModelTier.BULK: resolved_bulk_model,
			ModelTier.SYNTHESIS: resolved_synthesis_model,
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
			response = self._create_chat_completion(model=model, prompt=prompt)
			response_text = response.choices[0].message.content
			result["status"] = "ok"
			result["response"] = response_text
		except Exception as exc:
			result["status"] = "error"
			result["response"] = _safe_error_message(exc)
			logger.exception("Semanticist LLM call failed")

		self._write_trace("semanticist.llm_call", result)
		return result

	@retry(
		retry=retry_if_exception(_is_rate_limit_error),
		wait=wait_fixed(2),
		stop=stop_after_attempt(5),
		reraise=True,
		before_sleep=_log_retry_before_sleep,
	)
	def _create_chat_completion(self, *, model: str, prompt: str):
		if self._openai_client is None:
			raise RuntimeError("LLM client unavailable")
		return self._openai_client.chat.completions.create(
			model=model,
			messages=[{"role": "user", "content": prompt}],
		)

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
		self.tracer.log_action(
			agent_name="semanticist",
			action_type=action,
			evidence_source=self._trace_evidence_source(payload),
			confidence_level=self._resolve_semantic_confidence(payload),
			inference_method=InferenceMethod.LLM_INFERENCE,
			model=str(payload.get("model", "unknown")),
			tokens_used=int(payload.get("tokens_used", 0)),
			payload=payload,
		)

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
				"purpose_statement_confidence": 0.6,
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
		self.tracer.log_action(
			agent_name="semanticist",
			action_type="purpose_statement_generated",
			evidence_source=f"{module_node.path}:1",
			confidence_level=self._resolve_semantic_confidence(semanticist_metadata),
			inference_method=InferenceMethod.LLM_INFERENCE,
			purpose_statement=parsed["purpose_statement"],
		)
		if parsed["documentation_drift"]:
			self.tracer.log_action(
				agent_name="semanticist",
				action_type="documentation_drift_detected",
				evidence_source=f"{module_node.path}:1",
				confidence_level=self._resolve_semantic_confidence(semanticist_metadata),
				inference_method=InferenceMethod.LLM_INFERENCE,
				drift_reason=parsed["drift_reason"],
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

	def answer_day_one_questions(
		self,
		*,
		surveyor_data: dict[str, Any],
		hydrologist_data: dict[str, Any],
		nodes: list[ModuleNode],
	) -> dict[str, Any]:
		"""Answer the five FDE day-one onboarding questions with evidence-backed synthesis."""
		sources, sinks = self._extract_boundary_lists(hydrologist_data)
		top_pagerank = self._top_ranked_float_map(surveyor_data.get("pagerank", {}), limit=8)
		top_velocity = self._top_ranked_int_map(surveyor_data.get("git_velocity", {}), limit=12)
		cluster_distribution = self._cluster_distribution(nodes)
		purpose_samples = self._purpose_samples(nodes, limit=12)

		evidence_pack = {
			"repo": str(surveyor_data.get("repo") or "unknown"),
			"structural_graph_metrics": {
				"node_count": int(surveyor_data.get("structural_node_count", 0) or 0),
				"edge_count": int(surveyor_data.get("structural_edge_count", 0) or 0),
				"lineage_node_count": int(surveyor_data.get("lineage_node_count", 0) or 0),
				"lineage_edge_count": int(surveyor_data.get("lineage_edge_count", 0) or 0),
			},
			"architectural_hubs": list(surveyor_data.get("architectural_hubs", []) or [])[:8],
			"top_pagerank_modules": top_pagerank,
			"git_velocity_top_changes": top_velocity,
			"lineage_sources": sources,
			"lineage_sinks": sinks,
			"cluster_distribution": cluster_distribution,
			"module_purpose_samples": purpose_samples,
		}

		prompt = self._build_day_one_questions_prompt(evidence_pack)
		llm_run = self._call_llm(
			prompt,
			ModelTier.SYNTHESIS,
			metadata={
				"action": "answer_day_one_questions",
				"source_count": len(sources),
				"sink_count": len(sinks),
				"pagerank_count": len(top_pagerank),
				"velocity_count": len(top_velocity),
			},
		)
		answers = self._parse_day_one_questions(llm_run.get("response"), evidence_pack)

		result = {
			"status": llm_run["status"],
			"questions": answers,
			"evidence_pack": evidence_pack,
			"llm_run": {
				"tier": llm_run["tier"],
				"model": llm_run["model"],
				"status": llm_run["status"],
				"tokens_used": llm_run["tokens_used"],
				"context_exceeded": llm_run["context_exceeded"],
				"estimated_paid_cost": llm_run["usage"]["estimated_paid_cost"],
			},
		}
		self._write_trace(
			"semanticist.day_one_questions",
			{
				"model": llm_run["model"],
				"tokens_used": llm_run["tokens_used"],
				"status": llm_run["status"],
				"question_count": len(answers),
			},
		)
		return result

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

	def _build_day_one_questions_prompt(self, evidence_pack: dict[str, Any]) -> str:
		questions = [
			"What is the primary data ingestion path?",
			"What are the 3-5 most critical output datasets/endpoints?",
			"What is the blast radius if the most critical module fails?",
			"Where is the business logic concentrated vs. distributed?",
			"What has changed most frequently in the last 90 days (git velocity map)?",
		]
		return (
			"You are a senior forward-deployed engineer preparing a 72-hour onboarding brief.\n"
			"Answer exactly the five questions provided below using ONLY the evidence pack.\n"
			"For every answer, include concrete citations to evidence.\n"
			"Citations must be path-and-line strings (for example src/module.py:1) or explicit graph metrics "
			"(for example pagerank[src/module.py]=0.1234, degree_centrality[src/module.py]=0.3210, lineage_edge_count=42).\n"
			"Do not invent file paths, datasets, endpoints, or metrics that are not present in the evidence pack.\n"
			"Return JSON only in this shape:\n"
			"{\"answers\":[{\"question\":\"...\",\"answer\":\"...\",\"evidence\":[\"...\",\"...\"]}]}\n"
			"Ensure all five questions are present exactly once.\n\n"
			"Questions:\n"
			+ "\n".join(f"- {question}" for question in questions)
			+ "\n\nEvidence pack:\n"
			+ json.dumps(evidence_pack, indent=2)
		)

	def _extract_boundary_lists(self, hydrologist_data: dict[str, Any]) -> tuple[list[str], list[str]]:
		boundaries = hydrologist_data.get("boundaries")
		if isinstance(boundaries, dict):
			sources = [str(item) for item in boundaries.get("ultimate_sources", []) if str(item).strip()]
			sinks = [str(item) for item in boundaries.get("ultimate_sinks", []) if str(item).strip()]
			return sources, sinks

		sources = [str(item) for item in hydrologist_data.get("ultimate_sources", []) if str(item).strip()]
		sinks = [str(item) for item in hydrologist_data.get("ultimate_sinks", []) if str(item).strip()]
		return sources, sinks

	def _top_ranked_float_map(self, payload: Any, limit: int) -> list[dict[str, Any]]:
		if not isinstance(payload, dict):
			return []
		ranked = sorted(
			[(str(key), float(value)) for key, value in payload.items()],
			key=lambda item: item[1],
			reverse=True,
		)
		return [{"path": key, "value": value} for key, value in ranked[:limit]]

	def _top_ranked_int_map(self, payload: Any, limit: int) -> list[dict[str, Any]]:
		if not isinstance(payload, dict):
			return []
		ranked = sorted(
			[(str(key), int(value)) for key, value in payload.items()],
			key=lambda item: item[1],
			reverse=True,
		)
		return [{"path": key, "value": value} for key, value in ranked[:limit]]

	def _cluster_distribution(self, nodes: list[ModuleNode]) -> list[dict[str, Any]]:
		counts: dict[str, int] = {}
		for module_node in nodes:
			cluster = module_node.metadata.get("semanticist", {}).get("domain_cluster")
			if not isinstance(cluster, str) or not cluster.strip():
				cluster = "unassigned"
			counts[cluster] = counts.get(cluster, 0) + 1
		total = sum(counts.values())
		if total <= 0:
			return []
		ranked = sorted(counts.items(), key=lambda item: item[1], reverse=True)
		return [
			{
				"cluster": cluster,
				"module_count": count,
				"share": round(count / total, 4),
			}
			for cluster, count in ranked
		]

	def _purpose_samples(self, nodes: list[ModuleNode], limit: int) -> list[dict[str, str]]:
		rows: list[dict[str, str]] = []
		for module_node in nodes:
			semanticist_metadata = module_node.metadata.get("semanticist", {})
			purpose = str(semanticist_metadata.get("purpose_statement") or "").strip()
			if not purpose:
				continue
			rows.append({"path": module_node.path, "purpose_statement": purpose})
			if len(rows) >= limit:
				break
		return rows

	def _default_day_one_questions(self, evidence_pack: dict[str, Any]) -> list[dict[str, Any]]:
		sources = evidence_pack.get("lineage_sources", [])
		sinks = evidence_pack.get("lineage_sinks", [])
		top_pagerank = evidence_pack.get("top_pagerank_modules", [])
		top_velocity = evidence_pack.get("git_velocity_top_changes", [])
		cluster_distribution = evidence_pack.get("cluster_distribution", [])

		ingestion_answer = (
			f"The likely ingestion path begins from {sources[0]} and flows into downstream transformations and sinks."
			if sources
			else "The ingestion path cannot be confidently identified from available lineage boundaries."
		)
		outputs = [str(item) for item in sinks[:5]]
		if outputs:
			outputs_text = ", ".join(outputs)
		else:
			outputs_text = "No explicit output datasets/endpoints were detected in the lineage boundary list."

		critical_module = top_pagerank[0]["path"] if top_pagerank else "unknown"
		blast_radius_answer = (
			f"Failure of {critical_module} would likely impact connected modules on the structural critical path and downstream lineage sinks."
			if top_pagerank
			else "Blast radius cannot be quantified because no PageRank evidence is available."
		)

		if cluster_distribution:
			top_cluster = cluster_distribution[0]
			logic_answer = (
				f"Business logic appears most concentrated in cluster '{top_cluster['cluster']}' "
				f"({top_cluster['module_count']} modules, share={top_cluster['share']})."
			)
		else:
			logic_answer = "Business logic concentration cannot be determined because no cluster distribution evidence is available."

		velocity_lines = [f"{row['path']} ({row['value']} changes)" for row in top_velocity[:5]]
		velocity_answer = "; ".join(velocity_lines) if velocity_lines else "No git velocity evidence is available."

		return [
			{
				"question": "What is the primary data ingestion path?",
				"answer": ingestion_answer,
				"evidence": [str(item) for item in sources[:3]] or ["lineage_sources=none"],
			},
			{
				"question": "What are the 3-5 most critical output datasets/endpoints?",
				"answer": outputs_text,
				"evidence": outputs[:5] or ["lineage_sinks=none"],
			},
			{
				"question": "What is the blast radius if the most critical module fails?",
				"answer": blast_radius_answer,
				"evidence": [
					f"pagerank[{row['path']}]={row['value']}" for row in top_pagerank[:3]
				] or ["pagerank=none"],
			},
			{
				"question": "Where is the business logic concentrated vs. distributed?",
				"answer": logic_answer,
				"evidence": [
					f"cluster[{row['cluster']}]={row['module_count']} modules (share={row['share']})"
					for row in cluster_distribution[:4]
				] or ["cluster_distribution=none"],
			},
			{
				"question": "What has changed most frequently in the last 90 days (git velocity map)?",
				"answer": velocity_answer,
				"evidence": [
					f"git_velocity[{row['path']}]={row['value']}" for row in top_velocity[:8]
				] or ["git_velocity=none"],
			},
		]

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

	def _parse_day_one_questions(self, response_text: Any, evidence_pack: dict[str, Any]) -> list[dict[str, Any]]:
		required_questions = [
			"What is the primary data ingestion path?",
			"What are the 3-5 most critical output datasets/endpoints?",
			"What is the blast radius if the most critical module fails?",
			"Where is the business logic concentrated vs. distributed?",
			"What has changed most frequently in the last 90 days (git velocity map)?",
		]
		default_answers = {
			item["question"]: item
			for item in self._default_day_one_questions(evidence_pack)
		}

		payload = self._extract_json_payload(response_text)
		if not isinstance(payload, dict):
			return [default_answers[question] for question in required_questions]

		answer_rows: list[dict[str, Any]] = []
		raw_answers = payload.get("answers")
		if isinstance(raw_answers, list):
			for item in raw_answers:
				if isinstance(item, dict):
					answer_rows.append(item)
		elif isinstance(payload.get("questions"), list):
			for item in payload.get("questions", []):
				if isinstance(item, dict):
					answer_rows.append(item)

		resolved: dict[str, dict[str, Any]] = {}
		for item in answer_rows:
			question = str(item.get("question") or "").strip()
			if not question:
				continue
			answer = str(item.get("answer") or "").strip()
			evidence = item.get("evidence")
			if isinstance(evidence, list):
				evidence_list = [str(row).strip() for row in evidence if str(row).strip()]
			else:
				evidence_list = []
			if question in required_questions:
				resolved[question] = {
					"question": question,
					"answer": answer or default_answers[question]["answer"],
					"evidence": evidence_list or list(default_answers[question]["evidence"]),
				}

		final_rows: list[dict[str, Any]] = []
		for question in required_questions:
			final_rows.append(resolved.get(question, default_answers[question]))
		return final_rows

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

	def _resolve_semantic_confidence(self, metadata: dict[str, Any]) -> float:
		for key in ("confidence_level", "purpose_statement_confidence", "llm_confidence"):
			value = metadata.get(key)
			if isinstance(value, (int, float)):
				return max(0.0, min(1.0, float(value)))
			if isinstance(value, str):
				mapped = self._map_confidence_label(value)
				if mapped is not None:
					return mapped

		cluster_confidence = metadata.get("cluster_assignment_confidence")
		if isinstance(cluster_confidence, str):
			mapped = self._map_confidence_label(cluster_confidence)
			if mapped is not None:
				return mapped
		if isinstance(cluster_confidence, (int, float)):
			return max(0.0, min(1.0, float(cluster_confidence)))

		return 0.6

	def _map_confidence_label(self, value: str) -> float | None:
		label = value.strip().lower()
		mapping = {"high": 0.9, "medium": 0.6, "low": 0.3}
		return mapping.get(label)

	def _trace_evidence_source(self, payload: dict[str, Any]) -> str:
		usage = payload.get("usage")
		if isinstance(usage, dict):
			metadata = usage.get("metadata")
			if isinstance(metadata, dict):
				module_path = metadata.get("module_path")
				if isinstance(module_path, str) and module_path.strip():
					return f"{module_path}:1"
		return "semanticist.py:1"

