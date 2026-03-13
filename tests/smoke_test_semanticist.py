from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
	sys.path.insert(0, str(REPO_ROOT))

from src.agents.archivist import Archivist
from src.agents.semanticist import Semanticist
from src.models.nodes import ModuleNode


class _Msg:
	def __init__(self, content: str) -> None:
		self.content = content


class _Choice:
	def __init__(self, content: str) -> None:
		self.message = _Msg(content)


class _Resp:
	def __init__(self, content: str) -> None:
		self.choices = [_Choice(content)]


class _Completions:
	def create(self, *, model: str, messages: list[dict[str, str]]) -> _Resp:
		prompt = messages[0]["content"]

		if "Return JSON only with keys: purpose_statement, documentation_drift, drift_reason" in prompt:
			if "delete_user_records" in prompt:
				return _Resp(
					'{"purpose_statement":"Deletes user database records for retention and compliance workflows.",'
					'"documentation_drift":true,'
					'"drift_reason":"The docstring claims math helpers, but the implementation performs destructive database operations."}'
				)
			if "login_user" in prompt:
				return _Resp(
					'{"purpose_statement":"Handles user login and session creation for authenticated access.",'
					'"documentation_drift":false,'
					'"drift_reason":""}'
				)
			if "render_ui_and_stream_network" in prompt:
				return _Resp(
					'{"purpose_statement":"Combines UI rendering, network transport, and filesystem coordination in one module.",'
					'"documentation_drift":false,'
					'"drift_reason":""}'
				)
			return _Resp(
				'{"purpose_statement":"Persists binary and text files to durable storage backends.",'
				'"documentation_drift":false,'
				'"drift_reason":""}'
			)

		if "software architecture heatmap" in prompt:
			return _Resp(
				"Intro analysis text before JSON.\n```json\n"
				'{"clusters":[{"name":"Core Operations","definition":"Supports core user and storage workflows."},'
				'{"name":"Data Governance","definition":"Manages data lifecycle and retention-sensitive operations."},'
				'{"name":"Integration Surface","definition":"Handles cross-domain integration and orchestration concerns."},'
				'{"name":"Platform Reliability","definition":"Maintains runtime consistency and resilience controls."}]}'
				"\n```\nAdditional notes after JSON."
			)

		if "best-fit architectural domain cluster" in prompt:
			if "auth.py" in prompt or "storage.py" in prompt:
				return _Resp(
					'{"domain_cluster":"Core Operations","confidence":"high",'
					'"is_architectural_outlier":false,"outlier_reason":""}'
				)
			if "spaghetti.py" in prompt:
				return _Resp(
					'{"domain_cluster":null,"confidence":"low",'
					'"is_architectural_outlier":true,'
					'"outlier_reason":"This module spans network, UI, and file I/O concerns across domains."}'
				)
			return _Resp(
				'{"domain_cluster":"Data Governance","confidence":"medium",'
				'"is_architectural_outlier":false,"outlier_reason":""}'
			)

		if "Day-One Onboarding Brief" in prompt:
			return _Resp(
				"Lead-in text.\n```json\n"
				'{"primary_business_mission":"Enable reliable understanding and operation of a brownfield codebase.",'
				'"critical_path_clusters":["Core Operations","Data Governance","Platform Reliability"],'
				'"top_technical_risks":["Drift in destructive data modules can mislead maintainers.",'
				'"Cross-domain outliers increase regression risk and ownership ambiguity.",'
				'"Incomplete architecture boundaries slow safe onboarding and change planning."],'
				'"mental_model":"The system is a map-making pipeline that turns scattered code signals into operational guidance."}'
				"\n```"
			)

		return _Resp('{"status":"unsupported_prompt"}')


class _Chat:
	def __init__(self) -> None:
		self.completions = _Completions()


class _Client:
	def __init__(self) -> None:
		self.chat = _Chat()


def run_smoke_test() -> None:
	semanticist = Semanticist()
	semanticist._openai_client = _Client()

	node_a = ModuleNode(
		path="auth.py",
		language="python",
		metadata={
			"source_code": '"""Handles user login and session creation."""\n\n'
			'\ndef login_user(username: str, password: str) -> bool:\n    return True\n',
			"semanticist": {"purpose_statement": "Handles user login and session creation for authenticated access."},
		},
	)

	node_b = ModuleNode(
		path="data_utils.py",
		language="python",
		metadata={
			"source_code": '"""Math utilities for arithmetic helpers."""\n\n'
			'\ndef delete_user_records(user_id: str) -> None:\n    pass\n',
		},
	)

	node_c = ModuleNode(
		path="spaghetti.py",
		language="python",
		metadata={
			"source_code": '"""Mixed orchestration utility."""\n\n'
			'\ndef render_ui_and_stream_network(path: str) -> None:\n    pass\n',
			"semanticist": {
				"purpose_statement": "Coordinates network, UI rendering, and file I/O in one flow."
			},
		},
	)

	node_d = ModuleNode(
		path="storage.py",
		language="python",
		metadata={
			"source_code": '"""Save files to durable storage."""\n\n'
			'\ndef save_file(name: str, content: bytes) -> str:\n    return name\n',
			"semanticist": {"purpose_statement": "Persists files and retrieval metadata to storage backends."},
		},
	)

	nodes = [node_a, node_b, node_c, node_d]

	# Step 2 Check: Purpose extraction + documentation drift on Node B
	semanticist.generate_purpose_statement(node_b)
	assert node_b.metadata["semanticist"]["documentation_drift"] is True

	# Step 3 Check: Cluster generation and module assignment
	clusters = semanticist.identify_domain_clusters(nodes)
	semanticist.assign_modules_to_clusters(nodes, clusters)
	assert node_a.metadata["semanticist"]["domain_cluster"] == node_d.metadata["semanticist"]["domain_cluster"]
	assert node_c.metadata["semanticist"]["is_architectural_outlier"] is True

	# Step 4 Check: Day-One brief + report generation
	outliers = [
		node for node in nodes if node.metadata.get("semanticist", {}).get("is_architectural_outlier", False)
	]
	brief = semanticist.generate_day_one_brief(clusters, outliers)
	report_path = Archivist().write_cartography_report(brief, clusters, nodes)

	# Validation
	assert report_path == Path(".cartography") / "CODEBASE.md"
	assert report_path.exists()
	print(f"estimated_cost_saved={semanticist.context_budget.estimated_cost_saved:.6f}")
	print("smoke_test_semanticist: PASS")


if __name__ == "__main__":
	run_smoke_test()
