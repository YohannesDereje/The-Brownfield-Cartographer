from pathlib import Path

from src.cli import main


def test_cli_generates_required_artifacts(tmp_path: Path) -> None:
	repo = tmp_path / "mini_repo"
	repo.mkdir(parents=True, exist_ok=True)

	(repo / "sample.py").write_text(
		"import pandas as pd\n"
		"df = pd.read_csv('input.csv')\n"
		"df.to_csv('output.csv')\n",
		encoding="utf-8",
	)
	(repo / "input.csv").write_text("id,name\n1,Alice\n", encoding="utf-8")

	exit_code = main(["analyze", "--repo", str(repo)])
	assert exit_code == 0

	artifact_dir = repo / ".cartography"
	module_graph = artifact_dir / "module_graph.json"
	lineage_graph = artifact_dir / "lineage_graph.json"

	assert module_graph.exists(), "Expected module_graph.json to be generated"
	assert lineage_graph.exists(), "Expected lineage_graph.json to be generated"
