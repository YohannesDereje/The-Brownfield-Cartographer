from pathlib import Path


def touch(path: Path) -> None:
    """Create an empty file if it does not already exist."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)


def main() -> None:
    root = Path.cwd()

    directories = [
        root / "src",
        root / "src" / "models",
        root / "src" / "analyzers",
        root / "src" / "agents",
        root / "src" / "graph",
        root / "src" / "utils",
        root / ".cartography",
    ]

    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)

    # Mark all created directories as packages per requested scaffold.
    for package_dir in directories:
        touch(package_dir / "__init__.py")

    files = [
        root / "pyproject.toml",
        root / "README.md",
        root / ".gitignore",
        root / "RECONNAISSANCE.md",
        root / "src" / "cli.py",
        root / "src" / "orchestrator.py",
        root / "src" / "models" / "nodes.py",
        root / "src" / "models" / "edges.py",
        root / "src" / "models" / "graph.py",
        root / "src" / "analyzers" / "tree_sitter_analyzer.py",
        root / "src" / "analyzers" / "sql_lineage.py",
        root / "src" / "analyzers" / "dag_config_parser.py",
        root / "src" / "agents" / "surveyor.py",
        root / "src" / "agents" / "hydrologist.py",
        root / "src" / "agents" / "semanticist.py",
        root / "src" / "agents" / "archivist.py",
        root / "src" / "agents" / "navigator.py",
        root / "src" / "graph" / "knowledge_graph.py",
        root / "src" / "utils" / "context_budget.py",
        root / "src" / "utils" / "logger.py",
    ]

    for file_path in files:
        touch(file_path)

    print("Initialized The Brownfield Cartographer project structure.")


if __name__ == "__main__":
    main()
