import argparse
import os
import sys
from pathlib import Path

sys.path.append(os.getcwd())

from src.agents.navigator import Navigator


EXIT_TOKENS = {"exit", "quit", "q"}


def _discover_repo_options(cartography_dir: Path) -> list[str]:
    options: list[str] = []
    for child in sorted(cartography_dir.iterdir()):
        if not child.is_dir():
            continue
        if child.name in {"clones", "__pycache__"}:
            continue
        if (child / "CODEBASE.md").exists() or (child / "module_graph.json").exists():
            options.append(child.name)
    return options


def _resolve_repo_name(args: argparse.Namespace, cartography_dir: Path) -> str | None:
    if args.artifact_path:
        return None
    if args.repo:
        return args.repo

    options = _discover_repo_options(cartography_dir)
    if not options:
        return None

    print("\nAvailable repos:")
    for option in options:
        print(f"- {option}")

    while True:
        selected = input("Select repo (or type 'q' to cancel): ").strip()
        if selected.lower() in EXIT_TOKENS:
            return ""
        if selected in options:
            return selected
        print("Invalid repo name. Please choose one from the list above.")


def _print_response(response: str) -> None:
    print("\n### Navigator Response")
    print(response.strip() or "(No response)")
    print("-" * 72)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ask Navigator against cartography artifacts")
    parser.add_argument("--repo", required=False, help="Artifact repo slug under .cartography (example: jaffle-shop)")
    parser.add_argument("--artifact-path", required=False, help="Explicit path to a cartography artifact directory")
    parser.add_argument("--query", required=False, help="Optional one-shot question. If omitted, opens interactive mode.")
    args = parser.parse_args()

    cartography_dir = Path(".cartography")
    if not cartography_dir.exists() or not cartography_dir.is_dir():
        print("⚠️ .cartography directory not found.")
        print("Run the Orchestrator first to generate cartography artifacts, then retry.")
        return

    repo_name = _resolve_repo_name(args, cartography_dir)
    if repo_name == "":
        print("Session cancelled.")
        return

    nav = Navigator(repo_name=repo_name, artifact_path=args.artifact_path)

    try:
        nav._load_module_graph()
        nav._load_lineage_graph()
        nav._load_codebase_text()
    except Exception:
        pass

    active_target = args.artifact_path or (repo_name or ".cartography")
    print(f"\nNavigator ready for: {active_target}")
    print("Type a question, or 'exit' / 'quit' / 'q' to close.\n")

    if args.query:
        _print_response(nav.answer(args.query))
        return

    while True:
        try:
            query = input("Navigator > ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nSession closed.")
            break

        if not query:
            continue
        if query.lower() in EXIT_TOKENS:
            print("Session closed.")
            break

        response = nav.answer(query)
        _print_response(response)


if __name__ == "__main__":
    main()