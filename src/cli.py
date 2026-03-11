from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlparse

from loguru import logger


if __package__ in {None, ""}:
	sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.orchestrator import run_surveyor_pipeline


def _looks_like_url(value: str) -> bool:
	parsed = urlparse(value)
	return parsed.scheme in {"http", "https", "ssh", "git"}


def _clone_remote_repo(repo_url: str, base_dir: Path) -> Path | None:
	repo_name = Path(urlparse(repo_url).path).stem or "remote_repo"
	destination = base_dir / repo_name

	if destination.exists() and any(destination.iterdir()):
		logger.info("Using existing local clone at {}", destination)
		return destination

	base_dir.mkdir(parents=True, exist_ok=True)

	try:
		result = subprocess.run(
			["git", "clone", "--depth", "1", repo_url, str(destination)],
			capture_output=True,
			text=True,
			check=False,
		)
	except FileNotFoundError:
		logger.error("Git is not installed or not available in PATH.")
		return None
	except Exception as exc:
		logger.exception("Failed to clone remote repository {}: {}", repo_url, exc)
		return None

	if result.returncode != 0:
		logger.error(
			"Remote clone failed for {} (returncode={}): {}",
			repo_url,
			result.returncode,
			(result.stderr or "").strip() or "git clone failed",
		)
		return None

	return destination


def _resolve_repo_path(repo_arg: str) -> Path | None:
	repo_value = repo_arg.strip()

	if _looks_like_url(repo_value):
		logger.warning("Remote cloning is in progress")
		clone_root = Path.cwd() / ".cartography" / "clones"
		return _clone_remote_repo(repo_value, clone_root)

	local_path = Path(repo_value).expanduser().resolve()
	if not local_path.is_dir():
		logger.error("Provided repo path is not a local directory: {}", local_path)
		return None

	return local_path


def build_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(description="The Brownfield Cartographer CLI")
	subparsers = parser.add_subparsers(dest="command")

	analyze_parser = subparsers.add_parser("analyze", help="Run structural analysis")
	analyze_parser.add_argument("--repo", required=True, help="Local repo path or remote git URL")

	return parser


def main(argv: list[str] | None = None) -> int:
	parser = build_parser()
	args = parser.parse_args(argv)

	if args.command != "analyze":
		parser.print_help()
		return 1

	try:
		resolved_repo = _resolve_repo_path(args.repo)
		if resolved_repo is None:
			return 1

		run_surveyor_pipeline(str(resolved_repo))
		return 0
	except Exception as exc:
		logger.exception("CLI analyze command failed: {}", exc)
		return 1


if __name__ == "__main__":
	raise SystemExit(main())
