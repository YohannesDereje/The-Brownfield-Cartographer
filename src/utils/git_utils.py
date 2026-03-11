from __future__ import annotations

from collections import Counter
from math import ceil
from pathlib import Path
import subprocess

from loguru import logger

from src.models.nodes import ModuleNode


def get_git_velocity(repo_path: str, days: int = 30) -> dict[str, int]:
    """Return per-file change counts from recent git history."""
    command = [
        "git",
        "log",
        "--pretty=format:",
        "--name-only",
        f"--since={days} days ago",
    ]

    try:
        result = subprocess.run(
            command,
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        logger.warning("Git is not installed or not available in PATH.")
        return {}
    except Exception as exc:
        logger.warning("Failed to execute git log for {}: {}", repo_path, exc)
        return {}

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        logger.warning(
            "Unable to collect git velocity for {} (returncode={}): {}",
            repo_path,
            result.returncode,
            stderr or "git log failed",
        )
        return {}

    repo_root = Path(repo_path).resolve()
    counts: Counter[str] = Counter()
    for raw_line in result.stdout.splitlines():
        file_path = raw_line.strip()
        if not file_path:
            continue

        normalized = (repo_root / file_path).resolve()
        try:
            rel_path = normalized.relative_to(repo_root).as_posix()
        except ValueError:
            rel_path = file_path.replace("\\", "/")

        counts[rel_path] += 1

    return dict(counts)


def identify_high_velocity_core(velocity_map: dict[str, int]) -> list[str]:
    """Return top 20% of files by change frequency (80/20 heuristic)."""
    if not velocity_map:
        return []

    sorted_files = sorted(velocity_map.items(), key=lambda item: item[1], reverse=True)
    top_n = max(1, ceil(len(sorted_files) * 0.2))
    return [file_path for file_path, _ in sorted_files[:top_n]]


def enrich_node_with_velocity(node: ModuleNode, velocity_map: dict[str, int]) -> None:
    """Attach git velocity metadata to a ModuleNode in-place."""
    normalized_path = node.path.replace("\\", "/")
    git_velocity = velocity_map.get(normalized_path, 0)

    if not isinstance(node.metadata, dict):
        node.metadata = {}

    node.metadata["git_velocity"] = git_velocity
