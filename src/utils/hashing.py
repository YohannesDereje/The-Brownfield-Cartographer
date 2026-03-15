from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def sha256_text(content: str) -> str:
	return hashlib.sha256(content.encode("utf-8")).hexdigest()


def sha256_file(file_path: str | Path) -> str:
	path = Path(file_path)
	hasher = hashlib.sha256()
	with path.open("rb") as handle:
		for chunk in iter(lambda: handle.read(1024 * 1024), b""):
			hasher.update(chunk)
	return hasher.hexdigest()


class FileManifest:
	"""Persists last analyzed hashes for incremental cartography updates."""

	def __init__(self, manifest_path: str | Path = ".cartography/file_manifest.json") -> None:
		self.manifest_path = Path(manifest_path)
		self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
		self._data: dict[str, dict[str, Any]] = self._load()

	def _load(self) -> dict[str, dict[str, Any]]:
		if not self.manifest_path.exists():
			return {}
		try:
			payload = json.loads(self.manifest_path.read_text(encoding="utf-8"))
		except (json.JSONDecodeError, OSError, TypeError, ValueError):
			return {}
		if not isinstance(payload, dict):
			return {}
		results: dict[str, dict[str, Any]] = {}
		for file_path, item in payload.items():
			if isinstance(item, str):
				results[str(file_path)] = {"last_analyzed_hash": item}
			elif isinstance(item, dict):
				results[str(file_path)] = dict(item)
		return results

	def save(self) -> None:
		with self.manifest_path.open("w", encoding="utf-8") as handle:
			json.dump(self._data, handle, indent=2, sort_keys=True)

	def get(self, file_path: str | Path) -> dict[str, Any]:
		return dict(self._data.get(str(Path(file_path).resolve()), {}))

	def get_hash(self, file_path: str | Path) -> str | None:
		entry = self.get(file_path)
		value = entry.get("last_analyzed_hash")
		return str(value) if isinstance(value, str) else None

	def has_changed(self, file_path: str | Path, current_hash: str) -> bool:
		return self.get_hash(file_path) != current_hash

	def update(self, file_path: str | Path, current_hash: str, **extra: Any) -> None:
		resolved = str(Path(file_path).resolve())
		entry = self._data.setdefault(resolved, {})
		entry["last_analyzed_hash"] = current_hash
		if extra:
			entry.update(extra)

	def prune(self, valid_paths: set[str]) -> None:
		for existing_path in list(self._data.keys()):
			if existing_path not in valid_paths:
				del self._data[existing_path]


__all__ = ["FileManifest", "sha256_file", "sha256_text"]
