from __future__ import annotations

from pathlib import Path
import re
from typing import Optional

from loguru import logger
from tree_sitter import Language, Parser, Query, Tree
import tree_sitter_python

from src.models.nodes import FileNode


class LanguageRouter:
	"""Routes files to the correct Tree-sitter language implementation."""

	SQL_FALLBACK_SUFFIXES = {".sql"}

	def __init__(self) -> None:
		python_language = Language(tree_sitter_python.language())
		self._languages: dict[str, Language] = {
			".py": python_language,
		}

	def get_language_for_file(self, path: str | Path) -> Optional[Language]:
		suffix = Path(path).suffix.lower()
		return self._languages.get(suffix)

	def has_fallback_parser(self, path: str | Path) -> bool:
		suffix = Path(path).suffix.lower()
		return suffix in self.SQL_FALLBACK_SUFFIXES


class TreeSitterAnalyzer:
	"""Base analyzer that parses source files into Tree-sitter ASTs.

	Future extraction should rely on compiled S-expression queries for speed and
	consistent structure traversal.
	"""

	def __init__(self, router: LanguageRouter) -> None:
		self.router = router
		self.parser = Parser()
		self._dbt_ref_pattern = re.compile(r"\{\{\s*ref\(\s*['\"]([\w\.\-]+)['\"]\s*\)\s*\}\}")

	def _set_parser_language(self, language: Language) -> bool:
		try:
			set_language = getattr(self.parser, "set_language", None)
			if callable(set_language):
				set_language(language)
			else:
				self.parser.language = language
			return True
		except Exception as exc:
			logger.exception("Failed to set parser language: {}", exc)
			return False

	def get_tree(self, path: str | Path) -> Optional[Tree]:
		file_path = Path(path)
		language = self.router.get_language_for_file(file_path)

		if language is None:
			if self.router.has_fallback_parser(file_path):
				logger.debug("Using non-Tree-sitter fallback parser for file: {}", file_path)
			else:
				logger.warning("No Tree-sitter language mapping for file: {}", file_path)
			return None

		if not self._set_parser_language(language):
			return None

		try:
			source_bytes = file_path.read_bytes()
		except Exception as exc:
			logger.exception("Unable to read file for parsing: {} ({})", file_path, exc)
			return None

		try:
			tree = self.parser.parse(source_bytes)
			if tree is None:
				logger.error("Parser returned no tree for file: {}", file_path)
				return None
			return tree
		except Exception as exc:
			logger.exception("Failed to parse file: {} ({})", file_path, exc)
			return None

	def compile_query(self, path: str | Path, s_expression: str) -> Optional[Query]:
		"""Compile an S-expression query for future high-performance extraction."""
		language = self.router.get_language_for_file(path)
		if language is None:
			logger.warning("Cannot compile query without mapped language: {}", path)
			return None

		try:
			return Query(language, s_expression)
		except Exception as exc:
			logger.exception("Failed to compile query for {} ({})", path, exc)
			return None

	def extract_dbt_refs(self, path: str | Path) -> list[str]:
		"""Fallback SQL parser for dbt refs, e.g. {{ ref('model_name') }}."""
		file_path = Path(path)
		try:
			content = file_path.read_text(encoding="utf-8", errors="replace")
		except Exception as exc:
			logger.exception("Unable to read SQL file for dbt ref extraction: {} ({})", file_path, exc)
			return []

		matches = self._dbt_ref_pattern.findall(content)
		if not matches:
			return []

		seen: set[str] = set()
		ordered_refs: list[str] = []
		for model_name in matches:
			if model_name in seen:
				continue
			seen.add(model_name)
			ordered_refs.append(model_name)

		return ordered_refs


__all__ = ["LanguageRouter", "TreeSitterAnalyzer", "FileNode"]
