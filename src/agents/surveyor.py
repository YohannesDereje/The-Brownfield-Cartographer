from __future__ import annotations

import os
from pathlib import Path
import re

from loguru import logger
from tree_sitter import Node, Query, QueryCursor

from src.analyzers.tree_sitter_analyzer import TreeSitterAnalyzer
from src.models.nodes import ClassNode, FunctionNode, ImportNode, ModuleNode


IGNORED_DIRS = {
	".git",
	".venv",
	"venv",
	"__pycache__",
	".cartography",
	"node_modules",
	".idea",
	".vscode",
}

SOURCE_FILE_SUFFIXES = {".py", ".sql"}


class SurveyorAgent:
	"""Extracts module-level entities and dependencies from Python source files."""

	_DBT_REF_PATTERN = re.compile(r"ref\(['\"](.+?)['\"]\)")

	_FUNCTION_QUERY = """
	(function_definition
		name: (identifier) @function.name
	) @function.def
	"""

	_CLASS_QUERY = """
	(class_definition
		name: (identifier) @class.name
	) @class.def
	"""

	_IMPORT_QUERY = """
	[
	  (import_statement) @import.stmt
	  (import_from_statement) @import.from
	]
	"""

	def __init__(self, analyzer: TreeSitterAnalyzer) -> None:
		self.analyzer = analyzer

	def scan_directory(self, root_path: str | Path) -> list[Path]:
		"""Find relevant source files while skipping environment/metadata directories."""
		root = Path(root_path).resolve()
		files: list[Path] = []

		for current_dir, dirs, filenames in os.walk(root):
			skipped_dirs = [directory for directory in dirs if directory in IGNORED_DIRS]
			for directory in skipped_dirs:
				logger.debug("Skipping directory during scan: {}", Path(current_dir) / directory)

			dirs[:] = [directory for directory in dirs if directory not in IGNORED_DIRS]

			current_path = Path(current_dir)
			for filename in filenames:
				file_path = current_path / filename
				if file_path.suffix.lower() in SOURCE_FILE_SUFFIXES:
					files.append(file_path)

		return sorted(files)

	def analyze_module(self, path: str) -> ModuleNode:
		file_path = Path(path)
		language = self._detect_language(file_path)

		if file_path.suffix.lower() == ".sql":
			return self._analyze_sql_module(file_path, language)

		skeleton = ModuleNode(
			path=str(file_path),
			language=language,
			metadata={"error": False},
		)

		tree = self.analyzer.get_tree(file_path)
		if tree is None:
			skeleton.metadata.update(
				{
					"error": True,
					"error_type": "parse_unavailable",
					"message": "Unable to build AST for module.",
				}
			)
			return skeleton

		try:
			source_bytes = file_path.read_bytes()
		except Exception as exc:
			logger.exception("Failed to read module source {} ({})", file_path, exc)
			skeleton.metadata.update(
				{
					"error": True,
					"error_type": "read_failed",
					"message": str(exc),
				}
			)
			return skeleton

		functions = self._extract_functions(file_path, tree.root_node, source_bytes)
		classes = self._extract_classes(file_path, tree.root_node, source_bytes)
		imports = self._extract_imports(file_path, tree.root_node, source_bytes)

		return ModuleNode(
			path=str(file_path),
			language=language,
			functions=functions,
			classes=classes,
			imports=imports,
			metadata={"error": False},
		)

	def _analyze_sql_module(self, file_path: Path, language: str) -> ModuleNode:
		refs = self._extract_dbt_refs_regex(file_path)
		imports: list[ImportNode] = []

		for model_name in refs:
			imports.append(
				ImportNode(
					module=model_name,
					names=[model_name],
					is_from_import=False,
					is_relative=False,
					level=0,
					resolved_path=self._resolve_dbt_ref(file_path, model_name),
					line=0,
					column=0,
				)
			)

		return ModuleNode(
			path=str(file_path),
			language="sql",
			functions=[],
			classes=[],
			imports=imports,
			metadata={
				"error": False,
				"parser": "regex_dbt_ref_fallback",
				"ref_matches": len(refs),
			},
		)

	def _extract_dbt_refs_regex(self, file_path: Path) -> list[str]:
		try:
			content = file_path.read_text(encoding="utf-8", errors="replace")
		except Exception as exc:
			logger.exception("Failed to read SQL file for regex parsing {} ({})", file_path, exc)
			return []

		seen: set[str] = set()
		results: list[str] = []
		for match in self._DBT_REF_PATTERN.findall(content):
			model_name = match.strip()
			if not model_name or model_name in seen:
				continue
			seen.add(model_name)
			results.append(model_name)

		if results:
			logger.debug("SQL regex parser found {} dbt refs in {}", len(results), file_path)

		return results

	def _resolve_dbt_ref(self, current_file: Path, model_name: str) -> str | None:
		repo_root = self._find_repository_root(current_file)
		models_dir = repo_root / "models"

		if models_dir.exists():
			matches = sorted(models_dir.rglob(f"{model_name}.sql"))
			if matches:
				return str(matches[0].resolve())

		fallback_matches = sorted(repo_root.rglob(f"{model_name}.sql"))
		for candidate in fallback_matches:
			if any(part in IGNORED_DIRS for part in candidate.parts):
				continue
			return str(candidate.resolve())

		return None

	def _find_repository_root(self, file_path: Path) -> Path:
		for parent in [file_path.parent, *file_path.parents]:
			if (parent / ".git").exists() or (parent / "dbt_project.yml").exists():
				return parent
		return file_path.parent

	def _extract_functions(self, path: Path, root: Node, source: bytes) -> list[FunctionNode]:
		query = self.analyzer.compile_query(path, self._FUNCTION_QUERY)
		if query is None:
			return []

		function_nodes: list[FunctionNode] = []
		for capture_name, node in self._run_captures(query, root):
			if capture_name != "function.name":
				continue

			fn_name = self._node_text(node, source)
			if not fn_name or fn_name.startswith("_"):
				continue

			function_nodes.append(
				FunctionNode(
					name=fn_name,
					is_public=True,
					line=node.start_point[0] + 1,
					column=node.start_point[1] + 1,
				)
			)

		return function_nodes

	def _extract_classes(self, path: Path, root: Node, source: bytes) -> list[ClassNode]:
		query = self.analyzer.compile_query(path, self._CLASS_QUERY)
		if query is None:
			return []

		class_defs: list[Node] = []
		class_names: list[tuple[str, Node]] = []

		for capture_name, node in self._run_captures(query, root):
			if capture_name == "class.def":
				class_defs.append(node)
			if capture_name == "class.name":
				class_names.append((self._node_text(node, source), node))

		class_nodes: list[ClassNode] = []
		for class_name, name_node in class_names:
			if not class_name:
				continue

			parent = self._find_ancestor(name_node, "class_definition")
			if parent is None:
				continue

			bases = self._extract_class_bases(parent, source)
			class_nodes.append(
				ClassNode(
					name=class_name,
					bases=bases,
					line=name_node.start_point[0] + 1,
					column=name_node.start_point[1] + 1,
				)
			)

		if class_nodes:
			return class_nodes

		# Fallback to definition captures if name captures fail for grammar variance.
		fallback_nodes: list[ClassNode] = []
		for class_def in class_defs:
			name_node = class_def.child_by_field_name("name")
			if name_node is None:
				continue
			fallback_nodes.append(
				ClassNode(
					name=self._node_text(name_node, source),
					bases=self._extract_class_bases(class_def, source),
					line=name_node.start_point[0] + 1,
					column=name_node.start_point[1] + 1,
				)
			)
		return fallback_nodes

	def _extract_imports(self, path: Path, root: Node, source: bytes) -> list[ImportNode]:
		query = self.analyzer.compile_query(path, self._IMPORT_QUERY)
		if query is None:
			return []

		imports: list[ImportNode] = []
		for capture_name, node in self._run_captures(query, root):
			if capture_name == "import.stmt":
				imports.extend(self._parse_import_statement(node, source))
			elif capture_name == "import.from":
				parsed = self._parse_import_from_statement(node, source, path)
				if parsed is not None:
					imports.append(parsed)

		return imports

	def _parse_import_statement(self, node: Node, source: bytes) -> list[ImportNode]:
		text = self._node_text(node, source)
		if not text.startswith("import "):
			return []

		payload = text[len("import ") :]
		modules = [chunk.strip() for chunk in payload.split(",") if chunk.strip()]
		results: list[ImportNode] = []
		for module_entry in modules:
			module_name = module_entry.split(" as ")[0].strip()
			if not module_name:
				continue
			results.append(
				ImportNode(
					module=module_name,
					names=[],
					is_from_import=False,
					is_relative=False,
					level=0,
					resolved_path=None,
					line=node.start_point[0] + 1,
					column=node.start_point[1] + 1,
				)
			)
		return results

	def _parse_import_from_statement(
		self,
		node: Node,
		source: bytes,
		current_path: Path,
	) -> ImportNode | None:
		module_name_node = node.child_by_field_name("module_name")

		module_name = self._node_text(module_name_node, source) if module_name_node else ""
		text = self._node_text(node, source)

		level = len(module_name) - len(module_name.lstrip("."))
		cleaned_module_name = module_name.lstrip(".")

		if not module_name and text.startswith("from"):
			from_part = text.split("import", 1)[0].replace("from", "", 1).strip()
			level = len(from_part) - len(from_part.lstrip("."))
			cleaned_module_name = from_part.lstrip(".")

		names = self._extract_imported_names(text)
		is_relative = level > 0

		resolved_path = None
		if is_relative:
			resolved_path = self._resolve_relative_import(
				current_path=current_path,
				dotted_module=cleaned_module_name,
				level=level,
			)

		return ImportNode(
			module=cleaned_module_name or None,
			names=names,
			is_from_import=True,
			is_relative=is_relative,
			level=level,
			resolved_path=resolved_path,
			line=node.start_point[0] + 1,
			column=node.start_point[1] + 1,
		)

	def _resolve_relative_import(self, current_path: Path, dotted_module: str, level: int) -> str | None:
		try:
			anchor = current_path.parent
			for _ in range(max(level - 1, 0)):
				anchor = anchor.parent

			target = anchor
			if dotted_module:
				for part in dotted_module.split("."):
					if part:
						target = target / part

			py_candidate = target.with_suffix(".py")
			if py_candidate.exists():
				return str(py_candidate.resolve())

			init_candidate = target / "__init__.py"
			if init_candidate.exists():
				return str(init_candidate.resolve())

			return str(target.resolve())
		except Exception as exc:
			logger.exception(
				"Failed to resolve relative import in {} (module={}, level={}): {}",
				current_path,
				dotted_module,
				level,
				exc,
			)
			return None

	def _extract_class_bases(self, class_def: Node, source: bytes) -> list[str]:
		superclasses = class_def.child_by_field_name("superclasses")
		if superclasses is None:
			return []

		text = self._node_text(superclasses, source).strip()
		if not text.startswith("(") or not text.endswith(")"):
			return []

		inside = text[1:-1].strip()
		if not inside:
			return []

		return [part.strip() for part in inside.split(",") if part.strip()]

	def _extract_imported_names(self, import_from_text: str) -> list[str]:
		if "import" not in import_from_text:
			return []

		imported_part = import_from_text.split("import", 1)[1].strip()
		if imported_part.startswith("(") and imported_part.endswith(")"):
			imported_part = imported_part[1:-1]

		names: list[str] = []
		for chunk in imported_part.split(","):
			value = chunk.strip()
			if not value:
				continue
			names.append(value.split(" as ")[0].strip())
		return names

	def _run_captures(self, query: Query, root: Node) -> list[tuple[str, Node]]:
		try:
			cursor = QueryCursor(query)
			captures = cursor.captures(root)
			results: list[tuple[str, Node]] = []
			for capture_name, nodes in captures.items():
				for node in nodes:
					results.append((capture_name, node))
			return results
		except Exception:
			try:
				raw = query.captures(root)
				return [(capture_name, node) for node, capture_name in raw]
			except Exception as exc:
				logger.exception("Failed to execute Tree-sitter query captures: {}", exc)
				return []

	def _node_text(self, node: Node | None, source: bytes) -> str:
		if node is None:
			return ""
		return source[node.start_byte : node.end_byte].decode("utf-8", errors="replace")

	def _find_ancestor(self, node: Node, node_type: str) -> Node | None:
		current = node
		while current is not None:
			if current.type == node_type:
				return current
			current = current.parent
		return None

	def _detect_language(self, path: Path) -> str:
		if path.suffix.lower() == ".py":
			return "python"
		return path.suffix.lower().lstrip(".")

