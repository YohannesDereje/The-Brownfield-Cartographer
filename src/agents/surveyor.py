from __future__ import annotations

import os
from pathlib import Path
import subprocess

from loguru import logger
import networkx as nx
from tree_sitter import Node, Query, QueryCursor

from src.analyzers.tree_sitter_analyzer import TreeSitterAnalyzer
from src.models.nodes import FunctionNode, ImportNode, ModuleNode


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

SOURCE_FILE_SUFFIXES = {".py", ".sql", ".yml", ".yaml", ".csv"}


class SurveyorAgent:
    """Builds structural module intelligence and architecture metrics."""

    _FUNCTION_QUERY = """
    (function_definition
        name: (identifier) @function.name
    ) @function.def
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
        root = Path(root_path).resolve()
        files: list[Path] = []
        for current_dir, dirs, filenames in os.walk(root):
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
        node = ModuleNode(path=str(file_path), language=language, metadata={"error": False})

        if file_path.suffix.lower() != ".py":
            return node

        tree = self.analyzer.get_tree(file_path)
        if tree is None:
            node.metadata = {"error": True, "error_type": "parse_unavailable"}
            return node

        try:
            source_bytes = file_path.read_bytes()
        except Exception as exc:
            logger.exception("Failed to read {}: {}", file_path, exc)
            node.metadata = {"error": True, "error_type": "read_failed"}
            return node

        node.functions = self._extract_functions(file_path, tree.root_node, source_bytes)
        node.imports = self._extract_imports(file_path, tree.root_node, source_bytes)
        return node

    def build_import_graph(self, modules: list[ModuleNode]) -> nx.DiGraph:
        graph = nx.DiGraph()
        module_paths = {self._normalize_path(module.path) for module in modules}
        for module in modules:
            module_path = self._normalize_path(module.path)
            graph.add_node(module_path)
            for import_edge in module.imports:
                if not import_edge.resolved_path:
                    continue
                target_path = self._normalize_path(import_edge.resolved_path)
                if target_path in module_paths:
                    graph.add_edge(module_path, target_path)
        return graph

    def compute_architectural_hubs(self, graph: nx.DiGraph, top_n: int = 10) -> list[str]:
        if graph.number_of_nodes() == 0:
            return []
        try:
            scores = nx.pagerank(graph)
        except Exception as exc:
            logger.warning("PageRank failed in Surveyor: {}", exc)
            scores = {node: float(degree) for node, degree in graph.in_degree()}
        ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
        return [node for node, _ in ranked[:top_n]]

    def detect_circular_dependencies(self, graph: nx.DiGraph) -> list[list[str]]:
        return [sorted(list(component)) for component in nx.strongly_connected_components(graph) if len(component) > 1]

    def get_git_velocity(self, repo_path: str, files: list[str]) -> dict[str, int]:
        velocity: dict[str, int] = {}
        for file_path in files:
            rel_path = self._to_repo_relative(repo_path, file_path)
            if rel_path is None:
                continue
            try:
                result = subprocess.run(
                    ["git", "log", "--follow", "--pretty=format:%H", "--", rel_path],
                    cwd=repo_path,
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if result.returncode != 0:
                    continue
                commits = [line for line in result.stdout.splitlines() if line.strip()]
                velocity[self._normalize_path(file_path)] = len(commits)
            except Exception as exc:
                logger.warning("git velocity failed for {}: {}", file_path, exc)
        return velocity

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

    def _parse_import_from_statement(self, node: Node, source: bytes, current_path: Path) -> ImportNode | None:
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

    def _extract_imported_names(self, import_from_text: str) -> list[str]:
        if "import" not in import_from_text:
            return []
        imported_part = import_from_text.split("import", 1)[1].strip()
        if imported_part.startswith("(") and imported_part.endswith(")"):
            imported_part = imported_part[1:-1]

        names: list[str] = []
        for chunk in imported_part.split(","):
            value = chunk.strip()
            if value:
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

    def _detect_language(self, path: Path) -> str:
        if path.suffix.lower() == ".py":
            return "python"
        if path.suffix.lower() == ".sql":
            return "sql"
        return path.suffix.lower().lstrip(".")

    def _normalize_path(self, value: str) -> str:
        return value.replace("\\", "/")

    def _to_repo_relative(self, repo_path: str, file_path: str) -> str | None:
        try:
            return str(Path(file_path).resolve().relative_to(Path(repo_path).resolve())).replace("\\", "/")
        except Exception:
            return None


__all__ = ["SurveyorAgent", "IGNORED_DIRS", "SOURCE_FILE_SUFFIXES"]
