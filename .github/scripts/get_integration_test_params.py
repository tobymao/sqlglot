#!/usr/bin/env python3
"""
This script is intended to be used as part of a GitHub Actions workflow in order to decide if the integration tests should:

a) be triggered at all
b) if they should be triggered, should they be triggered for a subset of dialects or all dialects?

A `git diff` is performed between your PR branch and the base branch, and each changed file is
mapped to the set of supported dialects it can affect. This mapping is computed from sqlglot's
import graph: a changed module affects a dialect iff the dialect's module (or a module the
integration test harness imports) transitively imports it. Modules outside every closure
(e.g. the executor, planner, or most optimizer rules) don't trigger any dialect tests.

Note that integration tests in the remote workflow are only implemented for a subset of dialects.
If new ones are added, update the SUPPORTED_DIALECTS constant below.

Each dialect is tested against itself (roundtrip) and duckdb (transpilation).
Supplying a dialect not in this list will cause the tests to get skipped.
"""

import ast
import typing as t
import os
import sys
import json
import subprocess
from pathlib import Path

SUPPORTED_DIALECTS = ["duckdb", "bigquery", "snowflake"]

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = REPO_ROOT / "sqlglot"

# Modules imported by the integration test harness itself, on top of each dialect's module
HARNESS_MODULES = (
    "sqlglot.optimizer.normalize_identifiers",
    "sqlglot.optimizer.qualify_columns",
)

# Changes to the integration test setup itself should re-run everything
INTEGRATION_TEST_PATHS = (
    "sqlglot-integration-tests",
    ".github/workflows/run-integration-tests.yml",
    ".github/scripts/get_integration_test_params.py",
)


def _module_name(path: str) -> str:
    """Converts a repo-relative file path to a dotted module name."""
    return path.removesuffix(".py").removesuffix("/__init__").replace("/", ".")


def _walk_runtime_nodes(node: ast.AST) -> t.Iterator[ast.AST]:
    """Yields all AST nodes reachable at runtime, i.e. skips `if TYPE_CHECKING:` blocks."""
    for child in ast.iter_child_nodes(node):
        if isinstance(child, ast.If) and ast.unparse(child.test).endswith("TYPE_CHECKING"):
            continue
        yield child
        yield from _walk_runtime_nodes(child)


def _build_import_graph() -> dict[str, set[str]]:
    """
    Maps each sqlglot module to the set of sqlglot modules it imports. Imports nested inside
    function bodies count (the generator imports optimizer modules lazily), TYPE_CHECKING-only
    imports don't. Relative imports aren't supported because the codebase doesn't use them.
    """
    modules = {
        _module_name(f.relative_to(REPO_ROOT).as_posix()): f for f in PACKAGE_ROOT.rglob("*.py")
    }

    graph: dict[str, set[str]] = {}

    for module, path in modules.items():
        imports: set[str] = set()

        for node in _walk_runtime_nodes(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                imports.update(a.name for a in node.names if a.name in modules)
            elif isinstance(node, ast.ImportFrom) and node.module:
                # `from x import y` can import either the module x.y or an attribute of x
                imports.update(
                    m
                    for m in (node.module, *(f"{node.module}.{a.name}" for a in node.names))
                    if m in modules
                )

        graph[module] = imports

    return graph


def _closure(roots: t.Iterable[str], graph: dict[str, set[str]]) -> set[str]:
    seen: set[str] = set()
    stack = list(roots)

    while stack:
        module = stack.pop()
        if module in seen:
            continue

        seen.add(module)
        stack.extend(graph.get(module, ()))

        # Importing a module also executes its ancestor packages' __init__.py
        parts = module.split(".")
        stack.extend(".".join(parts[:i]) for i in range(1, len(parts)))

    return seen


def get_module_dialect_map() -> dict[str, set[str]]:
    """Maps each sqlglot module to the set of supported dialects whose integration tests it can affect."""
    graph = _build_import_graph()

    module_dialects: dict[str, set[str]] = {module: set() for module in graph}
    for dialect in SUPPORTED_DIALECTS:
        for module in _closure([f"sqlglot.dialects.{dialect}", *HARNESS_MODULES], graph):
            module_dialects[module].add(dialect)

    return module_dialects


def get_affected_dialects(path: str, module_dialects: dict[str, set[str]]) -> set[str]:
    """Maps a changed file to the set of supported dialects whose integration tests it can affect."""
    all_dialects = set(SUPPORTED_DIALECTS)

    if path in INTEGRATION_TEST_PATHS:
        return all_dialects

    # The remote workflow also installs the mypyc-compiled extension
    if path.startswith("sqlglotc/"):
        return all_dialects

    if not path.startswith("sqlglot/"):
        return set()

    if path.endswith(".py"):
        module = _module_name(path)
        if module in module_dialects:
            return module_dialects[module]

    # Fail open for files we can't map (e.g. deleted modules or non-python files)
    return all_dialects


def get_dialects_from_git(base_ref: str, current_ref: str) -> set[str]:
    """
    Takes two git refs and runs `git diff --name-only <base_ref> <current_ref>`,
    mapping each changed file to the dialects it affects
    """
    print(f"Checking for files changed between '{base_ref}' and '{current_ref}'")

    result = subprocess.run(
        ["git", "diff", "--name-only", base_ref, current_ref],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    output = result.stdout.decode("utf8")

    if result.returncode != 0:
        raise ValueError(f"Git process failed with exit code {result.returncode}:\n{output}")

    print(f"Git output:\n{output}")

    module_dialects = get_module_dialect_map()
    matching_dialects: set[str] = set()

    for l in output.splitlines():
        matching_dialects |= get_affected_dialects(l.strip(), module_dialects)

    return matching_dialects


if __name__ == "__main__":
    github_event_path = os.environ.get("GITHUB_EVENT_PATH")
    github_output = os.environ.get("GITHUB_OUTPUT")

    if not os.environ.get("GITHUB_ACTIONS") or not github_event_path or not github_output:
        print("This script needs to run within GitHub Actions")
        sys.exit(1)

    github_event_path = Path(github_event_path)
    github_output = Path(github_output)

    with github_event_path.open("r") as f:
        event: dict[str, t.Any] = json.load(f)

    print("Handling event: \n" + json.dumps(event, indent=2))

    pull_request_base_ref = event.get("pull_request", {}).get("base", {}).get("sha")
    if not pull_request_base_ref:
        raise ValueError("Unable to determine base ref")

    current_ref = event.get("pull_request", {}).get("head", {}).get("sha")

    if not current_ref:
        raise ValueError("Unable to determine current/head ref")

    print(f"Comparing '{current_ref}' against '{pull_request_base_ref}'")
    # look at git files changed and only trigger if a file relating
    # to a supported dialect has changed
    dialects = get_dialects_from_git(base_ref=pull_request_base_ref, current_ref=current_ref)

    if dialects:
        print(f"Conclusion: should run tests for the following dialects: {', '.join(dialects)}")
    else:
        print("Conclusion: No dialect-specific tests to run, but SQLGlot tests will still run")

    # Always dispatch so that run-sqlglot-tests (tests/sqlglot/) runs on every PR.
    # When no dialects are detected, pass "none" so the integration test matrix is empty.
    lines = ["skip=false"]
    if dialects:
        lines.append(f"dialects={','.join(dialects)}")
    else:
        lines.append("dialects=none")

    with github_output.open("a") as f:
        f.writelines(f"{l}\n" for l in lines)
