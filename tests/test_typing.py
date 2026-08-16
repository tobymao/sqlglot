import ast
import importlib
import pkgutil
import unittest
from pathlib import Path

import sqlglot.typing


def _same_code(f, g):
    if f is g:
        return True
    if not (callable(f) and callable(g) and hasattr(f, "__code__") and hasattr(g, "__code__")):
        return False

    cf, cg = f.__code__, g.__code__
    return (
        cf.co_code == cg.co_code
        and cf.co_names == cg.co_names
        and cf.co_varnames == cg.co_varnames
        and cf.co_consts == cg.co_consts
    )


def _equivalent(child_value, parent_value):
    if set(child_value) != set(parent_value):
        return False

    for key, value in child_value.items():
        parent_entry = parent_value[key]
        if key == "annotator":
            if not _same_code(value, parent_entry):
                return False
        elif value != parent_entry:
            return False

    return True


def _parent_module(source):
    for node in ast.walk(ast.parse(source)):
        if (
            isinstance(node, ast.ImportFrom)
            and node.module
            and node.module.startswith("sqlglot.typing")
            and any(alias.name == "EXPRESSION_METADATA" for alias in node.names)
        ):
            return node.module

    return None


class TestTyping(unittest.TestCase):
    def test_no_redundant_expression_metadata(self):
        """Dialect metadata entries that merely restate the parent module's entry are redundant."""
        package_path = Path(sqlglot.typing.__file__).parent
        redundant = []

        for info in pkgutil.iter_modules([str(package_path)]):
            parent_name = _parent_module((package_path / f"{info.name}.py").read_text("utf-8"))
            if not parent_name:
                continue

            child = importlib.import_module(f"sqlglot.typing.{info.name}").EXPRESSION_METADATA
            parent = importlib.import_module(parent_name).EXPRESSION_METADATA

            for expr_type, child_value in child.items():
                parent_value = parent.get(expr_type)

                # Entries inherited through the `**parent` spread are the same object; only
                # explicitly redefined entries can be redundant
                if (
                    parent_value is not None
                    and child_value is not parent_value
                    and _equivalent(child_value, parent_value)
                ):
                    redundant.append(
                        f"sqlglot.typing.{info.name}: exp.{expr_type.__name__} duplicates "
                        f"the entry inherited from {parent_name}"
                    )

        if redundant:
            self.fail("\n".join(["Redundant entries found:", *redundant]))
