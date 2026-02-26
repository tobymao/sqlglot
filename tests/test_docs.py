import doctest
import importlib
import pkgutil
import unittest

import sqlglot


def load_tests(loader, tests, ignore):
    """
    This finds and runs all the doctests
    """

    modules = set()
    for info in pkgutil.walk_packages(sqlglot.__path__, prefix="sqlglot."):
        if info.name == "sqlglot.__main__":
            continue
        try:
            modules.add(importlib.import_module(info.name))
        except Exception:
            continue

    assert len(modules) >= 20

    for module in sorted(modules, key=lambda m: m.__name__):
        tests.addTests(doctest.DocTestSuite(module))

    return tests


if __name__ == "__main__":
    unittest.main()
