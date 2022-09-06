import doctest
import inspect
import unittest

import sqlglot
import sqlglot.optimizer
import sqlglot.transforms


def load_tests(loader, tests, ignore):
    """
    This finds and runs all the doctests
    """

    modules = {
        mod
        for module in [sqlglot, sqlglot.transforms, sqlglot.optimizer]
        for _, mod in inspect.getmembers(module, inspect.ismodule)
    }

    assert len(modules) >= 20

    for module in modules:
        tests.addTests(doctest.DocTestSuite(module))

    return tests


if __name__ == "__main__":
    unittest.main()
