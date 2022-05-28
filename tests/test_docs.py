import doctest
import inspect

import sqlglot


def load_tests(loader, tests, ignore):  # pylint: disable=unused-argument
    """
    This finds and runs all the doctests
    """

    modules = [
        mod
        for module in [sqlglot, sqlglot.optimizer]
        for _, mod in inspect.getmembers(module, inspect.ismodule)
    ]

    assert len(modules) >= 20

    for module in modules:
        tests.addTests(doctest.DocTestSuite(module))

    return tests
