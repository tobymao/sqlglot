import os

INTEGRATION_TEST_DIR = os.path.join(
    os.path.dirname(__file__),
    "..",
    "sqlglot-integration-tests",
    "tests",
    "sqlglot",
)


def load_tests(loader, suite, pattern):
    if os.path.isdir(INTEGRATION_TEST_DIR):
        suite.addTests(loader.discover(INTEGRATION_TEST_DIR, pattern="test*.py"))
    return suite
