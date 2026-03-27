import os

_integration_dir = os.path.join(
    os.path.dirname(__file__), "..", "..", "sqlglot-integration-tests", "tests", "sqlglot"
)
if os.path.isdir(_integration_dir):
    __path__.append(os.path.normpath(_integration_dir))
