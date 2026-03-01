"""Pre-processing pipeline: normalize AST before decompilation."""

from __future__ import annotations

from sqlglot import exp
from sqlglot.optimizer import simplify


def preprocess(
    ast: exp.Expression,
    dialect: str = "sqlite",
    schema: dict | None = None,
) -> exp.Expression:
    """Apply normalization passes to the AST before decompilation.

    Passes applied:
    1. simplify: clean up boolean expressions

    Note: unnest_subqueries is NOT applied because it requires qualify() to work
    reliably, and can produce invalid SQL (empty column refs, ARRAY_AGG on SQLite).
    The decompiler preserves subqueries as-is in the pipe SQL, which is correct.

    qualify() is NOT applied by default because it changes SQL aesthetics
    (adds quotes, table qualifiers, aliases) which makes the pipe SQL look
    unnatural for training data.
    """
    result = ast.copy()

    # Simplify boolean expressions
    try:
        result = simplify.simplify(result)
    except Exception:
        pass  # If simplification fails, continue

    return result
