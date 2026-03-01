"""Pipe SQL syntax validation (design doc Section 10).

Validates pipe SQL is syntactically valid before transpiling to SQLite.
Uses the prefix property: every prefix of a valid pipe query (up to a |> boundary)
is itself a valid query.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot


@dataclass
class PrefixResult:
    """Result of validating a single pipe query prefix."""
    prefix_index: int
    prefix_sql: str
    valid: bool
    error: str = ""


@dataclass
class PipeValidationResult:
    """Result of pipe SQL syntax validation."""
    valid: bool
    error: str = ""
    prefix_results: list[PrefixResult] = field(default_factory=list)
    failing_operator_index: int = -1  # Index of the first invalid operator


def validate_pipe_syntax(pipe_sql: str) -> PipeValidationResult:
    """Verify pipe SQL parses without error.

    Returns a PipeValidationResult indicating whether the pipe SQL is valid.
    """
    try:
        ast = sqlglot.parse_one(pipe_sql)
        # Verify it can generate SQL back
        ast.sql(dialect="sqlite")
        return PipeValidationResult(valid=True)
    except sqlglot.errors.ParseError as e:
        return PipeValidationResult(valid=False, error=str(e))
    except Exception as e:
        return PipeValidationResult(valid=False, error=str(e))


def validate_pipe_prefixes(pipe_sql: str) -> PipeValidationResult:
    """Validate each prefix of the pipe query independently.

    Exploits the Prefix Property: every prefix of a valid pipe query
    (up to a |> boundary) is itself a valid query. If prefix N is valid
    but prefix N+1 is not, the bug is in the Nth pipe operator.
    """
    # Split on |> boundaries
    parts = pipe_sql.split("|>")
    if not parts:
        return PipeValidationResult(valid=False, error="Empty pipe SQL")

    prefix_results = []
    prefix = ""

    for i, part in enumerate(parts):
        if i == 0:
            prefix = part.strip()
        else:
            prefix = prefix + " |> " + part.strip()

        result = PrefixResult(prefix_index=i, prefix_sql=prefix, valid=True)

        try:
            ast = sqlglot.parse_one(prefix)
            ast.sql(dialect="sqlite")
        except Exception as e:
            result.valid = False
            result.error = str(e)

        prefix_results.append(result)

        if not result.valid:
            return PipeValidationResult(
                valid=False,
                error=result.error,
                prefix_results=prefix_results,
                failing_operator_index=i,
            )

    return PipeValidationResult(
        valid=True,
        prefix_results=prefix_results,
    )
