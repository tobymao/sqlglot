from __future__ import annotations

import typing as t
from enum import auto

from sqlglot.helper import AutoName


# ANSI escape codes for error formatting
ANSI_UNDERLINE = "\033[4m"
ANSI_RESET = "\033[0m"
ERROR_MESSAGE_CONTEXT_DEFAULT = 100


class ErrorLevel(AutoName):
    IGNORE = auto()
    """Ignore all errors."""

    WARN = auto()
    """Log all errors."""

    RAISE = auto()
    """Collect all errors and raise a single exception."""

    IMMEDIATE = auto()
    """Immediately raise an exception on the first error found."""


class SqlglotError(Exception):
    pass


class UnsupportedError(SqlglotError):
    pass


class ParseError(SqlglotError):
    def __init__(
        self,
        message: str,
        errors: t.Optional[t.List[t.Dict[str, t.Any]]] = None,
    ):
        super().__init__(message)
        self.errors = errors or []

    @classmethod
    def new(
        cls,
        message: str,
        description: t.Optional[str] = None,
        line: t.Optional[int] = None,
        col: t.Optional[int] = None,
        start_context: t.Optional[str] = None,
        highlight: t.Optional[str] = None,
        end_context: t.Optional[str] = None,
        into_expression: t.Optional[str] = None,
    ) -> ParseError:
        return cls(
            message,
            [
                {
                    "description": description,
                    "line": line,
                    "col": col,
                    "start_context": start_context,
                    "highlight": highlight,
                    "end_context": end_context,
                    "into_expression": into_expression,
                }
            ],
        )


class TokenError(SqlglotError):
    pass


class OptimizeError(SqlglotError):
    pass


class SchemaError(SqlglotError):
    pass


class ExecuteError(SqlglotError):
    pass


def highlight_sql(
    sql: str,
    positions: t.List[t.Tuple[int, int]],
    context_length: int = ERROR_MESSAGE_CONTEXT_DEFAULT,
) -> t.Tuple[str, str, str, str]:
    """
    Highlight a SQL string using ANSI codes at the given positions.

    Args:
        sql: The complete SQL string.
        positions: List of (start, end) tuples where both start and end are inclusive 0-based
            indexes. For example, to highlight "foo" in "SELECT foo", use (7, 9).
            The positions will be sorted and de-duplicated if they overlap.
        context_length: Number of characters to show before the first highlight and after
            the last highlight.

    Returns:
        A tuple of (formatted_sql, start_context, highlight, end_context) where:
        - formatted_sql: The SQL with ANSI underline codes applied to highlighted sections
        - start_context: Plain text before the first highlight
        - highlight: Plain text from the first highlight start to the last highlight end,
            including any non-highlighted text in between (no ANSI)
        - end_context: Plain text after the last highlight

    Note:
        If positions is empty, raises a ValueError.
    """
    if not positions:
        raise ValueError("positions must contain at least one (start, end) tuple")

    start_context = ""
    end_context = ""
    first_highlight_start = 0
    formatted_parts = []
    previous_part_end = 0
    sorted_positions = sorted(positions, key=lambda pos: pos[0])

    if sorted_positions[0][0] > 0:
        first_highlight_start = sorted_positions[0][0]
        start_context = sql[max(0, first_highlight_start - context_length) : first_highlight_start]
        formatted_parts.append(start_context)
        previous_part_end = first_highlight_start

    for start, end in sorted_positions:
        highlight_start = max(start, previous_part_end)
        highlight_end = end + 1
        if highlight_start >= highlight_end:
            continue  # Skip invalid or overlapping highlights
        if highlight_start > previous_part_end:
            formatted_parts.append(sql[previous_part_end:highlight_start])
        formatted_parts.append(f"{ANSI_UNDERLINE}{sql[highlight_start:highlight_end]}{ANSI_RESET}")
        previous_part_end = highlight_end

    if previous_part_end < len(sql):
        end_context = sql[previous_part_end : previous_part_end + context_length]
        formatted_parts.append(end_context)

    formatted_sql = "".join(formatted_parts)
    highlight = sql[first_highlight_start:previous_part_end]

    return formatted_sql, start_context, highlight, end_context


def concat_messages(errors: t.Sequence[t.Any], maximum: int) -> str:
    msg = [str(e) for e in errors[:maximum]]
    remaining = len(errors) - maximum
    if remaining > 0:
        msg.append(f"... and {remaining} more")
    return "\n\n".join(msg)


def merge_errors(errors: t.Sequence[ParseError]) -> t.List[t.Dict[str, t.Any]]:
    return [e_dict for error in errors for e_dict in error.errors]
