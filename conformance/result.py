from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Result:
    """
    The rows returned by a query, along with engine-native type names and type families.

    Families are engine-agnostic type categories (e.g. "integer", "timestamptz", "json") that the
    comparator uses to normalize values and to report result type differences between engines.
    """

    columns: list[str]
    types: list[str]
    families: list[str]
    rows: list[tuple]

    @property
    def vacuous(self) -> bool:
        """True if the result can't distinguish correct from incorrect SQL: no rows or only NULLs."""
        return all(value is None for row in self.rows for value in row)
