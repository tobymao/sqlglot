"""Data types for the pipe SQL decompiler."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto


class PipeOpType(Enum):
    FROM = auto()
    SELECT = auto()
    EXTEND = auto()
    WHERE = auto()
    AGGREGATE = auto()
    JOIN = auto()
    ORDER_BY = auto()
    LIMIT = auto()
    AS = auto()
    UNION = auto()
    INTERSECT = auto()
    EXCEPT = auto()


@dataclass
class PipeOperator:
    op_type: PipeOpType
    sql_fragment: str


@dataclass
class PipeQuery:
    operators: list[PipeOperator] = field(default_factory=list)
    ctes: list[tuple[str, PipeQuery]] = field(default_factory=list)
    cte_names: list[str] = field(default_factory=list)


@dataclass
class TransformResult:
    pipe_sql: str
    pipe_query: PipeQuery
    warnings: list[str] = field(default_factory=list)
    unsupported: list[str] = field(default_factory=list)
