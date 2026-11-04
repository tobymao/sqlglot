from __future__ import annotations

from conformance.dataset.model import Column, Table
from conformance.dataset.tables import TABLES as _TABLES
from conformance.dataset.tpch import TPCH_TABLES

TABLES: list[Table] = [*_TABLES, *TPCH_TABLES]
TABLES_BY_NAME = {table.name: table for table in TABLES}

__all__ = ["TABLES", "TABLES_BY_NAME", "Column", "Table"]
