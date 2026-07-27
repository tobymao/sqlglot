from __future__ import annotations

from sqlglot.dialects.postgres import Postgres
from sqlglot.generators.materialize import MaterializeGenerator
from sqlglot.parsers.materialize import MaterializeParser


class Materialize(Postgres):
    NORMALIZE_NOT_NULL = True

    Parser = MaterializeParser

    Generator = MaterializeGenerator
