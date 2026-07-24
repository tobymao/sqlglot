from __future__ import annotations

from sqlglot.dialects.postgres import Postgres
from sqlglot.generators.materialize import MaterializeGenerator
from sqlglot.parsers.materialize import MaterializeParser


class Materialize(Postgres):
    SAFE_TO_NORMALIZE_IS_NOT_NULL = True

    Parser = MaterializeParser

    Generator = MaterializeGenerator
