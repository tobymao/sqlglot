from __future__ import annotations

from sqlglot.dialects.postgres import Postgres
from sqlglot.generators.materialize import MaterializeGenerator
from sqlglot.parsers.materialize import MaterializeParser


class Materialize(Postgres):
    Parser = MaterializeParser

    Generator = MaterializeGenerator
