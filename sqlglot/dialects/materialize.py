from __future__ import annotations

from sqlglot.dialects.postgres import Postgres
from sqlglot.generators.materialize import MaterializeGenerator
from sqlglot.parsers.materialize import MaterializeParser
from sqlglot.typing.materialize import EXPRESSION_METADATA


class Materialize(Postgres):
    EXPRESSION_METADATA = EXPRESSION_METADATA.copy()
    Parser = MaterializeParser

    Generator = MaterializeGenerator
