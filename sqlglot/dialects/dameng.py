from __future__ import annotations

from sqlglot.dialects.oracle import Oracle


class Dameng(Oracle):
    class Parser(Oracle.Parser):
        FUNCTIONS = {
            **Oracle.Parser.FUNCTIONS
        }

    class Generator(Oracle.Generator):
        CAST_MAPPING = {}

        TYPE_MAPPING = {
            **Oracle.Generator.TYPE_MAPPING
        }

        LAST_DAY_SUPPORTS_DATE_PART = False

        TIMESTAMP_FUNC_TYPES = set()

        TRANSFORMS = {
            **Oracle.Generator.TRANSFORMS
        }
