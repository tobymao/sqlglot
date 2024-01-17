from __future__ import annotations

from sqlglot.dialects.oracle import Oracle


class Dameng(Oracle):
    class Parser(Oracle.Parser):
        FUNCTIONS = {
            **Oracle.Parser.FUNCTIONS
        }

    class Generator(Oracle.Generator):

        TYPE_MAPPING = {
            **Oracle.Generator.TYPE_MAPPING
        }

        LAST_DAY_SUPPORTS_DATE_PART = False

        TRANSFORMS = {
            **Oracle.Generator.TRANSFORMS
        }
