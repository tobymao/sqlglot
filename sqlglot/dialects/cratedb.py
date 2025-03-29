from __future__ import annotations

import json
from collections import OrderedDict

from sqlglot.generator import exp, Generator
from sqlglot.dialects.postgres import Postgres
from sqlglot.helper import seq_get

from sqlglot.tokens import TokenType
import typing as t


def var_map_sql(
        self: Generator,
        expression: t.Union[exp.Map, exp.VarMap],
        map_func_name: str = "MAP",
) -> str:
    """
    CrateDB accepts values for `OBJECT` types serialized as JSON.
    """
    #print("Generator:", self)
    #sdcsdc
    keys = expression.args["keys"]
    values = expression.args["values"]
    data = OrderedDict()
    for key, value in zip(keys.expressions, values.expressions):
        data[str(key).strip("'")] = str(value).strip("'")
    return "'{}'".format(json.dumps(data))


class CrateDB(Postgres):

    class Parser(Postgres.Parser):
        pass

    class Tokenizer(Postgres.Tokenizer):
        KEYWORDS = {
            **Postgres.Tokenizer.KEYWORDS,
            "GEO_POINT": TokenType.GEOGRAPHY,
            "GEO_SHAPE": TokenType.GEOGRAPHY,
        }

    class GeneratorBase(Postgres.Generator):
        def cast_sql(
                self, expression: exp.Cast, safe_prefix: t.Optional[str] = None
        ) -> str:
            """
            Omit CASTs with CrateDB.

            Some values for special data types get reflected as `TEXT`, which is wrong.

            TODO: REVIEW: Is it sane to do it this way?
                  Can the type mapping be improved somehow?
            """
            format_sql = self.sql(expression, "format")
            format_sql = f" FORMAT {format_sql}" if format_sql else ""
            to_sql = self.sql(expression, "to")
            to_sql = f" {to_sql}" if to_sql else ""
            action = self.sql(expression, "action")
            action = f" {action}" if action else ""
            # Original:
            # return f"{safe_prefix or ''}CAST({self.sql(expression, 'this')} AS{to_sql}{format_sql}{action})"
            # CrateDB adjusted:
            return f"{self.sql(expression, 'this')}"

    class Generator(GeneratorBase):
        LOCKING_READS_SUPPORTED = False
        JSON_TYPE_REQUIRED_FOR_EXTRACTION = False
        SUPPORTS_CREATE_TABLE_LIKE = False

        SUPPORTED_JSON_PATH_PARTS = set()

        TRANSFORMS = {
            **Postgres.Generator.TRANSFORMS,
            exp.Map: var_map_sql,
            exp.VarMap: var_map_sql,
        }
        TRANSFORMS.pop(exp.ToMap)
