from sqlglot import exp
from sqlglot.dialects.dialect import (
    Dialect,
    no_ilike_sql,
    no_paren_current_date_sql,
    no_tablesample_sql,
    no_trycast_sql,
)
from sqlglot.generator import Generator
from sqlglot.parser import Parser


class MySQL(Dialect):
    identifier = "`"

    # https://prestodb.io/docs/current/functions/datetime.html#mysql-date-functions
    time_mapping = {
        "%M": "%B",
        "%c": "%-m",
        "%e": "%-d",
        "%h": "%I",
        "%i": "%M",
        "%s": "%S",
        "%S": "%S",
    }

    class Parser(Parser):
        STRICT_CAST = False

    class Generator(Generator):
        TRANSFORMS = {
            **Generator.TRANSFORMS,
            exp.CurrentDate: no_paren_current_date_sql,
            exp.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
            exp.ILike: no_ilike_sql,
            exp.TableSample: no_tablesample_sql,
            exp.TryCast: no_trycast_sql,
        }
