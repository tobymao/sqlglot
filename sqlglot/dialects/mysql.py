from sqlglot import exp
from sqlglot.dialects.dialect import (
    Dialect,
    no_ilike_sql,
    no_paren_current_date_sql,
    no_tablesample_sql,
    no_trycast_sql,
)
from sqlglot.generator import Generator
from sqlglot.tokens import TokenType
from sqlglot.parser import Parser


def ordered_sql(self, expression):
    # SO: https://stackoverflow.com/questions/2051602/mysql-orderby-a-number-nulls-last
    desc = expression.args.get("desc")
    asc = not desc
    nulls_first = expression.args.get("nulls_first")
    nulls_last = not nulls_first
    field = self.sql(expression, "this")

    sort_order = " DESC" if desc else ""
    if nulls_first and desc:
        return f"ISNULL({field}) ASC, {field}{sort_order}"
    if nulls_last and asc:
        return f"ISNULL({field}) DESC, {field}{sort_order}"
    return f"{field}{sort_order}"


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

        def _parse_ordered(self):
            is_null = self._match(TokenType.ISNULL)
            if is_null:
                self._match(TokenType.L_PAREN)
                this = self._parse_conjunction()
                self._match(TokenType.R_PAREN)
                nulls_first = self._match(TokenType.ASC)
                self._match(TokenType.DESC)
                self._match(TokenType.COMMA)
                self._parse_conjunction()
                is_desc = self._match(TokenType.DESC)
                self._match(TokenType.ASC)
            else:
                this = self._parse_conjunction()
                is_asc = self._match(TokenType.ASC)
                is_desc = self._match(TokenType.DESC)
                nulls_first = False
                if is_asc or (is_asc is None and is_desc is None):
                    nulls_first = True
            desc = is_desc or False
            return self.expression(
                exp.Ordered, this=this, desc=desc, nulls_first=nulls_first
            )

    class Generator(Generator):
        TRANSFORMS = {
            **Generator.TRANSFORMS,
            exp.CurrentDate: no_paren_current_date_sql,
            exp.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
            exp.ILike: no_ilike_sql,
            exp.Ordered: ordered_sql,
            exp.TableSample: no_tablesample_sql,
            exp.TryCast: no_trycast_sql,
        }
