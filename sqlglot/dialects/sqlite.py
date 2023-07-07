from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    arrow_json_extract_scalar_sql,
    arrow_json_extract_sql,
    concat_to_dpipe_sql,
    count_if_to_sum,
    no_ilike_sql,
    no_pivot_sql,
    no_tablesample_sql,
    no_trycast_sql,
    rename_func,
)
from sqlglot.tokens import TokenType


def _date_add_sql(self: generator.Generator, expression: exp.DateAdd) -> str:
    modifier = expression.expression
    modifier = modifier.name if modifier.is_string else self.sql(modifier)
    unit = expression.args.get("unit")
    modifier = f"'{modifier} {unit.name}'" if unit else f"'{modifier}'"
    return self.func("DATE", expression.this, modifier)


def _transform_create(expression: exp.Expression) -> exp.Expression:
    """Move primary key to a column and enforce auto_increment on primary keys."""
    schema = expression.this

    if isinstance(expression, exp.Create) and isinstance(schema, exp.Schema):
        defs = {}
        primary_key = None

        for e in schema.expressions:
            if isinstance(e, exp.ColumnDef):
                defs[e.name] = e
            elif isinstance(e, exp.PrimaryKey):
                primary_key = e

        if primary_key and len(primary_key.expressions) == 1:
            column = defs[primary_key.expressions[0].name]
            column.append(
                "constraints", exp.ColumnConstraint(kind=exp.PrimaryKeyColumnConstraint())
            )
            schema.expressions.remove(primary_key)
        else:
            for column in defs.values():
                auto_increment = None
                for constraint in column.constraints.copy():
                    if isinstance(constraint.kind, exp.PrimaryKeyColumnConstraint):
                        break
                    if isinstance(constraint.kind, exp.AutoIncrementColumnConstraint):
                        auto_increment = constraint
                if auto_increment:
                    column.constraints.remove(auto_increment)

    return expression


class SQLite(Dialect):
    # https://sqlite.org/forum/forumpost/5e575586ac5c711b?raw
    RESOLVES_IDENTIFIERS_AS_UPPERCASE = None

    class Tokenizer(tokens.Tokenizer):
        IDENTIFIERS = ['"', ("[", "]"), "`"]
        HEX_STRINGS = [("x'", "'"), ("X'", "'"), ("0x", ""), ("0X", "")]

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "EDITDIST3": exp.Levenshtein.from_arg_list,
        }

    class Generator(generator.Generator):
        JOIN_HINTS = False
        TABLE_HINTS = False
        QUERY_HINTS = False

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.BOOLEAN: "INTEGER",
            exp.DataType.Type.TINYINT: "INTEGER",
            exp.DataType.Type.SMALLINT: "INTEGER",
            exp.DataType.Type.INT: "INTEGER",
            exp.DataType.Type.BIGINT: "INTEGER",
            exp.DataType.Type.FLOAT: "REAL",
            exp.DataType.Type.DOUBLE: "REAL",
            exp.DataType.Type.DECIMAL: "REAL",
            exp.DataType.Type.CHAR: "TEXT",
            exp.DataType.Type.NCHAR: "TEXT",
            exp.DataType.Type.VARCHAR: "TEXT",
            exp.DataType.Type.NVARCHAR: "TEXT",
            exp.DataType.Type.BINARY: "BLOB",
            exp.DataType.Type.VARBINARY: "BLOB",
        }

        TOKEN_MAPPING = {
            TokenType.AUTO_INCREMENT: "AUTOINCREMENT",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.Concat: concat_to_dpipe_sql,
            exp.CountIf: count_if_to_sum,
            exp.Create: transforms.preprocess([_transform_create]),
            exp.CurrentDate: lambda *_: "CURRENT_DATE",
            exp.CurrentTime: lambda *_: "CURRENT_TIME",
            exp.CurrentTimestamp: lambda *_: "CURRENT_TIMESTAMP",
            exp.DateAdd: _date_add_sql,
            exp.DateStrToDate: lambda self, e: self.sql(e, "this"),
            exp.ILike: no_ilike_sql,
            exp.JSONExtract: arrow_json_extract_sql,
            exp.JSONExtractScalar: arrow_json_extract_scalar_sql,
            exp.JSONBExtract: arrow_json_extract_sql,
            exp.JSONBExtractScalar: arrow_json_extract_scalar_sql,
            exp.Levenshtein: rename_func("EDITDIST3"),
            exp.LogicalOr: rename_func("MAX"),
            exp.LogicalAnd: rename_func("MIN"),
            exp.Pivot: no_pivot_sql,
            exp.SafeConcat: concat_to_dpipe_sql,
            exp.Select: transforms.preprocess(
                [transforms.eliminate_distinct_on, transforms.eliminate_qualify]
            ),
            exp.TableSample: no_tablesample_sql,
            exp.TimeStrToTime: lambda self, e: self.sql(e, "this"),
            exp.TryCast: no_trycast_sql,
        }

        PROPERTIES_LOCATION = {
            k: exp.Properties.Location.UNSUPPORTED
            for k, v in generator.Generator.PROPERTIES_LOCATION.items()
        }

        LIMIT_FETCH = "LIMIT"

        def cast_sql(self, expression: exp.Cast, safe_prefix: t.Optional[str] = None) -> str:
            if expression.is_type("date"):
                return self.func("DATE", expression.this)

            return super().cast_sql(expression)

        def datediff_sql(self, expression: exp.DateDiff) -> str:
            unit = expression.args.get("unit")
            unit = unit.name.upper() if unit else "DAY"

            sql = f"(JULIANDAY({self.sql(expression, 'this')}) - JULIANDAY({self.sql(expression, 'expression')}))"

            if unit == "MONTH":
                sql = f"{sql} / 30.0"
            elif unit == "YEAR":
                sql = f"{sql} / 365.0"
            elif unit == "HOUR":
                sql = f"{sql} * 24.0"
            elif unit == "MINUTE":
                sql = f"{sql} * 1440.0"
            elif unit == "SECOND":
                sql = f"{sql} * 86400.0"
            elif unit == "MILLISECOND":
                sql = f"{sql} * 86400000.0"
            elif unit == "MICROSECOND":
                sql = f"{sql} * 86400000000.0"
            elif unit == "NANOSECOND":
                sql = f"{sql} * 8640000000000.0"
            else:
                self.unsupported("DATEDIFF unsupported for '{unit}'.")

            return f"CAST({sql} AS INTEGER)"

        # https://www.sqlite.org/lang_aggfunc.html#group_concat
        def groupconcat_sql(self, expression: exp.GroupConcat) -> str:
            this = expression.this
            distinct = expression.find(exp.Distinct)

            if distinct:
                this = distinct.expressions[0]
                distinct_sql = "DISTINCT "
            else:
                distinct_sql = ""

            if isinstance(expression.this, exp.Order):
                self.unsupported("SQLite GROUP_CONCAT doesn't support ORDER BY.")
                if expression.this.this and not distinct:
                    this = expression.this.this

            separator = expression.args.get("separator")
            return f"GROUP_CONCAT({distinct_sql}{self.format_args(this, separator)})"

        def least_sql(self, expression: exp.Least) -> str:
            if len(expression.expressions) > 1:
                return rename_func("MIN")(self, expression)

            return self.sql(expression, "this")

        def transaction_sql(self, expression: exp.Transaction) -> str:
            this = expression.this
            this = f" {this}" if this else ""
            return f"BEGIN{this} TRANSACTION"
