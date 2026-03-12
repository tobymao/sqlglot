from __future__ import annotations

import typing as t
from functools import reduce

from sqlglot import exp, generator, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
    any_value_to_max_sql,
    date_delta_sql,
    datestrtodate_sql,
    generatedasidentitycolumnconstraint_sql,
    max_or_greatest,
    min_or_least,
    rename_func,
    strposition_sql,
    timestrtotime_sql,
    trim_sql,
)
from sqlglot.helper import seq_get
from sqlglot.parsers.tsql import OPTIONS_THAT_REQUIRE_EQUAL, TSQLParser
from sqlglot.time import format_time
from sqlglot.tokens import TokenType
from sqlglot.typing.tsql import EXPRESSION_METADATA

DATE_PART_UNMAPPING = {
    "WEEKISO": "ISO_WEEK",
    "DAYOFWEEK": "WEEKDAY",
    "TIMEZONE_MINUTE": "TZOFFSET",
}

BIT_TYPES = {exp.EQ, exp.NEQ, exp.Is, exp.In, exp.Select, exp.Alias}


def _format_sql(self: TSQL.Generator, expression: exp.NumberToStr | exp.TimeToStr) -> str:
    fmt = expression.args["format"]

    if not isinstance(expression, exp.NumberToStr):
        if fmt.is_string:
            mapped_fmt = format_time(fmt.name, TSQL.INVERSE_TIME_MAPPING)
            fmt_sql = self.sql(exp.Literal.string(mapped_fmt))
        else:
            fmt_sql = self.format_time(expression) or self.sql(fmt)
    else:
        fmt_sql = self.sql(fmt)

    return self.func("FORMAT", expression.this, fmt_sql, expression.args.get("culture"))


def _string_agg_sql(self: TSQL.Generator, expression: exp.GroupConcat) -> str:
    this = expression.this
    distinct = expression.find(exp.Distinct)
    if distinct:
        # exp.Distinct can appear below an exp.Order or an exp.GroupConcat expression
        self.unsupported("T-SQL STRING_AGG doesn't support DISTINCT.")
        this = distinct.pop().expressions[0]

    order = ""
    if isinstance(expression.this, exp.Order):
        if expression.this.this:
            this = expression.this.this.pop()
        # Order has a leading space
        order = f" WITHIN GROUP ({self.sql(expression.this)[1:]})"

    separator = expression.args.get("separator") or exp.Literal.string(",")
    return f"STRING_AGG({self.format_args(this, separator)}){order}"


def qualify_derived_table_outputs(expression: exp.Expr) -> exp.Expr:
    """Ensures all (unnamed) output columns are aliased for CTEs and Subqueries."""
    alias = expression.args.get("alias")

    if (
        isinstance(expression, (exp.CTE, exp.Subquery))
        and isinstance(alias, exp.TableAlias)
        and not alias.columns
    ):
        from sqlglot.optimizer.qualify_columns import qualify_outputs

        # We keep track of the unaliased column projection indexes instead of the expressions
        # themselves, because the latter are going to be replaced by new nodes when the aliases
        # are added and hence we won't be able to reach these newly added Alias parents
        query = expression.this
        unaliased_column_indexes = (
            i for i, c in enumerate(query.selects) if isinstance(c, exp.Column) and not c.alias
        )

        qualify_outputs(query)

        # Preserve the quoting information of columns for newly added Alias nodes
        query_selects = query.selects
        for select_index in unaliased_column_indexes:
            alias = query_selects[select_index]
            column = alias.this
            if isinstance(column.this, exp.Identifier):
                alias.args["alias"].set("quoted", column.this.quoted)

    return expression


def _json_extract_sql(
    self: TSQL.Generator, expression: exp.JSONExtract | exp.JSONExtractScalar
) -> str:
    json_query = self.func("JSON_QUERY", expression.this, expression.expression)
    json_value = self.func("JSON_VALUE", expression.this, expression.expression)
    return self.func("ISNULL", json_query, json_value)


def _timestrtotime_sql(self: TSQL.Generator, expression: exp.TimeStrToTime):
    sql = timestrtotime_sql(self, expression)
    if expression.args.get("zone"):
        # If there is a timezone, produce an expression like:
        # CAST('2020-01-01 12:13:14-08:00' AS DATETIMEOFFSET) AT TIME ZONE 'UTC'
        # If you dont have AT TIME ZONE 'UTC', wrapping that expression in another cast back to DATETIME2 just drops the timezone information
        return self.sql(exp.AtTimeZone(this=sql, zone=exp.Literal.string("UTC")))
    return sql


class TSQL(Dialect):
    LOG_BASE_FIRST = False
    TYPED_DIVISION = True
    CONCAT_COALESCE = True
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE
    ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN = False

    TIME_FORMAT = "'yyyy-mm-dd hh:mm:ss'"

    EXPRESSION_METADATA = EXPRESSION_METADATA.copy()

    DATE_PART_MAPPING = {
        **Dialect.DATE_PART_MAPPING,
        "QQ": "QUARTER",
        "M": "MONTH",
        "Y": "DAYOFYEAR",
        "WW": "WEEK",
        "N": "MINUTE",
        "SS": "SECOND",
        "MCS": "MICROSECOND",
        "TZOFFSET": "TIMEZONE_MINUTE",
        "TZ": "TIMEZONE_MINUTE",
        "ISO_WEEK": "WEEKISO",
        "ISOWK": "WEEKISO",
        "ISOWW": "WEEKISO",
    }

    TIME_MAPPING = {
        "year": "%Y",
        "dayofyear": "%j",
        "day": "%d",
        "dy": "%d",
        "y": "%Y",
        "week": "%W",
        "ww": "%W",
        "wk": "%W",
        "isowk": "%V",
        "isoww": "%V",
        "iso_week": "%V",
        "hour": "%h",
        "hh": "%I",
        "minute": "%M",
        "mi": "%M",
        "n": "%M",
        "second": "%S",
        "ss": "%S",
        "s": "%-S",
        "millisecond": "%f",
        "ms": "%f",
        "weekday": "%w",
        "dw": "%w",
        "month": "%m",
        "mm": "%M",
        "m": "%-M",
        "Y": "%Y",
        "YYYY": "%Y",
        "YY": "%y",
        "MMMM": "%B",
        "MMM": "%b",
        "MM": "%m",
        "M": "%-m",
        "dddd": "%A",
        "dd": "%d",
        "d": "%-d",
        "HH": "%H",
        "H": "%-H",
        "h": "%-I",
        "ffffff": "%f",
        "yyyy": "%Y",
        "yy": "%y",
    }

    CONVERT_FORMAT_MAPPING = {
        "0": "%b %d %Y %-I:%M%p",
        "1": "%m/%d/%y",
        "2": "%y.%m.%d",
        "3": "%d/%m/%y",
        "4": "%d.%m.%y",
        "5": "%d-%m-%y",
        "6": "%d %b %y",
        "7": "%b %d, %y",
        "8": "%H:%M:%S",
        "9": "%b %d %Y %-I:%M:%S:%f%p",
        "10": "mm-dd-yy",
        "11": "yy/mm/dd",
        "12": "yymmdd",
        "13": "%d %b %Y %H:%M:ss:%f",
        "14": "%H:%M:%S:%f",
        "20": "%Y-%m-%d %H:%M:%S",
        "21": "%Y-%m-%d %H:%M:%S.%f",
        "22": "%m/%d/%y %-I:%M:%S %p",
        "23": "%Y-%m-%d",
        "24": "%H:%M:%S",
        "25": "%Y-%m-%d %H:%M:%S.%f",
        "100": "%b %d %Y %-I:%M%p",
        "101": "%m/%d/%Y",
        "102": "%Y.%m.%d",
        "103": "%d/%m/%Y",
        "104": "%d.%m.%Y",
        "105": "%d-%m-%Y",
        "106": "%d %b %Y",
        "107": "%b %d, %Y",
        "108": "%H:%M:%S",
        "109": "%b %d %Y %-I:%M:%S:%f%p",
        "110": "%m-%d-%Y",
        "111": "%Y/%m/%d",
        "112": "%Y%m%d",
        "113": "%d %b %Y %H:%M:%S:%f",
        "114": "%H:%M:%S:%f",
        "120": "%Y-%m-%d %H:%M:%S",
        "121": "%Y-%m-%d %H:%M:%S.%f",
        "126": "%Y-%m-%dT%H:%M:%S.%f",
    }

    FORMAT_TIME_MAPPING = {
        "y": "%B %Y",
        "d": "%m/%d/%Y",
        "H": "%-H",
        "h": "%-I",
        "s": "%Y-%m-%d %H:%M:%S",
        "D": "%A,%B,%Y",
        "f": "%A,%B,%Y %-I:%M %p",
        "F": "%A,%B,%Y %-I:%M:%S %p",
        "g": "%m/%d/%Y %-I:%M %p",
        "G": "%m/%d/%Y %-I:%M:%S %p",
        "M": "%B %-d",
        "m": "%B %-d",
        "O": "%Y-%m-%dT%H:%M:%S",
        "u": "%Y-%M-%D %H:%M:%S%z",
        "U": "%A, %B %D, %Y %H:%M:%S%z",
        "T": "%-I:%M:%S %p",
        "t": "%-I:%M",
        "Y": "%a %Y",
    }

    class Tokenizer(tokens.Tokenizer):
        IDENTIFIERS = [("[", "]"), '"']
        QUOTES = ["'", '"']
        HEX_STRINGS = [("0x", ""), ("0X", "")]
        VAR_SINGLE_TOKENS = {"@", "$", "#"}

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "CLUSTERED INDEX": TokenType.INDEX,
            "DATETIME2": TokenType.DATETIME2,
            "DATETIMEOFFSET": TokenType.TIMESTAMPTZ,
            "DECLARE": TokenType.DECLARE,
            "EXEC": TokenType.EXECUTE,
            "FOR SYSTEM_TIME": TokenType.TIMESTAMP_SNAPSHOT,
            "GO": TokenType.COMMAND,
            "IMAGE": TokenType.IMAGE,
            "MONEY": TokenType.MONEY,
            "NONCLUSTERED INDEX": TokenType.INDEX,
            "NTEXT": TokenType.TEXT,
            "OPTION": TokenType.OPTION,
            "OUTPUT": TokenType.RETURNING,
            "PRINT": TokenType.COMMAND,
            "PROC": TokenType.PROCEDURE,
            "REAL": TokenType.FLOAT,
            "ROWVERSION": TokenType.ROWVERSION,
            "SMALLDATETIME": TokenType.SMALLDATETIME,
            "SMALLMONEY": TokenType.SMALLMONEY,
            "SQL_VARIANT": TokenType.VARIANT,
            "SYSTEM_USER": TokenType.CURRENT_USER,
            "TOP": TokenType.TOP,
            "TIMESTAMP": TokenType.ROWVERSION,
            "TINYINT": TokenType.UTINYINT,
            "UNIQUEIDENTIFIER": TokenType.UUID,
            "UPDATE STATISTICS": TokenType.COMMAND,
            "XML": TokenType.XML,
        }
        KEYWORDS.pop("/*+")

        COMMANDS = {*tokens.Tokenizer.COMMANDS, TokenType.END} - {TokenType.EXECUTE}

    Parser = TSQLParser

    class Generator(generator.Generator):
        LIMIT_IS_TOP = True
        QUERY_HINTS = False
        RETURNING_END = False
        NVL2_SUPPORTED = False
        ALTER_TABLE_INCLUDE_COLUMN_KEYWORD = False
        LIMIT_FETCH = "FETCH"
        COMPUTED_COLUMN_WITH_TYPE = False
        CTE_RECURSIVE_KEYWORD_REQUIRED = False
        ENSURE_BOOLS = True
        NULL_ORDERING_SUPPORTED = None
        SUPPORTS_SINGLE_ARG_CONCAT = False
        TABLESAMPLE_SEED_KEYWORD = "REPEATABLE"
        SUPPORTS_SELECT_INTO = True
        JSON_PATH_BRACKETED_KEY_SUPPORTED = False
        SUPPORTS_TO_NUMBER = False
        SET_OP_MODIFIERS = False
        COPY_PARAMS_EQ_REQUIRED = True
        PARSE_JSON_NAME = None
        EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE = False
        ALTER_SET_WRAPPED = True
        ALTER_SET_TYPE = ""

        EXPRESSIONS_WITHOUT_NESTED_CTES = {
            exp.Create,
            exp.Delete,
            exp.Insert,
            exp.Intersect,
            exp.Except,
            exp.Merge,
            exp.Select,
            exp.Subquery,
            exp.Union,
            exp.Update,
        }

        SUPPORTED_JSON_PATH_PARTS = {
            exp.JSONPathKey,
            exp.JSONPathRoot,
            exp.JSONPathSubscript,
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DType.BOOLEAN: "BIT",
            exp.DType.DATETIME2: "DATETIME2",
            exp.DType.DECIMAL: "NUMERIC",
            exp.DType.DOUBLE: "FLOAT",
            exp.DType.INT: "INTEGER",
            exp.DType.ROWVERSION: "ROWVERSION",
            exp.DType.TEXT: "VARCHAR(MAX)",
            exp.DType.TIMESTAMP: "DATETIME2",
            exp.DType.TIMESTAMPNTZ: "DATETIME2",
            exp.DType.TIMESTAMPTZ: "DATETIMEOFFSET",
            exp.DType.SMALLDATETIME: "SMALLDATETIME",
            exp.DType.UTINYINT: "TINYINT",
            exp.DType.VARIANT: "SQL_VARIANT",
            exp.DType.UUID: "UNIQUEIDENTIFIER",
        }

        TYPE_MAPPING.pop(exp.DType.NCHAR)
        TYPE_MAPPING.pop(exp.DType.NVARCHAR)

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.AnyValue: any_value_to_max_sql,
            exp.Atan2: rename_func("ATN2"),
            exp.ArrayToString: rename_func("STRING_AGG"),
            exp.AutoIncrementColumnConstraint: lambda *_: "IDENTITY",
            exp.Ceil: rename_func("CEILING"),
            exp.Chr: rename_func("CHAR"),
            exp.DateAdd: date_delta_sql("DATEADD"),
            exp.CTE: transforms.preprocess([qualify_derived_table_outputs]),
            exp.CurrentDate: rename_func("GETDATE"),
            exp.CurrentTimestamp: rename_func("GETDATE"),
            exp.CurrentTimestampLTZ: rename_func("SYSDATETIMEOFFSET"),
            exp.DateStrToDate: datestrtodate_sql,
            exp.GeneratedAsIdentityColumnConstraint: generatedasidentitycolumnconstraint_sql,
            exp.GroupConcat: _string_agg_sql,
            exp.If: rename_func("IIF"),
            exp.JSONExtract: _json_extract_sql,
            exp.JSONExtractScalar: _json_extract_sql,
            exp.LastDay: lambda self, e: self.func("EOMONTH", e.this),
            exp.Ln: rename_func("LOG"),
            exp.Max: max_or_greatest,
            exp.MD5: lambda self, e: self.func("HASHBYTES", exp.Literal.string("MD5"), e.this),
            exp.Min: min_or_least,
            exp.NumberToStr: _format_sql,
            exp.Repeat: rename_func("REPLICATE"),
            exp.CurrentSchema: rename_func("SCHEMA_NAME"),
            exp.Select: transforms.preprocess(
                [
                    transforms.eliminate_distinct_on,
                    transforms.eliminate_semi_and_anti_joins,
                    transforms.eliminate_qualify,
                    transforms.unnest_generate_date_array_using_recursive_cte,
                ]
            ),
            exp.Stddev: rename_func("STDEV"),
            exp.StrPosition: lambda self, e: strposition_sql(
                self, e, func_name="CHARINDEX", supports_position=True
            ),
            exp.Subquery: transforms.preprocess([qualify_derived_table_outputs]),
            exp.SHA: lambda self, e: self.func("HASHBYTES", exp.Literal.string("SHA1"), e.this),
            exp.SHA1Digest: lambda self, e: self.func(
                "HASHBYTES", exp.Literal.string("SHA1"), e.this
            ),
            exp.SHA2: lambda self, e: self.func(
                "HASHBYTES", exp.Literal.string(f"SHA2_{e.args.get('length', 256)}"), e.this
            ),
            exp.TemporaryProperty: lambda self, e: "",
            exp.TimeStrToTime: _timestrtotime_sql,
            exp.TimeToStr: _format_sql,
            exp.Trim: trim_sql,
            exp.TsOrDsAdd: date_delta_sql("DATEADD", cast=True),
            exp.TsOrDsDiff: date_delta_sql("DATEDIFF"),
            exp.TimestampTrunc: lambda self, e: self.func("DATETRUNC", e.unit, e.this),
            exp.Trunc: lambda self, e: self.func(
                "ROUND",
                e.this,
                e.args.get("decimals") or exp.Literal.number(0),
                exp.Literal.number(1),
            ),
            exp.Uuid: lambda *_: "NEWID()",
            exp.DateFromParts: rename_func("DATEFROMPARTS"),
        }

        TRANSFORMS.pop(exp.ReturnsProperty)

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        }

        def scope_resolution(self, rhs: str, scope_name: str) -> str:
            return f"{scope_name}::{rhs}"

        def select_sql(self, expression: exp.Select) -> str:
            limit = expression.args.get("limit")
            offset = expression.args.get("offset")

            if isinstance(limit, exp.Fetch) and not offset:
                # Dialects like Oracle can FETCH directly from a row set but
                # T-SQL requires an ORDER BY + OFFSET clause in order to FETCH
                offset = exp.Offset(expression=exp.Literal.number(0))
                expression.set("offset", offset)

            if offset:
                if not expression.args.get("order"):
                    # ORDER BY is required in order to use OFFSET in a query, so we use
                    # a noop order by, since we don't really care about the order.
                    # See: https://www.microsoftpressstore.com/articles/article.aspx?p=2314819
                    expression.order_by(exp.select(exp.null()).subquery(), copy=False)

                if isinstance(limit, exp.Limit):
                    # TOP and OFFSET can't be combined, we need use FETCH instead of TOP
                    # we replace here because otherwise TOP would be generated in select_sql
                    limit.replace(exp.Fetch(direction="FIRST", count=limit.expression))

            return super().select_sql(expression)

        def convert_sql(self, expression: exp.Convert) -> str:
            name = "TRY_CONVERT" if expression.args.get("safe") else "CONVERT"
            return self.func(
                name, expression.this, expression.expression, expression.args.get("style")
            )

        def queryoption_sql(self, expression: exp.QueryOption) -> str:
            option = self.sql(expression, "this")
            value = self.sql(expression, "expression")
            if value:
                optional_equal_sign = "= " if option in OPTIONS_THAT_REQUIRE_EQUAL else ""
                return f"{option} {optional_equal_sign}{value}"
            return option

        def lateral_op(self, expression: exp.Lateral) -> str:
            cross_apply = expression.args.get("cross_apply")
            if cross_apply is True:
                return "CROSS APPLY"
            if cross_apply is False:
                return "OUTER APPLY"

            # TODO: perhaps we can check if the parent is a Join and transpile it appropriately
            self.unsupported("LATERAL clause is not supported.")
            return "LATERAL"

        def splitpart_sql(self: TSQL.Generator, expression: exp.SplitPart) -> str:
            this = expression.this
            split_count = len(this.name.split("."))
            delimiter = expression.args.get("delimiter")
            part_index = expression.args.get("part_index")

            if (
                not all(isinstance(arg, exp.Literal) for arg in (this, delimiter, part_index))
                or (delimiter and delimiter.name != ".")
                or not part_index
                or split_count > 4
            ):
                self.unsupported(
                    "SPLIT_PART can be transpiled to PARSENAME only for '.' delimiter and literal values"
                )
                return ""

            return self.func(
                "PARSENAME", this, exp.Literal.number(split_count + 1 - part_index.to_py())
            )

        def extract_sql(self, expression: exp.Extract) -> str:
            part = expression.this
            name = DATE_PART_UNMAPPING.get(part.name.upper()) or part

            return self.func("DATEPART", name, expression.expression)

        def timefromparts_sql(self, expression: exp.TimeFromParts) -> str:
            nano = expression.args.get("nano")
            if nano is not None:
                nano.pop()
                self.unsupported("Specifying nanoseconds is not supported in TIMEFROMPARTS.")

            if expression.args.get("fractions") is None:
                expression.set("fractions", exp.Literal.number(0))
            if expression.args.get("precision") is None:
                expression.set("precision", exp.Literal.number(0))

            return rename_func("TIMEFROMPARTS")(self, expression)

        def timestampfromparts_sql(self, expression: exp.TimestampFromParts) -> str:
            zone = expression.args.get("zone")
            if zone is not None:
                zone.pop()
                self.unsupported("Time zone is not supported in DATETIMEFROMPARTS.")

            nano = expression.args.get("nano")
            if nano is not None:
                nano.pop()
                self.unsupported("Specifying nanoseconds is not supported in DATETIMEFROMPARTS.")

            if expression.args.get("milli") is None:
                expression.set("milli", exp.Literal.number(0))

            return rename_func("DATETIMEFROMPARTS")(self, expression)

        def setitem_sql(self, expression: exp.SetItem) -> str:
            this = expression.this
            if isinstance(this, exp.EQ) and not isinstance(this.left, exp.Parameter):
                # T-SQL does not use '=' in SET command, except when the LHS is a variable.
                return f"{self.sql(this.left)} {self.sql(this.right)}"

            return super().setitem_sql(expression)

        def boolean_sql(self, expression: exp.Boolean) -> str:
            if type(expression.parent) in BIT_TYPES or isinstance(
                expression.find_ancestor(exp.Values, exp.Select), exp.Values
            ):
                return "1" if expression.this else "0"

            return "(1 = 1)" if expression.this else "(1 = 0)"

        def is_sql(self, expression: exp.Is) -> str:
            if isinstance(expression.expression, exp.Boolean):
                return self.binary(expression, "=")
            return self.binary(expression, "IS")

        def createable_sql(self, expression: exp.Create, locations: t.DefaultDict) -> str:
            sql = self.sql(expression, "this")
            properties = expression.args.get("properties")

            if sql[:1] != "#" and any(
                isinstance(prop, exp.TemporaryProperty)
                for prop in (properties.expressions if properties else [])
            ):
                sql = f"[#{sql[1:]}" if sql.startswith("[") else f"#{sql}"

            return sql

        def create_sql(self, expression: exp.Create) -> str:
            kind = expression.kind
            exists = expression.args.get("exists")
            expression.set("exists", None)

            like_property = expression.find(exp.LikeProperty)
            if like_property:
                ctas_expression = like_property.this
            else:
                ctas_expression = expression.expression

            if kind == "VIEW":
                expression.this.set("catalog", None)
                with_ = expression.args.get("with_")
                if ctas_expression and with_:
                    # We've already preprocessed the Create expression to bubble up any nested CTEs,
                    # but CREATE VIEW actually requires the WITH clause to come after it so we need
                    # to amend the AST by moving the CTEs to the CREATE VIEW statement's query.
                    ctas_expression.set("with_", with_.pop())

            table = expression.find(exp.Table)

            # Convert CTAS statement to SELECT .. INTO ..
            if kind == "TABLE" and ctas_expression:
                if isinstance(ctas_expression, exp.UNWRAPPED_QUERIES):
                    ctas_expression = ctas_expression.subquery()

                properties = expression.args.get("properties") or exp.Properties()
                is_temp = any(isinstance(p, exp.TemporaryProperty) for p in properties.expressions)

                select_into = exp.select("*").from_(exp.alias_(ctas_expression, "temp", table=True))
                select_into.set("into", exp.Into(this=table, temporary=is_temp))

                if like_property:
                    select_into.limit(0, copy=False)

                sql = self.sql(select_into)
            else:
                sql = super().create_sql(expression)

            if exists:
                identifier = self.sql(exp.Literal.string(exp.table_name(table) if table else ""))
                sql_with_ctes = self.prepend_ctes(expression, sql)
                sql_literal = self.sql(exp.Literal.string(sql_with_ctes))
                if kind == "SCHEMA":
                    return f"""IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = {identifier}) EXEC({sql_literal})"""
                elif kind == "TABLE":
                    assert table
                    where = exp.and_(
                        exp.column("TABLE_NAME").eq(table.name),
                        exp.column("TABLE_SCHEMA").eq(table.db) if table.db else None,
                        exp.column("TABLE_CATALOG").eq(table.catalog) if table.catalog else None,
                    )
                    return f"""IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE {where}) EXEC({sql_literal})"""
                elif kind == "INDEX":
                    index = self.sql(exp.Literal.string(expression.this.text("this")))
                    return f"""IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = object_id({identifier}) AND name = {index}) EXEC({sql_literal})"""
            elif expression.args.get("replace"):
                sql = sql.replace("CREATE OR REPLACE ", "CREATE OR ALTER ", 1)

            return self.prepend_ctes(expression, sql)

        @generator.unsupported_args("unlogged", "expressions")
        def into_sql(self, expression: exp.Into) -> str:
            if expression.args.get("temporary"):
                # If the Into expression has a temporary property, push this down to the Identifier
                table = expression.find(exp.Table)
                if table and isinstance(table.this, exp.Identifier):
                    table.this.set("temporary", True)

            return f"{self.seg('INTO')} {self.sql(expression, 'this')}"

        def count_sql(self, expression: exp.Count) -> str:
            func_name = "COUNT_BIG" if expression.args.get("big_int") else "COUNT"
            return rename_func(func_name)(self, expression)

        def datediff_sql(self, expression: exp.DateDiff) -> str:
            func_name = "DATEDIFF_BIG" if expression.args.get("big_int") else "DATEDIFF"
            return date_delta_sql(func_name)(self, expression)

        def offset_sql(self, expression: exp.Offset) -> str:
            return f"{super().offset_sql(expression)} ROWS"

        def version_sql(self, expression: exp.Version) -> str:
            name = "SYSTEM_TIME" if expression.name == "TIMESTAMP" else expression.name
            this = f"FOR {name}"
            expr = expression.expression
            kind = expression.text("kind")
            if kind in ("FROM", "BETWEEN"):
                args = expr.expressions
                sep = "TO" if kind == "FROM" else "AND"
                expr_sql = f"{self.sql(seq_get(args, 0))} {sep} {self.sql(seq_get(args, 1))}"
            else:
                expr_sql = self.sql(expr)

            expr_sql = f" {expr_sql}" if expr_sql else ""
            return f"{this} {kind}{expr_sql}"

        def returnsproperty_sql(self, expression: exp.ReturnsProperty) -> str:
            table = expression.args.get("table")
            table = f"{table} " if table else ""
            return f"RETURNS {table}{self.sql(expression, 'this')}"

        def returning_sql(self, expression: exp.Returning) -> str:
            into = self.sql(expression, "into")
            into = self.seg(f"INTO {into}") if into else ""
            return f"{self.seg('OUTPUT')} {self.expressions(expression, flat=True)}{into}"

        def transaction_sql(self, expression: exp.Transaction) -> str:
            this = self.sql(expression, "this")
            this = f" {this}" if this else ""
            mark = self.sql(expression, "mark")
            mark = f" WITH MARK {mark}" if mark else ""
            return f"BEGIN TRANSACTION{this}{mark}"

        def commit_sql(self, expression: exp.Commit) -> str:
            this = self.sql(expression, "this")
            this = f" {this}" if this else ""
            durability = expression.args.get("durability")
            durability = (
                f" WITH (DELAYED_DURABILITY = {'ON' if durability else 'OFF'})"
                if durability is not None
                else ""
            )
            return f"COMMIT TRANSACTION{this}{durability}"

        def rollback_sql(self, expression: exp.Rollback) -> str:
            this = self.sql(expression, "this")
            this = f" {this}" if this else ""
            return f"ROLLBACK TRANSACTION{this}"

        def identifier_sql(self, expression: exp.Identifier) -> str:
            identifier = super().identifier_sql(expression)

            if expression.args.get("global_"):
                identifier = f"##{identifier}"
            elif expression.args.get("temporary"):
                identifier = f"#{identifier}"

            return identifier

        def constraint_sql(self, expression: exp.Constraint) -> str:
            this = self.sql(expression, "this")
            expressions = self.expressions(expression, flat=True, sep=" ")
            return f"CONSTRAINT {this} {expressions}"

        def length_sql(self, expression: exp.Length) -> str:
            return self._uncast_text(expression, "LEN")

        def right_sql(self, expression: exp.Right) -> str:
            return self._uncast_text(expression, "RIGHT")

        def left_sql(self, expression: exp.Left) -> str:
            return self._uncast_text(expression, "LEFT")

        def _uncast_text(self, expression: exp.Expr, name: str) -> str:
            this = expression.this
            if isinstance(this, exp.Cast) and this.is_type(exp.DType.TEXT):
                this_sql = self.sql(this, "this")
            else:
                this_sql = self.sql(this)
            expression_sql = self.sql(expression, "expression")
            return self.func(name, this_sql, expression_sql if expression_sql else None)

        def partition_sql(self, expression: exp.Partition) -> str:
            return f"WITH (PARTITIONS({self.expressions(expression, flat=True)}))"

        def alter_sql(self, expression: exp.Alter) -> str:
            action = seq_get(expression.args.get("actions") or [], 0)
            if isinstance(action, exp.AlterRename):
                return f"EXEC sp_rename '{self.sql(expression.this)}', '{action.this.name}'"
            return super().alter_sql(expression)

        def drop_sql(self, expression: exp.Drop) -> str:
            if expression.args["kind"] == "VIEW":
                expression.this.set("catalog", None)
            return super().drop_sql(expression)

        def options_modifier(self, expression: exp.Expr) -> str:
            options = self.expressions(expression, key="options")
            return f" OPTION{self.wrap(options)}" if options else ""

        def dpipe_sql(self, expression: exp.DPipe) -> str:
            return self.sql(
                reduce(lambda x, y: exp.Add(this=x, expression=y), expression.flatten())
            )

        def isascii_sql(self, expression: exp.IsAscii) -> str:
            return f"(PATINDEX(CONVERT(VARCHAR(MAX), 0x255b5e002d7f5d25) COLLATE Latin1_General_BIN, {self.sql(expression.this)}) = 0)"

        def columndef_sql(self, expression: exp.ColumnDef, sep: str = " ") -> str:
            this = super().columndef_sql(expression, sep)
            default = self.sql(expression, "default")
            default = f" = {default}" if default else ""
            output = self.sql(expression, "output")
            output = f" {output}" if output else ""
            return f"{this}{default}{output}"

        def coalesce_sql(self, expression: exp.Coalesce) -> str:
            func_name = "ISNULL" if expression.args.get("is_null") else "COALESCE"
            return rename_func(func_name)(self, expression)

        def storedprocedure_sql(self, expression: exp.StoredProcedure) -> str:
            this = self.sql(expression, "this")
            expressions = self.expressions(expression)
            expressions = (
                self.wrap(expressions) if expression.args.get("wrapped") else f" {expressions}"
            )
            return f"{this}{expressions}" if expressions.strip() != "" else this

        def ifblock_sql(self, expression: exp.IfBlock) -> str:
            this = self.sql(expression, "this")
            true = self.sql(expression, "true")
            true = f" {true}" if true else " "
            false = self.sql(expression, "false")
            false = f"; ELSE BEGIN {false}" if false else ""
            return f"IF {this} BEGIN{true}{false}"

        def whileblock_sql(self, expression: exp.WhileBlock) -> str:
            this = self.sql(expression, "this")
            body = self.sql(expression, "body")
            body = f" {body}" if body else " "
            return f"WHILE {this} BEGIN{body}"

        def execute_sql(self, expression: exp.Execute) -> str:
            this = self.sql(expression, "this")
            expressions = self.expressions(expression)
            expressions = f" {expressions}" if expressions else ""
            return f"EXECUTE {this}{expressions}"

        def executesql_sql(self, expression: exp.ExecuteSql) -> str:
            return self.execute_sql(expression)
