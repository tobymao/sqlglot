from __future__ import annotations

import typing as t
from typing import Any

from sqlglot import exp, generator, parser, tokens, transforms, Expression
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
    binary_from_function,
    build_formatted_time,
    groupconcat_sql,
    rename_func,
    strposition_sql,
    timestrtotime_sql,
    timestamptrunc_sql,
    build_date_delta,
    no_last_day_sql,
    DATE_ADD_OR_SUB,
)
from sqlglot.expressions import Paren, Tuple
from sqlglot.generator import unsupported_args
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType
from sqlglot.optimizer.scope import build_scope

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType


def _sha2_sql(self: Exasol.Generator, expression: exp.SHA2) -> str:
    length = expression.text("length")
    func_name = "HASH_SHA256" if length == "256" else "HASH_SHA512"
    return self.func(func_name, expression.this)


def _date_diff_sql(self: Exasol.Generator, expression: exp.DateDiff | exp.TsOrDsDiff) -> str:
    unit = expression.text("unit").upper() or "DAY"

    if unit not in DATE_UNITS:
        self.unsupported(f"'{unit}' is not supported in Exasol.")
        return self.function_fallback_sql(expression)

    return self.func(f"{unit}S_BETWEEN", expression.this, expression.expression)


# https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/trunc%5Bate%5D%20(datetime).htm
# https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/trunc%5Bate%5D%20(number).htm
def _build_trunc(args: t.List[exp.Expression], dialect: DialectType) -> exp.Expression:
    first, second = seq_get(args, 0), seq_get(args, 1)

    if not first or not second:
        return exp.Anonymous(this="TRUNC", expressions=args)

    if not first.type:
        from sqlglot.optimizer.annotate_types import annotate_types

        first = annotate_types(first, dialect=dialect)

    if first.is_type(exp.DataType.Type.DATE, exp.DataType.Type.TIMESTAMP) and second.is_string:
        return exp.DateTrunc(this=first, unit=second)

    return exp.Anonymous(this="TRUNC", expressions=args)


# https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/zeroifnull.htm
def _build_zeroifnull(args: t.List) -> exp.If:
    cond = exp.Is(this=seq_get(args, 0), expression=exp.Null())
    return exp.If(this=cond, true=exp.Literal.number(0), false=seq_get(args, 0))


# https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/nullifzero.htm
def _build_nullifzero(args: t.List) -> exp.If:
    cond = exp.EQ(this=seq_get(args, 0), expression=exp.Literal.number(0))
    return exp.If(this=cond, true=exp.Null(), false=seq_get(args, 0))


# https://docs.exasol.com/db/latest/sql/select.htm#:~:text=If%20you%20have,local.x%3E10
def _add_local_prefix_for_aliases(expression: exp.Expression) -> exp.Expression:
    if isinstance(expression, exp.Select):
        aliases: dict[str, bool] = {
            alias.name: bool(alias.args.get("quoted"))
            for sel in expression.selects
            if isinstance(sel, exp.Alias) and (alias := sel.args.get("alias"))
        }

        table = expression.find(exp.Table)
        table_ident = table.this if table else None

        if (
            table_ident
            and table_ident.name.upper() == "LOCAL"
            and not bool(table_ident.args.get("quoted"))
        ):
            table_ident.replace(exp.to_identifier(table_ident.name.upper(), quoted=True))

        def prefix_local(node, visible_aliases: dict[str, bool]) -> exp.Expression:
            if isinstance(node, exp.Column) and not node.table:
                if node.name in visible_aliases:
                    return exp.Column(
                        this=exp.to_identifier(node.name, quoted=visible_aliases[node.name]),
                        table=exp.to_identifier("LOCAL", quoted=False),
                    )
            return node

        for key in ("where", "group", "having"):
            if arg := expression.args.get(key):
                expression.set(key, arg.transform(lambda node: prefix_local(node, aliases)))

        seen_aliases: dict[str, bool] = {}
        new_selects: list[exp.Expression] = []
        for sel in expression.selects:
            if isinstance(sel, exp.Alias):
                inner = sel.this.transform(lambda node: prefix_local(node, seen_aliases))
                sel.set("this", inner)

                alias_node = sel.args.get("alias")

                seen_aliases[sel.alias] = bool(alias_node and getattr(alias_node, "quoted", False))
                new_selects.append(sel)
            else:
                new_selects.append(sel.transform(lambda node: prefix_local(node, seen_aliases)))
        expression.set("expressions", new_selects)

    return expression


def _trunc_sql(self: Exasol.Generator, kind: str, expression: exp.DateTrunc) -> str:
    unit = expression.text("unit")
    node = expression.this.this if isinstance(expression.this, exp.Cast) else expression.this
    expr_sql = self.sql(node)
    if isinstance(node, exp.Literal) and node.is_string:
        expr_sql = (
            f"{kind} '{node.this.replace('T', ' ')}'"
            if kind == "TIMESTAMP"
            else f"DATE '{node.this}'"
        )
    return f"DATE_TRUNC('{unit}', {expr_sql})"


def _date_trunc_sql(self: Exasol.Generator, expression: exp.DateTrunc) -> str:
    return _trunc_sql(self, "DATE", expression)


def _timestamp_trunc_sql(self: Exasol.Generator, expression: exp.DateTrunc) -> str:
    return _trunc_sql(self, "TIMESTAMP", expression)


def is_case_insensitive(node: exp.Expression) -> bool:
    return isinstance(node, exp.Collate) and node.text("expression").upper() == "UTF8_LCASE"


def _substring_index_sql(self: Exasol.Generator, expression: exp.SubstringIndex) -> str:
    this = expression.this
    delimiter = expression.args["delimiter"]
    count_node = expression.args["count"]
    count_sql = self.sql(expression, "count")
    num = count_node.to_py() if count_node.is_number else 0

    haystack_sql = self.sql(this)
    if num == 0:
        return self.func("SUBSTR", haystack_sql, "1", "0")

    from_right = num < 0
    direction = "-1" if from_right else "1"
    occur = self.func("ABS", count_sql) if from_right else count_sql

    delimiter_sql = self.sql(delimiter)

    position = self.func(
        "INSTR",
        self.func("LOWER", haystack_sql) if is_case_insensitive(this) else haystack_sql,
        self.func("LOWER", delimiter_sql) if is_case_insensitive(delimiter) else delimiter_sql,
        direction,
        occur,
    )
    nullable_pos = self.func("NULLIF", position, "0")

    if from_right:
        start = self.func(
            "NVL", f"{nullable_pos} + {self.func('LENGTH', delimiter_sql)}", direction
        )
        return self.func("SUBSTR", haystack_sql, start)

    length = self.func("NVL", f"{nullable_pos} - 1", self.func("LENGTH", haystack_sql))
    return self.func("SUBSTR", haystack_sql, direction, length)


# https://docs.exasol.com/db/latest/sql/select.htm#:~:text=The%20select_list%20defines%20the%20columns%20of%20the%20result%20table.%20If%20*%20is%20used%2C%20all%20columns%20are%20listed.%20You%20can%20use%20an%20expression%20like%20t.*%20to%20list%20all%20columns%20of%20the%20table%20t%2C%20the%20view%20t%2C%20or%20the%20object%20with%20the%20table%20alias%20t.
def _qualify_unscoped_star(expression: exp.Expression) -> exp.Expression:
    """
    Exasol doesn't support a bare * alongside other select items, so we rewrite it
    Rewrite: SELECT *, <other> FROM <Table>
    Into: SELECT T.*, <other> FROM <Table> AS T
    """

    if not isinstance(expression, exp.Select):
        return expression

    select_expressions = expression.expressions or []

    def is_bare_star(expr: exp.Expression) -> bool:
        return isinstance(expr, exp.Star) and expr.this is None

    has_other_expression = False
    bare_star_expr: exp.Expression | None = None
    for expr in select_expressions:
        has_bare_star = is_bare_star(expr)
        if has_bare_star and bare_star_expr is None:
            bare_star_expr = expr
        elif not has_bare_star:
            has_other_expression = True
        if bare_star_expr and has_other_expression:
            break

    if not (bare_star_expr and has_other_expression):
        return expression

    scope = build_scope(expression)

    if not scope or not scope.selected_sources:
        return expression

    table_identifiers: list[exp.Identifier] = []

    for source_name, (source_expr, _) in scope.selected_sources.items():
        ident = (
            source_expr.this.copy()
            if isinstance(source_expr, exp.Table) and isinstance(source_expr.this, exp.Identifier)
            else exp.to_identifier(source_name)
        )
        table_identifiers.append(ident)

    qualified_star_columns = [
        exp.Column(this=bare_star_expr.copy(), table=ident) for ident in table_identifiers
    ]

    new_select_expressions: list[exp.Expression] = []

    for select_expr in select_expressions:
        new_select_expressions.extend(qualified_star_columns) if is_bare_star(
            select_expr
        ) else new_select_expressions.append(select_expr)

    expression.set("expressions", new_select_expressions)
    return expression


def _add_date_sql(self: Exasol.Generator, expression: DATE_ADD_OR_SUB) -> str:
    interval = expression.expression if isinstance(expression.expression, exp.Interval) else None

    unit = (
        (interval.text("unit") or "DAY").upper()
        if interval is not None
        else (expression.text("unit") or "DAY").upper()
    )

    if unit not in DATE_UNITS:
        self.unsupported(f"'{unit}' is not supported in Exasol.")
        return self.function_fallback_sql(expression)

    offset_expr: exp.Expression = expression.expression
    if interval is not None:
        offset_expr = interval.this

    if isinstance(expression, exp.DateSub):
        offset_expr = exp.Neg(this=offset_expr)

    return self.func(f"ADD_{unit}S", expression.this, offset_expr)


DATE_UNITS = {"DAY", "WEEK", "MONTH", "YEAR", "HOUR", "MINUTE", "SECOND"}


class Exasol(Dialect):
    # https://docs.exasol.com/db/latest/sql_references/basiclanguageelements.htm#SQLidentifier
    NORMALIZATION_STRATEGY = NormalizationStrategy.UPPERCASE
    # https://docs.exasol.com/db/latest/sql_references/data_types/datatypesoverview.htm
    SUPPORTS_USER_DEFINED_TYPES = False
    # https://docs.exasol.com/db/latest/sql/select.htm
    SUPPORTS_SEMI_ANTI_JOIN = False
    SUPPORTS_COLUMN_JOIN_MARKS = True
    NULL_ORDERING = "nulls_are_last"
    # https://docs.exasol.com/db/latest/sql_references/literals.htm#StringLiterals
    CONCAT_COALESCE = True

    TIME_MAPPING = {
        "yyyy": "%Y",
        "YYYY": "%Y",
        "yy": "%y",
        "YY": "%y",
        "mm": "%m",
        "MM": "%m",
        "MONTH": "%B",
        "MON": "%b",
        "dd": "%d",
        "DD": "%d",
        "DAY": "%A",
        "DY": "%a",
        "H12": "%I",
        "H24": "%H",
        "HH": "%H",
        "ID": "%u",
        "vW": "%V",
        "IW": "%V",
        "vYYY": "%G",
        "IYYY": "%G",
        "MI": "%M",
        "SS": "%S",
        "uW": "%W",
        "UW": "%U",
        "Z": "%z",
    }

    class Tokenizer(tokens.Tokenizer):
        IDENTIFIERS = ['"', ("[", "]")]
        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "USER": TokenType.CURRENT_USER,
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/if.htm
            "ENDIF": TokenType.END,
            "LONG VARCHAR": TokenType.TEXT,
            "SEPARATOR": TokenType.SEPARATOR,
        }
        KEYWORDS.pop("DIV")

    class Parser(parser.Parser):
        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            **{
                f"ADD_{unit}S": build_date_delta(exp.DateAdd, default_unit=unit)
                for unit in DATE_UNITS
            },
            **{
                f"{unit}S_BETWEEN": build_date_delta(exp.DateDiff, default_unit=unit)
                for unit in DATE_UNITS
            },
            "BIT_AND": binary_from_function(exp.BitwiseAnd),
            "BIT_OR": binary_from_function(exp.BitwiseOr),
            "BIT_XOR": binary_from_function(exp.BitwiseXor),
            "BIT_NOT": lambda args: exp.BitwiseNot(this=seq_get(args, 0)),
            "BIT_LSHIFT": binary_from_function(exp.BitwiseLeftShift),
            "BIT_RSHIFT": binary_from_function(exp.BitwiseRightShift),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/date_trunc.htm#DATE_TRUNC
            "DATE_TRUNC": lambda args: exp.TimestampTrunc(
                this=seq_get(args, 1), unit=seq_get(args, 0)
            ),
            "DIV": binary_from_function(exp.IntDiv),
            "EVERY": lambda args: exp.All(this=seq_get(args, 0)),
            "EDIT_DISTANCE": exp.Levenshtein.from_arg_list,
            "HASH_SHA": exp.SHA.from_arg_list,
            "HASH_SHA1": exp.SHA.from_arg_list,
            "HASH_MD5": exp.MD5.from_arg_list,
            "HASHTYPE_MD5": exp.MD5Digest.from_arg_list,
            "REGEXP_SUBSTR": exp.RegexpExtract.from_arg_list,
            "REGEXP_REPLACE": lambda args: exp.RegexpReplace(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
                replacement=seq_get(args, 2),
                position=seq_get(args, 3),
                occurrence=seq_get(args, 4),
            ),
            "HASH_SHA256": lambda args: exp.SHA2(
                this=seq_get(args, 0), length=exp.Literal.number(256)
            ),
            "HASH_SHA512": lambda args: exp.SHA2(
                this=seq_get(args, 0), length=exp.Literal.number(512)
            ),
            "TRUNC": _build_trunc,
            "TRUNCATE": _build_trunc,
            "VAR_POP": exp.VariancePop.from_arg_list,
            "APPROXIMATE_COUNT_DISTINCT": exp.ApproxDistinct.from_arg_list,
            "TO_CHAR": build_formatted_time(exp.ToChar, "exasol"),
            "TO_DATE": build_formatted_time(exp.TsOrDsToDate, "exasol"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/convert_tz.htm
            "CONVERT_TZ": lambda args: exp.ConvertTimezone(
                source_tz=seq_get(args, 1),
                target_tz=seq_get(args, 2),
                timestamp=seq_get(args, 0),
                options=seq_get(args, 3),
            ),
            "NULLIFZERO": _build_nullifzero,
            "ZEROIFNULL": _build_zeroifnull,
        }
        CONSTRAINT_PARSERS = {
            **parser.Parser.CONSTRAINT_PARSERS,
            "COMMENT": lambda self: self.expression(
                exp.CommentColumnConstraint,
                this=self._match(TokenType.IS) and self._parse_string(),
            ),
        }
        FUNCTION_PARSERS = {
            **parser.Parser.FUNCTION_PARSERS,
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/listagg.htm
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/group_concat.htm
            **dict.fromkeys(("GROUP_CONCAT", "LISTAGG"), lambda self: self._parse_group_concat()),
        }

        def _parse_column(self) -> t.Optional[exp.Expression]:
            column = super()._parse_column()
            if not isinstance(column, exp.Column):
                return column
            table_ident = column.args.get("table")
            if (
                isinstance(table_ident, exp.Identifier)
                and table_ident.name.upper() == "LOCAL"
                and not bool(table_ident.args.get("quoted"))
            ):
                column.set("table", None)
            return column

        ODBC_DATETIME_LITERALS = {
            "d": exp.Date,
            "ts": exp.Timestamp,
        }

    class Generator(generator.Generator):
        # https://docs.exasol.com/db/latest/sql_references/data_types/datatypedetails.htm#StringDataType
        STRING_TYPE_MAPPING = {
            exp.DataType.Type.BLOB: "VARCHAR",
            exp.DataType.Type.LONGBLOB: "VARCHAR",
            exp.DataType.Type.LONGTEXT: "VARCHAR",
            exp.DataType.Type.MEDIUMBLOB: "VARCHAR",
            exp.DataType.Type.MEDIUMTEXT: "VARCHAR",
            exp.DataType.Type.TINYBLOB: "VARCHAR",
            exp.DataType.Type.TINYTEXT: "VARCHAR",
            # https://docs.exasol.com/db/latest/sql_references/data_types/datatypealiases.htm
            exp.DataType.Type.TEXT: "LONG VARCHAR",
            exp.DataType.Type.VARBINARY: "VARCHAR",
        }

        # https://docs.exasol.com/db/latest/sql_references/data_types/datatypealiases.htm
        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            **STRING_TYPE_MAPPING,
            exp.DataType.Type.TINYINT: "SMALLINT",
            exp.DataType.Type.MEDIUMINT: "INT",
            exp.DataType.Type.DECIMAL32: "DECIMAL",
            exp.DataType.Type.DECIMAL64: "DECIMAL",
            exp.DataType.Type.DECIMAL128: "DECIMAL",
            exp.DataType.Type.DECIMAL256: "DECIMAL",
            exp.DataType.Type.DATETIME: "TIMESTAMP",
        }

        def datatype_sql(self, expression: exp.DataType) -> str:
            # Exasol supports a fixed default precision of 3 for TIMESTAMP WITH LOCAL TIME ZONE
            # and does not allow specifying a different custom precision
            if expression.is_type(exp.DataType.Type.TIMESTAMPLTZ):
                return "TIMESTAMP WITH LOCAL TIME ZONE"

            return super().datatype_sql(expression)

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/every.htm
            exp.All: rename_func("EVERY"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/bit_and.htm
            exp.BitwiseAnd: rename_func("BIT_AND"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/bit_or.htm
            exp.BitwiseOr: rename_func("BIT_OR"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/bit_not.htm
            exp.BitwiseNot: rename_func("BIT_NOT"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/bit_lshift.htm
            exp.BitwiseLeftShift: rename_func("BIT_LSHIFT"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/bit_rshift.htm
            exp.BitwiseRightShift: rename_func("BIT_RSHIFT"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/bit_xor.htm
            exp.BitwiseXor: rename_func("BIT_XOR"),
            exp.DateDiff: _date_diff_sql,
            exp.DateAdd: _add_date_sql,
            exp.TsOrDsAdd: _add_date_sql,
            exp.DateSub: _add_date_sql,
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/div.htm#DIV
            exp.IntDiv: rename_func("DIV"),
            exp.TsOrDsDiff: _date_diff_sql,
            exp.DateTrunc: _date_trunc_sql,
            exp.DayOfWeek: lambda self, e: f"CAST(TO_CHAR({self.sql(e, 'this')}, 'D') AS INTEGER)",
            exp.DatetimeTrunc: timestamptrunc_sql(),
            exp.GroupConcat: lambda self, e: groupconcat_sql(
                self, e, func_name="LISTAGG", within_group=True
            ),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/edit_distance.htm#EDIT_DISTANCE
            exp.Levenshtein: unsupported_args("ins_cost", "del_cost", "sub_cost", "max_dist")(
                rename_func("EDIT_DISTANCE")
            ),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/mod.htm
            exp.Mod: rename_func("MOD"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/regexp_substr.htm
            exp.RegexpExtract: unsupported_args("parameters", "group")(
                rename_func("REGEXP_SUBSTR")
            ),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/regexp_replace.htm
            exp.RegexpReplace: unsupported_args("modifiers")(rename_func("REGEXP_REPLACE")),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/var_pop.htm
            exp.VariancePop: rename_func("VAR_POP"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/approximate_count_distinct.htm
            exp.ApproxDistinct: unsupported_args("accuracy")(
                rename_func("APPROXIMATE_COUNT_DISTINCT")
            ),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/to_char%20(datetime).htm
            exp.ToChar: lambda self, e: self.func("TO_CHAR", e.this, self.format_time(e)),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/to_date.htm
            exp.TsOrDsToDate: lambda self, e: self.func("TO_DATE", e.this, self.format_time(e)),
            exp.TimeToStr: lambda self, e: self.func("TO_CHAR", e.this, self.format_time(e)),
            exp.TimeStrToTime: timestrtotime_sql,
            exp.TimestampTrunc: _timestamp_trunc_sql,
            exp.StrToTime: lambda self, e: self.func("TO_DATE", e.this, self.format_time(e)),
            exp.CurrentUser: lambda *_: "CURRENT_USER",
            exp.AtTimeZone: lambda self, e: self.func(
                "CONVERT_TZ",
                e.this,
                "'UTC'",
                e.args.get("zone"),
            ),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/instr.htm
            exp.StrPosition: lambda self, e: (
                strposition_sql(
                    self, e, func_name="INSTR", supports_position=True, supports_occurrence=True
                )
            ),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/hash_sha%5B1%5D.htm#HASH_SHA%5B1%5D
            exp.SHA: rename_func("HASH_SHA"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/hash_sha256.htm
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/hash_sha512.htm
            exp.SHA2: _sha2_sql,
            exp.MD5: rename_func("HASH_MD5"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/hashtype_md5.htm
            exp.MD5Digest: rename_func("HASHTYPE_MD5"),
            # https://docs.exasol.com/db/latest/sql/create_view.htm
            exp.CommentColumnConstraint: lambda self, e: f"COMMENT IS {self.sql(e, 'this')}",
            exp.Select: transforms.preprocess(
                [
                    _qualify_unscoped_star,
                    _add_local_prefix_for_aliases,
                ]
            ),
            exp.SubstringIndex: _substring_index_sql,
            exp.WeekOfYear: rename_func("WEEK"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/to_date.htm
            exp.Date: rename_func("TO_DATE"),
            # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/to_timestamp.htm
            exp.Timestamp: rename_func("TO_TIMESTAMP"),
            exp.Quarter: lambda self, e: f"CEIL(MONTH(TO_DATE({self.sql(e, 'this')}))/3)",
            exp.LastDay: no_last_day_sql,
        }

        def converttimezone_sql(self, expression: exp.ConvertTimezone) -> str:
            from_tz = expression.args.get("source_tz")
            to_tz = expression.args.get("target_tz")
            datetime = expression.args.get("timestamp")
            options = expression.args.get("options")

            return self.func("CONVERT_TZ", datetime, from_tz, to_tz, options)

        def if_sql(self, expression: exp.If) -> str:
            this = self.sql(expression, "this")
            true = self.sql(expression, "true")
            false = self.sql(expression, "false")
            return f"IF {this} THEN {true} ELSE {false} ENDIF"

        def collate_sql(self, expression: exp.Collate) -> str:
            return self.sql(expression.this)

        # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/rank.htm
        def rank_sql(self, expression: exp.Rank) -> str:
            if expression.args.get("expressions"):
                self.unsupported("Exasol does not support arguments in RANK")
            return self.func("RANK")

        def table_sql(self, expression: exp.Table, sep: str = " AS ") -> str:
            """
            If a table has PIVOTs attached, let the Pivot render a derived table .
            """
            pivots = expression.args.get("pivots") or []
            if not pivots:
                return super().table_sql(expression)

            if len(pivots) > 1:
                self.unsupported("Multiple PIVOT clauses are not supported by Exasol")
                return super().table_sql(expression)
            pivot = pivots[0]
            return self.sql(pivot)

        def subquery_sql(self, expression: exp.Subquery, sep: str = " AS ") -> str:
            pivots = expression.args.get("pivots") or []
            if not pivots:
                return super().subquery_sql(expression)

            if len(pivots) > 1:
                self.unsupported("Multiple PIVOT clauses are not supported by Exasol")
                return super().subquery_sql(expression)
            pivot = pivots[0]
            return self.sql(pivot)

        def pivot_sql(self, expression: exp.Pivot) -> str:
            """
            Exasol does not support PIVOT, so we rewrite it.

            Rewrite:
            SELECT ... FROM T PIVOT (...)

            Into:
            SELECT ... FROM (
                SELECT <group cols>,
                        <agg(CASE WHEN ...)>
                FROM T
                GROUP BY <group cols>
            )
            """

            if expression.unpivot:
                self.unsupported("UNPIVOT is not supported in Exasol.")
                return super().pivot_sql(expression)
            source_relation = expression.find_ancestor(exp.From)

            if not source_relation:
                return super().pivot_sql(expression)

            if isinstance(source_relation.this, exp.Table) or isinstance(
                source_relation.this, exp.Subquery
            ):
                source_table_sql = (
                    f"({self.sql(source_relation.this.this)})"
                    if isinstance(source_relation.this, exp.Subquery)
                    else self.sql(source_relation.this.this)
                )
                source_alias_expr = source_relation.this.args.get("alias")
                from_source_sql = (
                    f"{source_table_sql} AS {self.sql(source_alias_expr)}"
                    if source_alias_expr
                    else source_table_sql
                )
                source_name = (
                    source_alias_expr.this
                    if isinstance(source_alias_expr, exp.TableAlias)
                    else source_table_sql
                )
            else:
                return super().pivot_sql(expression)

            aggregate_aliases = expression.expressions or []

            if not aggregate_aliases:
                return super().pivot_sql(expression)

            pivot_fields = expression.fields or []

            if len(pivot_fields) != 1 or not isinstance(pivot_fields[0], exp.In):
                return super().pivot_sql(expression)

            pivot_in_condition = pivot_fields[0]
            pivot_key_expr = pivot_in_condition.this
            pivot_values_nodes = pivot_in_condition.expressions or []

            if not pivot_values_nodes:
                return super().pivot_sql(expression)

            pivot_alias_expr = expression.args.get("alias")

            has_pivot_alias = isinstance(pivot_alias_expr, exp.TableAlias)

            def unwrap_tuple(
                node: exp.Expression | None,
            ) -> Tuple | Expression | None | Paren | Any:
                if isinstance(node, exp.Tuple):
                    return node
                if isinstance(node, exp.Paren) and isinstance(node.this, exp.Tuple):
                    return node.this
                return None

            pivot_case_specs: list[tuple[exp.Expression, str, exp.Expression, str]] = []
            pivot_output_column_names: set[str] = set()

            for pivot_value in pivot_values_nodes:
                pivot_value_alias = (
                    pivot_value.alias_or_name
                    if isinstance(pivot_value, exp.PivotAlias)
                    else pivot_value.sql(dialect=self.dialect)
                )
                pivot_value_expr = (
                    pivot_value.this if isinstance(pivot_value, exp.PivotAlias) else pivot_value
                )

                for aggregate_alias in aggregate_aliases:
                    aggregate_func_expr = aggregate_alias.this
                    aggregate_func_name = (
                        getattr(aggregate_func_expr, "key", None) or aggregate_func_expr.name
                    ).upper()
                    aggregate_input_expr = aggregate_func_expr.this
                    aggregate_result_suffix = aggregate_alias.alias

                    output_column_name = (
                        f"{pivot_value_alias}_{aggregate_result_suffix}"
                        if aggregate_result_suffix and len(aggregate_aliases) > 1
                        else pivot_value_alias
                    )
                    pivot_case_specs.append(
                        (
                            pivot_value_expr,
                            aggregate_func_name,
                            aggregate_input_expr,
                            output_column_name,
                        )
                    )

                    pivot_output_column_names.add(output_column_name)

            group_by_columns = list(expression.args.get("group") or [])
            if not group_by_columns and has_pivot_alias and source_name:
                outer_select = expression.find_ancestor(exp.Select)

                if isinstance(outer_select, exp.Select):
                    for projection in outer_select.expressions or []:
                        projected_expr = (
                            projection.this if isinstance(projection, exp.Alias) else projection
                        )
                        if not isinstance(projected_expr, exp.Column):
                            continue

                        projected_column_name = projected_expr.name

                        if projected_column_name in pivot_output_column_names:
                            continue

                        group_by_columns.append(
                            exp.Column(
                                this=exp.to_identifier(projected_column_name, True),
                                table=exp.to_identifier(source_name),
                            )
                        )

            group_columns_sql = [self.sql(col) for col in group_by_columns]
            select_list_sql_parts: list[str] = list(group_columns_sql)

            for (
                pivot_value_expr,
                aggregate_func_name,
                aggregate_input_expr,
                output_column_name,
            ) in pivot_case_specs:
                key_tuple = unwrap_tuple(pivot_key_expr)
                value_tuple = unwrap_tuple(pivot_value_expr)

                if key_tuple and value_tuple:
                    comparisons: list[str] = []
                    for key_part, value_part in zip(
                        key_tuple.expressions or [], value_tuple.expressions or []
                    ):
                        comparisons.append(f"{self.sql(key_part)} = {self.sql(value_part)}")
                    condition_sql = " AND ".join(comparisons)
                else:
                    condition_sql = f"{self.sql(pivot_key_expr)} = {self.sql(pivot_value_expr)}"
                aggregate_input_sql = self.sql(aggregate_input_expr)
                output_column_sql = self.sql(exp.to_identifier(output_column_name, True))
                case_expr_sql = f"CASE WHEN {condition_sql} THEN {aggregate_input_sql} END"
                select_list_sql_parts.append(
                    f"{aggregate_func_name}({case_expr_sql}) AS {output_column_sql}"
                )

            inner_select_sql = ", ".join(select_list_sql_parts)

            if group_columns_sql:
                group_by_sql = ", ".join(group_columns_sql)
                inner_query = (
                    f"SELECT {inner_select_sql} FROM {from_source_sql} GROUP BY {group_by_sql}"
                )
            else:
                inner_query = f"SELECT {inner_select_sql} FROM {from_source_sql}"
            pivot_alias_sql = f"{self.sql(pivot_alias_expr)}" if pivot_alias_expr else ""
            return f"({inner_query}){pivot_alias_sql}"
