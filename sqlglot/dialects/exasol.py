from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens, transforms
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
)
from sqlglot.generator import unsupported_args
from sqlglot.helper import seq_get, find_new_name
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
        aliases: dict[str, bool] = {}
        for sel in expression.selects:
            alias = sel.args.get("alias")

            if isinstance(sel, exp.Alias) and alias:
                aliases[alias.name] = bool(alias.args.get("quoted"))

        table = expression.find(exp.Table)
        table_ident = table.this if table else None

        if (
            table_ident
            and table_ident.name.upper() == "LOCAL"
            and not bool(table_ident.args.get("quoted"))
        ):
            table_ident.replace(exp.to_identifier(table_ident.name.upper(), quoted=True))

        def prefix_local(node):
            if isinstance(node, exp.Column) and not node.table:
                if node.name in aliases:
                    return exp.Column(
                        this=exp.to_identifier(node.name, quoted=aliases[node.name]),
                        table=exp.to_identifier("LOCAL", quoted=False),
                    )
            return node

        for key in ("where", "group", "having"):
            if arg := expression.args.get(key):
                expression.set(key, arg.transform(prefix_local))

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


def _qualify_unscoped_star(node: exp.Expression) -> exp.Expression:
    """
    Exasol doesn't support a bare * alongside other select items, so we rewrite it
    Rewrite: SELECT *, <other> FROM <Table>
    Into: SELECT T.*, <other> FROM <Table> AS T
    """

    if not isinstance(node, exp.Select):
        return node
    select_expressions = list(node.expressions or [])

    has_bare_star = any(isinstance(expr, exp.Star) and expr.this is None for expr in select_expressions)

    if not has_bare_star or len(select_expressions) <= 1:
        return node

    from_clause = node.args.get("from_")

    base_source = from_clause.this if from_clause else None

    if not base_source:
        return node

    table_sources: list[exp.Expression] = [base_source]

    table_sources.extend(
        join.this
        for join in (node.args.get("joins") or [])
        if isinstance(join, exp.Join) and join.this
    )

    if not table_sources:
        return node

    scope = build_scope(node)
    used_alias_names = set(scope.sources.keys()) if scope else set()

    qualifiers: list[exp.Identifier] = []

    for src in table_sources:
        alias = src.args.get("alias")
        if isinstance(alias, (exp.TableAlias, exp.Alias)) and alias.name:
            name = alias.name
        else:
            name = find_new_name(used_alias_names, base="T")
            src.set("alias", exp.TableAlias(this=exp.to_identifier(name, quoted=False)))
            used_alias_names.add(name)
        qualifiers.append(exp.to_identifier(name, quoted=False))

    star_columns = [
        exp.Column(this=exp.Star(), table=alias_identifier) for alias_identifier in qualifiers
    ]

    new_items: list[exp.Expression] = []
    for select_expression in select_expressions:
        new_items.extend(star_columns) if isinstance(
            select_expression, exp.Star
        ) and select_expression.this is None else new_items.append(select_expression)
    node.set("expressions", new_items)
    return node


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

        def dateadd_sql(self, expression: exp.DateAdd) -> str:
            unit = expression.text("unit").upper() or "DAY"
            if unit not in DATE_UNITS:
                self.unsupported(f"'{unit}' is not supported in Exasol.")
                return self.function_fallback_sql(expression)

            return self.func(f"ADD_{unit}S", expression.this, expression.expression)

        def collate_sql(self, expression: exp.Collate) -> str:
            return self.sql(expression.this)

        # https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/rank.htm
        def rank_sql(self, expression: exp.Rank) -> str:
            if expression.args.get("expressions"):
                self.unsupported("Exasol does not support arguments in RANK")
            return self.func("RANK")
