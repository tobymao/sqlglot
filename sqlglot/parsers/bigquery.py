from __future__ import annotations

import re
import typing as t

from sqlglot import exp, parser
from sqlglot.dialects.dialect import (
    binary_from_function,
    build_date_delta_with_interval,
    build_formatted_time,
)
from sqlglot.helper import seq_get, split_num_words
from sqlglot.tokens import TokenType

if t.TYPE_CHECKING:
    from sqlglot._typing import E


def _build_contains_substring(args: t.List) -> exp.Contains:
    this = exp.Lower(this=seq_get(args, 0))
    expr = exp.Lower(this=seq_get(args, 1))
    return exp.Contains(this=this, expression=expr, json_scope=seq_get(args, 2))


def _build_date(args: t.List) -> exp.Date | exp.DateFromParts:
    expr_type = exp.DateFromParts if len(args) == 3 else exp.Date
    return expr_type.from_arg_list(args)


def build_date_diff(args: t.List) -> exp.Expr:
    expr = exp.DateDiff(
        this=seq_get(args, 0),
        expression=seq_get(args, 1),
        unit=seq_get(args, 2),
        date_part_boundary=True,
    )

    unit = expr.args.get("unit")
    if isinstance(unit, exp.Var) and unit.name.upper() == "WEEK":
        expr.set("unit", exp.WeekStart(this=exp.var("SUNDAY")))

    return expr


def _build_datetime(args: t.List) -> exp.Func:
    if len(args) == 1:
        return exp.TsOrDsToDatetime.from_arg_list(args)
    if len(args) == 2:
        return exp.Datetime.from_arg_list(args)
    return exp.TimestampFromParts.from_arg_list(args)


def _build_extract_json_with_default_path(
    expr_type: t.Type[E],
) -> t.Callable:
    def _builder(args: t.List, dialect: t.Any) -> E:
        if len(args) == 1:
            args.append(exp.Literal.string("$"))
        return parser.build_extract_json_with_path(expr_type)(args, dialect)

    return _builder


def _build_format_time(expr_type: t.Type[exp.Expr]) -> t.Callable[[t.List], exp.TimeToStr]:
    def _builder(args: t.List) -> exp.TimeToStr:
        formatted_time = build_formatted_time(exp.TimeToStr, "bigquery")(
            [expr_type(this=seq_get(args, 1)), seq_get(args, 0)]
        )
        formatted_time.set("zone", seq_get(args, 2))
        return formatted_time

    return _builder


def _build_json_strip_nulls(args: t.List) -> exp.JSONStripNulls:
    expression = exp.JSONStripNulls(this=seq_get(args, 0))
    for arg in args[1:]:
        if isinstance(arg, exp.Kwarg):
            expression.set(arg.this.name.lower(), arg)
        else:
            expression.set("expression", arg)
    return expression


def _build_levenshtein(args: t.List) -> exp.Levenshtein:
    max_dist = seq_get(args, 2)
    return exp.Levenshtein(
        this=seq_get(args, 0),
        expression=seq_get(args, 1),
        max_dist=max_dist.expression if max_dist else None,
    )


def _build_parse_timestamp(args: t.List) -> exp.StrToTime:
    this = build_formatted_time(exp.StrToTime, "bigquery")([seq_get(args, 1), seq_get(args, 0)])
    this.set("zone", seq_get(args, 2))
    return this


def _build_regexp_extract(
    expr_type: t.Type[E], default_group: t.Optional[exp.Expr] = None
) -> t.Callable:
    def _builder(args: t.List, dialect: t.Any) -> E:
        try:
            group = re.compile(args[1].name).groups == 1
        except re.error:
            group = False

        return expr_type(
            this=seq_get(args, 0),
            expression=seq_get(args, 1),
            position=seq_get(args, 2),
            occurrence=seq_get(args, 3),
            group=exp.Literal.number(1) if group else default_group,
            **(
                {"null_if_pos_overflow": dialect.REGEXP_EXTRACT_POSITION_OVERFLOW_RETURNS_NULL}
                if expr_type is exp.RegexpExtract
                else {}
            ),
        )

    return _builder


def _build_time(args: t.List) -> exp.Func:
    if len(args) == 1:
        return exp.TsOrDsToTime(this=args[0])
    if len(args) == 2:
        return exp.Time.from_arg_list(args)
    return exp.TimeFromParts.from_arg_list(args)


def _build_timestamp(args: t.List) -> exp.Timestamp:
    timestamp = exp.Timestamp.from_arg_list(args)
    timestamp.set("with_tz", True)
    return timestamp


def _build_to_hex(args: t.List) -> exp.Hex | exp.MD5:
    arg = seq_get(args, 0)
    return exp.MD5(this=arg.this) if isinstance(arg, exp.MD5Digest) else exp.LowerHex(this=arg)


MAKE_INTERVAL_KWARGS = ["year", "month", "day", "hour", "minute", "second"]


class BigQueryParser(parser.Parser):
    PREFIXED_PIVOT_COLUMNS: t.ClassVar = True
    LOG_DEFAULTS_TO_LN: t.ClassVar = True
    SUPPORTS_IMPLICIT_UNNEST: t.ClassVar = True
    JOINS_HAVE_EQUAL_PRECEDENCE: t.ClassVar = True

    # BigQuery does not allow ASC/DESC to be used as an identifier, allows GRANT as an identifier
    ID_VAR_TOKENS: t.ClassVar = {
        *parser.Parser.ID_VAR_TOKENS,
        TokenType.GRANT,
    } - {TokenType.ASC, TokenType.DESC}

    ALIAS_TOKENS: t.ClassVar = {
        *parser.Parser.ALIAS_TOKENS,
        TokenType.GRANT,
    } - {TokenType.ASC, TokenType.DESC}

    TABLE_ALIAS_TOKENS: t.ClassVar = {
        *parser.Parser.TABLE_ALIAS_TOKENS,
        TokenType.ANTI,
        TokenType.GRANT,
        TokenType.SEMI,
    } - {TokenType.ASC, TokenType.DESC}

    COMMENT_TABLE_ALIAS_TOKENS: t.ClassVar = {
        *parser.Parser.COMMENT_TABLE_ALIAS_TOKENS,
        TokenType.GRANT,
    } - {TokenType.ASC, TokenType.DESC}

    UPDATE_ALIAS_TOKENS: t.ClassVar = {
        *parser.Parser.UPDATE_ALIAS_TOKENS,
        TokenType.GRANT,
    } - {TokenType.ASC, TokenType.DESC}

    FUNCTIONS: t.ClassVar[t.Dict[str, t.Callable]] = {
        **{k: v for k, v in parser.Parser.FUNCTIONS.items() if k != "SEARCH"},
        "APPROX_TOP_COUNT": exp.ApproxTopK.from_arg_list,
        "BIT_AND": exp.BitwiseAndAgg.from_arg_list,
        "BIT_OR": exp.BitwiseOrAgg.from_arg_list,
        "BIT_XOR": exp.BitwiseXorAgg.from_arg_list,
        "BIT_COUNT": exp.BitwiseCount.from_arg_list,
        "BOOL": exp.JSONBool.from_arg_list,
        "CONTAINS_SUBSTR": _build_contains_substring,
        "DATE": _build_date,
        "DATE_ADD": build_date_delta_with_interval(exp.DateAdd),
        "DATE_DIFF": build_date_diff,
        "DATE_SUB": build_date_delta_with_interval(exp.DateSub),
        "DATE_TRUNC": lambda args: exp.DateTrunc(
            unit=seq_get(args, 1),
            this=seq_get(args, 0),
            zone=seq_get(args, 2),
        ),
        "DATETIME": _build_datetime,
        "DATETIME_ADD": build_date_delta_with_interval(exp.DatetimeAdd),
        "DATETIME_SUB": build_date_delta_with_interval(exp.DatetimeSub),
        "DIV": binary_from_function(exp.IntDiv),
        "EDIT_DISTANCE": _build_levenshtein,
        "FORMAT_DATE": _build_format_time(exp.TsOrDsToDate),
        "GENERATE_ARRAY": exp.GenerateSeries.from_arg_list,
        "JSON_EXTRACT_SCALAR": _build_extract_json_with_default_path(exp.JSONExtractScalar),
        "JSON_EXTRACT_ARRAY": _build_extract_json_with_default_path(exp.JSONExtractArray),
        "JSON_EXTRACT_STRING_ARRAY": _build_extract_json_with_default_path(exp.JSONValueArray),
        "JSON_KEYS": exp.JSONKeysAtDepth.from_arg_list,
        "JSON_QUERY": parser.build_extract_json_with_path(exp.JSONExtract),
        "JSON_QUERY_ARRAY": _build_extract_json_with_default_path(exp.JSONExtractArray),
        "JSON_STRIP_NULLS": _build_json_strip_nulls,
        "JSON_VALUE": _build_extract_json_with_default_path(exp.JSONExtractScalar),
        "JSON_VALUE_ARRAY": _build_extract_json_with_default_path(exp.JSONValueArray),
        "LENGTH": lambda args: exp.Length(this=seq_get(args, 0), binary=True),
        "MD5": exp.MD5Digest.from_arg_list,
        "SHA1": exp.SHA1Digest.from_arg_list,
        "NORMALIZE_AND_CASEFOLD": lambda args: exp.Normalize(
            this=seq_get(args, 0), form=seq_get(args, 1), is_casefold=True
        ),
        "OCTET_LENGTH": exp.ByteLength.from_arg_list,
        "TO_HEX": _build_to_hex,
        "PARSE_DATE": lambda args: build_formatted_time(exp.StrToDate, "bigquery")(
            [seq_get(args, 1), seq_get(args, 0)]
        ),
        "PARSE_TIME": lambda args: build_formatted_time(exp.ParseTime, "bigquery")(
            [seq_get(args, 1), seq_get(args, 0)]
        ),
        "PARSE_TIMESTAMP": _build_parse_timestamp,
        "PARSE_DATETIME": lambda args: build_formatted_time(exp.ParseDatetime, "bigquery")(
            [seq_get(args, 1), seq_get(args, 0)]
        ),
        "REGEXP_CONTAINS": exp.RegexpLike.from_arg_list,
        "REGEXP_EXTRACT": _build_regexp_extract(exp.RegexpExtract),
        "REGEXP_SUBSTR": _build_regexp_extract(exp.RegexpExtract),
        "REGEXP_EXTRACT_ALL": _build_regexp_extract(
            exp.RegexpExtractAll, default_group=exp.Literal.number(0)
        ),
        "SHA256": lambda args: exp.SHA2Digest(
            this=seq_get(args, 0), length=exp.Literal.number(256)
        ),
        "SHA512": lambda args: exp.SHA2(this=seq_get(args, 0), length=exp.Literal.number(512)),
        "SPLIT": lambda args: exp.Split(
            # https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#split
            this=seq_get(args, 0),
            expression=seq_get(args, 1) or exp.Literal.string(","),
        ),
        "STRPOS": exp.StrPosition.from_arg_list,
        "TIME": _build_time,
        "TIME_ADD": build_date_delta_with_interval(exp.TimeAdd),
        "TIME_SUB": build_date_delta_with_interval(exp.TimeSub),
        "TIMESTAMP": _build_timestamp,
        "TIMESTAMP_ADD": build_date_delta_with_interval(exp.TimestampAdd),
        "TIMESTAMP_SUB": build_date_delta_with_interval(exp.TimestampSub),
        "TIMESTAMP_MICROS": lambda args: exp.UnixToTime(
            this=seq_get(args, 0), scale=exp.UnixToTime.MICROS
        ),
        "TIMESTAMP_MILLIS": lambda args: exp.UnixToTime(
            this=seq_get(args, 0), scale=exp.UnixToTime.MILLIS
        ),
        "TIMESTAMP_SECONDS": lambda args: exp.UnixToTime(this=seq_get(args, 0)),
        "TO_JSON": lambda args: exp.JSONFormat(
            this=seq_get(args, 0), options=seq_get(args, 1), to_json=True
        ),
        "TO_JSON_STRING": exp.JSONFormat.from_arg_list,
        "FORMAT_DATETIME": _build_format_time(exp.TsOrDsToDatetime),
        "FORMAT_TIMESTAMP": _build_format_time(exp.TsOrDsToTimestamp),
        "FORMAT_TIME": _build_format_time(exp.TsOrDsToTime),
        "FROM_HEX": exp.Unhex.from_arg_list,
        "WEEK": lambda args: exp.WeekStart(this=exp.var(seq_get(args, 0))),
    }

    FUNCTION_PARSERS = {
        **{k: v for k, v in parser.Parser.FUNCTION_PARSERS.items() if k != "TRIM"},
        "ARRAY": lambda self: self.expression(
            exp.Array(expressions=[self._parse_statement()], struct_name_inheritance=True)
        ),
        "JSON_ARRAY": lambda self: self.expression(
            exp.JSONArray(expressions=self._parse_csv(self._parse_bitwise))
        ),
        "MAKE_INTERVAL": lambda self: self._parse_make_interval(),
        "PREDICT": lambda self: self._parse_ml(exp.Predict),
        "TRANSLATE": lambda self: self._parse_translate(),
        "FEATURES_AT_TIME": lambda self: self._parse_features_at_time(),
        "GENERATE_EMBEDDING": lambda self: self._parse_ml(exp.GenerateEmbedding),
        "GENERATE_TEXT_EMBEDDING": lambda self: self._parse_ml(exp.GenerateEmbedding, is_text=True),
        "VECTOR_SEARCH": lambda self: self._parse_vector_search(),
        "FORECAST": lambda self: self._parse_forecast(),
    }

    NO_PAREN_FUNCTIONS: t.ClassVar = {
        **parser.Parser.NO_PAREN_FUNCTIONS,
        TokenType.CURRENT_DATETIME: exp.CurrentDatetime,
    }

    NESTED_TYPE_TOKENS: t.ClassVar = {
        *parser.Parser.NESTED_TYPE_TOKENS,
        TokenType.TABLE,
    }

    PROPERTY_PARSERS: t.ClassVar = {
        **parser.Parser.PROPERTY_PARSERS,
        "NOT DETERMINISTIC": lambda self: self.expression(
            exp.StabilityProperty(this=exp.Literal.string("VOLATILE"))
        ),
        "OPTIONS": lambda self: self._parse_with_property(),
    }

    CONSTRAINT_PARSERS: t.ClassVar = {
        **parser.Parser.CONSTRAINT_PARSERS,
        "OPTIONS": lambda self: exp.Properties(expressions=self._parse_with_property()),
    }

    RANGE_PARSERS: t.ClassVar = {
        k: v for k, v in parser.Parser.RANGE_PARSERS.items() if k != TokenType.OVERLAPS
    }

    DASHED_TABLE_PART_FOLLOW_TOKENS: t.ClassVar = {
        TokenType.DOT,
        TokenType.L_PAREN,
        TokenType.R_PAREN,
    }

    STATEMENT_PARSERS: t.ClassVar = {
        **parser.Parser.STATEMENT_PARSERS,
        TokenType.ELSE: lambda self: self._parse_as_command(self._prev),
        TokenType.END: lambda self: self._parse_as_command(self._prev),
        TokenType.FOR: lambda self: self._parse_for_in(),
        TokenType.EXPORT: lambda self: self._parse_export_data(),
        TokenType.DECLARE: lambda self: self._parse_declare(),
    }

    BRACKET_OFFSETS: t.ClassVar = {
        "OFFSET": (0, False),
        "ORDINAL": (1, False),
        "SAFE_OFFSET": (0, True),
        "SAFE_ORDINAL": (1, True),
    }

    def _parse_for_in(self) -> t.Union[exp.ForIn, exp.Command]:
        index = self._index
        this = self._parse_range()
        self._match_text_seq("DO")
        if self._match(TokenType.COMMAND):
            self._retreat(index)
            return self._parse_as_command(self._prev)
        return self.expression(exp.ForIn(this=this, expression=self._parse_statement()))

    def _parse_table_part(self, schema: bool = False) -> t.Optional[exp.Expr]:
        this = super()._parse_table_part(schema=schema) or self._parse_number()

        # https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#table_names
        if isinstance(this, exp.Identifier):
            table_name = this.name
            while self._match(TokenType.DASH, advance=False) and self._next:
                start = self._curr
                while self._is_connected() and not self._match_set(
                    self.DASHED_TABLE_PART_FOLLOW_TOKENS, advance=False
                ):
                    self._advance()

                if start == self._curr:
                    break

                table_name += self._find_sql(start, self._prev)

            this = exp.Identifier(this=table_name, quoted=this.args.get("quoted")).update_positions(
                this
            )
        elif isinstance(this, exp.Literal):
            table_name = this.name

            if self._is_connected() and self._parse_var(any_token=True):
                table_name += self._prev.text

            this = exp.Identifier(this=table_name, quoted=True).update_positions(this)

        return this

    def _parse_table_parts(
        self,
        schema: bool = False,
        is_db_reference: bool = False,
        wildcard: bool = False,
        fast: bool = False,
    ) -> t.Optional[exp.Table | exp.Dot]:
        table = super()._parse_table_parts(
            schema=schema, is_db_reference=is_db_reference, wildcard=True, fast=fast
        )

        if not isinstance(table, exp.Table):
            return table

        # proj-1.db.tbl -- `1.` is tokenized as a float so we need to unravel it here
        if not table.catalog:
            if table.db:
                previous_db = table.args["db"]
                parts = table.db.split(".")
                if len(parts) == 2 and not table.args["db"].quoted:
                    table.set(
                        "catalog", exp.Identifier(this=parts[0]).update_positions(previous_db)
                    )
                    table.set("db", exp.Identifier(this=parts[1]).update_positions(previous_db))
            else:
                previous_this = table.this
                parts = table.name.split(".")
                if len(parts) == 2 and not table.this.quoted:
                    table.set("db", exp.Identifier(this=parts[0]).update_positions(previous_this))
                    table.set("this", exp.Identifier(this=parts[1]).update_positions(previous_this))

        if isinstance(table.this, exp.Identifier) and any("." in p.name for p in table.parts):
            alias = table.this
            catalog, db, this_id, *rest = (
                exp.to_identifier(p, quoted=True)
                for p in split_num_words(".".join(p.name for p in table.parts), ".", 3)
            )

            for part in (catalog, db, this_id):
                if part:
                    part.update_positions(table.this)

            this: t.Optional[exp.Expr] = this_id
            if rest and this:
                this = exp.Dot.build([this, *rest])  # type: ignore[list-item]

            table = exp.Table(this=this, db=db, catalog=catalog, pivots=table.args.get("pivots"))
            table.meta["quoted_table"] = True
        else:
            alias = None

        # The `INFORMATION_SCHEMA` views in BigQuery need to be qualified by a region or
        # dataset, so if the project identifier is omitted we need to fix the ast so that
        # the `INFORMATION_SCHEMA.X` bit is represented as a single (quoted) Identifier.
        # Otherwise, we wouldn't correctly qualify a `Table` node that references these
        # views, because it would seem like the "catalog" part is set, when it'd actually
        # be the region/dataset. Merging the two identifiers into a single one is done to
        # avoid producing a 4-part Table reference, which would cause issues in the schema
        # module, when there are 3-part table names mixed with information schema views.
        #
        # See: https://cloud.google.com/bigquery/docs/information-schema-intro#syntax
        table_parts = table.parts
        if len(table_parts) > 1 and table_parts[-2].name.upper() == "INFORMATION_SCHEMA":
            # We need to alias the table here to avoid breaking existing qualified columns.
            # This is expected to be safe, because if there's an actual alias coming up in
            # the token stream, it will overwrite this one. If there isn't one, we are only
            # exposing the name that can be used to reference the view explicitly (a no-op).
            exp.alias_(
                table,
                t.cast(exp.Identifier, alias or table_parts[-1]),
                table=True,
                copy=False,
            )

            info_schema_view = f"{table_parts[-2].name}.{table_parts[-1].name}"
            new_this = exp.Identifier(this=info_schema_view, quoted=True).update_positions(
                line=table_parts[-2].meta.get("line"),
                col=table_parts[-1].meta.get("col"),
                start=table_parts[-2].meta.get("start"),
                end=table_parts[-1].meta.get("end"),
            )
            table.set("this", new_this)
            table.set("db", seq_get(table_parts, -3))
            table.set("catalog", seq_get(table_parts, -4))

        return table

    def _parse_column(self) -> t.Optional[exp.Expr]:
        column = super()._parse_column()
        if isinstance(column, exp.Column):
            parts = column.parts
            if any("." in p.name for p in parts):
                catalog, db, table, this, *rest = (
                    exp.to_identifier(p, quoted=True)
                    for p in split_num_words(".".join(p.name for p in parts), ".", 4)
                )

                if rest and this:
                    this = exp.Dot.build([this, *rest])  # type: ignore

                column = exp.Column(this=this, table=table, db=db, catalog=catalog)
                column.meta["quoted_column"] = True

        return column

    @t.overload
    def _parse_json_object(self, agg: t.Literal[False]) -> exp.JSONObject: ...

    @t.overload
    def _parse_json_object(self, agg: t.Literal[True]) -> exp.JSONObjectAgg: ...

    def _parse_json_object(self, agg=False):
        json_object = super()._parse_json_object()
        array_kv_pair = seq_get(json_object.expressions, 0)

        # Converts BQ's "signature 2" of JSON_OBJECT into SQLGlot's canonical representation
        # https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_object_signature2
        if (
            array_kv_pair
            and isinstance(array_kv_pair.this, exp.Array)
            and isinstance(array_kv_pair.expression, exp.Array)
        ):
            keys = array_kv_pair.this.expressions
            values = array_kv_pair.expression.expressions

            json_object.set(
                "expressions",
                [exp.JSONKeyValue(this=k, expression=v) for k, v in zip(keys, values)],
            )

        return json_object

    def _parse_bracket(self, this: t.Optional[exp.Expr] = None) -> t.Optional[exp.Expr]:
        bracket = super()._parse_bracket(this)

        if isinstance(bracket, exp.Array):
            bracket.set("struct_name_inheritance", True)

        if this is bracket:
            return bracket

        if isinstance(bracket, exp.Bracket):
            for expression in bracket.expressions:
                name = expression.name.upper()

                expressions = expression.expressions

                if name not in self.BRACKET_OFFSETS or not expressions:
                    break

                offset, safe = self.BRACKET_OFFSETS[name]
                bracket.set("offset", offset)
                bracket.set("safe", safe)
                expression.replace(expressions[0])

        return bracket

    def _parse_unnest(self, with_alias: bool = True) -> t.Optional[exp.Unnest]:
        unnest = super()._parse_unnest(with_alias=with_alias)

        if not unnest:
            return None

        unnest_expr = seq_get(unnest.expressions, 0)
        if unnest_expr:
            from sqlglot.optimizer.annotate_types import annotate_types

            unnest_expr = annotate_types(unnest_expr, dialect=self.dialect)

            # Unnesting a nested array (i.e array of structs) explodes the top-level struct fields,
            # in contrast to other dialects such as DuckDB which flattens only the array by default
            if unnest_expr.is_type(exp.DType.ARRAY) and any(
                array_elem.is_type(exp.DType.STRUCT) for array_elem in unnest_expr._type.expressions
            ):
                unnest.set("explode_array", True)

        return unnest

    def _parse_make_interval(self) -> exp.MakeInterval:
        expr = exp.MakeInterval()

        for arg_key in MAKE_INTERVAL_KWARGS:
            value = self._parse_lambda()

            if not value:
                break

            # Non-named arguments are filled sequentially, (optionally) followed by named arguments
            # that can appear in any order e.g MAKE_INTERVAL(1, minute => 5, day => 2)
            if isinstance(value, exp.Kwarg):
                arg_key = value.this.name

            expr.set(arg_key, value)

            self._match(TokenType.COMMA)

        return expr

    def _parse_ml(self, expr_type: t.Type[E], **kwargs: t.Any) -> E:
        self._match_text_seq("MODEL")
        this = self._parse_table()

        self._match(TokenType.COMMA)
        self._match_text_seq("TABLE")

        # Certain functions like ML.FORECAST require a STRUCT argument but not a TABLE/SELECT one
        expression = (
            self._parse_table() if not self._match(TokenType.STRUCT, advance=False) else None
        )

        self._match(TokenType.COMMA)

        return self.expression(
            expr_type(
                this=this, expression=expression, params_struct=self._parse_bitwise(), **kwargs
            )
        )

    def _parse_translate(self) -> exp.Translate | exp.MLTranslate:
        # Check if this is ML.TRANSLATE by looking at previous tokens
        token = seq_get(self._tokens, self._index - 4)
        if token and token.text.upper() == "ML":
            return self._parse_ml(exp.MLTranslate)

        return exp.Translate.from_arg_list(self._parse_function_args())

    def _parse_forecast(self) -> exp.AIForecast | exp.MLForecast:
        # Check if this is ML.FORECAST by looking at previous tokens.
        token = seq_get(self._tokens, self._index - 4)
        if token and token.text.upper() == "ML":
            return self._parse_ml(exp.MLForecast)

        # AI.FORECAST is a TVF, where the first argument is either TABLE <table>
        # or a parenthesized query statement, followed by named arguments.
        self._match(TokenType.TABLE)
        this = self._parse_table()
        if not this:
            self.raise_error("Expected table or query statement")

        expr = self.expression(exp.AIForecast(this=this))
        if self._match(TokenType.COMMA):
            while True:
                arg = self._parse_lambda()
                if arg:
                    expr.set(arg.this.name, arg)

                if not self._match(TokenType.COMMA):
                    break

        return expr

    def _parse_features_at_time(self) -> exp.FeaturesAtTime:
        self._match(TokenType.TABLE)
        this = self._parse_table()

        expr = self.expression(exp.FeaturesAtTime(this=this))

        while self._match(TokenType.COMMA):
            arg = self._parse_lambda()

            # Get the LHS of the Kwarg and set the arg to that value, e.g
            # "num_rows => 1" sets the expr's `num_rows` arg
            if arg:
                expr.set(arg.this.name, arg)

        return expr

    def _parse_vector_search(self) -> exp.VectorSearch:
        self._match(TokenType.TABLE)
        base_table = self._parse_table()

        self._match(TokenType.COMMA)

        column_to_search = self._parse_bitwise()
        self._match(TokenType.COMMA)

        self._match(TokenType.TABLE)
        query_table = self._parse_table()

        expr = self.expression(
            exp.VectorSearch(
                this=base_table, column_to_search=column_to_search, query_table=query_table
            )
        )

        while self._match(TokenType.COMMA):
            # query_column_to_search can be named argument or positional
            if self._match(TokenType.STRING, advance=False):
                query_column = self._parse_string()
                expr.set("query_column_to_search", query_column)
            else:
                arg = self._parse_lambda()
                if arg:
                    expr.set(arg.this.name, arg)

        return expr

    def _parse_export_data(self) -> exp.Export:
        self._match_text_seq("DATA")

        return self.expression(
            exp.Export(
                connection=self._match_text_seq("WITH", "CONNECTION") and self._parse_table_parts(),
                options=self._parse_properties(),
                this=self._match_text_seq("AS") and self._parse_select(nested=True),
            )
        )

    def _parse_column_ops(self, this: t.Optional[exp.Expr]) -> t.Optional[exp.Expr]:
        func_index = self._index + 1
        this = super()._parse_column_ops(this)

        if isinstance(this, exp.Dot) and isinstance(this.expression, exp.Func):
            prefix = this.this.name.upper()

            func: t.Optional[t.Type[exp.Func]] = None
            if prefix == "NET":
                func = exp.NetFunc
            elif prefix == "SAFE":
                func = exp.SafeFunc

            if func:
                # Retreat to try and parse a known function instead of an anonymous one,
                # which is parsed by the base column ops parser due to anonymous_func=true
                self._retreat(func_index)
                this = func(this=self._parse_function(any_token=True))

        return this
