from __future__ import annotations

import typing as t

from sqlglot import exp, parser
from sqlglot.trie import new_trie
from sqlglot.dialects.dialect import (
    binary_from_function,
    build_default_decimal_type,
    build_formatted_time,
    build_regexp_extract,
    date_trunc_to_time,
    pivot_column_names,
)
from sqlglot.helper import seq_get
from sqlglot.parser import binary_range_parser
from sqlglot.tokens import TokenType
from collections.abc import Collection


def _build_sort_array_desc(args: t.List) -> exp.Expr:
    return exp.SortArray(this=seq_get(args, 0), asc=exp.false())


def _build_array_prepend(args: t.List) -> exp.Expr:
    return exp.ArrayPrepend(this=seq_get(args, 1), expression=seq_get(args, 0))


def _build_date_diff(args: t.List) -> exp.Expr:
    return exp.DateDiff(this=seq_get(args, 2), expression=seq_get(args, 1), unit=seq_get(args, 0))


def _build_generate_series(end_exclusive: bool = False) -> t.Callable[[t.List], exp.GenerateSeries]:
    def _builder(args: t.List) -> exp.GenerateSeries:
        # Check https://duckdb.org/docs/sql/functions/nested.html#range-functions
        if len(args) == 1:
            # DuckDB uses 0 as a default for the series' start when it's omitted
            args.insert(0, exp.Literal.number("0"))

        gen_series = exp.GenerateSeries.from_arg_list(args)
        gen_series.set("is_end_exclusive", end_exclusive)

        return gen_series

    return _builder


def _build_make_timestamp(args: t.List) -> exp.Expr:
    if len(args) == 1:
        return exp.UnixToTime(this=seq_get(args, 0), scale=exp.UnixToTime.MICROS)

    return exp.TimestampFromParts(
        year=seq_get(args, 0),
        month=seq_get(args, 1),
        day=seq_get(args, 2),
        hour=seq_get(args, 3),
        min=seq_get(args, 4),
        sec=seq_get(args, 5),
    )


def _show_parser(*args: t.Any, **kwargs: t.Any) -> t.Callable[[DuckDBParser], exp.Show]:
    def _parse(self: DuckDBParser) -> exp.Show:
        return self._parse_show_duckdb(*args, **kwargs)

    return _parse


class DuckDBParser(parser.Parser):
    MAP_KEYS_ARE_ARBITRARY_EXPRESSIONS = True

    NO_PAREN_FUNCTIONS = {
        **parser.Parser.NO_PAREN_FUNCTIONS,
        TokenType.LOCALTIME: exp.Localtime,
        TokenType.LOCALTIMESTAMP: exp.Localtimestamp,
        TokenType.CURRENT_CATALOG: exp.CurrentCatalog,
        TokenType.SESSION_USER: exp.SessionUser,
    }

    BITWISE = {k: v for k, v in parser.Parser.BITWISE.items() if k != TokenType.CARET}

    RANGE_PARSERS = {
        **parser.Parser.RANGE_PARSERS,
        TokenType.DAMP: binary_range_parser(exp.ArrayOverlaps),
        TokenType.CARET_AT: binary_range_parser(exp.StartsWith),
        TokenType.TILDE: binary_range_parser(exp.RegexpFullMatch),
    }

    EXPONENT = {
        **parser.Parser.EXPONENT,
        TokenType.CARET: exp.Pow,
        TokenType.DSTAR: exp.Pow,
    }

    FUNCTIONS_WITH_ALIASED_ARGS = {*parser.Parser.FUNCTIONS_WITH_ALIASED_ARGS, "STRUCT_PACK"}

    SHOW_PARSERS = {
        "TABLES": _show_parser("TABLES"),
        "ALL TABLES": _show_parser("ALL TABLES"),
    }

    FUNCTIONS = {
        **{k: v for k, v in parser.Parser.FUNCTIONS.items() if k not in ("DATE_SUB", "GLOB")},
        "ANY_VALUE": lambda args: exp.IgnoreNulls(this=exp.AnyValue.from_arg_list(args)),
        "ARRAY_PREPEND": _build_array_prepend,
        "ARRAY_REVERSE_SORT": _build_sort_array_desc,
        "ARRAY_INTERSECT": lambda args: exp.ArrayIntersect(expressions=args),
        "ARRAY_SORT": exp.SortArray.from_arg_list,
        "BIT_AND": exp.BitwiseAndAgg.from_arg_list,
        "BIT_OR": exp.BitwiseOrAgg.from_arg_list,
        "BIT_XOR": exp.BitwiseXorAgg.from_arg_list,
        "CURRENT_LOCALTIMESTAMP": exp.Localtimestamp.from_arg_list,
        "DATEDIFF": _build_date_diff,
        "DATE_DIFF": _build_date_diff,
        "DATE_TRUNC": date_trunc_to_time,
        "DATETRUNC": date_trunc_to_time,
        "DECODE": lambda args: exp.Decode(
            this=seq_get(args, 0), charset=exp.Literal.string("utf-8")
        ),
        "EDITDIST3": exp.Levenshtein.from_arg_list,
        "ENCODE": lambda args: exp.Encode(
            this=seq_get(args, 0), charset=exp.Literal.string("utf-8")
        ),
        "EPOCH": exp.TimeToUnix.from_arg_list,
        "EPOCH_MS": lambda args: exp.UnixToTime(this=seq_get(args, 0), scale=exp.UnixToTime.MILLIS),
        "GENERATE_SERIES": _build_generate_series(),
        "GET_CURRENT_TIME": exp.CurrentTime.from_arg_list,
        "GET_BIT": lambda args: exp.Getbit(
            this=seq_get(args, 0), expression=seq_get(args, 1), zero_is_msb=True
        ),
        "JARO_WINKLER_SIMILARITY": exp.JarowinklerSimilarity.from_arg_list,
        "JSON": exp.ParseJSON.from_arg_list,
        "JSON_EXTRACT_PATH": parser.build_extract_json_with_path(exp.JSONExtract),
        "JSON_EXTRACT_STRING": parser.build_extract_json_with_path(exp.JSONExtractScalar),
        "LIST_APPEND": exp.ArrayAppend.from_arg_list,
        "LIST_CONCAT": parser.build_array_concat,
        "LIST_CONTAINS": exp.ArrayContains.from_arg_list,
        "LIST_COSINE_DISTANCE": exp.CosineDistance.from_arg_list,
        "LIST_DISTANCE": exp.EuclideanDistance.from_arg_list,
        "LIST_FILTER": exp.ArrayFilter.from_arg_list,
        "LIST_HAS": exp.ArrayContains.from_arg_list,
        "LIST_HAS_ANY": exp.ArrayOverlaps.from_arg_list,
        "LIST_MAX": exp.ArrayMax.from_arg_list,
        "LIST_MIN": exp.ArrayMin.from_arg_list,
        "LIST_PREPEND": _build_array_prepend,
        "LIST_REVERSE_SORT": _build_sort_array_desc,
        "LIST_SORT": exp.SortArray.from_arg_list,
        "LIST_TRANSFORM": exp.Transform.from_arg_list,
        "LIST_VALUE": lambda args: exp.Array(expressions=args),
        "MAKE_DATE": exp.DateFromParts.from_arg_list,
        "MAKE_TIME": exp.TimeFromParts.from_arg_list,
        "MAKE_TIMESTAMP": _build_make_timestamp,
        "QUANTILE_CONT": exp.PercentileCont.from_arg_list,
        "QUANTILE_DISC": exp.PercentileDisc.from_arg_list,
        "RANGE": _build_generate_series(end_exclusive=True),
        "REGEXP_EXTRACT": build_regexp_extract(exp.RegexpExtract),
        "REGEXP_EXTRACT_ALL": build_regexp_extract(exp.RegexpExtractAll),
        "REGEXP_MATCHES": exp.RegexpLike.from_arg_list,
        "REGEXP_REPLACE": lambda args: exp.RegexpReplace(
            this=seq_get(args, 0),
            expression=seq_get(args, 1),
            replacement=seq_get(args, 2),
            modifiers=seq_get(args, 3),
            single_replace=True,
        ),
        "SHA256": lambda args: exp.SHA2(this=seq_get(args, 0), length=exp.Literal.number(256)),
        "STRFTIME": build_formatted_time(exp.TimeToStr, "duckdb"),
        "STRING_SPLIT": exp.Split.from_arg_list,
        "STRING_SPLIT_REGEX": exp.RegexpSplit.from_arg_list,
        "STRING_TO_ARRAY": exp.Split.from_arg_list,
        "STRPTIME": build_formatted_time(exp.StrToTime, "duckdb"),
        "STRUCT_PACK": exp.Struct.from_arg_list,
        "STR_SPLIT": exp.Split.from_arg_list,
        "STR_SPLIT_REGEX": exp.RegexpSplit.from_arg_list,
        "TODAY": exp.CurrentDate.from_arg_list,
        "TIME_BUCKET": exp.DateBin.from_arg_list,
        "TO_TIMESTAMP": exp.UnixToTime.from_arg_list,
        "UNNEST": exp.Explode.from_arg_list,
        "VERSION": exp.CurrentVersion.from_arg_list,
        "XOR": binary_from_function(exp.BitwiseXor),
    }

    FUNCTION_PARSERS = {
        **{k: v for k, v in parser.Parser.FUNCTION_PARSERS.items() if k != "DECODE"},
        **dict.fromkeys(
            ("GROUP_CONCAT", "LISTAGG", "STRINGAGG"), lambda self: self._parse_string_agg()
        ),
    }

    NO_PAREN_FUNCTION_PARSERS = {
        **parser.Parser.NO_PAREN_FUNCTION_PARSERS,
        "MAP": lambda self: self._parse_map(),
        "@": lambda self: exp.Abs(this=self._parse_bitwise()),
    }

    PLACEHOLDER_PARSERS = {
        **parser.Parser.PLACEHOLDER_PARSERS,
        TokenType.PARAMETER: lambda self: (
            self.expression(exp.Placeholder(this=self._prev.text))
            if self._match(TokenType.NUMBER) or self._match_set(self.ID_VAR_TOKENS)
            else None
        ),
    }

    TYPE_CONVERTERS = {
        # https://duckdb.org/docs/sql/data_types/numeric
        exp.DType.DECIMAL: build_default_decimal_type(precision=18, scale=3),
        # https://duckdb.org/docs/sql/data_types/text
        exp.DType.TEXT: lambda dtype: exp.DataType.build(exp.DType.TEXT),
    }

    STATEMENT_PARSERS = {
        **parser.Parser.STATEMENT_PARSERS,
        TokenType.ATTACH: lambda self: self._parse_attach_detach(),
        TokenType.DETACH: lambda self: self._parse_attach_detach(is_attach=False),
        TokenType.FORCE: lambda self: self._parse_force(),
        TokenType.INSTALL: lambda self: self._parse_install(),
        TokenType.SHOW: lambda self: self._parse_show(),
    }

    SET_PARSERS = {
        **parser.Parser.SET_PARSERS,
        "VARIABLE": lambda self: self._parse_set_item_assignment("VARIABLE"),
    }

    SHOW_TRIE = new_trie(key.split(" ") for key in SHOW_PARSERS)
    SET_TRIE = new_trie(key.split(" ") for key in SET_PARSERS)

    def _parse_lambda(self, alias: bool = False) -> t.Optional[exp.Expr]:
        index = self._index
        if not self._match_text_seq("LAMBDA"):
            return super()._parse_lambda(alias=alias)

        expressions = self._parse_csv(self._parse_lambda_arg)
        if not self._match(TokenType.COLON):
            self._retreat(index)
            return None

        this = self._replace_lambda(self._parse_assignment(), expressions)
        return self.expression(exp.Lambda(this=this, expressions=expressions, colon=True))

    def _parse_expression(self) -> t.Optional[exp.Expr]:
        # DuckDB supports prefix aliases, e.g. foo: 1
        if self._next.token_type == TokenType.COLON:
            alias = self._parse_id_var(tokens=self.ALIAS_TOKENS)
            self._match(TokenType.COLON)
            comments = self._prev_comments

            this = self._parse_assignment()
            if isinstance(this, exp.Expr):
                # Moves the comment next to the alias in `alias: expr /* comment */`
                comments += this.pop_comments() or []

            return self.expression(exp.Alias(this=this, alias=alias), comments=comments)

        return super()._parse_expression()

    def _parse_table(
        self,
        schema: bool = False,
        joins: bool = False,
        alias_tokens: t.Optional[Collection[TokenType]] = None,
        parse_bracket: bool = False,
        is_db_reference: bool = False,
        parse_partition: bool = False,
        consume_pipe: bool = False,
    ) -> t.Optional[exp.Expr]:
        # DuckDB supports prefix aliases, e.g. FROM foo: bar
        if self._next.token_type == TokenType.COLON:
            alias = self._parse_table_alias(alias_tokens=alias_tokens or self.TABLE_ALIAS_TOKENS)
            self._match(TokenType.COLON)
            comments = self._prev_comments
        else:
            alias = None
            comments = []

        table = super()._parse_table(
            schema=schema,
            joins=joins,
            alias_tokens=alias_tokens,
            parse_bracket=parse_bracket,
            is_db_reference=is_db_reference,
            parse_partition=parse_partition,
        )
        if isinstance(table, exp.Expr) and isinstance(alias, exp.TableAlias):
            # Moves the comment next to the alias in `alias: table /* comment */`
            comments += table.pop_comments() or []
            alias.comments = alias.pop_comments() + comments
            table.set("alias", alias)

        return table

    def _parse_table_sample(self, as_modifier: bool = False) -> t.Optional[exp.TableSample]:
        # https://duckdb.org/docs/sql/samples.html
        sample = super()._parse_table_sample(as_modifier=as_modifier)
        if sample and not sample.args.get("method"):
            if sample.args.get("size"):
                sample.set("method", exp.var("RESERVOIR"))
            else:
                sample.set("method", exp.var("SYSTEM"))

        return sample

    def _parse_bracket(self, this: t.Optional[exp.Expr] = None) -> t.Optional[exp.Expr]:
        bracket = super()._parse_bracket(this)

        if self.dialect.version < (1, 2) and isinstance(bracket, exp.Bracket):
            # https://duckdb.org/2025/02/05/announcing-duckdb-120.html#breaking-changes
            bracket.set("returns_list_for_maps", True)

        return bracket

    def _parse_map(self) -> exp.ToMap | exp.Map:
        if self._match(TokenType.L_BRACE, advance=False):
            return self.expression(exp.ToMap(this=self._parse_bracket()))

        args = self._parse_wrapped_csv(self._parse_assignment)
        return self.expression(exp.Map(keys=seq_get(args, 0), values=seq_get(args, 1)))

    def _parse_struct_types(self, type_required: bool = False) -> t.Optional[exp.Expr]:
        return self._parse_field_def()

    def _pivot_column_names(self, aggregations: t.List[exp.Expr]) -> t.List[str]:
        if len(aggregations) == 1:
            return super()._pivot_column_names(aggregations)
        return pivot_column_names(aggregations, dialect="duckdb")

    def _parse_attach_detach(self, is_attach: bool = True) -> exp.Attach | exp.Detach:
        def _parse_attach_option() -> exp.AttachOption:
            return self.expression(
                exp.AttachOption(
                    this=self._parse_var(any_token=True),
                    expression=self._parse_field(any_token=True),
                )
            )

        self._match(TokenType.DATABASE)
        exists = self._parse_exists(not_=is_attach)
        this = self._parse_alias(self._parse_primary_or_var(), explicit=True)

        if self._match(TokenType.L_PAREN, advance=False):
            expressions = self._parse_wrapped_csv(_parse_attach_option)
        else:
            expressions = None

        return (
            self.expression(exp.Attach(this=this, exists=exists, expressions=expressions))
            if is_attach
            else self.expression(exp.Detach(this=this, exists=exists))
        )

    def _parse_show_duckdb(self, this: str) -> exp.Show:
        from_ = self._parse_table(schema=True) if self._match(TokenType.FROM) else None
        return self.expression(exp.Show(this=this, from_=from_))

    def _parse_force(self) -> exp.Install | exp.Command:
        # FORCE can only be followed by INSTALL or CHECKPOINT
        # In the case of CHECKPOINT, we fallback
        if not self._match(TokenType.INSTALL):
            return self._parse_as_command(self._prev)

        return self._parse_install(force=True)

    def _parse_install(self, force: bool = False) -> exp.Install:
        return self.expression(
            exp.Install(
                this=self._parse_id_var(),
                from_=self._parse_var_or_string() if self._match(TokenType.FROM) else None,
                force=force,
            )
        )

    def _parse_primary(self) -> t.Optional[exp.Expr]:
        if self._match_pair(TokenType.HASH, TokenType.NUMBER):
            return exp.PositionalColumn(this=exp.Literal.number(self._prev.text))

        return super()._parse_primary()
