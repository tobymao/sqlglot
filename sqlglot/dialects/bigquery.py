from __future__ import annotations

import logging
import re
import typing as t
from dataclasses import dataclass

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.errors import ErrorLevel, ParseError
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
    arg_max_or_min_no_count,
    binary_from_function,
    date_add_interval_sql,
    datestrtodate_sql,
    build_formatted_time,
    filter_array_using_unnest,
    if_sql,
    inline_array_sql,
    max_or_greatest,
    min_or_least,
    no_ilike_sql,
    build_date_delta_with_interval,
    regexp_replace_sql,
    rename_func,
    timestrtotime_sql,
    ts_or_ds_add_cast,
)
from sqlglot.helper import seq_get, split_num_words
from sqlglot.tokens import TokenType, Token

if t.TYPE_CHECKING:
    from sqlglot._typing import E, Lit

logger = logging.getLogger("sqlglot")


def _derived_table_values_to_unnest(self: BigQuery.Generator, expression: exp.Values) -> str:
    if not expression.find_ancestor(exp.From, exp.Join):
        return self.values_sql(expression)

    structs = []
    alias = expression.args.get("alias")
    for tup in expression.find_all(exp.Tuple):
        field_aliases = alias.columns if alias else (f"_c{i}" for i in range(len(tup.expressions)))
        expressions = [
            exp.PropertyEQ(this=exp.to_identifier(name), expression=fld)
            for name, fld in zip(field_aliases, tup.expressions)
        ]
        structs.append(exp.Struct(expressions=expressions))

    return self.unnest_sql(exp.Unnest(expressions=[exp.array(*structs, copy=False)]))


def _returnsproperty_sql(self: BigQuery.Generator, expression: exp.ReturnsProperty) -> str:
    this = expression.this
    if isinstance(this, exp.Schema):
        this = f"{self.sql(this, 'this')} <{self.expressions(this)}>"
    else:
        this = self.sql(this)
    return f"RETURNS {this}"


def _create_sql(self: BigQuery.Generator, expression: exp.Create) -> str:
    returns = expression.find(exp.ReturnsProperty)
    if expression.kind == "FUNCTION" and returns and returns.args.get("is_table"):
        expression.set("kind", "TABLE FUNCTION")

        if isinstance(expression.expression, (exp.Subquery, exp.Literal)):
            expression.set("expression", expression.expression.this)

    return self.create_sql(expression)


def _unqualify_unnest(expression: exp.Expression) -> exp.Expression:
    """Remove references to unnest table aliases since bigquery doesn't allow them.

    These are added by the optimizer's qualify_column step.
    """
    from sqlglot.optimizer.scope import find_all_in_scope

    if isinstance(expression, exp.Select):
        unnest_aliases = {
            unnest.alias
            for unnest in find_all_in_scope(expression, exp.Unnest)
            if isinstance(unnest.parent, (exp.From, exp.Join))
        }
        if unnest_aliases:
            for column in expression.find_all(exp.Column):
                if column.table in unnest_aliases:
                    column.set("table", None)
                elif column.db in unnest_aliases:
                    column.set("db", None)

    return expression


# https://issuetracker.google.com/issues/162294746
# workaround for bigquery bug when grouping by an expression and then ordering
# WITH x AS (SELECT 1 y)
# SELECT y + 1 z
# FROM x
# GROUP BY x + 1
# ORDER by z
def _alias_ordered_group(expression: exp.Expression) -> exp.Expression:
    if isinstance(expression, exp.Select):
        group = expression.args.get("group")
        order = expression.args.get("order")

        if group and order:
            aliases = {
                select.this: select.args["alias"]
                for select in expression.selects
                if isinstance(select, exp.Alias)
            }

            for grouped in group.expressions:
                if grouped.is_int:
                    continue
                alias = aliases.get(grouped)
                if alias:
                    grouped.replace(exp.column(alias))

    return expression


def _pushdown_cte_column_names(expression: exp.Expression) -> exp.Expression:
    """BigQuery doesn't allow column names when defining a CTE, so we try to push them down."""
    if isinstance(expression, exp.CTE) and expression.alias_column_names:
        cte_query = expression.this

        if cte_query.is_star:
            logger.warning(
                "Can't push down CTE column names for star queries. Run the query through"
                " the optimizer or use 'qualify' to expand the star projections first."
            )
            return expression

        column_names = expression.alias_column_names
        expression.args["alias"].set("columns", None)

        for name, select in zip(column_names, cte_query.selects):
            to_replace = select

            if isinstance(select, exp.Alias):
                select = select.this

            # Inner aliases are shadowed by the CTE column names
            to_replace.replace(exp.alias_(select, name))

    return expression


def _build_parse_timestamp(args: t.List) -> exp.StrToTime:
    this = build_formatted_time(exp.StrToTime, "bigquery")([seq_get(args, 1), seq_get(args, 0)])
    this.set("zone", seq_get(args, 2))
    return this


def _build_timestamp(args: t.List) -> exp.Timestamp:
    timestamp = exp.Timestamp.from_arg_list(args)
    timestamp.set("with_tz", True)
    return timestamp


def _build_date(args: t.List) -> exp.Date | exp.DateFromParts:
    expr_type = exp.DateFromParts if len(args) == 3 else exp.Date
    return expr_type.from_arg_list(args)


def _build_to_hex(args: t.List) -> exp.Hex | exp.MD5:
    # TO_HEX(MD5(..)) is common in BigQuery, so it's parsed into MD5 to simplify its transpilation
    arg = seq_get(args, 0)
    return exp.MD5(this=arg.this) if isinstance(arg, exp.MD5Digest) else exp.Hex(this=arg)


def _array_contains_sql(self: BigQuery.Generator, expression: exp.ArrayContains) -> str:
    return self.sql(
        exp.Exists(
            this=exp.select("1")
            .from_(exp.Unnest(expressions=[expression.left]).as_("_unnest", table=["_col"]))
            .where(exp.column("_col").eq(expression.right))
        )
    )


def _ts_or_ds_add_sql(self: BigQuery.Generator, expression: exp.TsOrDsAdd) -> str:
    return date_add_interval_sql("DATE", "ADD")(self, ts_or_ds_add_cast(expression))


def _ts_or_ds_diff_sql(self: BigQuery.Generator, expression: exp.TsOrDsDiff) -> str:
    expression.this.replace(exp.cast(expression.this, "TIMESTAMP", copy=True))
    expression.expression.replace(exp.cast(expression.expression, "TIMESTAMP", copy=True))
    unit = expression.args.get("unit") or "DAY"
    return self.func("DATE_DIFF", expression.this, expression.expression, unit)


def _unix_to_time_sql(self: BigQuery.Generator, expression: exp.UnixToTime) -> str:
    scale = expression.args.get("scale")
    timestamp = expression.this

    if scale in (None, exp.UnixToTime.SECONDS):
        return self.func("TIMESTAMP_SECONDS", timestamp)
    if scale == exp.UnixToTime.MILLIS:
        return self.func("TIMESTAMP_MILLIS", timestamp)
    if scale == exp.UnixToTime.MICROS:
        return self.func("TIMESTAMP_MICROS", timestamp)

    unix_seconds = exp.cast(exp.Div(this=timestamp, expression=exp.func("POW", 10, scale)), "int64")
    return self.func("TIMESTAMP_SECONDS", unix_seconds)


def _build_time(args: t.List) -> exp.Func:
    if len(args) == 1:
        return exp.TsOrDsToTime(this=args[0])
    if len(args) == 3:
        return exp.TimeFromParts.from_arg_list(args)

    return exp.Anonymous(this="TIME", expressions=args)


class BigQuery(Dialect):
    WEEK_OFFSET = -1
    UNNEST_COLUMN_ONLY = True
    SUPPORTS_USER_DEFINED_TYPES = False
    SUPPORTS_SEMI_ANTI_JOIN = False
    LOG_BASE_FIRST = False

    # https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#case_sensitivity
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE

    # bigquery udfs are case sensitive
    NORMALIZE_FUNCTIONS = False

    # https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements#format_elements_date_time
    TIME_MAPPING = {
        "%D": "%m/%d/%y",
        "%E*S": "%S.%f",
        "%E6S": "%S.%f",
    }

    ESCAPE_SEQUENCES = {
        "\\a": "\a",
        "\\b": "\b",
        "\\f": "\f",
        "\\n": "\n",
        "\\r": "\r",
        "\\t": "\t",
        "\\v": "\v",
    }

    FORMAT_MAPPING = {
        "DD": "%d",
        "MM": "%m",
        "MON": "%b",
        "MONTH": "%B",
        "YYYY": "%Y",
        "YY": "%y",
        "HH": "%I",
        "HH12": "%I",
        "HH24": "%H",
        "MI": "%M",
        "SS": "%S",
        "SSSSS": "%f",
        "TZH": "%z",
    }

    # The _PARTITIONTIME and _PARTITIONDATE pseudo-columns are not returned by a SELECT * statement
    # https://cloud.google.com/bigquery/docs/querying-partitioned-tables#query_an_ingestion-time_partitioned_table
    PSEUDOCOLUMNS = {"_PARTITIONTIME", "_PARTITIONDATE"}

    def normalize_identifier(self, expression: E) -> E:
        if isinstance(expression, exp.Identifier):
            parent = expression.parent
            while isinstance(parent, exp.Dot):
                parent = parent.parent

            # In BigQuery, CTEs are case-insensitive, but UDF and table names are case-sensitive
            # by default. The following check uses a heuristic to detect tables based on whether
            # they are qualified. This should generally be correct, because tables in BigQuery
            # must be qualified with at least a dataset, unless @@dataset_id is set.
            case_sensitive = (
                isinstance(parent, exp.UserDefinedFunction)
                or (
                    isinstance(parent, exp.Table)
                    and parent.db
                    and (parent.meta.get("quoted_table") or not parent.meta.get("maybe_column"))
                )
                or expression.meta.get("is_table")
            )
            if not case_sensitive:
                expression.set("this", expression.this.lower())

        return expression

    class Tokenizer(tokens.Tokenizer):
        QUOTES = ["'", '"', '"""', "'''"]
        COMMENTS = ["--", "#", ("/*", "*/")]
        IDENTIFIERS = ["`"]
        STRING_ESCAPES = ["\\"]

        HEX_STRINGS = [("0x", ""), ("0X", "")]

        BYTE_STRINGS = [
            (prefix + q, q) for q in t.cast(t.List[str], QUOTES) for prefix in ("b", "B")
        ]

        RAW_STRINGS = [
            (prefix + q, q) for q in t.cast(t.List[str], QUOTES) for prefix in ("r", "R")
        ]

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "ANY TYPE": TokenType.VARIANT,
            "BEGIN TRANSACTION": TokenType.BEGIN_TRANSACTION,
            "BYTES": TokenType.BINARY,
            "CURRENT_DATETIME": TokenType.CURRENT_DATETIME,
            "FLOAT64": TokenType.DOUBLE,
            "FOR SYSTEM_TIME": TokenType.TIMESTAMP_SNAPSHOT,
            "MODEL": TokenType.MODEL,
            "NOT DETERMINISTIC": TokenType.VOLATILE,
            "RECORD": TokenType.STRUCT,
            "TIMESTAMP": TokenType.TIMESTAMPTZ,
            "EXCEPTION": TokenType.COMMAND,
            "WHILE": TokenType.WHILE,
            "END WHILE": TokenType.END_WHILE,
            "END IF": TokenType.END_IF,
            "ELSEIF": TokenType.ELSE_IF,
            "CALL": TokenType.CALL,
            "DECLARE": TokenType.DECLARE,
            "END FOR": TokenType.END_FOR,
            "DO": TokenType.DO,
        }
        KEYWORDS.pop("DIV")
        KEYWORDS.pop("VALUES")

    class Parser(parser.Parser):
        PREFIXED_PIVOT_COLUMNS = True
        LOG_DEFAULTS_TO_LN = True
        SUPPORTS_IMPLICIT_UNNEST = True

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "DATE": _build_date,
            "DATE_ADD": build_date_delta_with_interval(exp.DateAdd),
            "DATE_SUB": build_date_delta_with_interval(exp.DateSub),
            "DATE_TRUNC": lambda args: exp.DateTrunc(
                unit=exp.Literal.string(str(seq_get(args, 1))),
                this=seq_get(args, 0),
            ),
            "DATETIME_ADD": build_date_delta_with_interval(exp.DatetimeAdd),
            "DATETIME_SUB": build_date_delta_with_interval(exp.DatetimeSub),
            "DIV": binary_from_function(exp.IntDiv),
            "FORMAT_DATE": lambda args: exp.TimeToStr(
                this=exp.TsOrDsToDate(this=seq_get(args, 1)), format=seq_get(args, 0)
            ),
            "GENERATE_ARRAY": exp.GenerateSeries.from_arg_list,
            "JSON_EXTRACT_SCALAR": lambda args: exp.JSONExtractScalar(
                this=seq_get(args, 0), expression=seq_get(args, 1) or exp.Literal.string("$")
            ),
            "MD5": exp.MD5Digest.from_arg_list,
            "TO_HEX": _build_to_hex,
            "PARSE_DATE": lambda args: build_formatted_time(exp.StrToDate, "bigquery")(
                [seq_get(args, 1), seq_get(args, 0)]
            ),
            "PARSE_TIMESTAMP": _build_parse_timestamp,
            "REGEXP_CONTAINS": exp.RegexpLike.from_arg_list,
            "REGEXP_EXTRACT": lambda args: exp.RegexpExtract(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
                position=seq_get(args, 2),
                occurrence=seq_get(args, 3),
                group=exp.Literal.number(1) if re.compile(args[1].name).groups == 1 else None,
            ),
            "SHA256": lambda args: exp.SHA2(this=seq_get(args, 0), length=exp.Literal.number(256)),
            "SHA512": lambda args: exp.SHA2(this=seq_get(args, 0), length=exp.Literal.number(512)),
            "SPLIT": lambda args: exp.Split(
                # https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#split
                this=seq_get(args, 0),
                expression=seq_get(args, 1) or exp.Literal.string(","),
            ),
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
            "TO_JSON_STRING": exp.JSONFormat.from_arg_list,
        }

        FUNCTION_PARSERS = {
            **parser.Parser.FUNCTION_PARSERS,
            "ARRAY": lambda self: self.expression(exp.Array, expressions=[self._parse_statement()]),
        }
        FUNCTION_PARSERS.pop("TRIM")

        NO_PAREN_FUNCTIONS = {
            **parser.Parser.NO_PAREN_FUNCTIONS,
            TokenType.CURRENT_DATETIME: exp.CurrentDatetime,
        }

        NESTED_TYPE_TOKENS = {
            *parser.Parser.NESTED_TYPE_TOKENS,
            TokenType.TABLE,
        }

        ID_VAR_TOKENS = {
            *parser.Parser.ID_VAR_TOKENS,
            TokenType.VALUES,
            TokenType.CALL,
            TokenType.WHILE,
            TokenType.DECLARE,
            TokenType.DO,
        }

        PROPERTY_PARSERS = {
            **parser.Parser.PROPERTY_PARSERS,
            "NOT DETERMINISTIC": lambda self: self.expression(
                exp.StabilityProperty, this=exp.Literal.string("VOLATILE")
            ),
            "OPTIONS": lambda self: self._parse_with_property(),
        }

        CONSTRAINT_PARSERS = {
            **parser.Parser.CONSTRAINT_PARSERS,
            "OPTIONS": lambda self: exp.Properties(expressions=self._parse_with_property()),
        }

        RANGE_PARSERS = parser.Parser.RANGE_PARSERS.copy()
        RANGE_PARSERS.pop(TokenType.OVERLAPS)

        NULL_TOKENS = {TokenType.NULL, TokenType.UNKNOWN}

        STATEMENT_PARSERS = {
            **parser.Parser.STATEMENT_PARSERS,
            TokenType.ELSE: lambda self: self._parse_as_command(self._prev),
            TokenType.END: lambda self: self._parse_as_command(self._prev),
            TokenType.FOR: lambda self: self._parse_for_in(),
            TokenType.WHILE: lambda self: self._parse_while(),
            TokenType.CALL: lambda self: self._parse_call(),
            TokenType.DECLARE: lambda self: self._parse_declare(),
            TokenType.BEGIN: lambda self: self._parse_begin_block(),
            TokenType.BEGIN_TRANSACTION: lambda self: self._parse_begin_transaction(),
        }

        BRACKET_OFFSETS = {
            "OFFSET": (0, False),
            "ORDINAL": (1, False),
            "SAFE_OFFSET": (0, True),
            "SAFE_ORDINAL": (1, True),
        }

        OPENING_TOKEN_TO_SHOULD_MERGE = {
            TokenType.BEGIN: lambda previous: previous == TokenType.CREATE,
            TokenType.THEN: lambda previous: (
                previous.token_type == TokenType.VAR and previous.text == "IF"
            )
            or (previous.token_type == TokenType.WHEN),
            TokenType.DO: lambda previous: previous.token_type in (TokenType.FOR, TokenType.WHILE),
        }

        OPENING_TOKEN_TO_CLOSING_TOKEN = {
            TokenType.END: lambda stack_token: stack_token.token_type
            in (TokenType.CREATE, TokenType.BEGIN, TokenType.WHEN),
            TokenType.END_IF: lambda stack_token: stack_token.token_type == TokenType.VAR
            and stack_token.text == "IF",
            TokenType.END_WHILE: lambda stack_token: stack_token.token_type == TokenType.WHILE,
            TokenType.END_FOR: lambda stack_token: stack_token.token_type == TokenType.FOR,
        }

        @staticmethod
        def _is_opening_scope_token(token: Token, previous_chunk_token: t.Optional[Token]) -> bool:
            # Merge statement has THEN tokens, but doesn't create a new scope.
            if (
                previous_chunk_token is not None
                and previous_chunk_token.token_type == TokenType.MERGE
            ):
                return False
            if token.token_type in (TokenType.BEGIN, TokenType.THEN, TokenType.DO):
                return True
            return False

        @classmethod
        def _should_merge_chunks(cls, previous_chunk_token: Token, cur_token: Token) -> bool:
            return cls.OPENING_TOKEN_TO_SHOULD_MERGE.get(cur_token.token_type, lambda p: False)(
                previous_chunk_token
            )

        @classmethod
        def _is_closing_token(cls, stack_token: Token, cur_token: Token) -> bool:
            return cls.OPENING_TOKEN_TO_CLOSING_TOKEN.get(cur_token.token_type, lambda s: False)(
                stack_token
            )

        def _chunkify(self, raw_tokens):
            chunks = [[]]
            stack = []

            for i, token in enumerate(raw_tokens):
                if len(stack) > 0:
                    chunks[-1].append(token)
                    if self._is_closing_token(stack[-1], token):
                        stack.pop()
                    continue

                if token.token_type == TokenType.SEMICOLON:
                    if len(chunks[-1]) > 0:
                        chunks.append([])
                    continue

                opening_token = None if len(chunks[-1]) == 0 else chunks[-1][0]
                if not self._is_opening_scope_token(token, opening_token):
                    chunks[-1].append(token)
                    continue
                # This is a bit tricky - some opening scope tokens (like BEGIN), are actually part
                # of another scope (for example, CREATE PROCEDURE ... BEGIN) - we need to backtrack
                # and merge.
                for prev_token in chunks[-1][::-1]:
                    if self._should_merge_chunks(prev_token, token):
                        chunks[-1].append(token)
                        stack.append(prev_token)
                        break
                else:
                    chunks.append([token])
                    stack.append(token)

            # The stack should be empty here - raise an error if not.
            if len(stack):
                self.raise_error("Didn't close a scope.")
            # We can have potentially the first and last chunks as empty - remove as necessary.
            # This can happen due to the last token being a semicolon, or having an opening scope token
            # as the first token.
            if len(chunks[0]) == 0:
                chunks = chunks[1:]
            if len(chunks) > 0 and len(chunks[-1]) == 0:
                chunks = chunks[:-1]
            return chunks

        @dataclass(frozen=True)
        class BacktrackingInfo:
            error_level: ErrorLevel
            error_message_context: int
            max_errors: int
            dialect: Dialect
            sql: str
            errors: t.List[ParseError]
            tokens: t.List[Token]
            index: int
            curr: t.Optional[Token]
            next: t.Optional[Token]
            prev: t.Optional[Token]
            prev_comments: t.Optional[str]

            @classmethod
            def from_parser(cls, parser_instance: parser.Parser):
                return cls(
                    error_level=parser_instance.error_level,
                    error_message_context=parser_instance.error_message_context,
                    max_errors=parser_instance.max_errors,
                    dialect=parser_instance.dialect,
                    sql=parser_instance.sql,
                    errors=parser_instance.errors.copy(),
                    tokens=parser_instance._tokens.copy(),
                    index=parser_instance._index,
                    curr=parser_instance._curr,
                    next=parser_instance._next,
                    prev=parser_instance._prev,
                    prev_comments=parser_instance._prev_comments,
                )

        def reset(self, context: t.Optional[BacktrackingInfo] = None):
            if context is None:
                return super().reset()

            self.error_level = context.error_level
            self.error_message_context = context.error_message_context
            self.max_errors = context.max_errors
            self.dialect = context.dialect
            self.sql = context.sql
            self.erros = context.errors
            self._tokens = context.tokens
            self._index = context.index
            self._curr = context.curr
            self._next = context.next
            self._prev = context.prev
            self._prev_comments = context.prev_comments

        def _parse_begin_transaction(self):
            return super()._parse_transaction()

        def _parse_set(self, unset: bool = False, tag: bool = False) -> exp.Set | exp.Command:
            # TODO: While this parses correctly set statements as described here:
            # https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#set
            # It also allows setting local variables with TO, for example, accepting invalid syntax.
            # This needs to be resolved somehow.
            set_ = self.expression(
                exp.Set, expressions=self._parse_csv(self._parse_set_item), unset=unset, tag=tag
            )

            return set_

        def _parse_declare(self):
            # Based on this documentation:
            # https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#declare
            this = self.expression(exp.Tuple, expressions=self._parse_csv(self._parse_id_var))
            vars_type = self._parse_types()
            default_value = None
            if self._match(TokenType.DEFAULT):
                default_value = self._parse_expression()
            return self.expression(
                exp.Declare, this=this, vars_type=vars_type, default_value=default_value
            )

        def _parse_if(self) -> t.Optional[exp.Expression]:
            # First, attempt to parse this as an if statement (not the ternary expression).
            # If this fails, fall back to previous behavior.
            # To do so, we need to back track - record current state so that we can reset if needed
            context = self.BacktrackingInfo.from_parser(self)
            conjunction = self._parse_conjunction()
            if not self._match(TokenType.THEN):
                # OK - fall back to previous behavior.
                self.reset(context=context)
                return super()._parse_if()
            if_block, end_token = self._parse_statement_block(
                [TokenType.END_IF, TokenType.ELSE, TokenType.ELSE_IF]
            )
            elseif_blocks = []
            while end_token == TokenType.ELSE_IF:
                condition = self._parse_conjunction()
                self._match(TokenType.THEN)
                elseif_block, end_token = self._parse_statement_block(
                    [TokenType.ELSE_IF, TokenType.ELSE, TokenType.END_IF]
                )
                elseif = self.expression(exp.ElseIf, this=condition, block=elseif_block)
                elseif_blocks.append(elseif)
            else_block = []
            if end_token == TokenType.ELSE:
                else_block, _ = self._parse_statement_block([TokenType.END_IF])
            return self.expression(
                exp.IfStatement,
                this=conjunction,
                if_block=if_block,
                elseif_blocks=elseif_blocks,
                else_block=else_block,
                else_=end_token == TokenType.ELSE,
            )

        def _parse_statement_block(self, end_tokens: t.Sequence[TokenType]):
            body = []
            matched_end_token = None
            while self._curr:
                found_end_token = False
                for end_token in end_tokens:
                    if self._match(end_token):
                        matched_end_token = end_token
                        found_end_token = True
                        break
                if found_end_token:
                    break
                body.append(self._parse_statement())
            return body, matched_end_token

        def _parse_begin_block(self):
            this, end = self._parse_statement_block([TokenType.END])
            return self.expression(exp.BeginBlock, this=this, end=end is not None)

        def _parse_statement(self) -> t.Optional[exp.Expression]:
            result = super()._parse_statement()
            self._match(TokenType.SEMICOLON)
            return result

        def _parse_call(self):
            this = self._parse_id_var()

            while self._match(TokenType.DOT):
                this = self.expression(exp.Dot, this=this, expression=self._parse_id_var())

            call_args = []
            if self._match(TokenType.L_PAREN):
                call_args = self._parse_csv(self._parse_expression)
                self._match(TokenType.R_PAREN)

            return self.expression(exp.Call, this=this, call_args=call_args)

        def _parse_while(self):
            conjunction = self._parse_conjunction()
            self._match(TokenType.DO)
            body, end_token = self._parse_statement_block([TokenType.END_WHILE])
            return self.expression(
                exp.While, this=conjunction, body=body, end=end_token is not None
            )

        def _parse_procedure_body(self, create_token: Token, extend_props: t.Callable):
            this = self._parse_user_defined_function(kind=create_token.token_type)

            # exp.Properties.Location.POST_SCHEMA ("schema" here is the UDF's type signature)
            extend_props(self._parse_properties())

            # Big query stored procedures don't support alias,
            # except for spark ones, which we do not support at the moment.
            # Reference:
            # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_procedure
            begin = self._match(TokenType.BEGIN)
            procedure_body, end_token = self._parse_statement_block([TokenType.END])
            return this, procedure_body, begin, end_token is not None

        def _parse_for_in(self) -> exp.ForIn:
            this = self._parse_range()
            self._match_text_seq("DO")
            expression, end_token = self._parse_statement_block([TokenType.END_FOR])
            return self.expression(exp.ForIn, this=this, expression=expression)

        def _parse_table_part(self, schema: bool = False) -> t.Optional[exp.Expression]:
            this = super()._parse_table_part(schema=schema) or self._parse_number()

            # https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#table_names
            if isinstance(this, exp.Identifier):
                table_name = this.name
                while self._match(TokenType.DASH, advance=False) and self._next:
                    text = ""
                    while self._curr and self._curr.token_type != TokenType.DOT:
                        self._advance()
                        text += self._prev.text
                    table_name += text

                this = exp.Identifier(this=table_name, quoted=this.args.get("quoted"))
            elif isinstance(this, exp.Literal):
                table_name = this.name

                if self._is_connected() and self._parse_var(any_token=True):
                    table_name += self._prev.text

                this = exp.Identifier(this=table_name, quoted=True)

            return this

        def _parse_table_parts(
            self, schema: bool = False, is_db_reference: bool = False, wildcard: bool = False
        ) -> exp.Table:
            table = super()._parse_table_parts(
                schema=schema, is_db_reference=is_db_reference, wildcard=True
            )

            # proj-1.db.tbl -- `1.` is tokenized as a float so we need to unravel it here
            if not table.catalog:
                if table.db:
                    parts = table.db.split(".")
                    if len(parts) == 2 and not table.args["db"].quoted:
                        table.set("catalog", exp.Identifier(this=parts[0]))
                        table.set("db", exp.Identifier(this=parts[1]))
                else:
                    parts = table.name.split(".")
                    if len(parts) == 2 and not table.this.quoted:
                        table.set("db", exp.Identifier(this=parts[0]))
                        table.set("this", exp.Identifier(this=parts[1]))

            if any("." in p.name for p in table.parts):
                catalog, db, this, *rest = (
                    exp.to_identifier(p, quoted=True)
                    for p in split_num_words(".".join(p.name for p in table.parts), ".", 3)
                )

                if rest and this:
                    this = exp.Dot.build([this, *rest])  # type: ignore

                table = exp.Table(this=this, db=db, catalog=catalog)
                table.meta["quoted_table"] = True

            return table

        @t.overload
        def _parse_json_object(self, agg: Lit[False]) -> exp.JSONObject:
            ...

        @t.overload
        def _parse_json_object(self, agg: Lit[True]) -> exp.JSONObjectAgg:
            ...

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

        def _parse_bracket(self, this: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
            bracket = super()._parse_bracket(this)

            if this is bracket:
                return bracket

            if isinstance(bracket, exp.Bracket):
                for expression in bracket.expressions:
                    name = expression.name.upper()

                    if name not in self.BRACKET_OFFSETS:
                        break

                    offset, safe = self.BRACKET_OFFSETS[name]
                    bracket.set("offset", offset)
                    bracket.set("safe", safe)
                    expression.replace(expression.expressions[0])

            return bracket

    class Generator(generator.Generator):
        EXPLICIT_UNION = True
        INTERVAL_ALLOWS_PLURAL_FORM = False
        JOIN_HINTS = False
        QUERY_HINTS = False
        TABLE_HINTS = False
        LIMIT_FETCH = "LIMIT"
        RENAME_TABLE_WITH_DB = False
        NVL2_SUPPORTED = False
        UNNEST_WITH_ORDINALITY = False
        COLLATE_IS_FUNC = True
        LIMIT_ONLY_LITERALS = True
        SUPPORTS_TABLE_ALIAS_COLUMNS = False
        UNPIVOT_ALIASES_ARE_IDENTIFIERS = False
        JSON_KEY_VALUE_PAIR_SEP = ","
        NULL_ORDERING_SUPPORTED = False
        IGNORE_NULLS_IN_FUNC = True
        JSON_PATH_SINGLE_QUOTE_ESCAPE = True
        CAN_IMPLEMENT_ARRAY_ANY = True
        SUPPORTS_TO_NUMBER = False
        NAMED_PLACEHOLDER_TOKEN = "@"

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.ApproxDistinct: rename_func("APPROX_COUNT_DISTINCT"),
            exp.ArgMax: arg_max_or_min_no_count("MAX_BY"),
            exp.ArgMin: arg_max_or_min_no_count("MIN_BY"),
            exp.ArrayContains: _array_contains_sql,
            exp.ArrayFilter: filter_array_using_unnest,
            exp.ArraySize: rename_func("ARRAY_LENGTH"),
            exp.Cast: transforms.preprocess([transforms.remove_precision_parameterized_types]),
            exp.CollateProperty: lambda self, e: (
                f"DEFAULT COLLATE {self.sql(e, 'this')}"
                if e.args.get("default")
                else f"COLLATE {self.sql(e, 'this')}"
            ),
            exp.Commit: lambda *_: "COMMIT TRANSACTION",
            exp.CountIf: rename_func("COUNTIF"),
            exp.Create: _create_sql,
            exp.CTE: transforms.preprocess([_pushdown_cte_column_names]),
            exp.DateAdd: date_add_interval_sql("DATE", "ADD"),
            exp.DateDiff: lambda self, e: self.func(
                "DATE_DIFF", e.this, e.expression, e.unit or "DAY"
            ),
            exp.DateFromParts: rename_func("DATE"),
            exp.DateStrToDate: datestrtodate_sql,
            exp.DateSub: date_add_interval_sql("DATE", "SUB"),
            exp.DatetimeAdd: date_add_interval_sql("DATETIME", "ADD"),
            exp.DatetimeSub: date_add_interval_sql("DATETIME", "SUB"),
            exp.DateTrunc: lambda self, e: self.func("DATE_TRUNC", e.this, e.text("unit")),
            exp.FromTimeZone: lambda self, e: self.func(
                "DATETIME", self.func("TIMESTAMP", e.this, e.args.get("zone")), "'UTC'"
            ),
            exp.GenerateSeries: rename_func("GENERATE_ARRAY"),
            exp.GroupConcat: rename_func("STRING_AGG"),
            exp.Hex: rename_func("TO_HEX"),
            exp.If: if_sql(false_value="NULL"),
            exp.ILike: no_ilike_sql,
            exp.IntDiv: rename_func("DIV"),
            exp.JSONFormat: rename_func("TO_JSON_STRING"),
            exp.Max: max_or_greatest,
            exp.MD5: lambda self, e: self.func("TO_HEX", self.func("MD5", e.this)),
            exp.MD5Digest: rename_func("MD5"),
            exp.Min: min_or_least,
            exp.PartitionedByProperty: lambda self, e: f"PARTITION BY {self.sql(e, 'this')}",
            exp.RegexpExtract: lambda self, e: self.func(
                "REGEXP_EXTRACT",
                e.this,
                e.expression,
                e.args.get("position"),
                e.args.get("occurrence"),
            ),
            exp.RegexpReplace: regexp_replace_sql,
            exp.RegexpLike: rename_func("REGEXP_CONTAINS"),
            exp.ReturnsProperty: _returnsproperty_sql,
            exp.Rollback: lambda *_: "ROLLBACK TRANSACTION",
            exp.Select: transforms.preprocess(
                [
                    transforms.explode_to_unnest(),
                    _unqualify_unnest,
                    transforms.eliminate_distinct_on,
                    _alias_ordered_group,
                    transforms.eliminate_semi_and_anti_joins,
                ]
            ),
            exp.SHA2: lambda self, e: self.func(
                "SHA256" if e.text("length") == "256" else "SHA512", e.this
            ),
            exp.StabilityProperty: lambda self, e: (
                "DETERMINISTIC" if e.name == "IMMUTABLE" else "NOT DETERMINISTIC"
            ),
            exp.StrToDate: lambda self, e: self.func("PARSE_DATE", self.format_time(e), e.this),
            exp.StrToTime: lambda self, e: self.func(
                "PARSE_TIMESTAMP", self.format_time(e), e.this, e.args.get("zone")
            ),
            exp.TimeAdd: date_add_interval_sql("TIME", "ADD"),
            exp.TimeFromParts: rename_func("TIME"),
            exp.TimeSub: date_add_interval_sql("TIME", "SUB"),
            exp.TimestampAdd: date_add_interval_sql("TIMESTAMP", "ADD"),
            exp.TimestampDiff: rename_func("TIMESTAMP_DIFF"),
            exp.TimestampSub: date_add_interval_sql("TIMESTAMP", "SUB"),
            exp.TimeStrToTime: timestrtotime_sql,
            exp.Transaction: lambda *_: "BEGIN TRANSACTION",
            exp.Trim: lambda self, e: self.func("TRIM", e.this, e.expression),
            exp.TsOrDsAdd: _ts_or_ds_add_sql,
            exp.TsOrDsDiff: _ts_or_ds_diff_sql,
            exp.TsOrDsToTime: rename_func("TIME"),
            exp.Unhex: rename_func("FROM_HEX"),
            exp.UnixDate: rename_func("UNIX_DATE"),
            exp.UnixToTime: _unix_to_time_sql,
            exp.Values: _derived_table_values_to_unnest,
            exp.VariancePop: rename_func("VAR_POP"),
        }

        SUPPORTED_JSON_PATH_PARTS = {
            exp.JSONPathKey,
            exp.JSONPathRoot,
            exp.JSONPathSubscript,
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.BIGDECIMAL: "BIGNUMERIC",
            exp.DataType.Type.BIGINT: "INT64",
            exp.DataType.Type.BINARY: "BYTES",
            exp.DataType.Type.BOOLEAN: "BOOL",
            exp.DataType.Type.CHAR: "STRING",
            exp.DataType.Type.DECIMAL: "NUMERIC",
            exp.DataType.Type.DOUBLE: "FLOAT64",
            exp.DataType.Type.FLOAT: "FLOAT64",
            exp.DataType.Type.INT: "INT64",
            exp.DataType.Type.NCHAR: "STRING",
            exp.DataType.Type.NVARCHAR: "STRING",
            exp.DataType.Type.SMALLINT: "INT64",
            exp.DataType.Type.TEXT: "STRING",
            exp.DataType.Type.TIMESTAMP: "DATETIME",
            exp.DataType.Type.TIMESTAMPTZ: "TIMESTAMP",
            exp.DataType.Type.TIMESTAMPLTZ: "TIMESTAMP",
            exp.DataType.Type.TINYINT: "INT64",
            exp.DataType.Type.VARBINARY: "BYTES",
            exp.DataType.Type.VARCHAR: "STRING",
            exp.DataType.Type.VARIANT: "ANY TYPE",
        }

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
            exp.PartitionedByProperty: exp.Properties.Location.POST_SCHEMA,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        }

        # from: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#reserved_keywords
        RESERVED_KEYWORDS = {
            *generator.Generator.RESERVED_KEYWORDS,
            "all",
            "and",
            "any",
            "array",
            "as",
            "asc",
            "assert_rows_modified",
            "at",
            "between",
            "by",
            "case",
            "cast",
            "collate",
            "contains",
            "create",
            "cross",
            "cube",
            "current",
            "default",
            "define",
            "desc",
            "distinct",
            "else",
            "end",
            "enum",
            "escape",
            "except",
            "exclude",
            "exists",
            "extract",
            "false",
            "fetch",
            "following",
            "for",
            "from",
            "full",
            "group",
            "grouping",
            "groups",
            "hash",
            "having",
            "if",
            "ignore",
            "in",
            "inner",
            "intersect",
            "interval",
            "into",
            "is",
            "join",
            "lateral",
            "left",
            "like",
            "limit",
            "lookup",
            "merge",
            "natural",
            "new",
            "no",
            "not",
            "null",
            "nulls",
            "of",
            "on",
            "or",
            "order",
            "outer",
            "over",
            "partition",
            "preceding",
            "proto",
            "qualify",
            "range",
            "recursive",
            "respect",
            "right",
            "rollup",
            "rows",
            "select",
            "set",
            "some",
            "struct",
            "tablesample",
            "then",
            "to",
            "treat",
            "true",
            "unbounded",
            "union",
            "unnest",
            "using",
            "when",
            "where",
            "window",
            "with",
            "within",
        }

        def table_parts(self, expression: exp.Table) -> str:
            # Depending on the context, `x.y` may not resolve to the same data source as `x`.`y`, so
            # we need to make sure the correct quoting is used in each case.
            #
            # For example, if there is a CTE x that clashes with a schema name, then the former will
            # return the table y in that schema, whereas the latter will return the CTE's y column:
            #
            # - WITH x AS (SELECT [1, 2] AS y) SELECT * FROM x, `x.y`   -> cross join
            # - WITH x AS (SELECT [1, 2] AS y) SELECT * FROM x, `x`.`y` -> implicit unnest
            if expression.meta.get("quoted_table"):
                table_parts = ".".join(p.name for p in expression.parts)
                return self.sql(exp.Identifier(this=table_parts, quoted=True))

            return super().table_parts(expression)

        def timetostr_sql(self, expression: exp.TimeToStr) -> str:
            this = expression.this if isinstance(expression.this, exp.TsOrDsToDate) else expression
            return self.func("FORMAT_DATE", self.format_time(expression), this.this)

        def eq_sql(self, expression: exp.EQ) -> str:
            # Operands of = cannot be NULL in BigQuery
            if isinstance(expression.left, exp.Null) or isinstance(expression.right, exp.Null):
                if not isinstance(expression.parent, exp.Update):
                    return "NULL"

            return self.binary(expression, "=")

        def attimezone_sql(self, expression: exp.AtTimeZone) -> str:
            parent = expression.parent

            # BigQuery allows CAST(.. AS {STRING|TIMESTAMP} [FORMAT <fmt> [AT TIME ZONE <tz>]]).
            # Only the TIMESTAMP one should use the below conversion, when AT TIME ZONE is included.
            if not isinstance(parent, exp.Cast) or not parent.to.is_type("text"):
                return self.func(
                    "TIMESTAMP", self.func("DATETIME", expression.this, expression.args.get("zone"))
                )

            return super().attimezone_sql(expression)

        def trycast_sql(self, expression: exp.TryCast) -> str:
            return self.cast_sql(expression, safe_prefix="SAFE_")

        def array_sql(self, expression: exp.Array) -> str:
            first_arg = seq_get(expression.expressions, 0)
            if isinstance(first_arg, exp.Query):
                return f"ARRAY{self.wrap(self.sql(first_arg))}"

            return inline_array_sql(self, expression)

        def bracket_sql(self, expression: exp.Bracket) -> str:
            this = expression.this
            expressions = expression.expressions

            if len(expressions) == 1 and this and this.is_type(exp.DataType.Type.STRUCT):
                arg = expressions[0]
                if arg.type is None:
                    from sqlglot.optimizer.annotate_types import annotate_types

                    arg = annotate_types(arg)

                if arg.type and arg.type.this in exp.DataType.TEXT_TYPES:
                    # BQ doesn't support bracket syntax with string values for structs
                    return f"{self.sql(this)}.{arg.name}"

            expressions_sql = self.expressions(expression, flat=True)
            offset = expression.args.get("offset")

            if offset == 0:
                expressions_sql = f"OFFSET({expressions_sql})"
            elif offset == 1:
                expressions_sql = f"ORDINAL({expressions_sql})"
            elif offset is not None:
                self.unsupported(f"Unsupported array offset: {offset}")

            if expression.args.get("safe"):
                expressions_sql = f"SAFE_{expressions_sql}"

            return f"{self.sql(this)}[{expressions_sql}]"

        def in_unnest_op(self, expression: exp.Unnest) -> str:
            return self.sql(expression)

        def except_op(self, expression: exp.Except) -> str:
            if not expression.args.get("distinct"):
                self.unsupported("EXCEPT without DISTINCT is not supported in BigQuery")
            return f"EXCEPT{' DISTINCT' if expression.args.get('distinct') else ' ALL'}"

        def intersect_op(self, expression: exp.Intersect) -> str:
            if not expression.args.get("distinct"):
                self.unsupported("INTERSECT without DISTINCT is not supported in BigQuery")
            return f"INTERSECT{' DISTINCT' if expression.args.get('distinct') else ' ALL'}"

        def with_properties(self, properties: exp.Properties) -> str:
            return self.properties(properties, prefix=self.seg("OPTIONS"))

        def version_sql(self, expression: exp.Version) -> str:
            if expression.name == "TIMESTAMP":
                expression.set("this", "SYSTEM_TIME")
            return super().version_sql(expression)

        def while_sql(self, expression: exp.While):
            body = self._sql_statements_block(expression.args.get("body", []))
            suffix = "\nEND WHILE" if body else "END WHILE"
            return f"WHILE {self.sql(expression, 'this')} DO\n{body}{suffix}"

        def call_sql(self, expression: exp.Call):
            call_args = ""
            if expression.args.get("call_args", []):
                call_args = ",".join([self.sql(e) for e in expression.args.get("call_args", [])])
            return f"CALL {self.sql(expression, 'this')}({call_args})"

        def beginblock_sql(self, expression: exp.BeginBlock):
            statements_sql = self._sql_statements_block(expression.args.get("this", []))
            suffix = "\nEND" if statements_sql else "END"
            return f"BEGIN\n{statements_sql}{suffix}"

        def declare_sql(self, expression: exp.Declare):
            comma_separated = ",".join([self.sql(e) for e in expression.args.get("this", [])])
            result = f"DECLARE {comma_separated}"
            if expression.args.get("vars_type", False):
                result += f" {self.sql(expression, 'vars_type')}"
            if expression.args.get("default_value", False):
                result += f" DEFAULT {self.sql(expression, 'default_value')}"
            return result

        def elseif_sql(self, expression: exp.ElseIf):
            body = self._sql_statements_block(expression.args.get("block", []))
            if len(body) > 0:
                body = f"\n{body}"
            condition = self.sql(expression.args.get("this"))
            return f"ELSEIF {condition} THEN{body}"

        def ifstatement_sql(self, expression: exp.IfStatement):
            if_body = self._sql_statements_block(expression.args.get("if_block", []))
            if len(if_body) > 0:
                if_body = f"\n{if_body}"
            result = f"IF {self.sql(expression, 'this')} THEN{if_body}"
            elseif_blocks = expression.args.get("elseif_blocks", [])
            if len(elseif_blocks):
                result += "\n"
            for elseif in elseif_blocks:
                result += self.sql(elseif)
            if expression.args.get("else_", False):
                else_body = self._sql_statements_block(expression.args.get("else_block"))
                if len(else_body) > 0:
                    else_body = f"\n{else_body}"
                result += f"\nELSE{else_body}"
            result += "\nEND IF"
            return result

        def forin_sql(self, expression: exp.ForIn) -> str:
            this = self.sql(expression, "this")
            expression_sql = self._sql_statements_block(expression.args.get("expression", []))
            return f"FOR {this} DO\n{expression_sql}\nEND FOR"

        def _sql_statements_block(self, block):
            return "\n".join(f"{self.sql(statement)};" for statement in block)
