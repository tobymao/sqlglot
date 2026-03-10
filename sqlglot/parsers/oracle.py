from __future__ import annotations

import typing as t

from sqlglot import exp, parser
from sqlglot.dialects.dialect import build_formatted_time, build_timetostr_or_tochar, build_trunc
from sqlglot.helper import mypyc_attr, seq_get
from sqlglot.parser import OPTIONS_TYPE, build_coalesce
from sqlglot.tokens import TokenType

if t.TYPE_CHECKING:
    from sqlglot._typing import E


def _build_to_timestamp(args: t.List) -> exp.StrToTime | exp.Anonymous:
    if len(args) == 1:
        return exp.Anonymous(this="TO_TIMESTAMP", expressions=args)

    return build_formatted_time(exp.StrToTime, "oracle")(args)


@mypyc_attr(allow_interpreted_subclasses=True)
class OracleParser(parser.Parser):
    WINDOW_BEFORE_PAREN_TOKENS = {TokenType.OVER, TokenType.KEEP}
    VALUES_FOLLOWED_BY_PAREN = False

    FUNCTIONS = {
        **{k: v for k, v in parser.Parser.FUNCTIONS.items() if k != "TO_BOOLEAN"},
        "CONVERT": exp.ConvertToCharset.from_arg_list,
        "L2_DISTANCE": exp.EuclideanDistance.from_arg_list,
        "NVL": lambda args: build_coalesce(args, is_nvl=True),
        "SQUARE": lambda args: exp.Pow(this=seq_get(args, 0), expression=exp.Literal.number(2)),
        "TO_CHAR": build_timetostr_or_tochar,
        "TO_TIMESTAMP": _build_to_timestamp,
        "TO_DATE": build_formatted_time(exp.StrToDate, "oracle"),
        "TRUNC": lambda args, dialect: build_trunc(
            args, dialect, date_trunc_unabbreviate=False, default_date_trunc_unit="DD"
        ),
    }

    NO_PAREN_FUNCTION_PARSERS = {
        **parser.Parser.NO_PAREN_FUNCTION_PARSERS,
        "NEXT": lambda self: self._parse_next_value_for(),
        "PRIOR": lambda self: self.expression(exp.Prior(this=self._parse_bitwise())),
        "SYSDATE": lambda self: self.expression(exp.CurrentTimestamp(sysdate=True)),
        "DBMS_RANDOM": lambda self: self._parse_dbms_random(),
    }

    NO_PAREN_FUNCTIONS = {
        **parser.Parser.NO_PAREN_FUNCTIONS,
        TokenType.SYSTIMESTAMP: exp.Systimestamp,
    }

    FUNCTION_PARSERS = {
        **{k: v for k, v in parser.Parser.FUNCTION_PARSERS.items() if k != "CONVERT"},
        "JSON_ARRAY": lambda self: self._parse_oracle_json_array(),
        "JSON_ARRAYAGG": lambda self: self._parse_oracle_json_arrayagg(),
        "JSON_EXISTS": lambda self: self._parse_json_exists(),
    }

    PROPERTY_PARSERS = {
        **parser.Parser.PROPERTY_PARSERS,
        "GLOBAL": lambda self: self._match_text_seq("TEMPORARY")
        and self.expression(exp.TemporaryProperty(this="GLOBAL")),
        "PRIVATE": lambda self: self._match_text_seq("TEMPORARY")
        and self.expression(exp.TemporaryProperty(this="PRIVATE")),
        "FORCE": lambda self: self.expression(exp.ForceProperty()),
    }

    QUERY_MODIFIER_PARSERS = {
        **parser.Parser.QUERY_MODIFIER_PARSERS,
        TokenType.ORDER_SIBLINGS_BY: lambda self: ("order", self._parse_order()),
        TokenType.WITH: lambda self: ("options", [self._parse_query_restrictions()]),
    }

    TYPE_LITERAL_PARSERS = {
        exp.DType.DATE: lambda self, this, _: self.expression(exp.DateStrToDate(this=this)),
        # https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/NLS_TIMESTAMP_FORMAT.html
        exp.DType.TIMESTAMP: lambda self, this, _: _build_to_timestamp(
            [this, '"%Y-%m-%d %H:%M:%S.%f"']
        ),
    }

    # SELECT UNIQUE .. is old-style Oracle syntax for SELECT DISTINCT ..
    # Reference: https://stackoverflow.com/a/336455
    DISTINCT_TOKENS = {TokenType.DISTINCT, TokenType.UNIQUE}

    QUERY_RESTRICTIONS: t.ClassVar[OPTIONS_TYPE] = {
        "WITH": (
            ("READ", "ONLY"),
            ("CHECK", "OPTION"),
        ),
    }

    def _parse_dbms_random(self) -> t.Optional[exp.Expr]:
        if self._match_text_seq(".", "VALUE"):
            lower, upper = None, None
            if self._match(TokenType.L_PAREN, advance=False):
                lower_upper = self._parse_wrapped_csv(self._parse_bitwise)
                if len(lower_upper) == 2:
                    lower, upper = lower_upper

            return exp.Rand(lower=lower, upper=upper)

        self._retreat(self._index - 1)
        return None

    def _parse_oracle_json_array(self) -> exp.JSONArray:
        return self._parse_json_array(
            exp.JSONArray,
            expressions=self._parse_csv(lambda: self._parse_format_json(self._parse_bitwise())),
        )

    def _parse_oracle_json_arrayagg(self) -> exp.JSONArrayAgg:
        return self._parse_json_array(
            exp.JSONArrayAgg,
            this=self._parse_format_json(self._parse_bitwise()),
            order=self._parse_order(),
        )

    def _parse_json_array(self, expr_type: t.Type[E], **kwargs) -> E:
        return self.expression(
            expr_type(
                null_handling=self._parse_on_handling("NULL", "NULL", "ABSENT"),
                return_type=self._match_text_seq("RETURNING") and self._parse_type(),
                strict=self._match_text_seq("STRICT"),
                **kwargs,
            )
        )

    def _parse_hint_function_call(self) -> t.Optional[exp.Expr]:
        if not self._curr or not self._next or self._next.token_type != TokenType.L_PAREN:
            return None

        this = self._curr.text

        self._advance(2)
        args = self._parse_hint_args()
        this = self.expression(exp.Anonymous(this=this, expressions=args))
        self._match_r_paren(this)
        return this

    def _parse_hint_args(self):
        args = []
        result = self._parse_var()

        while result:
            args.append(result)
            result = self._parse_var()

        return args

    def _parse_query_restrictions(self) -> t.Optional[exp.Expr]:
        kind = self._parse_var_from_options(self.QUERY_RESTRICTIONS, raise_unmatched=False)

        if not kind:
            return None

        return self.expression(
            exp.QueryOption(
                this=kind, expression=self._match(TokenType.CONSTRAINT) and self._parse_field()
            )
        )

    def _parse_json_exists(self) -> exp.JSONExists:
        this = self._parse_format_json(self._parse_bitwise())
        self._match(TokenType.COMMA)
        return self.expression(
            exp.JSONExists(
                this=this,
                path=self.dialect.to_json_path(self._parse_bitwise()),
                passing=self._match_text_seq("PASSING")
                and self._parse_csv(lambda: self._parse_alias(self._parse_bitwise())),
                on_condition=self._parse_on_condition(),
            )
        )

    def _parse_into(self) -> t.Optional[exp.Into]:
        # https://docs.oracle.com/en/database/oracle/oracle-database/19/lnpls/SELECT-INTO-statement.html
        bulk_collect = self._match(TokenType.BULK_COLLECT_INTO)
        if not bulk_collect and not self._match(TokenType.INTO):
            return None

        index = self._index

        expressions = self._parse_expressions()
        if len(expressions) == 1:
            self._retreat(index)
            self._match(TokenType.TABLE)
            return self.expression(
                exp.Into(this=self._parse_table(schema=True), bulk_collect=bulk_collect)
            )

        return self.expression(exp.Into(bulk_collect=bulk_collect, expressions=expressions))

    def _parse_connect_with_prior(self):
        return self._parse_assignment()

    def _parse_column_ops(self, this: t.Optional[exp.Expr]) -> t.Optional[exp.Expr]:
        this = super()._parse_column_ops(this)

        if not this:
            return this

        index = self._index

        # https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Interval-Exprs.html
        interval_span = self._try_parse(lambda: self._parse_interval_span(this))
        if interval_span and isinstance(interval_span.args.get("unit"), exp.IntervalSpan):
            return interval_span

        self._retreat(index)
        return this

    def _parse_insert_table(self) -> t.Optional[exp.Expr]:
        # Oracle does not use AS for INSERT INTO alias
        # https://docs.oracle.com/en/database/oracle/oracle-database/18/sqlrf/INSERT.html
        # Parse table parts without schema to avoid parsing the alias with its columns
        this = self._parse_table_parts(schema=True)

        if isinstance(this, exp.Table):
            alias_name = self._parse_id_var(any_token=False)
            if alias_name:
                this.set("alias", exp.TableAlias(this=alias_name))

            this.set("partition", self._parse_partition())

            # Now parse the schema (column list) if present
            return self._parse_schema(this=this)

        return this
