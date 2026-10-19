from __future__ import annotations

import typing as t

from sqlglot import exp, parser
from sqlglot.dialects.dialect import build_formatted_time, build_timetostr_or_tochar, build_trunc
from sqlglot.helper import ensure_list, seq_get
from sqlglot.parser import OPTIONS_TYPE, build_coalesce
from sqlglot.tokens import Token, TokenType

if t.TYPE_CHECKING:
    from sqlglot._typing import E
    from sqlglot.dialects.dialect import Dialect


def _build_to_timestamp(args: list, dialect: Dialect) -> exp.StrToTime | exp.Anonymous:
    if len(args) == 1:
        return exp.Anonymous(this="TO_TIMESTAMP", expressions=args)

    return build_formatted_time(exp.StrToTime)(args, dialect)


class OracleParser(parser.Parser):
    WINDOW_BEFORE_PAREN_TOKENS = {TokenType.OVER, TokenType.KEEP}
    VALUES_FOLLOWED_BY_PAREN = False
    SUPPORTS_NTH_VALUE_FROM_MODIFIER = True
    SUPPORTS_PARTITION_SELECTION = True

    VERSION_PHRASES = {
        **parser.Parser.VERSION_PHRASES,
        ("AS", "OF", "SCN"): "SCN",
        ("AS", "OF", "TIMESTAMP"): "TIMESTAMP",
    }

    FUNCTIONS = {
        **{k: v for k, v in parser.Parser.FUNCTIONS.items() if k != "TO_BOOLEAN"},
        "CONVERT": exp.ConvertToCharset.from_arg_list,
        "L2_DISTANCE": exp.EuclideanDistance.from_arg_list,
        "NVL": lambda args: build_coalesce(args, is_nvl=True),
        "SQUARE": lambda args: exp.Pow(this=seq_get(args, 0), expression=exp.Literal.number(2)),
        "TO_CHAR": build_timetostr_or_tochar,
        "TO_TIMESTAMP": _build_to_timestamp,
        "TO_DATE": build_formatted_time(exp.StrToDate),
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
        TokenType.LOCALTIMESTAMP: exp.Localtimestamp,
        TokenType.SYSTIMESTAMP: exp.Systimestamp,
    }

    FUNCTION_PARSERS = {
        **{k: v for k, v in parser.Parser.FUNCTION_PARSERS.items() if k != "CONVERT"},
        "JSON_ARRAY": lambda self: self._parse_oracle_json_array(),
        "JSON_ARRAYAGG": lambda self: self._parse_oracle_json_arrayagg(),
        "JSON_EXISTS": lambda self: self._parse_json_exists(),
        "LISTAGG": lambda self: self._parse_string_agg(),
        "TO_NUMBER": lambda self: self._parse_to_number(),
    }

    PROPERTY_PARSERS = {
        **parser.Parser.PROPERTY_PARSERS,
        "GLOBAL": lambda self: (
            self._match_text_seq("TEMPORARY")
            and self.expression(exp.TemporaryProperty(this="GLOBAL"))
        ),
        "PRIVATE": lambda self: (
            self._match_text_seq("TEMPORARY")
            and self.expression(exp.TemporaryProperty(this="PRIVATE"))
        ),
        "FORCE": lambda self: self.expression(exp.ForceProperty()),
        "TABLESPACE": lambda self: self.expression(
            exp.TableSpaceProperty(this=self._parse_id_var())
        ),
        **dict.fromkeys(
            ("PCTFREE", "PCTUSED", "INITRANS", "MAXTRANS"),
            lambda self: self._parse_physical_property(),
        ),
        **dict.fromkeys(
            ("LOGGING", "NOLOGGING"),
            lambda self: self.expression(exp.PhysicalProperty(this=self._prev.text.upper())),
        ),
        "STORAGE": lambda self: self._parse_storage(),
    }

    STORAGE_OPTIONS = {
        "INITIAL",
        "NEXT",
        "MINEXTENTS",
        "MAXEXTENTS",
        "MAXSIZE",
        "PCTINCREASE",
        "FREELISTS",
        "FREELIST",
        "BUFFER_POOL",
        "FLASH_CACHE",
        "CELL_FLASH_CACHE",
    }

    QUERY_MODIFIER_PARSERS = {
        **parser.Parser.QUERY_MODIFIER_PARSERS,
        TokenType.ORDER_SIBLINGS_BY: lambda self: ("order", self._parse_order()),
        TokenType.WITH: lambda self: (
            "options",
            ensure_list(self._parse_query_restrictions()),
        ),
    }

    TYPE_LITERAL_PARSERS = {
        exp.DType.DATE: lambda self, this, _: self.expression(exp.DateStrToDate(this=this)),
        # https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/NLS_TIMESTAMP_FORMAT.html
        exp.DType.TIMESTAMP: lambda self, this, _: _build_to_timestamp(
            [this, '"%Y-%m-%d %H:%M:%S.%f"'], self.dialect
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

    def _parse_with(self, skip_with_token: bool = False) -> exp.With | None:
        # https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/SELECT.html
        # PL/SQL function declarations precede the CTEs of the same WITH clause, and each one is
        # terminated by its own `END;` rather than separated by a comma
        if not skip_with_token and not self._match(TokenType.WITH):
            return None

        comments = self._prev_comments
        udfs = []

        while (
            self._match(TokenType.FUNCTION, advance=False)
            and self._next
            and self._next.token_type in self.ID_VAR_TOKENS
        ):
            self._advance()
            udfs.append(self._parse_plsql_function())

        if not udfs:
            return super()._parse_with(skip_with_token=True)

        if self._match_set(self.ID_VAR_TOKENS, advance=False):
            with_ = super()._parse_with(skip_with_token=True)
        else:
            with_ = self.expression(exp.With())

        if with_:
            with_.set("udfs", udfs)
            with_.add_comments(comments)

        return with_

    def _parse_plsql_function(self) -> exp.FunctionSpecification:
        this = self._parse_user_defined_function(kind=TokenType.FUNCTION)

        if not self._match_text_seq("RETURN"):
            self.raise_error("Expected RETURN")

        returns = self.expression(exp.ReturnsProperty(this=self._parse_types()))

        if not self._match_set((TokenType.IS, TokenType.ALIAS)):
            self.raise_error("Expected IS or AS")

        # The body is kept verbatim, since there is no PL/SQL statement grammar to parse it with
        start = self._prev
        end = self._advance_plsql_body()
        body = self.sql[start.end + 1 : end.end + 1].strip() if end else ""

        return self.expression(
            exp.FunctionSpecification(
                this=this,
                characteristics=self.expression(exp.Properties(expressions=[returns])),
                expression=exp.var(body),
            )
        )

    def _advance_plsql_token(self) -> bool:
        # The body's own `;` separators split it across statement chunks, so stepping through
        # it has to carry on into the next chunk whenever the current one runs out
        self._advance()

        while not self._curr or self._curr.token_type == TokenType.SEMICOLON:
            if self._curr:
                self._advance()
            elif self._chunk_index < len(self._chunks):
                self._advance_chunk()
            else:
                return False

        return True

    def _advance_plsql_body(self) -> Token | None:
        """
        Advances past a subprogram body (its declarations, then BEGIN ... END [name];) and
        returns the body's last token, leaving the parser on the token after the closing `;`.
        """
        while self._curr and self._curr.token_type != TokenType.BEGIN:
            if self._curr.token_type in (TokenType.FUNCTION, TokenType.PROCEDURE):
                # A nested subprogram is either a forward declaration, which ends at the next
                # `;`, or a definition whose IS/AS is followed by a body of its own
                chunk_index = self._chunk_index
                while (
                    self._advance_plsql_token()
                    and self._chunk_index == chunk_index
                    and not self._match_set((TokenType.IS, TokenType.ALIAS), advance=False)
                ):
                    pass

                if self._curr and self._chunk_index == chunk_index:
                    self._advance()
                    self._advance_plsql_body()

                continue

            self._advance_plsql_token()

        # END IF and END LOOP close blocks that are not counted here, whereas END CASE and a
        # bare END close a BEGIN or a CASE
        depth = 0
        while self._curr:
            token_type = self._curr.token_type

            if token_type in (TokenType.BEGIN, TokenType.CASE):
                depth += 1
            elif token_type == TokenType.END:
                closes = self._next.text.upper() if self._next else ""

                if closes in ("IF", "LOOP"):
                    self._advance()
                else:
                    if closes == "CASE":
                        self._advance()

                    depth -= 1

                    if not depth:
                        if self._next:
                            self._advance()

                        end = self._curr
                        if self._next:
                            self.raise_error("Expected ; after END")

                        self._advance_plsql_token()
                        return end

            self._advance_plsql_token()

        self.raise_error("Expected END to close the PL/SQL body")
        return None

    def _parse_physical_property(self) -> exp.PhysicalProperty:
        name = self._prev.text.upper()
        value = self._parse_number()
        if value is None:
            self.raise_error(f"Expected value for {name}")
        return self.expression(exp.PhysicalProperty(this=name, value=value))

    def _parse_storage(self) -> exp.StorageProperty:
        def parse_options() -> list[exp.Expr]:
            options: list[exp.Expr] = []
            while self._match_texts(self.STORAGE_OPTIONS):
                name = self._prev.text.upper()
                if name == "FREELIST":
                    if not self._match_text_seq("GROUPS"):
                        self.raise_error("Expected GROUPS after FREELIST")
                    name = "FREELIST GROUPS"

                if name in ("BUFFER_POOL", "FLASH_CACHE", "CELL_FLASH_CACHE"):
                    value: exp.Expr | None = self._parse_var_from_options(
                        {
                            "KEEP": (),
                            "DEFAULT": (),
                            "RECYCLE" if name == "BUFFER_POOL" else "NONE": (),
                        }
                    )
                elif name in ("MAXEXTENTS", "MAXSIZE") and self._match_text_seq("UNLIMITED"):
                    value = exp.var("UNLIMITED")
                else:
                    value = self._parse_number()
                    if (
                        value is not None
                        and name in ("INITIAL", "NEXT", "MAXSIZE")
                        and self._match_texts(("K", "M", "G", "T", "P", "E"))
                    ):
                        value = exp.var(f"{value.name}{self._prev.text.upper()}")
                if value is None:
                    self.raise_error(f"Expected value for {name}")
                options.append(self.expression(exp.PhysicalProperty(this=name, value=value)))
            return options

        return self.expression(exp.StorageProperty(expressions=self._parse_wrapped(parse_options)))

    def _parse_to_number(self) -> exp.ToNumber:
        # https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/TO_NUMBER.html
        this = self._parse_bitwise()

        default = None
        if self._match(TokenType.DEFAULT):
            default = self._parse_bitwise()
            self._match_text_seq("ON", "CONVERSION", "ERROR")

        fmt = None
        nlsparam = None
        if self._match(TokenType.COMMA):
            fmt = self._parse_bitwise()
            if self._match(TokenType.COMMA):
                nlsparam = self._parse_bitwise()

        return self.expression(
            exp.ToNumber(this=this, format=fmt, nlsparam=nlsparam, default=default)
        )

    def _parse_dbms_random(self) -> exp.Expr | None:
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

    def _parse_json_array(self, expr_type: type[E], **kwargs) -> E:
        return self.expression(
            expr_type(
                null_handling=self._parse_on_handling("NULL", "NULL", "ABSENT"),
                return_type=self._match_text_seq("RETURNING") and self._parse_type(),
                strict=self._match_text_seq("STRICT"),
                **kwargs,
            )
        )

    def _parse_hint_function_call(self) -> exp.Expr | None:
        if not self._curr or not self._next or self._next.token_type != TokenType.L_PAREN:
            return None

        name = self._curr.text

        self._advance(2)
        args = self._parse_hint_args()
        this = self.expression(exp.Anonymous(this=name, expressions=args))
        self._match_r_paren(this)
        return this

    def _parse_hint_args(self):
        args = []
        result = self._parse_var()

        while result:
            args.append(result)
            result = self._parse_var()

        return args

    def _parse_query_restrictions(self) -> exp.Expr | None:
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

    def _parse_into(self) -> exp.Into | None:
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

    def _parse_column_ops(self, this: exp.Expr | None) -> exp.Expr | None:
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

    def _parse_insert_table(self) -> exp.Expr | None:
        # Oracle does not use AS for INSERT INTO alias
        # https://docs.oracle.com/en/database/oracle/oracle-database/18/sqlrf/INSERT.html
        # Parse table parts without schema to avoid parsing the alias with its columns
        this = self._parse_table_parts(schema=True)

        if isinstance(this, exp.Table):
            this.set("partition", self._parse_partition())

            alias_name = self._parse_id_var(any_token=False)
            if alias_name:
                this.set("alias", exp.TableAlias(this=alias_name))

            # Now parse the schema (column list) if present
            return self._parse_schema(this=this)

        return this
