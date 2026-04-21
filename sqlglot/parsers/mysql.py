from __future__ import annotations

import typing as t

from sqlglot import exp, parser
from sqlglot.trie import new_trie
from sqlglot.dialects.dialect import (
    Dialect,
    build_date_delta,
    build_date_delta_with_interval,
    build_formatted_time,
    isnull_to_is_null,
)
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType


# All specifiers for time parts (as opposed to date parts)
# https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format
TIME_SPECIFIERS = {"f", "H", "h", "I", "i", "k", "l", "p", "r", "S", "s", "T"}


def _has_time_specifier(date_format: str) -> bool:
    i = 0
    length = len(date_format)

    while i < length:
        if date_format[i] == "%":
            i += 1
            if i < length and date_format[i] in TIME_SPECIFIERS:
                return True
        i += 1
    return False


def _str_to_date(args: t.List) -> exp.StrToDate | exp.StrToTime:
    mysql_date_format = seq_get(args, 1)
    date_format = Dialect["mysql"].format_time(mysql_date_format)
    this = seq_get(args, 0)

    if mysql_date_format and _has_time_specifier(mysql_date_format.name):
        return exp.StrToTime(this=this, format=date_format)

    return exp.StrToDate(this=this, format=date_format)


def _show_parser(*args: t.Any, **kwargs: t.Any) -> t.Callable[[MySQLParser], exp.Show]:
    def _parse(self: MySQLParser) -> exp.Show:
        return self._parse_show_mysql(*args, **kwargs)

    return _parse


class MySQLParser(parser.Parser):
    NO_PAREN_FUNCTIONS = {
        **parser.Parser.NO_PAREN_FUNCTIONS,
        TokenType.LOCALTIME: exp.Localtime,
        TokenType.LOCALTIMESTAMP: exp.Localtimestamp,
    }

    ID_VAR_TOKENS = parser.Parser.ID_VAR_TOKENS - {TokenType.STRAIGHT_JOIN}

    FUNC_TOKENS = {
        *parser.Parser.FUNC_TOKENS,
        TokenType.DATABASE,
        TokenType.MOD,
        TokenType.SCHEMA,
        TokenType.VALUES,
        TokenType.CHARACTER_SET,
    }

    CONJUNCTION = {
        **parser.Parser.CONJUNCTION,
        TokenType.DAMP: exp.And,
        TokenType.XOR: exp.Xor,
    }

    DISJUNCTION = {
        **parser.Parser.DISJUNCTION,
        TokenType.DPIPE: exp.Or,
    }

    TABLE_ALIAS_TOKENS = (
        (parser.Parser.TABLE_ALIAS_TOKENS | {TokenType.ANTI, TokenType.SEMI})
        - parser.Parser.TABLE_INDEX_HINT_TOKENS
        - {TokenType.STRAIGHT_JOIN}
    )

    RANGE_PARSERS = {
        **parser.Parser.RANGE_PARSERS,
        TokenType.SOUNDS_LIKE: lambda self, this: self.expression(
            exp.EQ(
                this=self.expression(exp.Soundex(this=this)),
                expression=self.expression(exp.Soundex(this=self._parse_term())),
            )
        ),
        TokenType.MEMBER_OF: lambda self, this: self.expression(
            exp.JSONArrayContains(this=this, expression=self._parse_wrapped(self._parse_expression))
        ),
    }

    FUNCTIONS = {
        **parser.Parser.FUNCTIONS,
        "BIT_AND": exp.BitwiseAndAgg.from_arg_list,
        "BIT_OR": exp.BitwiseOrAgg.from_arg_list,
        "BIT_XOR": exp.BitwiseXorAgg.from_arg_list,
        "BIT_COUNT": exp.BitwiseCount.from_arg_list,
        "CONVERT_TZ": lambda args: exp.ConvertTimezone(
            source_tz=seq_get(args, 1), target_tz=seq_get(args, 2), timestamp=seq_get(args, 0)
        ),
        "CURDATE": exp.CurrentDate.from_arg_list,
        "CURTIME": exp.CurrentTime.from_arg_list,
        "DATE": lambda args: exp.TsOrDsToDate(this=seq_get(args, 0)),
        "DATE_ADD": build_date_delta_with_interval(exp.DateAdd),
        "DATE_FORMAT": lambda args: exp.TimeToStr(
            this=exp.TsOrDsToTimestamp(this=seq_get(args, 0)),
            format=Dialect["mysql"].format_time(seq_get(args, 1)),
        ),
        "DATE_SUB": build_date_delta_with_interval(exp.DateSub),
        "DAY": lambda args: exp.Day(this=exp.TsOrDsToDate(this=seq_get(args, 0))),
        "DAYOFMONTH": lambda args: exp.DayOfMonth(this=exp.TsOrDsToDate(this=seq_get(args, 0))),
        "DAYOFWEEK": lambda args: exp.DayOfWeek(this=exp.TsOrDsToDate(this=seq_get(args, 0))),
        "DAYOFYEAR": lambda args: exp.DayOfYear(this=exp.TsOrDsToDate(this=seq_get(args, 0))),
        "FORMAT": exp.NumberToStr.from_arg_list,
        "FROM_UNIXTIME": build_formatted_time(exp.UnixToTime, "mysql"),
        "ISNULL": isnull_to_is_null,
        "LENGTH": lambda args: exp.Length(this=seq_get(args, 0), binary=True),
        "MAKETIME": exp.TimeFromParts.from_arg_list,
        "MONTH": lambda args: exp.Month(this=exp.TsOrDsToDate(this=seq_get(args, 0))),
        "MONTHNAME": lambda args: exp.TimeToStr(
            this=exp.TsOrDsToDate(this=seq_get(args, 0)),
            format=exp.Literal.string("%B"),
        ),
        "SCHEMA": exp.CurrentSchema.from_arg_list,
        "DATABASE": exp.CurrentSchema.from_arg_list,
        "STR_TO_DATE": _str_to_date,
        "TIMESTAMPDIFF": build_date_delta(exp.TimestampDiff),
        "TO_DAYS": lambda args: exp.paren(
            exp.DateDiff(
                this=exp.TsOrDsToDate(this=seq_get(args, 0)),
                expression=exp.TsOrDsToDate(this=exp.Literal.string("0000-01-01")),
                unit=exp.var("DAY"),
            )
            + 1
        ),
        "VERSION": exp.CurrentVersion.from_arg_list,
        "WEEK": lambda args: exp.Week(
            this=exp.TsOrDsToDate(this=seq_get(args, 0)), mode=seq_get(args, 1)
        ),
        "WEEKOFYEAR": lambda args: exp.WeekOfYear(this=exp.TsOrDsToDate(this=seq_get(args, 0))),
        "YEAR": lambda args: exp.Year(this=exp.TsOrDsToDate(this=seq_get(args, 0))),
    }

    FUNCTION_PARSERS = {
        **parser.Parser.FUNCTION_PARSERS,
        "GROUP_CONCAT": lambda self: self._parse_group_concat(),
        # https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
        "VALUES": lambda self: self.expression(
            exp.Anonymous(this="VALUES", expressions=[self._parse_id_var()])
        ),
        "JSON_VALUE": lambda self: self._parse_json_value(),
        "SUBSTR": lambda self: self._parse_substring(),
    }

    STATEMENT_PARSERS = {
        **parser.Parser.STATEMENT_PARSERS,
        TokenType.SHOW: lambda self: self._parse_show(),
    }

    SHOW_PARSERS = {
        "BINARY LOGS": _show_parser("BINARY LOGS"),
        "MASTER LOGS": _show_parser("BINARY LOGS"),
        "BINLOG EVENTS": _show_parser("BINLOG EVENTS"),
        "CHARACTER SET": _show_parser("CHARACTER SET"),
        "CHARSET": _show_parser("CHARACTER SET"),
        "COLLATION": _show_parser("COLLATION"),
        "FULL COLUMNS": _show_parser("COLUMNS", target="FROM", full=True),
        "COLUMNS": _show_parser("COLUMNS", target="FROM"),
        "CREATE DATABASE": _show_parser("CREATE DATABASE", target=True),
        "CREATE EVENT": _show_parser("CREATE EVENT", target=True),
        "CREATE FUNCTION": _show_parser("CREATE FUNCTION", target=True),
        "CREATE PROCEDURE": _show_parser("CREATE PROCEDURE", target=True),
        "CREATE TABLE": _show_parser("CREATE TABLE", target=True),
        "CREATE TRIGGER": _show_parser("CREATE TRIGGER", target=True),
        "CREATE VIEW": _show_parser("CREATE VIEW", target=True),
        "DATABASES": _show_parser("DATABASES"),
        "SCHEMAS": _show_parser("DATABASES"),
        "ENGINE": _show_parser("ENGINE", target=True),
        "STORAGE ENGINES": _show_parser("ENGINES"),
        "ENGINES": _show_parser("ENGINES"),
        "ERRORS": _show_parser("ERRORS"),
        "EVENTS": _show_parser("EVENTS"),
        "FUNCTION CODE": _show_parser("FUNCTION CODE", target=True),
        "FUNCTION STATUS": _show_parser("FUNCTION STATUS"),
        "GRANTS": _show_parser("GRANTS", target="FOR"),
        "INDEX": _show_parser("INDEX", target="FROM"),
        "MASTER STATUS": _show_parser("MASTER STATUS"),
        "OPEN TABLES": _show_parser("OPEN TABLES"),
        "PLUGINS": _show_parser("PLUGINS"),
        "PROCEDURE CODE": _show_parser("PROCEDURE CODE", target=True),
        "PROCEDURE STATUS": _show_parser("PROCEDURE STATUS"),
        "PRIVILEGES": _show_parser("PRIVILEGES"),
        "FULL PROCESSLIST": _show_parser("PROCESSLIST", full=True),
        "PROCESSLIST": _show_parser("PROCESSLIST"),
        "PROFILE": _show_parser("PROFILE"),
        "PROFILES": _show_parser("PROFILES"),
        "RELAYLOG EVENTS": _show_parser("RELAYLOG EVENTS"),
        "REPLICAS": _show_parser("REPLICAS"),
        "SLAVE HOSTS": _show_parser("REPLICAS"),
        "REPLICA STATUS": _show_parser("REPLICA STATUS"),
        "SLAVE STATUS": _show_parser("REPLICA STATUS"),
        "GLOBAL STATUS": _show_parser("STATUS", global_=True),
        "SESSION STATUS": _show_parser("STATUS"),
        "STATUS": _show_parser("STATUS"),
        "TABLE STATUS": _show_parser("TABLE STATUS"),
        "FULL TABLES": _show_parser("TABLES", full=True),
        "TABLES": _show_parser("TABLES"),
        "TRIGGERS": _show_parser("TRIGGERS"),
        "GLOBAL VARIABLES": _show_parser("VARIABLES", global_=True),
        "SESSION VARIABLES": _show_parser("VARIABLES"),
        "VARIABLES": _show_parser("VARIABLES"),
        "WARNINGS": _show_parser("WARNINGS"),
    }

    PROPERTY_PARSERS = {
        **parser.Parser.PROPERTY_PARSERS,
        "LOCK": lambda self: self._parse_property_assignment(exp.LockProperty),
        "PARTITION BY": lambda self: self._parse_partition_property(),
    }

    SET_PARSERS = {
        **parser.Parser.SET_PARSERS,
        "PERSIST": lambda self: self._parse_set_item_assignment("PERSIST"),
        "PERSIST_ONLY": lambda self: self._parse_set_item_assignment("PERSIST_ONLY"),
        "CHARACTER SET": lambda self: self._parse_set_item_charset("CHARACTER SET"),
        "CHARSET": lambda self: self._parse_set_item_charset("CHARACTER SET"),
        "NAMES": lambda self: self._parse_set_item_names(),
    }

    SHOW_TRIE = new_trie(key.split(" ") for key in SHOW_PARSERS)
    SET_TRIE = new_trie(key.split(" ") for key in SET_PARSERS)

    CONSTRAINT_PARSERS = {
        **parser.Parser.CONSTRAINT_PARSERS,
        "FULLTEXT": lambda self: self._parse_index_constraint(kind="FULLTEXT"),
        "INDEX": lambda self: self._parse_index_constraint(),
        "KEY": lambda self: self._parse_index_constraint(),
        "SPATIAL": lambda self: self._parse_index_constraint(kind="SPATIAL"),
        "ZEROFILL": lambda self: self.expression(exp.ZeroFillColumnConstraint()),
    }

    ALTER_PARSERS = {
        **parser.Parser.ALTER_PARSERS,
        "MODIFY": lambda self: self._parse_alter_table_alter(),
    }

    ALTER_ALTER_PARSERS = {
        **parser.Parser.ALTER_ALTER_PARSERS,
        "INDEX": lambda self: self._parse_alter_table_alter_index(),
    }

    SCHEMA_UNNAMED_CONSTRAINTS = {
        *parser.Parser.SCHEMA_UNNAMED_CONSTRAINTS,
        "FULLTEXT",
        "INDEX",
        "KEY",
        "SPATIAL",
    }

    PROFILE_TYPES: parser.OPTIONS_TYPE = {
        **dict.fromkeys(("ALL", "CPU", "IPC", "MEMORY", "SOURCE", "SWAPS"), tuple()),
        "BLOCK": ("IO",),
        "CONTEXT": ("SWITCHES",),
        "PAGE": ("FAULTS",),
    }

    TYPE_TOKENS = {
        *parser.Parser.TYPE_TOKENS,
        TokenType.SET,
    }

    ENUM_TYPE_TOKENS = {
        *parser.Parser.ENUM_TYPE_TOKENS,
        TokenType.SET,
    }

    # SELECT [ ALL | DISTINCT | DISTINCTROW ] [ <OPERATION_MODIFIERS> ]
    OPERATION_MODIFIERS = {
        "HIGH_PRIORITY",
        "STRAIGHT_JOIN",
        "SQL_SMALL_RESULT",
        "SQL_BIG_RESULT",
        "SQL_BUFFER_RESULT",
        "SQL_NO_CACHE",
        "SQL_CALC_FOUND_ROWS",
    }

    LOG_DEFAULTS_TO_LN = True
    STRING_ALIASES = True
    VALUES_FOLLOWED_BY_PAREN = False
    SUPPORTS_PARTITION_SELECTION = True

    def _parse_generated_as_identity(
        self,
    ) -> (
        exp.GeneratedAsIdentityColumnConstraint
        | exp.ComputedColumnConstraint
        | exp.GeneratedAsRowColumnConstraint
    ):
        this = super()._parse_generated_as_identity()

        if self._match_texts(("STORED", "VIRTUAL")):
            persisted = self._prev.text.upper() == "STORED"

            if isinstance(this, exp.ComputedColumnConstraint):
                this.set("persisted", persisted)
            elif isinstance(this, exp.GeneratedAsIdentityColumnConstraint):
                this = self.expression(
                    exp.ComputedColumnConstraint(this=this.expression, persisted=persisted)
                )

        return this

    def _parse_primary_key_part(self) -> t.Optional[exp.Expr]:
        this = self._parse_id_var()
        if not self._match(TokenType.L_PAREN):
            return this

        expression = self._parse_number()
        self._match_r_paren()
        return self.expression(exp.ColumnPrefix(this=this, expression=expression))

    def _parse_index_constraint(self, kind: t.Optional[str] = None) -> exp.IndexColumnConstraint:
        if kind:
            self._match_texts(("INDEX", "KEY"))

        this = self._parse_id_var(any_token=False)
        index_type = self._match(TokenType.USING) and self._advance_any() and self._prev.text
        expressions = self._parse_wrapped_csv(self._parse_ordered)

        options = []
        while True:
            if self._match_text_seq("KEY_BLOCK_SIZE"):
                self._match(TokenType.EQ)
                opt = exp.IndexConstraintOption(key_block_size=self._parse_number())
            elif self._match(TokenType.USING):
                opt = exp.IndexConstraintOption(using=self._advance_any() and self._prev.text)
            elif self._match_text_seq("WITH", "PARSER"):
                opt = exp.IndexConstraintOption(parser=self._parse_var(any_token=True))
            elif self._match(TokenType.COMMENT):
                opt = exp.IndexConstraintOption(comment=self._parse_string())
            elif self._match_text_seq("VISIBLE"):
                opt = exp.IndexConstraintOption(visible=True)
            elif self._match_text_seq("INVISIBLE"):
                opt = exp.IndexConstraintOption(visible=False)
            elif self._match_text_seq("ENGINE_ATTRIBUTE"):
                self._match(TokenType.EQ)
                opt = exp.IndexConstraintOption(engine_attr=self._parse_string())
            elif self._match_text_seq("SECONDARY_ENGINE_ATTRIBUTE"):
                self._match(TokenType.EQ)
                opt = exp.IndexConstraintOption(secondary_engine_attr=self._parse_string())
            else:
                opt = None

            if not opt:
                break

            options.append(opt)

        return self.expression(
            exp.IndexColumnConstraint(
                this=this,
                expressions=expressions,
                kind=kind,
                index_type=index_type,
                options=options,
            )
        )

    def _parse_show_mysql(
        self,
        this: str,
        target: bool | str = False,
        full: t.Optional[bool] = None,
        global_: t.Optional[bool] = None,
    ) -> exp.Show:
        json = self._match_text_seq("JSON")

        if target:
            if isinstance(target, str):
                self._match_text_seq(*target.split(" "))
            target_id = self._parse_id_var()
        else:
            target_id = None

        index = self._index
        if self._match_text_seq("IN"):
            log = self._parse_string()
            if log is None:
                self._retreat(index)
        else:
            log = None

        if this in ("BINLOG EVENTS", "RELAYLOG EVENTS"):
            position = self._parse_number() if self._match_text_seq("FROM") else None
            db = None
        else:
            position = None
            db = None

            if self._match(TokenType.FROM) or self._match_text_seq("IN"):
                db = self._parse_id_var()
            elif self._match(TokenType.DOT):
                db = target_id
                target_id = self._parse_id_var()

        channel = self._parse_id_var() if self._match_text_seq("FOR", "CHANNEL") else None

        like = self._parse_string() if self._match_text_seq("LIKE") else None
        where = self._parse_where()

        if this == "PROFILE":
            types = self._parse_csv(lambda: self._parse_var_from_options(self.PROFILE_TYPES))
            query = self._parse_number() if self._match_text_seq("FOR", "QUERY") else None
            offset = self._parse_number() if self._match_text_seq("OFFSET") else None
            limit = self._parse_number() if self._match_text_seq("LIMIT") else None
        else:
            types, query = None, None
            offset, limit = self._parse_oldstyle_limit()

        mutex = True if self._match_text_seq("MUTEX") else None
        mutex = False if self._match_text_seq("STATUS") else mutex

        for_table = self._parse_id_var() if self._match_text_seq("FOR", "TABLE") else None
        for_group = self._parse_string() if self._match_text_seq("FOR", "GROUP") else None
        for_user = self._parse_string() if self._match_text_seq("FOR", "USER") else None
        for_role = self._parse_string() if self._match_text_seq("FOR", "ROLE") else None
        into_outfile = self._parse_string() if self._match_text_seq("INTO", "OUTFILE") else None

        return self.expression(
            exp.Show(
                this=this,
                target=target_id,
                full=full,
                log=log,
                position=position,
                db=db,
                channel=channel,
                like=like,
                where=where,
                types=types,
                query=query,
                offset=offset,
                limit=limit,
                mutex=mutex,
                for_table=for_table,
                for_group=for_group,
                for_user=for_user,
                for_role=for_role,
                into_outfile=into_outfile,
                json=json,
                global_=global_,
            )
        )

    def _parse_oldstyle_limit(
        self,
    ) -> t.Tuple[t.Optional[exp.Expr], t.Optional[exp.Expr]]:
        limit = None
        offset = None
        if self._match_text_seq("LIMIT"):
            parts = self._parse_csv(self._parse_number)
            if len(parts) == 1:
                limit = parts[0]
            elif len(parts) == 2:
                limit = parts[1]
                offset = parts[0]

        return offset, limit

    def _parse_set_item_charset(self, kind: str) -> exp.Expr:
        this = self._parse_string() or self._parse_unquoted_field()
        return self.expression(exp.SetItem(this=this, kind=kind))

    def _parse_set_item_names(self) -> exp.Expr:
        charset = self._parse_string() or self._parse_unquoted_field()
        if self._match_text_seq("COLLATE"):
            collate = self._parse_string() or self._parse_unquoted_field()
        else:
            collate = None

        return self.expression(exp.SetItem(this=charset, collate=collate, kind="NAMES"))

    def _parse_type(
        self, parse_interval: bool = True, fallback_to_identifier: bool = False
    ) -> t.Optional[exp.Expr]:
        # mysql binary is special and can work anywhere, even in order by operations
        # it operates like a no paren func
        if self._match(TokenType.BINARY, advance=False):
            data_type = self._parse_types(check_func=True, allow_identifiers=False)

            if isinstance(data_type, exp.DataType):
                return self.expression(exp.Cast(this=self._parse_column(), to=data_type))

        return super()._parse_type(
            parse_interval=parse_interval, fallback_to_identifier=fallback_to_identifier
        )

    def _parse_alter_table_alter_index(self) -> exp.AlterIndex:
        index = self._parse_field(any_token=True)

        if self._match_text_seq("VISIBLE"):
            visible = True
        elif self._match_text_seq("INVISIBLE"):
            visible = False
        else:
            visible = None

        return self.expression(exp.AlterIndex(this=index, visible=visible))

    def _parse_partition_property(
        self,
    ) -> t.Optional[exp.Expr] | t.List[exp.Expr]:
        partition_cls: t.Optional[t.Type[exp.Expr]] = None
        value_parser = None

        if self._match_text_seq("RANGE"):
            partition_cls = exp.PartitionByRangeProperty
            value_parser = self._parse_partition_range_value
        elif self._match_text_seq("LIST"):
            partition_cls = exp.PartitionByListProperty
            value_parser = self._parse_partition_list_value

        if not partition_cls or not value_parser:
            return None

        partition_expressions = self._parse_wrapped_csv(self._parse_assignment)

        # For Doris and Starrocks
        if not self._match_text_seq("(", "PARTITION", advance=False):
            return partition_expressions

        create_expressions = self._parse_wrapped_csv(value_parser)

        return self.expression(
            partition_cls(
                partition_expressions=partition_expressions, create_expressions=create_expressions
            )
        )

    def _parse_partition_range_value(self) -> t.Optional[exp.Expr]:
        self._match_text_seq("PARTITION")
        name = self._parse_id_var()

        if not self._match_text_seq("VALUES", "LESS", "THAN"):
            return name

        values = self._parse_wrapped_csv(self._parse_expression)

        if (
            len(values) == 1
            and isinstance(values[0], exp.Column)
            and values[0].name.upper() == "MAXVALUE"
        ):
            values = [exp.var("MAXVALUE")]

        part_range = self.expression(exp.PartitionRange(this=name, expressions=values))
        return self.expression(exp.Partition(expressions=[part_range]))

    def _parse_partition_list_value(self) -> exp.Partition:
        self._match_text_seq("PARTITION")
        name = self._parse_id_var()
        self._match_text_seq("VALUES", "IN")
        values = self._parse_wrapped_csv(self._parse_expression)
        part_list = self.expression(exp.PartitionList(this=name, expressions=values))
        return self.expression(exp.Partition(expressions=[part_list]))

    def _parse_primary_key(
        self,
        wrapped_optional: bool = False,
        in_props: bool = False,
        named_primary_key: bool = False,
    ) -> exp.PrimaryKeyColumnConstraint | exp.PrimaryKey:
        return super()._parse_primary_key(
            wrapped_optional=wrapped_optional, in_props=in_props, named_primary_key=True
        )
