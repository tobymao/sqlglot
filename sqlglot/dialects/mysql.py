from __future__ import annotations

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    arrow_json_extract_scalar_sql,
    datestrtodate_sql,
    format_time_lambda,
    locate_to_strposition,
    max_or_greatest,
    min_or_least,
    no_ilike_sql,
    no_paren_current_date_sql,
    no_pivot_sql,
    no_tablesample_sql,
    no_trycast_sql,
    parse_date_delta_with_interval,
    rename_func,
    strposition_to_locate_sql,
)
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType


def _show_parser(*args, **kwargs):
    def _parse(self):
        return self._parse_show_mysql(*args, **kwargs)

    return _parse


def _date_trunc_sql(self, expression):
    expr = self.sql(expression, "this")
    unit = expression.text("unit")

    if unit == "day":
        return f"DATE({expr})"

    if unit == "week":
        concat = f"CONCAT(YEAR({expr}), ' ', WEEK({expr}, 1), ' 1')"
        date_format = "%Y %u %w"
    elif unit == "month":
        concat = f"CONCAT(YEAR({expr}), ' ', MONTH({expr}), ' 1')"
        date_format = "%Y %c %e"
    elif unit == "quarter":
        concat = f"CONCAT(YEAR({expr}), ' ', QUARTER({expr}) * 3 - 2, ' 1')"
        date_format = "%Y %c %e"
    elif unit == "year":
        concat = f"CONCAT(YEAR({expr}), ' 1 1')"
        date_format = "%Y %c %e"
    else:
        self.unsupported(f"Unexpected interval unit: {unit}")
        return f"DATE({expr})"

    return f"STR_TO_DATE({concat}, '{date_format}')"


def _str_to_date(args):
    date_format = MySQL.format_time(seq_get(args, 1))
    return exp.StrToDate(this=seq_get(args, 0), format=date_format)


def _str_to_date_sql(self, expression):
    date_format = self.format_time(expression)
    return f"STR_TO_DATE({self.sql(expression.this)}, {date_format})"


def _trim_sql(self, expression):
    target = self.sql(expression, "this")
    trim_type = self.sql(expression, "position")
    remove_chars = self.sql(expression, "expression")

    # Use TRIM/LTRIM/RTRIM syntax if the expression isn't mysql-specific
    if not remove_chars:
        return self.trim_sql(expression)

    trim_type = f"{trim_type} " if trim_type else ""
    remove_chars = f"{remove_chars} " if remove_chars else ""
    from_part = "FROM " if trim_type or remove_chars else ""
    return f"TRIM({trim_type}{remove_chars}{from_part}{target})"


def _date_add_sql(kind):
    def func(self, expression):
        this = self.sql(expression, "this")
        unit = expression.text("unit").upper() or "DAY"
        return (
            f"DATE_{kind}({this}, {self.sql(exp.Interval(this=expression.expression, unit=unit))})"
        )

    return func


class MySQL(Dialect):
    time_format = "'%Y-%m-%d %T'"

    # https://prestodb.io/docs/current/functions/datetime.html#mysql-date-functions
    time_mapping = {
        "%M": "%B",
        "%c": "%-m",
        "%e": "%-d",
        "%h": "%I",
        "%i": "%M",
        "%s": "%S",
        "%S": "%S",
        "%u": "%W",
        "%k": "%-H",
        "%l": "%-I",
        "%T": "%H:%M:%S",
        "%W": "%a",
    }

    class Tokenizer(tokens.Tokenizer):
        QUOTES = ["'", '"']
        COMMENTS = ["--", "#", ("/*", "*/")]
        IDENTIFIERS = ["`"]
        STRING_ESCAPES = ["'", "\\"]
        BIT_STRINGS = [("b'", "'"), ("B'", "'"), ("0b", "")]
        HEX_STRINGS = [("x'", "'"), ("X'", "'"), ("0x", "")]

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "CHARSET": TokenType.CHARACTER_SET,
            "LONGBLOB": TokenType.LONGBLOB,
            "LONGTEXT": TokenType.LONGTEXT,
            "MEDIUMBLOB": TokenType.MEDIUMBLOB,
            "MEDIUMTEXT": TokenType.MEDIUMTEXT,
            "SEPARATOR": TokenType.SEPARATOR,
            "START": TokenType.BEGIN,
            "_ARMSCII8": TokenType.INTRODUCER,
            "_ASCII": TokenType.INTRODUCER,
            "_BIG5": TokenType.INTRODUCER,
            "_BINARY": TokenType.INTRODUCER,
            "_CP1250": TokenType.INTRODUCER,
            "_CP1251": TokenType.INTRODUCER,
            "_CP1256": TokenType.INTRODUCER,
            "_CP1257": TokenType.INTRODUCER,
            "_CP850": TokenType.INTRODUCER,
            "_CP852": TokenType.INTRODUCER,
            "_CP866": TokenType.INTRODUCER,
            "_CP932": TokenType.INTRODUCER,
            "_DEC8": TokenType.INTRODUCER,
            "_EUCJPMS": TokenType.INTRODUCER,
            "_EUCKR": TokenType.INTRODUCER,
            "_GB18030": TokenType.INTRODUCER,
            "_GB2312": TokenType.INTRODUCER,
            "_GBK": TokenType.INTRODUCER,
            "_GEOSTD8": TokenType.INTRODUCER,
            "_GREEK": TokenType.INTRODUCER,
            "_HEBREW": TokenType.INTRODUCER,
            "_HP8": TokenType.INTRODUCER,
            "_KEYBCS2": TokenType.INTRODUCER,
            "_KOI8R": TokenType.INTRODUCER,
            "_KOI8U": TokenType.INTRODUCER,
            "_LATIN1": TokenType.INTRODUCER,
            "_LATIN2": TokenType.INTRODUCER,
            "_LATIN5": TokenType.INTRODUCER,
            "_LATIN7": TokenType.INTRODUCER,
            "_MACCE": TokenType.INTRODUCER,
            "_MACROMAN": TokenType.INTRODUCER,
            "_SJIS": TokenType.INTRODUCER,
            "_SWE7": TokenType.INTRODUCER,
            "_TIS620": TokenType.INTRODUCER,
            "_UCS2": TokenType.INTRODUCER,
            "_UJIS": TokenType.INTRODUCER,
            # https://dev.mysql.com/doc/refman/8.0/en/string-literals.html
            "_UTF8": TokenType.INTRODUCER,
            "_UTF16": TokenType.INTRODUCER,
            "_UTF16LE": TokenType.INTRODUCER,
            "_UTF32": TokenType.INTRODUCER,
            "_UTF8MB3": TokenType.INTRODUCER,
            "_UTF8MB4": TokenType.INTRODUCER,
            "@@": TokenType.SESSION_PARAMETER,
        }

        COMMANDS = tokens.Tokenizer.COMMANDS - {TokenType.SHOW}

    class Parser(parser.Parser):
        FUNC_TOKENS = {*parser.Parser.FUNC_TOKENS, TokenType.SCHEMA, TokenType.DATABASE}  # type: ignore

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,  # type: ignore
            "DATE_ADD": parse_date_delta_with_interval(exp.DateAdd),
            "DATE_FORMAT": format_time_lambda(exp.TimeToStr, "mysql"),
            "DATE_SUB": parse_date_delta_with_interval(exp.DateSub),
            "INSTR": lambda args: exp.StrPosition(substr=seq_get(args, 1), this=seq_get(args, 0)),
            "LEFT": lambda args: exp.Substring(
                this=seq_get(args, 0), start=exp.Literal.number(1), length=seq_get(args, 1)
            ),
            "LOCATE": locate_to_strposition,
            "STR_TO_DATE": _str_to_date,
        }

        FUNCTION_PARSERS = {
            **parser.Parser.FUNCTION_PARSERS,  # type: ignore
            "GROUP_CONCAT": lambda self: self.expression(
                exp.GroupConcat,
                this=self._parse_lambda(),
                separator=self._match(TokenType.SEPARATOR) and self._parse_field(),
            ),
        }

        STATEMENT_PARSERS = {
            **parser.Parser.STATEMENT_PARSERS,  # type: ignore
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

        SET_PARSERS = {
            **parser.Parser.SET_PARSERS,
            "PERSIST": lambda self: self._parse_set_item_assignment("PERSIST"),
            "PERSIST_ONLY": lambda self: self._parse_set_item_assignment("PERSIST_ONLY"),
            "CHARACTER SET": lambda self: self._parse_set_item_charset("CHARACTER SET"),
            "CHARSET": lambda self: self._parse_set_item_charset("CHARACTER SET"),
            "NAMES": lambda self: self._parse_set_item_names(),
        }

        PROFILE_TYPES = {
            "ALL",
            "BLOCK IO",
            "CONTEXT SWITCHES",
            "CPU",
            "IPC",
            "MEMORY",
            "PAGE FAULTS",
            "SOURCE",
            "SWAPS",
        }

        LOG_DEFAULTS_TO_LN = True

        def _parse_show_mysql(self, this, target=False, full=None, global_=None):
            if target:
                if isinstance(target, str):
                    self._match_text_seq(target)
                target_id = self._parse_id_var()
            else:
                target_id = None

            log = self._parse_string() if self._match_text_seq("IN") else None

            if this in {"BINLOG EVENTS", "RELAYLOG EVENTS"}:
                position = self._parse_number() if self._match_text_seq("FROM") else None
                db = None
            else:
                position = None
                db = None

                if self._match(TokenType.FROM):
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

            return self.expression(
                exp.Show,
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
                **{"global": global_},
            )

        def _parse_oldstyle_limit(self):
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

        def _parse_set_item_charset(self, kind):
            this = self._parse_string() or self._parse_id_var()

            return self.expression(
                exp.SetItem,
                this=this,
                kind=kind,
            )

        def _parse_set_item_names(self):
            charset = self._parse_string() or self._parse_id_var()
            if self._match_text_seq("COLLATE"):
                collate = self._parse_string() or self._parse_id_var()
            else:
                collate = None
            return self.expression(
                exp.SetItem,
                this=charset,
                collate=collate,
                kind="NAMES",
            )

    class Generator(generator.Generator):
        LOCKING_READS_SUPPORTED = True
        NULL_ORDERING_SUPPORTED = False
        JOIN_HINTS = False
        TABLE_HINTS = False

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,  # type: ignore
            exp.CurrentDate: no_paren_current_date_sql,
            exp.DateDiff: lambda self, e: self.func("DATEDIFF", e.this, e.expression),
            exp.DateAdd: _date_add_sql("ADD"),
            exp.DateStrToDate: datestrtodate_sql,
            exp.DateSub: _date_add_sql("SUB"),
            exp.DateTrunc: _date_trunc_sql,
            exp.DayOfMonth: rename_func("DAYOFMONTH"),
            exp.DayOfWeek: rename_func("DAYOFWEEK"),
            exp.DayOfYear: rename_func("DAYOFYEAR"),
            exp.GroupConcat: lambda self, e: f"""GROUP_CONCAT({self.sql(e, "this")} SEPARATOR {self.sql(e, "separator") or "','"})""",
            exp.ILike: no_ilike_sql,
            exp.JSONExtractScalar: arrow_json_extract_scalar_sql,
            exp.Max: max_or_greatest,
            exp.Min: min_or_least,
            exp.NullSafeEQ: lambda self, e: self.binary(e, "<=>"),
            exp.NullSafeNEQ: lambda self, e: self.not_sql(self.binary(e, "<=>")),
            exp.Pivot: no_pivot_sql,
            exp.Select: transforms.preprocess([transforms.eliminate_distinct_on]),
            exp.StrPosition: strposition_to_locate_sql,
            exp.StrToDate: _str_to_date_sql,
            exp.StrToTime: _str_to_date_sql,
            exp.TableSample: no_tablesample_sql,
            exp.TimeStrToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.TimeToStr: lambda self, e: self.func("DATE_FORMAT", e.this, self.format_time(e)),
            exp.Trim: _trim_sql,
            exp.TryCast: no_trycast_sql,
            exp.WeekOfYear: rename_func("WEEKOFYEAR"),
        }

        TYPE_MAPPING = generator.Generator.TYPE_MAPPING.copy()
        TYPE_MAPPING.pop(exp.DataType.Type.MEDIUMTEXT)
        TYPE_MAPPING.pop(exp.DataType.Type.LONGTEXT)
        TYPE_MAPPING.pop(exp.DataType.Type.MEDIUMBLOB)
        TYPE_MAPPING.pop(exp.DataType.Type.LONGBLOB)

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,  # type: ignore
            exp.TransientProperty: exp.Properties.Location.UNSUPPORTED,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        }

        LIMIT_FETCH = "LIMIT"

        def show_sql(self, expression: exp.Show) -> str:
            this = f" {expression.name}"
            full = " FULL" if expression.args.get("full") else ""
            global_ = " GLOBAL" if expression.args.get("global") else ""

            target = self.sql(expression, "target")
            target = f" {target}" if target else ""
            if expression.name in {"COLUMNS", "INDEX"}:
                target = f" FROM{target}"
            elif expression.name == "GRANTS":
                target = f" FOR{target}"

            db = self._prefixed_sql("FROM", expression, "db")

            like = self._prefixed_sql("LIKE", expression, "like")
            where = self.sql(expression, "where")

            types = self.expressions(expression, key="types")
            types = f" {types}" if types else types
            query = self._prefixed_sql("FOR QUERY", expression, "query")

            if expression.name == "PROFILE":
                offset = self._prefixed_sql("OFFSET", expression, "offset")
                limit = self._prefixed_sql("LIMIT", expression, "limit")
            else:
                offset = ""
                limit = self._oldstyle_limit_sql(expression)

            log = self._prefixed_sql("IN", expression, "log")
            position = self._prefixed_sql("FROM", expression, "position")

            channel = self._prefixed_sql("FOR CHANNEL", expression, "channel")

            if expression.name == "ENGINE":
                mutex_or_status = " MUTEX" if expression.args.get("mutex") else " STATUS"
            else:
                mutex_or_status = ""

            return f"SHOW{full}{global_}{this}{target}{types}{db}{query}{log}{position}{channel}{mutex_or_status}{like}{where}{offset}{limit}"

        def _prefixed_sql(self, prefix: str, expression: exp.Expression, arg: str) -> str:
            sql = self.sql(expression, arg)
            if not sql:
                return ""
            return f" {prefix} {sql}"

        def _oldstyle_limit_sql(self, expression: exp.Show) -> str:
            limit = self.sql(expression, "limit")
            offset = self.sql(expression, "offset")
            if limit:
                limit_offset = f"{offset}, {limit}" if offset else limit
                return f" LIMIT {limit_offset}"
            return ""
