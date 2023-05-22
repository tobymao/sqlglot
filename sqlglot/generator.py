from __future__ import annotations

import logging
import typing as t

from sqlglot import exp
from sqlglot.errors import ErrorLevel, UnsupportedError, concat_messages
from sqlglot.helper import apply_index_offset, csv, seq_get, should_identify
from sqlglot.time import format_time
from sqlglot.tokens import TokenType

logger = logging.getLogger("sqlglot")


class Generator:
    """
    Generator interprets the given syntax tree and produces a SQL string as an output.

    Args:
        time_mapping (dict): the dictionary of custom time mappings in which the key
            represents a python time format and the output the target time format
        time_trie (trie): a trie of the time_mapping keys
        pretty (bool): if set to True the returned string will be formatted. Default: False.
        quote_start (str): specifies which starting character to use to delimit quotes. Default: '.
        quote_end (str): specifies which ending character to use to delimit quotes. Default: '.
        identifier_start (str): specifies which starting character to use to delimit identifiers. Default: ".
        identifier_end (str): specifies which ending character to use to delimit identifiers. Default: ".
        bit_start (str): specifies which starting character to use to delimit bit literals. Default: None.
        bit_end (str): specifies which ending character to use to delimit bit literals. Default: None.
        hex_start (str): specifies which starting character to use to delimit hex literals. Default: None.
        hex_end (str): specifies which ending character to use to delimit hex literals. Default: None.
        byte_start (str): specifies which starting character to use to delimit byte literals. Default: None.
        byte_end (str): specifies which ending character to use to delimit byte literals. Default: None.
        identify (bool | str): 'always': always quote, 'safe': quote identifiers if they don't contain an upcase, True defaults to always.
        normalize (bool): if set to True all identifiers will lower cased
        string_escape (str): specifies a string escape character. Default: '.
        identifier_escape (str): specifies an identifier escape character. Default: ".
        pad (int): determines padding in a formatted string. Default: 2.
        indent (int): determines the size of indentation in a formatted string. Default: 4.
        unnest_column_only (bool): if true unnest table aliases are considered only as column aliases
        normalize_functions (str): normalize function names, "upper", "lower", or None
            Default: "upper"
        alias_post_tablesample (bool): if the table alias comes after tablesample
            Default: False
        unsupported_level (ErrorLevel): determines the generator's behavior when it encounters
            unsupported expressions. Default ErrorLevel.WARN.
        null_ordering (str): Indicates the default null ordering method to use if not explicitly set.
            Options are "nulls_are_small", "nulls_are_large", "nulls_are_last".
            Default: "nulls_are_small"
        max_unsupported (int): Maximum number of unsupported messages to include in a raised UnsupportedError.
            This is only relevant if unsupported_level is ErrorLevel.RAISE.
            Default: 3
        leading_comma (bool): if the the comma is leading or trailing in select statements
            Default: False
        max_text_width: The max number of characters in a segment before creating new lines in pretty mode.
            The default is on the smaller end because the length only represents a segment and not the true
            line length.
            Default: 80
        comments: Whether or not to preserve comments in the output SQL code.
            Default: True
    """

    TRANSFORMS = {
        exp.DateAdd: lambda self, e: self.func(
            "DATE_ADD", e.this, e.expression, exp.Literal.string(e.text("unit"))
        ),
        exp.TsOrDsAdd: lambda self, e: self.func(
            "TS_OR_DS_ADD", e.this, e.expression, exp.Literal.string(e.text("unit"))
        ),
        exp.VarMap: lambda self, e: self.func("MAP", e.args["keys"], e.args["values"]),
        exp.CharacterSetProperty: lambda self, e: f"{'DEFAULT ' if e.args.get('default') else ''}CHARACTER SET={self.sql(e, 'this')}",
        exp.ExecuteAsProperty: lambda self, e: self.naked_property(e),
        exp.ExternalProperty: lambda self, e: "EXTERNAL",
        exp.LanguageProperty: lambda self, e: self.naked_property(e),
        exp.LocationProperty: lambda self, e: self.naked_property(e),
        exp.LogProperty: lambda self, e: f"{'NO ' if e.args.get('no') else ''}LOG",
        exp.MaterializedProperty: lambda self, e: "MATERIALIZED",
        exp.NoPrimaryIndexProperty: lambda self, e: "NO PRIMARY INDEX",
        exp.OnCommitProperty: lambda self, e: f"ON COMMIT {'DELETE' if e.args.get('delete') else 'PRESERVE'} ROWS",
        exp.ReturnsProperty: lambda self, e: self.naked_property(e),
        exp.SetProperty: lambda self, e: f"{'MULTI' if e.args.get('multi') else ''}SET",
        exp.SettingsProperty: lambda self, e: f"SETTINGS{self.seg('')}{(self.expressions(e))}",
        exp.SqlSecurityProperty: lambda self, e: f"SQL SECURITY {'DEFINER' if e.args.get('definer') else 'INVOKER'}",
        exp.TemporaryProperty: lambda self, e: f"{'GLOBAL ' if e.args.get('global_') else ''}TEMPORARY",
        exp.TransientProperty: lambda self, e: "TRANSIENT",
        exp.StabilityProperty: lambda self, e: e.name,
        exp.VolatileProperty: lambda self, e: "VOLATILE",
        exp.WithJournalTableProperty: lambda self, e: f"WITH JOURNAL TABLE={self.sql(e, 'this')}",
        exp.CaseSpecificColumnConstraint: lambda self, e: f"{'NOT ' if e.args.get('not_') else ''}CASESPECIFIC",
        exp.CharacterSetColumnConstraint: lambda self, e: f"CHARACTER SET {self.sql(e, 'this')}",
        exp.DateFormatColumnConstraint: lambda self, e: f"FORMAT {self.sql(e, 'this')}",
        exp.OnUpdateColumnConstraint: lambda self, e: f"ON UPDATE {self.sql(e, 'this')}",
        exp.UppercaseColumnConstraint: lambda self, e: f"UPPERCASE",
        exp.TitleColumnConstraint: lambda self, e: f"TITLE {self.sql(e, 'this')}",
        exp.PathColumnConstraint: lambda self, e: f"PATH {self.sql(e, 'this')}",
        exp.CheckColumnConstraint: lambda self, e: f"CHECK ({self.sql(e, 'this')})",
        exp.CommentColumnConstraint: lambda self, e: f"COMMENT {self.sql(e, 'this')}",
        exp.CollateColumnConstraint: lambda self, e: f"COLLATE {self.sql(e, 'this')}",
        exp.EncodeColumnConstraint: lambda self, e: f"ENCODE {self.sql(e, 'this')}",
        exp.DefaultColumnConstraint: lambda self, e: f"DEFAULT {self.sql(e, 'this')}",
        exp.InlineLengthColumnConstraint: lambda self, e: f"INLINE LENGTH {self.sql(e, 'this')}",
    }

    # Whether or not null ordering is supported in order by
    NULL_ORDERING_SUPPORTED = True

    # Whether or not locking reads (i.e. SELECT ... FOR UPDATE/SHARE) are supported
    LOCKING_READS_SUPPORTED = False

    # Always do union distinct or union all
    EXPLICIT_UNION = False

    # Wrap derived values in parens, usually standard but spark doesn't support it
    WRAP_DERIVED_VALUES = True

    # Whether or not create function uses an AS before the RETURN
    CREATE_FUNCTION_RETURN_AS = True

    # Whether or not MERGE ... WHEN MATCHED BY SOURCE is allowed
    MATCHED_BY_SOURCE = True

    # Whether or not the INTERVAL expression works only with values like '1 day'
    SINGLE_STRING_INTERVAL = False

    # Whether or not the plural form of date parts like day (i.e. "days") is supported in INTERVALs
    INTERVAL_ALLOWS_PLURAL_FORM = True

    # Whether or not the TABLESAMPLE clause supports a method name, like BERNOULLI
    TABLESAMPLE_WITH_METHOD = True

    # Whether or not to treat the number in TABLESAMPLE (50) as a percentage
    TABLESAMPLE_SIZE_IS_PERCENT = False

    # Whether or not limit and fetch are supported (possible values: "ALL", "LIMIT", "FETCH")
    LIMIT_FETCH = "ALL"

    # Whether a table is allowed to be renamed with a db
    RENAME_TABLE_WITH_DB = True

    # The separator for grouping sets and rollups
    GROUPINGS_SEP = ","

    TYPE_MAPPING = {
        exp.DataType.Type.NCHAR: "CHAR",
        exp.DataType.Type.NVARCHAR: "VARCHAR",
        exp.DataType.Type.MEDIUMTEXT: "TEXT",
        exp.DataType.Type.LONGTEXT: "TEXT",
        exp.DataType.Type.MEDIUMBLOB: "BLOB",
        exp.DataType.Type.LONGBLOB: "BLOB",
        exp.DataType.Type.INET: "INET",
    }

    STAR_MAPPING = {
        "except": "EXCEPT",
        "replace": "REPLACE",
    }

    TIME_PART_SINGULARS = {
        "microseconds": "microsecond",
        "seconds": "second",
        "minutes": "minute",
        "hours": "hour",
        "days": "day",
        "weeks": "week",
        "months": "month",
        "quarters": "quarter",
        "years": "year",
    }

    TOKEN_MAPPING: t.Dict[TokenType, str] = {}

    STRUCT_DELIMITER = ("<", ">")

    PARAMETER_TOKEN = "@"

    PROPERTIES_LOCATION = {
        exp.AfterJournalProperty: exp.Properties.Location.POST_NAME,
        exp.AlgorithmProperty: exp.Properties.Location.POST_CREATE,
        exp.AutoIncrementProperty: exp.Properties.Location.POST_SCHEMA,
        exp.BlockCompressionProperty: exp.Properties.Location.POST_NAME,
        exp.CharacterSetProperty: exp.Properties.Location.POST_SCHEMA,
        exp.ChecksumProperty: exp.Properties.Location.POST_NAME,
        exp.CollateProperty: exp.Properties.Location.POST_SCHEMA,
        exp.Cluster: exp.Properties.Location.POST_SCHEMA,
        exp.DataBlocksizeProperty: exp.Properties.Location.POST_NAME,
        exp.DefinerProperty: exp.Properties.Location.POST_CREATE,
        exp.DistKeyProperty: exp.Properties.Location.POST_SCHEMA,
        exp.DistStyleProperty: exp.Properties.Location.POST_SCHEMA,
        exp.EngineProperty: exp.Properties.Location.POST_SCHEMA,
        exp.ExecuteAsProperty: exp.Properties.Location.POST_SCHEMA,
        exp.ExternalProperty: exp.Properties.Location.POST_CREATE,
        exp.FallbackProperty: exp.Properties.Location.POST_NAME,
        exp.FileFormatProperty: exp.Properties.Location.POST_WITH,
        exp.FreespaceProperty: exp.Properties.Location.POST_NAME,
        exp.IsolatedLoadingProperty: exp.Properties.Location.POST_NAME,
        exp.JournalProperty: exp.Properties.Location.POST_NAME,
        exp.LanguageProperty: exp.Properties.Location.POST_SCHEMA,
        exp.LikeProperty: exp.Properties.Location.POST_SCHEMA,
        exp.LocationProperty: exp.Properties.Location.POST_SCHEMA,
        exp.LockingProperty: exp.Properties.Location.POST_ALIAS,
        exp.LogProperty: exp.Properties.Location.POST_NAME,
        exp.MaterializedProperty: exp.Properties.Location.POST_CREATE,
        exp.MergeBlockRatioProperty: exp.Properties.Location.POST_NAME,
        exp.NoPrimaryIndexProperty: exp.Properties.Location.POST_EXPRESSION,
        exp.OnCommitProperty: exp.Properties.Location.POST_EXPRESSION,
        exp.Order: exp.Properties.Location.POST_SCHEMA,
        exp.PartitionedByProperty: exp.Properties.Location.POST_WITH,
        exp.PrimaryKey: exp.Properties.Location.POST_SCHEMA,
        exp.Property: exp.Properties.Location.POST_WITH,
        exp.ReturnsProperty: exp.Properties.Location.POST_SCHEMA,
        exp.RowFormatProperty: exp.Properties.Location.POST_SCHEMA,
        exp.RowFormatDelimitedProperty: exp.Properties.Location.POST_SCHEMA,
        exp.RowFormatSerdeProperty: exp.Properties.Location.POST_SCHEMA,
        exp.SchemaCommentProperty: exp.Properties.Location.POST_SCHEMA,
        exp.SerdeProperties: exp.Properties.Location.POST_SCHEMA,
        exp.Set: exp.Properties.Location.POST_SCHEMA,
        exp.SettingsProperty: exp.Properties.Location.POST_SCHEMA,
        exp.SetProperty: exp.Properties.Location.POST_CREATE,
        exp.SortKeyProperty: exp.Properties.Location.POST_SCHEMA,
        exp.SqlSecurityProperty: exp.Properties.Location.POST_CREATE,
        exp.StabilityProperty: exp.Properties.Location.POST_SCHEMA,
        exp.TemporaryProperty: exp.Properties.Location.POST_CREATE,
        exp.TransientProperty: exp.Properties.Location.POST_CREATE,
        exp.MergeTreeTTL: exp.Properties.Location.POST_SCHEMA,
        exp.VolatileProperty: exp.Properties.Location.POST_CREATE,
        exp.WithDataProperty: exp.Properties.Location.POST_EXPRESSION,
        exp.WithJournalTableProperty: exp.Properties.Location.POST_NAME,
    }

    JOIN_HINTS = True
    TABLE_HINTS = True

    RESERVED_KEYWORDS: t.Set[str] = set()
    WITH_SEPARATED_COMMENTS = (exp.Select, exp.From, exp.Where, exp.With)
    UNWRAPPED_INTERVAL_VALUES = (exp.Column, exp.Literal, exp.Neg, exp.Paren)

    SENTINEL_LINE_BREAK = "__SQLGLOT__LB__"

    __slots__ = (
        "time_mapping",
        "time_trie",
        "pretty",
        "quote_start",
        "quote_end",
        "identifier_start",
        "identifier_end",
        "bit_start",
        "bit_end",
        "hex_start",
        "hex_end",
        "byte_start",
        "byte_end",
        "identify",
        "normalize",
        "string_escape",
        "identifier_escape",
        "pad",
        "index_offset",
        "unnest_column_only",
        "alias_post_tablesample",
        "normalize_functions",
        "unsupported_level",
        "unsupported_messages",
        "null_ordering",
        "max_unsupported",
        "_indent",
        "_escaped_quote_end",
        "_escaped_identifier_end",
        "_leading_comma",
        "_max_text_width",
        "_comments",
        "_cache",
    )

    def __init__(
        self,
        time_mapping=None,
        time_trie=None,
        pretty=None,
        quote_start=None,
        quote_end=None,
        identifier_start=None,
        identifier_end=None,
        bit_start=None,
        bit_end=None,
        hex_start=None,
        hex_end=None,
        byte_start=None,
        byte_end=None,
        identify=False,
        normalize=False,
        string_escape=None,
        identifier_escape=None,
        pad=2,
        indent=2,
        index_offset=0,
        unnest_column_only=False,
        alias_post_tablesample=False,
        normalize_functions="upper",
        unsupported_level=ErrorLevel.WARN,
        null_ordering=None,
        max_unsupported=3,
        leading_comma=False,
        max_text_width=80,
        comments=True,
    ):
        import sqlglot

        self.time_mapping = time_mapping or {}
        self.time_trie = time_trie
        self.pretty = pretty if pretty is not None else sqlglot.pretty
        self.quote_start = quote_start or "'"
        self.quote_end = quote_end or "'"
        self.identifier_start = identifier_start or '"'
        self.identifier_end = identifier_end or '"'
        self.bit_start = bit_start
        self.bit_end = bit_end
        self.hex_start = hex_start
        self.hex_end = hex_end
        self.byte_start = byte_start
        self.byte_end = byte_end
        self.identify = identify
        self.normalize = normalize
        self.string_escape = string_escape or "'"
        self.identifier_escape = identifier_escape or '"'
        self.pad = pad
        self.index_offset = index_offset
        self.unnest_column_only = unnest_column_only
        self.alias_post_tablesample = alias_post_tablesample
        self.normalize_functions = normalize_functions
        self.unsupported_level = unsupported_level
        self.unsupported_messages = []
        self.max_unsupported = max_unsupported
        self.null_ordering = null_ordering
        self._indent = indent
        self._escaped_quote_end = self.string_escape + self.quote_end
        self._escaped_identifier_end = self.identifier_escape + self.identifier_end
        self._leading_comma = leading_comma
        self._max_text_width = max_text_width
        self._comments = comments
        self._cache = None

    def generate(
        self,
        expression: t.Optional[exp.Expression],
        cache: t.Optional[t.Dict[int, str]] = None,
    ) -> str:
        """
        Generates a SQL string by interpreting the given syntax tree.

        Args
            expression: the syntax tree.
            cache: an optional sql string cache. this leverages the hash of an expression which is slow, so only use this if you set _hash on each node.

        Returns
            the SQL string.
        """
        if cache is not None:
            self._cache = cache
        self.unsupported_messages = []
        sql = self.sql(expression).strip()
        self._cache = None

        if self.unsupported_level == ErrorLevel.IGNORE:
            return sql

        if self.unsupported_level == ErrorLevel.WARN:
            for msg in self.unsupported_messages:
                logger.warning(msg)
        elif self.unsupported_level == ErrorLevel.RAISE and self.unsupported_messages:
            raise UnsupportedError(concat_messages(self.unsupported_messages, self.max_unsupported))

        if self.pretty:
            sql = sql.replace(self.SENTINEL_LINE_BREAK, "\n")
        return sql

    def unsupported(self, message: str) -> None:
        if self.unsupported_level == ErrorLevel.IMMEDIATE:
            raise UnsupportedError(message)
        self.unsupported_messages.append(message)

    def sep(self, sep: str = " ") -> str:
        return f"{sep.strip()}\n" if self.pretty else sep

    def seg(self, sql: str, sep: str = " ") -> str:
        return f"{self.sep(sep)}{sql}"

    def pad_comment(self, comment: str) -> str:
        comment = " " + comment if comment[0].strip() else comment
        comment = comment + " " if comment[-1].strip() else comment
        return comment

    def maybe_comment(
        self,
        sql: str,
        expression: t.Optional[exp.Expression] = None,
        comments: t.Optional[t.List[str]] = None,
    ) -> str:
        comments = ((expression and expression.comments) if comments is None else comments) if self._comments else None  # type: ignore

        if not comments or isinstance(expression, exp.Binary):
            return sql

        sep = "\n" if self.pretty else " "
        comments_sql = sep.join(
            f"/*{self.pad_comment(comment)}*/" for comment in comments if comment
        )

        if not comments_sql:
            return sql

        if isinstance(expression, self.WITH_SEPARATED_COMMENTS):
            return (
                f"{self.sep()}{comments_sql}{sql}"
                if sql[0].isspace()
                else f"{comments_sql}{self.sep()}{sql}"
            )

        return f"{sql} {comments_sql}"

    def wrap(self, expression: exp.Expression | str) -> str:
        this_sql = self.indent(
            self.sql(expression)
            if isinstance(expression, (exp.Select, exp.Union))
            else self.sql(expression, "this"),
            level=1,
            pad=0,
        )
        return f"({self.sep('')}{this_sql}{self.seg(')', sep='')}"

    def no_identify(self, func: t.Callable[..., str], *args, **kwargs) -> str:
        original = self.identify
        self.identify = False
        result = func(*args, **kwargs)
        self.identify = original
        return result

    def normalize_func(self, name: str) -> str:
        if self.normalize_functions == "upper":
            return name.upper()
        if self.normalize_functions == "lower":
            return name.lower()
        return name

    def indent(
        self,
        sql: str,
        level: int = 0,
        pad: t.Optional[int] = None,
        skip_first: bool = False,
        skip_last: bool = False,
    ) -> str:
        if not self.pretty:
            return sql

        pad = self.pad if pad is None else pad
        lines = sql.split("\n")

        return "\n".join(
            line
            if (skip_first and i == 0) or (skip_last and i == len(lines) - 1)
            else f"{' ' * (level * self._indent + pad)}{line}"
            for i, line in enumerate(lines)
        )

    def sql(
        self,
        expression: t.Optional[str | exp.Expression],
        key: t.Optional[str] = None,
        comment: bool = True,
    ) -> str:
        if not expression:
            return ""

        if isinstance(expression, str):
            return expression

        if key:
            return self.sql(expression.args.get(key))

        if self._cache is not None:
            expression_id = hash(expression)

            if expression_id in self._cache:
                return self._cache[expression_id]

        transform = self.TRANSFORMS.get(expression.__class__)

        if callable(transform):
            sql = transform(self, expression)
        elif transform:
            sql = transform
        elif isinstance(expression, exp.Expression):
            exp_handler_name = f"{expression.key}_sql"

            if hasattr(self, exp_handler_name):
                sql = getattr(self, exp_handler_name)(expression)
            elif isinstance(expression, exp.Func):
                sql = self.function_fallback_sql(expression)
            elif isinstance(expression, exp.Property):
                sql = self.property_sql(expression)
            else:
                raise ValueError(f"Unsupported expression type {expression.__class__.__name__}")
        else:
            raise ValueError(f"Expected an Expression. Received {type(expression)}: {expression}")

        sql = self.maybe_comment(sql, expression) if self._comments and comment else sql

        if self._cache is not None:
            self._cache[expression_id] = sql
        return sql

    def uncache_sql(self, expression: exp.Uncache) -> str:
        table = self.sql(expression, "this")
        exists_sql = " IF EXISTS" if expression.args.get("exists") else ""
        return f"UNCACHE TABLE{exists_sql} {table}"

    def cache_sql(self, expression: exp.Cache) -> str:
        lazy = " LAZY" if expression.args.get("lazy") else ""
        table = self.sql(expression, "this")
        options = expression.args.get("options")
        options = f" OPTIONS({self.sql(options[0])} = {self.sql(options[1])})" if options else ""
        sql = self.sql(expression, "expression")
        sql = f" AS{self.sep()}{sql}" if sql else ""
        sql = f"CACHE{lazy} TABLE {table}{options}{sql}"
        return self.prepend_ctes(expression, sql)

    def characterset_sql(self, expression: exp.CharacterSet) -> str:
        if isinstance(expression.parent, exp.Cast):
            return f"CHAR CHARACTER SET {self.sql(expression, 'this')}"
        default = "DEFAULT " if expression.args.get("default") else ""
        return f"{default}CHARACTER SET={self.sql(expression, 'this')}"

    def column_sql(self, expression: exp.Column) -> str:
        return ".".join(
            self.sql(part)
            for part in (
                expression.args.get("catalog"),
                expression.args.get("db"),
                expression.args.get("table"),
                expression.args.get("this"),
            )
            if part
        )

    def columnposition_sql(self, expression: exp.ColumnPosition) -> str:
        this = self.sql(expression, "this")
        this = f" {this}" if this else ""
        position = self.sql(expression, "position")
        return f"{position}{this}"

    def columndef_sql(self, expression: exp.ColumnDef, sep: str = " ") -> str:
        column = self.sql(expression, "this")
        kind = self.sql(expression, "kind")
        constraints = self.expressions(expression, key="constraints", sep=" ", flat=True)
        exists = "IF NOT EXISTS " if expression.args.get("exists") else ""
        kind = f"{sep}{kind}" if kind else ""
        constraints = f" {constraints}" if constraints else ""
        position = self.sql(expression, "position")
        position = f" {position}" if position else ""

        return f"{exists}{column}{kind}{constraints}{position}"

    def columnconstraint_sql(self, expression: exp.ColumnConstraint) -> str:
        this = self.sql(expression, "this")
        kind_sql = self.sql(expression, "kind").strip()
        return f"CONSTRAINT {this} {kind_sql}" if this else kind_sql

    def autoincrementcolumnconstraint_sql(self, _) -> str:
        return self.token_sql(TokenType.AUTO_INCREMENT)

    def compresscolumnconstraint_sql(self, expression: exp.CompressColumnConstraint) -> str:
        if isinstance(expression.this, list):
            this = self.wrap(self.expressions(expression, key="this", flat=True))
        else:
            this = self.sql(expression, "this")

        return f"COMPRESS {this}"

    def generatedasidentitycolumnconstraint_sql(
        self, expression: exp.GeneratedAsIdentityColumnConstraint
    ) -> str:
        this = ""
        if expression.this is not None:
            on_null = "ON NULL " if expression.args.get("on_null") else ""
            this = " ALWAYS " if expression.this else f" BY DEFAULT {on_null}"

        start = expression.args.get("start")
        start = f"START WITH {start}" if start else ""
        increment = expression.args.get("increment")
        increment = f" INCREMENT BY {increment}" if increment else ""
        minvalue = expression.args.get("minvalue")
        minvalue = f" MINVALUE {minvalue}" if minvalue else ""
        maxvalue = expression.args.get("maxvalue")
        maxvalue = f" MAXVALUE {maxvalue}" if maxvalue else ""
        cycle = expression.args.get("cycle")
        cycle_sql = ""
        if cycle is not None:
            cycle_sql = f"{' NO' if not cycle else ''} CYCLE"
            cycle_sql = cycle_sql.strip() if not start and not increment else cycle_sql
        sequence_opts = ""
        if start or increment or cycle_sql:
            sequence_opts = f"{start}{increment}{minvalue}{maxvalue}{cycle_sql}"
            sequence_opts = f" ({sequence_opts.strip()})"
        return f"GENERATED{this}AS IDENTITY{sequence_opts}"

    def notnullcolumnconstraint_sql(self, expression: exp.NotNullColumnConstraint) -> str:
        return f"{'' if expression.args.get('allow_null') else 'NOT '}NULL"

    def primarykeycolumnconstraint_sql(self, expression: exp.PrimaryKeyColumnConstraint) -> str:
        desc = expression.args.get("desc")
        if desc is not None:
            return f"PRIMARY KEY{' DESC' if desc else ' ASC'}"
        return f"PRIMARY KEY"

    def uniquecolumnconstraint_sql(self, _) -> str:
        return "UNIQUE"

    def create_sql(self, expression: exp.Create) -> str:
        kind = self.sql(expression, "kind").upper()
        properties = expression.args.get("properties")
        properties_exp = expression.copy()
        properties_locs = self.locate_properties(properties) if properties else {}
        if properties_locs.get(exp.Properties.Location.POST_SCHEMA) or properties_locs.get(
            exp.Properties.Location.POST_WITH
        ):
            properties_exp.set(
                "properties",
                exp.Properties(
                    expressions=[
                        *properties_locs[exp.Properties.Location.POST_SCHEMA],
                        *properties_locs[exp.Properties.Location.POST_WITH],
                    ]
                ),
            )
        if kind == "TABLE" and properties_locs.get(exp.Properties.Location.POST_NAME):
            this_name = self.sql(expression.this, "this")
            this_properties = self.properties(
                exp.Properties(expressions=properties_locs[exp.Properties.Location.POST_NAME]),
                wrapped=False,
            )
            this_schema = f"({self.expressions(expression.this)})"
            this = f"{this_name}, {this_properties} {this_schema}"
            properties_sql = ""
        else:
            this = self.sql(expression, "this")
            properties_sql = self.sql(properties_exp, "properties")
        begin = " BEGIN" if expression.args.get("begin") else ""
        expression_sql = self.sql(expression, "expression")
        if expression_sql:
            expression_sql = f"{begin}{self.sep()}{expression_sql}"

            if self.CREATE_FUNCTION_RETURN_AS or not isinstance(expression.expression, exp.Return):
                if properties_locs.get(exp.Properties.Location.POST_ALIAS):
                    postalias_props_sql = self.properties(
                        exp.Properties(
                            expressions=properties_locs[exp.Properties.Location.POST_ALIAS]
                        ),
                        wrapped=False,
                    )
                    expression_sql = f" AS {postalias_props_sql}{expression_sql}"
                else:
                    expression_sql = f" AS{expression_sql}"

        postindex_props_sql = ""
        if properties_locs.get(exp.Properties.Location.POST_INDEX):
            postindex_props_sql = self.properties(
                exp.Properties(expressions=properties_locs[exp.Properties.Location.POST_INDEX]),
                wrapped=False,
                prefix=" ",
            )

        indexes = expression.args.get("indexes")
        if indexes:
            indexes_sql: t.List[str] = []
            for index in indexes:
                ind_unique = " UNIQUE" if index.args.get("unique") else ""
                ind_primary = " PRIMARY" if index.args.get("primary") else ""
                ind_amp = " AMP" if index.args.get("amp") else ""
                ind_name = f" {index.name}" if index.name else ""
                ind_columns = (
                    f' ({self.expressions(index, key="columns", flat=True)})'
                    if index.args.get("columns")
                    else ""
                )
                ind_sql = f"{ind_unique}{ind_primary}{ind_amp} INDEX{ind_name}{ind_columns}"

                if indexes_sql:
                    indexes_sql.append(ind_sql)
                else:
                    indexes_sql.append(
                        f"{ind_sql}{postindex_props_sql}"
                        if index.args.get("primary")
                        else f"{postindex_props_sql}{ind_sql}"
                    )

            index_sql = "".join(indexes_sql)
        else:
            index_sql = postindex_props_sql

        replace = " OR REPLACE" if expression.args.get("replace") else ""
        unique = " UNIQUE" if expression.args.get("unique") else ""

        postcreate_props_sql = ""
        if properties_locs.get(exp.Properties.Location.POST_CREATE):
            postcreate_props_sql = self.properties(
                exp.Properties(expressions=properties_locs[exp.Properties.Location.POST_CREATE]),
                sep=" ",
                prefix=" ",
                wrapped=False,
            )

        modifiers = "".join((replace, unique, postcreate_props_sql))

        postexpression_props_sql = ""
        if properties_locs.get(exp.Properties.Location.POST_EXPRESSION):
            postexpression_props_sql = self.properties(
                exp.Properties(
                    expressions=properties_locs[exp.Properties.Location.POST_EXPRESSION]
                ),
                sep=" ",
                prefix=" ",
                wrapped=False,
            )

        exists_sql = " IF NOT EXISTS" if expression.args.get("exists") else ""
        no_schema_binding = (
            " WITH NO SCHEMA BINDING" if expression.args.get("no_schema_binding") else ""
        )

        clone = self.sql(expression, "clone")
        clone = f" {clone}" if clone else ""

        expression_sql = f"CREATE{modifiers} {kind}{exists_sql} {this}{properties_sql}{expression_sql}{postexpression_props_sql}{index_sql}{no_schema_binding}{clone}"
        return self.prepend_ctes(expression, expression_sql)

    def clone_sql(self, expression: exp.Clone) -> str:
        this = self.sql(expression, "this")
        when = self.sql(expression, "when")

        if when:
            kind = self.sql(expression, "kind")
            expr = self.sql(expression, "expression")
            return f"CLONE {this} {when} ({kind} => {expr})"

        return f"CLONE {this}"

    def describe_sql(self, expression: exp.Describe) -> str:
        return f"DESCRIBE {self.sql(expression, 'this')}"

    def prepend_ctes(self, expression: exp.Expression, sql: str) -> str:
        with_ = self.sql(expression, "with")
        if with_:
            sql = f"{with_}{self.sep()}{sql}"
        return sql

    def with_sql(self, expression: exp.With) -> str:
        sql = self.expressions(expression, flat=True)
        recursive = "RECURSIVE " if expression.args.get("recursive") else ""

        return f"WITH {recursive}{sql}"

    def cte_sql(self, expression: exp.CTE) -> str:
        alias = self.sql(expression, "alias")
        return f"{alias} AS {self.wrap(expression)}"

    def tablealias_sql(self, expression: exp.TableAlias) -> str:
        alias = self.sql(expression, "this")
        columns = self.expressions(expression, key="columns", flat=True)
        columns = f"({columns})" if columns else ""
        return f"{alias}{columns}"

    def bitstring_sql(self, expression: exp.BitString) -> str:
        this = self.sql(expression, "this")
        if self.bit_start:
            return f"{self.bit_start}{this}{self.bit_end}"
        return f"{int(this, 2)}"

    def hexstring_sql(self, expression: exp.HexString) -> str:
        this = self.sql(expression, "this")
        if self.hex_start:
            return f"{self.hex_start}{this}{self.hex_end}"
        return f"{int(this, 16)}"

    def bytestring_sql(self, expression: exp.ByteString) -> str:
        this = self.sql(expression, "this")
        if self.byte_start:
            return f"{self.byte_start}{this}{self.byte_end}"
        return this

    def datatypesize_sql(self, expression: exp.DataTypeSize) -> str:
        this = self.sql(expression, "this")
        specifier = self.sql(expression, "expression")
        specifier = f" {specifier}" if specifier else ""
        return f"{this}{specifier}"

    def datatype_sql(self, expression: exp.DataType) -> str:
        type_value = expression.this
        type_sql = self.TYPE_MAPPING.get(type_value, type_value.value)
        nested = ""
        interior = self.expressions(expression, flat=True)
        values = ""
        if interior:
            if expression.args.get("nested"):
                nested = f"{self.STRUCT_DELIMITER[0]}{interior}{self.STRUCT_DELIMITER[1]}"
                if expression.args.get("values") is not None:
                    delimiters = ("[", "]") if type_value == exp.DataType.Type.ARRAY else ("(", ")")
                    values = f"{delimiters[0]}{self.expressions(expression, key='values')}{delimiters[1]}"
            else:
                nested = f"({interior})"

        return f"{type_sql}{nested}{values}"

    def directory_sql(self, expression: exp.Directory) -> str:
        local = "LOCAL " if expression.args.get("local") else ""
        row_format = self.sql(expression, "row_format")
        row_format = f" {row_format}" if row_format else ""
        return f"{local}DIRECTORY {self.sql(expression, 'this')}{row_format}"

    def delete_sql(self, expression: exp.Delete) -> str:
        this = self.sql(expression, "this")
        this = f" FROM {this}" if this else ""
        using_sql = (
            f" USING {self.expressions(expression, key='using', sep=', USING ')}"
            if expression.args.get("using")
            else ""
        )
        where_sql = self.sql(expression, "where")
        returning = self.sql(expression, "returning")
        sql = f"DELETE{this}{using_sql}{where_sql}{returning}"
        return self.prepend_ctes(expression, sql)

    def drop_sql(self, expression: exp.Drop) -> str:
        this = self.sql(expression, "this")
        kind = expression.args["kind"]
        exists_sql = " IF EXISTS " if expression.args.get("exists") else " "
        temporary = " TEMPORARY" if expression.args.get("temporary") else ""
        materialized = " MATERIALIZED" if expression.args.get("materialized") else ""
        cascade = " CASCADE" if expression.args.get("cascade") else ""
        constraints = " CONSTRAINTS" if expression.args.get("constraints") else ""
        purge = " PURGE" if expression.args.get("purge") else ""
        return (
            f"DROP{temporary}{materialized} {kind}{exists_sql}{this}{cascade}{constraints}{purge}"
        )

    def except_sql(self, expression: exp.Except) -> str:
        return self.prepend_ctes(
            expression,
            self.set_operation(expression, self.except_op(expression)),
        )

    def except_op(self, expression: exp.Except) -> str:
        return f"EXCEPT{'' if expression.args.get('distinct') else ' ALL'}"

    def fetch_sql(self, expression: exp.Fetch) -> str:
        direction = expression.args.get("direction")
        direction = f" {direction.upper()}" if direction else ""
        count = expression.args.get("count")
        count = f" {count}" if count else ""
        if expression.args.get("percent"):
            count = f"{count} PERCENT"
        with_ties_or_only = "WITH TIES" if expression.args.get("with_ties") else "ONLY"
        return f"{self.seg('FETCH')}{direction}{count} ROWS {with_ties_or_only}"

    def filter_sql(self, expression: exp.Filter) -> str:
        this = self.sql(expression, "this")
        where = self.sql(expression, "expression")[1:]  # where has a leading space
        return f"{this} FILTER({where})"

    def hint_sql(self, expression: exp.Hint) -> str:
        if self.sql(expression, "this"):
            self.unsupported("Hints are not supported")
        return ""

    def index_sql(self, expression: exp.Index) -> str:
        this = self.sql(expression, "this")
        table = self.sql(expression, "table")
        columns = self.sql(expression, "columns")
        return f"{this} ON {table} {columns}"

    def identifier_sql(self, expression: exp.Identifier) -> str:
        text = expression.name
        lower = text.lower()
        text = lower if self.normalize and not expression.quoted else text
        text = text.replace(self.identifier_end, self._escaped_identifier_end)
        if (
            expression.quoted
            or should_identify(text, self.identify)
            or lower in self.RESERVED_KEYWORDS
        ):
            text = f"{self.identifier_start}{text}{self.identifier_end}"
        return text

    def inputoutputformat_sql(self, expression: exp.InputOutputFormat) -> str:
        input_format = self.sql(expression, "input_format")
        input_format = f"INPUTFORMAT {input_format}" if input_format else ""
        output_format = self.sql(expression, "output_format")
        output_format = f"OUTPUTFORMAT {output_format}" if output_format else ""
        return self.sep().join((input_format, output_format))

    def national_sql(self, expression: exp.National) -> str:
        return f"N{self.sql(expression, 'this')}"

    def partition_sql(self, expression: exp.Partition) -> str:
        return f"PARTITION({self.expressions(expression)})"

    def properties_sql(self, expression: exp.Properties) -> str:
        root_properties = []
        with_properties = []

        for p in expression.expressions:
            p_loc = self.PROPERTIES_LOCATION[p.__class__]
            if p_loc == exp.Properties.Location.POST_WITH:
                with_properties.append(p)
            elif p_loc == exp.Properties.Location.POST_SCHEMA:
                root_properties.append(p)

        return self.root_properties(
            exp.Properties(expressions=root_properties)
        ) + self.with_properties(exp.Properties(expressions=with_properties))

    def root_properties(self, properties: exp.Properties) -> str:
        if properties.expressions:
            return self.sep() + self.expressions(properties, indent=False, sep=" ")
        return ""

    def properties(
        self,
        properties: exp.Properties,
        prefix: str = "",
        sep: str = ", ",
        suffix: str = "",
        wrapped: bool = True,
    ) -> str:
        if properties.expressions:
            expressions = self.expressions(properties, sep=sep, indent=False)
            expressions = self.wrap(expressions) if wrapped else expressions
            return f"{prefix}{' ' if prefix and prefix != ' ' else ''}{expressions}{suffix}"
        return ""

    def with_properties(self, properties: exp.Properties) -> str:
        return self.properties(properties, prefix=self.seg("WITH"))

    def locate_properties(
        self, properties: exp.Properties
    ) -> t.Dict[exp.Properties.Location, list[exp.Property]]:
        properties_locs: t.Dict[exp.Properties.Location, list[exp.Property]] = {
            key: [] for key in exp.Properties.Location
        }

        for p in properties.expressions:
            p_loc = self.PROPERTIES_LOCATION[p.__class__]
            if p_loc == exp.Properties.Location.POST_NAME:
                properties_locs[exp.Properties.Location.POST_NAME].append(p)
            elif p_loc == exp.Properties.Location.POST_INDEX:
                properties_locs[exp.Properties.Location.POST_INDEX].append(p)
            elif p_loc == exp.Properties.Location.POST_SCHEMA:
                properties_locs[exp.Properties.Location.POST_SCHEMA].append(p)
            elif p_loc == exp.Properties.Location.POST_WITH:
                properties_locs[exp.Properties.Location.POST_WITH].append(p)
            elif p_loc == exp.Properties.Location.POST_CREATE:
                properties_locs[exp.Properties.Location.POST_CREATE].append(p)
            elif p_loc == exp.Properties.Location.POST_ALIAS:
                properties_locs[exp.Properties.Location.POST_ALIAS].append(p)
            elif p_loc == exp.Properties.Location.POST_EXPRESSION:
                properties_locs[exp.Properties.Location.POST_EXPRESSION].append(p)
            elif p_loc == exp.Properties.Location.UNSUPPORTED:
                self.unsupported(f"Unsupported property {p.key}")

        return properties_locs

    def property_sql(self, expression: exp.Property) -> str:
        property_cls = expression.__class__
        if property_cls == exp.Property:
            return f"{expression.name}={self.sql(expression, 'value')}"

        property_name = exp.Properties.PROPERTY_TO_NAME.get(property_cls)
        if not property_name:
            self.unsupported(f"Unsupported property {expression.key}")

        return f"{property_name}={self.sql(expression, 'this')}"

    def likeproperty_sql(self, expression: exp.LikeProperty) -> str:
        options = " ".join(f"{e.name} {self.sql(e, 'value')}" for e in expression.expressions)
        options = f" {options}" if options else ""
        return f"LIKE {self.sql(expression, 'this')}{options}"

    def fallbackproperty_sql(self, expression: exp.FallbackProperty) -> str:
        no = "NO " if expression.args.get("no") else ""
        protection = " PROTECTION" if expression.args.get("protection") else ""
        return f"{no}FALLBACK{protection}"

    def journalproperty_sql(self, expression: exp.JournalProperty) -> str:
        no = "NO " if expression.args.get("no") else ""
        dual = "DUAL " if expression.args.get("dual") else ""
        before = "BEFORE " if expression.args.get("before") else ""
        return f"{no}{dual}{before}JOURNAL"

    def freespaceproperty_sql(self, expression: exp.FreespaceProperty) -> str:
        freespace = self.sql(expression, "this")
        percent = " PERCENT" if expression.args.get("percent") else ""
        return f"FREESPACE={freespace}{percent}"

    def afterjournalproperty_sql(self, expression: exp.AfterJournalProperty) -> str:
        no = "NO " if expression.args.get("no") else ""
        dual = "DUAL " if expression.args.get("dual") else ""
        local = ""
        if expression.args.get("local") is not None:
            local = "LOCAL " if expression.args.get("local") else "NOT LOCAL "
        return f"{no}{dual}{local}AFTER JOURNAL"

    def checksumproperty_sql(self, expression: exp.ChecksumProperty) -> str:
        if expression.args.get("default"):
            property = "DEFAULT"
        elif expression.args.get("on"):
            property = "ON"
        else:
            property = "OFF"
        return f"CHECKSUM={property}"

    def mergeblockratioproperty_sql(self, expression: exp.MergeBlockRatioProperty) -> str:
        if expression.args.get("no"):
            return "NO MERGEBLOCKRATIO"
        if expression.args.get("default"):
            return "DEFAULT MERGEBLOCKRATIO"

        percent = " PERCENT" if expression.args.get("percent") else ""
        return f"MERGEBLOCKRATIO={self.sql(expression, 'this')}{percent}"

    def datablocksizeproperty_sql(self, expression: exp.DataBlocksizeProperty) -> str:
        default = expression.args.get("default")
        min = expression.args.get("min")
        if default is not None or min is not None:
            if default:
                property = "DEFAULT"
            elif min:
                property = "MINIMUM"
            else:
                property = "MAXIMUM"
            return f"{property} DATABLOCKSIZE"
        else:
            units = expression.args.get("units")
            units = f" {units}" if units else ""
            return f"DATABLOCKSIZE={self.sql(expression, 'size')}{units}"

    def blockcompressionproperty_sql(self, expression: exp.BlockCompressionProperty) -> str:
        autotemp = expression.args.get("autotemp")
        always = expression.args.get("always")
        default = expression.args.get("default")
        manual = expression.args.get("manual")
        never = expression.args.get("never")

        if autotemp is not None:
            property = f"AUTOTEMP({self.expressions(autotemp)})"
        elif always:
            property = "ALWAYS"
        elif default:
            property = "DEFAULT"
        elif manual:
            property = "MANUAL"
        elif never:
            property = "NEVER"
        return f"BLOCKCOMPRESSION={property}"

    def isolatedloadingproperty_sql(self, expression: exp.IsolatedLoadingProperty) -> str:
        no = expression.args.get("no")
        no = " NO" if no else ""
        concurrent = expression.args.get("concurrent")
        concurrent = " CONCURRENT" if concurrent else ""

        for_ = ""
        if expression.args.get("for_all"):
            for_ = " FOR ALL"
        elif expression.args.get("for_insert"):
            for_ = " FOR INSERT"
        elif expression.args.get("for_none"):
            for_ = " FOR NONE"
        return f"WITH{no}{concurrent} ISOLATED LOADING{for_}"

    def lockingproperty_sql(self, expression: exp.LockingProperty) -> str:
        kind = expression.args.get("kind")
        this: str = f" {this}" if expression.this else ""
        for_or_in = expression.args.get("for_or_in")
        lock_type = expression.args.get("lock_type")
        override = " OVERRIDE" if expression.args.get("override") else ""
        return f"LOCKING {kind}{this} {for_or_in} {lock_type}{override}"

    def withdataproperty_sql(self, expression: exp.WithDataProperty) -> str:
        data_sql = f"WITH {'NO ' if expression.args.get('no') else ''}DATA"
        statistics = expression.args.get("statistics")
        statistics_sql = ""
        if statistics is not None:
            statistics_sql = f" AND {'NO ' if not statistics else ''}STATISTICS"
        return f"{data_sql}{statistics_sql}"

    def insert_sql(self, expression: exp.Insert) -> str:
        overwrite = expression.args.get("overwrite")

        if isinstance(expression.this, exp.Directory):
            this = "OVERWRITE " if overwrite else "INTO "
        else:
            this = "OVERWRITE TABLE " if overwrite else "INTO "

        alternative = expression.args.get("alternative")
        alternative = f" OR {alternative} " if alternative else " "
        this = f"{this}{self.sql(expression, 'this')}"

        exists = " IF EXISTS " if expression.args.get("exists") else " "
        partition_sql = (
            self.sql(expression, "partition") if expression.args.get("partition") else ""
        )
        expression_sql = self.sql(expression, "expression")
        conflict = self.sql(expression, "conflict")
        returning = self.sql(expression, "returning")
        sep = self.sep() if partition_sql else ""
        sql = f"INSERT{alternative}{this}{exists}{partition_sql}{sep}{expression_sql}{conflict}{returning}"
        return self.prepend_ctes(expression, sql)

    def intersect_sql(self, expression: exp.Intersect) -> str:
        return self.prepend_ctes(
            expression,
            self.set_operation(expression, self.intersect_op(expression)),
        )

    def intersect_op(self, expression: exp.Intersect) -> str:
        return f"INTERSECT{'' if expression.args.get('distinct') else ' ALL'}"

    def introducer_sql(self, expression: exp.Introducer) -> str:
        return f"{self.sql(expression, 'this')} {self.sql(expression, 'expression')}"

    def pseudotype_sql(self, expression: exp.PseudoType) -> str:
        return expression.name.upper()

    def onconflict_sql(self, expression: exp.OnConflict) -> str:
        conflict = "ON DUPLICATE KEY" if expression.args.get("duplicate") else "ON CONFLICT"
        constraint = self.sql(expression, "constraint")
        if constraint:
            constraint = f"ON CONSTRAINT {constraint}"
        key = self.expressions(expression, key="key", flat=True)
        do = "" if expression.args.get("duplicate") else " DO "
        nothing = "NOTHING" if expression.args.get("nothing") else ""
        expressions = self.expressions(expression, flat=True)
        if expressions:
            expressions = f"UPDATE SET {expressions}"
        return f"{self.seg(conflict)} {constraint}{key}{do}{nothing}{expressions}"

    def returning_sql(self, expression: exp.Returning) -> str:
        return f"{self.seg('RETURNING')} {self.expressions(expression, flat=True)}"

    def rowformatdelimitedproperty_sql(self, expression: exp.RowFormatDelimitedProperty) -> str:
        fields = expression.args.get("fields")
        fields = f" FIELDS TERMINATED BY {fields}" if fields else ""
        escaped = expression.args.get("escaped")
        escaped = f" ESCAPED BY {escaped}" if escaped else ""
        items = expression.args.get("collection_items")
        items = f" COLLECTION ITEMS TERMINATED BY {items}" if items else ""
        keys = expression.args.get("map_keys")
        keys = f" MAP KEYS TERMINATED BY {keys}" if keys else ""
        lines = expression.args.get("lines")
        lines = f" LINES TERMINATED BY {lines}" if lines else ""
        null = expression.args.get("null")
        null = f" NULL DEFINED AS {null}" if null else ""
        return f"ROW FORMAT DELIMITED{fields}{escaped}{items}{keys}{lines}{null}"

    def table_sql(self, expression: exp.Table, sep: str = " AS ") -> str:
        table = ".".join(
            part
            for part in [
                self.sql(expression, "catalog"),
                self.sql(expression, "db"),
                self.sql(expression, "this"),
            ]
            if part
        )

        alias = self.sql(expression, "alias")
        alias = f"{sep}{alias}" if alias else ""
        hints = self.expressions(expression, key="hints", flat=True)
        hints = f" WITH ({hints})" if hints and self.TABLE_HINTS else ""
        pivots = self.expressions(expression, key="pivots", sep=" ", flat=True)
        pivots = f" {pivots}" if pivots else ""
        joins = self.expressions(expression, key="joins", sep="")
        laterals = self.expressions(expression, key="laterals", sep="")
        system_time = expression.args.get("system_time")
        system_time = f" {self.sql(expression, 'system_time')}" if system_time else ""

        return f"{table}{system_time}{alias}{hints}{pivots}{joins}{laterals}"

    def tablesample_sql(
        self, expression: exp.TableSample, seed_prefix: str = "SEED", sep=" AS "
    ) -> str:
        if self.alias_post_tablesample and expression.this.alias:
            table = expression.this.copy()
            table.set("alias", None)
            this = self.sql(table)
            alias = f"{sep}{self.sql(expression.this, 'alias')}"
        else:
            this = self.sql(expression, "this")
            alias = ""
        method = self.sql(expression, "method")
        method = f"{method.upper()} " if method and self.TABLESAMPLE_WITH_METHOD else ""
        numerator = self.sql(expression, "bucket_numerator")
        denominator = self.sql(expression, "bucket_denominator")
        field = self.sql(expression, "bucket_field")
        field = f" ON {field}" if field else ""
        bucket = f"BUCKET {numerator} OUT OF {denominator}{field}" if numerator else ""
        percent = self.sql(expression, "percent")
        percent = f"{percent} PERCENT" if percent else ""
        rows = self.sql(expression, "rows")
        rows = f"{rows} ROWS" if rows else ""
        size = self.sql(expression, "size")
        if size and self.TABLESAMPLE_SIZE_IS_PERCENT:
            size = f"{size} PERCENT"
        seed = self.sql(expression, "seed")
        seed = f" {seed_prefix} ({seed})" if seed else ""
        kind = expression.args.get("kind", "TABLESAMPLE")
        return f"{this} {kind} {method}({bucket}{percent}{rows}{size}){seed}{alias}"

    def pivot_sql(self, expression: exp.Pivot) -> str:
        alias = self.sql(expression, "alias")
        alias = f" AS {alias}" if alias else ""
        unpivot = expression.args.get("unpivot")
        direction = "UNPIVOT" if unpivot else "PIVOT"
        expressions = self.expressions(expression, flat=True)
        field = self.sql(expression, "field")
        return f"{direction}({expressions} FOR {field}){alias}"

    def tuple_sql(self, expression: exp.Tuple) -> str:
        return f"({self.expressions(expression, flat=True)})"

    def update_sql(self, expression: exp.Update) -> str:
        this = self.sql(expression, "this")
        set_sql = self.expressions(expression, flat=True)
        from_sql = self.sql(expression, "from")
        where_sql = self.sql(expression, "where")
        returning = self.sql(expression, "returning")
        sql = f"UPDATE {this} SET {set_sql}{from_sql}{where_sql}{returning}"
        return self.prepend_ctes(expression, sql)

    def values_sql(self, expression: exp.Values) -> str:
        args = self.expressions(expression)
        alias = self.sql(expression, "alias")
        values = f"VALUES{self.seg('')}{args}"
        values = (
            f"({values})"
            if self.WRAP_DERIVED_VALUES and (alias or isinstance(expression.parent, exp.From))
            else values
        )
        return f"{values} AS {alias}" if alias else values

    def var_sql(self, expression: exp.Var) -> str:
        return self.sql(expression, "this")

    def into_sql(self, expression: exp.Into) -> str:
        temporary = " TEMPORARY" if expression.args.get("temporary") else ""
        unlogged = " UNLOGGED" if expression.args.get("unlogged") else ""
        return f"{self.seg('INTO')}{temporary or unlogged} {self.sql(expression, 'this')}"

    def from_sql(self, expression: exp.From) -> str:
        return f"{self.seg('FROM')} {self.sql(expression, 'this')}"

    def group_sql(self, expression: exp.Group) -> str:
        group_by = self.op_expressions("GROUP BY", expression)
        grouping_sets = self.expressions(expression, key="grouping_sets", indent=False)
        grouping_sets = (
            f"{self.seg('GROUPING SETS')} {self.wrap(grouping_sets)}" if grouping_sets else ""
        )

        cube = expression.args.get("cube", [])
        if seq_get(cube, 0) is True:
            return f"{group_by}{self.seg('WITH CUBE')}"
        else:
            cube_sql = self.expressions(expression, key="cube", indent=False)
            cube_sql = f"{self.seg('CUBE')} {self.wrap(cube_sql)}" if cube_sql else ""

        rollup = expression.args.get("rollup", [])
        if seq_get(rollup, 0) is True:
            return f"{group_by}{self.seg('WITH ROLLUP')}"
        else:
            rollup_sql = self.expressions(expression, key="rollup", indent=False)
            rollup_sql = f"{self.seg('ROLLUP')} {self.wrap(rollup_sql)}" if rollup_sql else ""

        groupings = csv(
            grouping_sets,
            cube_sql,
            rollup_sql,
            self.seg("WITH TOTALS") if expression.args.get("totals") else "",
            sep=self.GROUPINGS_SEP,
        )

        if expression.args.get("expressions") and groupings:
            group_by = f"{group_by}{self.GROUPINGS_SEP}"

        return f"{group_by}{groupings}"

    def having_sql(self, expression: exp.Having) -> str:
        this = self.indent(self.sql(expression, "this"))
        return f"{self.seg('HAVING')}{self.sep()}{this}"

    def join_sql(self, expression: exp.Join) -> str:
        op_sql = " ".join(
            op
            for op in (
                "NATURAL" if expression.args.get("natural") else None,
                "GLOBAL" if expression.args.get("global") else None,
                expression.side,
                expression.kind,
                expression.hint if self.JOIN_HINTS else None,
            )
            if op
        )
        on_sql = self.sql(expression, "on")
        using = expression.args.get("using")

        if not on_sql and using:
            on_sql = csv(*(self.sql(column) for column in using))

        this_sql = self.sql(expression, "this")

        if on_sql:
            on_sql = self.indent(on_sql, skip_first=True)
            space = self.seg(" " * self.pad) if self.pretty else " "
            if using:
                on_sql = f"{space}USING ({on_sql})"
            else:
                on_sql = f"{space}ON {on_sql}"
        elif not op_sql:
            return f", {this_sql}"

        op_sql = f"{op_sql} JOIN" if op_sql else "JOIN"
        return f"{self.seg(op_sql)} {this_sql}{on_sql}"

    def lambda_sql(self, expression: exp.Lambda, arrow_sep: str = "->") -> str:
        args = self.expressions(expression, flat=True)
        args = f"({args})" if len(args.split(",")) > 1 else args
        return f"{args} {arrow_sep} {self.sql(expression, 'this')}"

    def lateral_sql(self, expression: exp.Lateral) -> str:
        this = self.sql(expression, "this")

        if isinstance(expression.this, exp.Subquery):
            return f"LATERAL {this}"

        if expression.args.get("view"):
            alias = expression.args["alias"]
            columns = self.expressions(alias, key="columns", flat=True)
            table = f" {alias.name}" if alias.name else ""
            columns = f" AS {columns}" if columns else ""
            op_sql = self.seg(f"LATERAL VIEW{' OUTER' if expression.args.get('outer') else ''}")
            return f"{op_sql}{self.sep()}{this}{table}{columns}"

        alias = self.sql(expression, "alias")
        alias = f" AS {alias}" if alias else ""
        return f"LATERAL {this}{alias}"

    def limit_sql(self, expression: exp.Limit) -> str:
        this = self.sql(expression, "this")
        return f"{this}{self.seg('LIMIT')} {self.sql(expression, 'expression')}"

    def offset_sql(self, expression: exp.Offset) -> str:
        this = self.sql(expression, "this")
        return f"{this}{self.seg('OFFSET')} {self.sql(expression, 'expression')}"

    def setitem_sql(self, expression: exp.SetItem) -> str:
        kind = self.sql(expression, "kind")
        kind = f"{kind} " if kind else ""
        this = self.sql(expression, "this")
        expressions = self.expressions(expression)
        collate = self.sql(expression, "collate")
        collate = f" COLLATE {collate}" if collate else ""
        global_ = "GLOBAL " if expression.args.get("global") else ""
        return f"{global_}{kind}{this}{expressions}{collate}"

    def set_sql(self, expression: exp.Set) -> str:
        expressions = (
            f" {self.expressions(expression, flat=True)}" if expression.expressions else ""
        )
        return f"SET{expressions}"

    def pragma_sql(self, expression: exp.Pragma) -> str:
        return f"PRAGMA {self.sql(expression, 'this')}"

    def lock_sql(self, expression: exp.Lock) -> str:
        if not self.LOCKING_READS_SUPPORTED:
            self.unsupported("Locking reads using 'FOR UPDATE/SHARE' are not supported")
            return ""

        lock_type = "FOR UPDATE" if expression.args["update"] else "FOR SHARE"
        expressions = self.expressions(expression, flat=True)
        expressions = f" OF {expressions}" if expressions else ""
        wait = expression.args.get("wait")

        if wait is not None:
            if isinstance(wait, exp.Literal):
                wait = f" WAIT {self.sql(wait)}"
            else:
                wait = " NOWAIT" if wait else " SKIP LOCKED"

        return f"{lock_type}{expressions}{wait or ''}"

    def literal_sql(self, expression: exp.Literal) -> str:
        text = expression.this or ""
        if expression.is_string:
            text = text.replace(self.quote_end, self._escaped_quote_end)
            if self.pretty:
                text = text.replace("\n", self.SENTINEL_LINE_BREAK)
            text = f"{self.quote_start}{text}{self.quote_end}"
        return text

    def loaddata_sql(self, expression: exp.LoadData) -> str:
        local = " LOCAL" if expression.args.get("local") else ""
        inpath = f" INPATH {self.sql(expression, 'inpath')}"
        overwrite = " OVERWRITE" if expression.args.get("overwrite") else ""
        this = f" INTO TABLE {self.sql(expression, 'this')}"
        partition = self.sql(expression, "partition")
        partition = f" {partition}" if partition else ""
        input_format = self.sql(expression, "input_format")
        input_format = f" INPUTFORMAT {input_format}" if input_format else ""
        serde = self.sql(expression, "serde")
        serde = f" SERDE {serde}" if serde else ""
        return f"LOAD DATA{local}{inpath}{overwrite}{this}{partition}{input_format}{serde}"

    def null_sql(self, *_) -> str:
        return "NULL"

    def boolean_sql(self, expression: exp.Boolean) -> str:
        return "TRUE" if expression.this else "FALSE"

    def order_sql(self, expression: exp.Order, flat: bool = False) -> str:
        this = self.sql(expression, "this")
        this = f"{this} " if this else this
        return self.op_expressions(f"{this}ORDER BY", expression, flat=this or flat)  # type: ignore

    def cluster_sql(self, expression: exp.Cluster) -> str:
        return self.op_expressions("CLUSTER BY", expression)

    def distribute_sql(self, expression: exp.Distribute) -> str:
        return self.op_expressions("DISTRIBUTE BY", expression)

    def sort_sql(self, expression: exp.Sort) -> str:
        return self.op_expressions("SORT BY", expression)

    def ordered_sql(self, expression: exp.Ordered) -> str:
        desc = expression.args.get("desc")
        asc = not desc

        nulls_first = expression.args.get("nulls_first")
        nulls_last = not nulls_first
        nulls_are_large = self.null_ordering == "nulls_are_large"
        nulls_are_small = self.null_ordering == "nulls_are_small"
        nulls_are_last = self.null_ordering == "nulls_are_last"

        sort_order = " DESC" if desc else ""
        nulls_sort_change = ""
        if nulls_first and (
            (asc and nulls_are_large) or (desc and nulls_are_small) or nulls_are_last
        ):
            nulls_sort_change = " NULLS FIRST"
        elif (
            nulls_last
            and ((asc and nulls_are_small) or (desc and nulls_are_large))
            and not nulls_are_last
        ):
            nulls_sort_change = " NULLS LAST"

        if nulls_sort_change and not self.NULL_ORDERING_SUPPORTED:
            self.unsupported(
                "Sorting in an ORDER BY on NULLS FIRST/NULLS LAST is not supported by this dialect"
            )
            nulls_sort_change = ""

        return f"{self.sql(expression, 'this')}{sort_order}{nulls_sort_change}"

    def matchrecognize_sql(self, expression: exp.MatchRecognize) -> str:
        partition = self.partition_by_sql(expression)
        order = self.sql(expression, "order")
        measures = self.expressions(expression, key="measures")
        measures = self.seg(f"MEASURES{self.seg(measures)}") if measures else ""
        rows = self.sql(expression, "rows")
        rows = self.seg(rows) if rows else ""
        after = self.sql(expression, "after")
        after = self.seg(after) if after else ""
        pattern = self.sql(expression, "pattern")
        pattern = self.seg(f"PATTERN ({pattern})") if pattern else ""
        definition_sqls = [
            f"{self.sql(definition, 'alias')} AS {self.sql(definition, 'this')}"
            for definition in expression.args.get("define", [])
        ]
        definitions = self.expressions(sqls=definition_sqls)
        define = self.seg(f"DEFINE{self.seg(definitions)}") if definitions else ""
        body = "".join(
            (
                partition,
                order,
                measures,
                rows,
                after,
                pattern,
                define,
            )
        )
        alias = self.sql(expression, "alias")
        alias = f" {alias}" if alias else ""
        return f"{self.seg('MATCH_RECOGNIZE')} {self.wrap(body)}{alias}"

    def query_modifiers(self, expression: exp.Expression, *sqls: str) -> str:
        limit = expression.args.get("limit")

        if self.LIMIT_FETCH == "LIMIT" and isinstance(limit, exp.Fetch):
            limit = exp.Limit(expression=limit.args.get("count"))
        elif self.LIMIT_FETCH == "FETCH" and isinstance(limit, exp.Limit):
            limit = exp.Fetch(direction="FIRST", count=limit.expression)

        fetch = isinstance(limit, exp.Fetch)

        return csv(
            *sqls,
            *[self.sql(join) for join in expression.args.get("joins") or []],
            self.sql(expression, "match"),
            *[self.sql(lateral) for lateral in expression.args.get("laterals") or []],
            self.sql(expression, "where"),
            self.sql(expression, "group"),
            self.sql(expression, "having"),
            *self.after_having_modifiers(expression),
            self.sql(expression, "order"),
            self.sql(expression, "offset") if fetch else self.sql(limit),
            self.sql(limit) if fetch else self.sql(expression, "offset"),
            *self.after_limit_modifiers(expression),
            sep="",
        )

    def after_having_modifiers(self, expression):
        return [
            self.sql(expression, "qualify"),
            self.seg("WINDOW ") + self.expressions(expression, key="windows", flat=True)
            if expression.args.get("windows")
            else "",
        ]

    def after_limit_modifiers(self, expression):
        locks = self.expressions(expression, key="locks", sep=" ")
        locks = f" {locks}" if locks else ""
        return [locks, self.sql(expression, "sample")]

    def select_sql(self, expression: exp.Select) -> str:
        hint = self.sql(expression, "hint")
        distinct = self.sql(expression, "distinct")
        distinct = f" {distinct}" if distinct else ""
        kind = expression.args.get("kind")
        kind = f" AS {kind}" if kind else ""
        expressions = self.expressions(expression)
        expressions = f"{self.sep()}{expressions}" if expressions else expressions
        sql = self.query_modifiers(
            expression,
            f"SELECT{hint}{distinct}{kind}{expressions}",
            self.sql(expression, "into", comment=False),
            self.sql(expression, "from", comment=False),
        )
        return self.prepend_ctes(expression, sql)

    def schema_sql(self, expression: exp.Schema) -> str:
        this = self.sql(expression, "this")
        this = f"{this} " if this else ""
        sql = f"({self.sep('')}{self.expressions(expression)}{self.seg(')', sep='')}"
        return f"{this}{sql}"

    def star_sql(self, expression: exp.Star) -> str:
        except_ = self.expressions(expression, key="except", flat=True)
        except_ = f"{self.seg(self.STAR_MAPPING['except'])} ({except_})" if except_ else ""
        replace = self.expressions(expression, key="replace", flat=True)
        replace = f"{self.seg(self.STAR_MAPPING['replace'])} ({replace})" if replace else ""
        return f"*{except_}{replace}"

    def parameter_sql(self, expression: exp.Parameter) -> str:
        this = self.sql(expression, "this")
        this = f"{{{this}}}" if expression.args.get("wrapped") else f"{this}"
        return f"{self.PARAMETER_TOKEN}{this}"

    def sessionparameter_sql(self, expression: exp.SessionParameter) -> str:
        this = self.sql(expression, "this")
        kind = expression.text("kind")
        if kind:
            kind = f"{kind}."
        return f"@@{kind}{this}"

    def placeholder_sql(self, expression: exp.Placeholder) -> str:
        return f":{expression.name}" if expression.name else "?"

    def subquery_sql(self, expression: exp.Subquery, sep: str = " AS ") -> str:
        alias = self.sql(expression, "alias")
        alias = f"{sep}{alias}" if alias else ""

        pivots = self.expressions(expression, key="pivots", sep=" ", flat=True)
        pivots = f" {pivots}" if pivots else ""

        sql = self.query_modifiers(expression, self.wrap(expression), alias, pivots)
        return self.prepend_ctes(expression, sql)

    def qualify_sql(self, expression: exp.Qualify) -> str:
        this = self.indent(self.sql(expression, "this"))
        return f"{self.seg('QUALIFY')}{self.sep()}{this}"

    def union_sql(self, expression: exp.Union) -> str:
        return self.prepend_ctes(
            expression,
            self.set_operation(expression, self.union_op(expression)),
        )

    def union_op(self, expression: exp.Union) -> str:
        kind = " DISTINCT" if self.EXPLICIT_UNION else ""
        kind = kind if expression.args.get("distinct") else " ALL"
        return f"UNION{kind}"

    def unnest_sql(self, expression: exp.Unnest) -> str:
        args = self.expressions(expression, flat=True)
        alias = expression.args.get("alias")
        if alias and self.unnest_column_only:
            columns = alias.columns
            alias = self.sql(columns[0]) if columns else ""
        else:
            alias = self.sql(expression, "alias")
        alias = f" AS {alias}" if alias else alias
        ordinality = " WITH ORDINALITY" if expression.args.get("ordinality") else ""
        offset = expression.args.get("offset")
        offset = f" WITH OFFSET AS {self.sql(offset)}" if offset else ""
        return f"UNNEST({args}){ordinality}{alias}{offset}"

    def where_sql(self, expression: exp.Where) -> str:
        this = self.indent(self.sql(expression, "this"))
        return f"{self.seg('WHERE')}{self.sep()}{this}"

    def window_sql(self, expression: exp.Window) -> str:
        this = self.sql(expression, "this")

        partition = self.partition_by_sql(expression)

        order = expression.args.get("order")
        order_sql = self.order_sql(order, flat=True) if order else ""

        partition_sql = partition + " " if partition and order else partition

        spec = expression.args.get("spec")
        spec_sql = " " + self.windowspec_sql(spec) if spec else ""

        alias = self.sql(expression, "alias")
        over = self.sql(expression, "over") or "OVER"
        this = f"{this} {'AS' if expression.arg_key == 'windows' else over}"

        first = expression.args.get("first")
        if first is not None:
            first = " FIRST " if first else " LAST "
        first = first or ""

        if not partition and not order and not spec and alias:
            return f"{this} {alias}"

        window_args = alias + first + partition_sql + order_sql + spec_sql

        return f"{this} ({window_args.strip()})"

    def partition_by_sql(self, expression: exp.Window | exp.MatchRecognize) -> str:
        partition = self.expressions(expression, key="partition_by", flat=True)
        return f"PARTITION BY {partition}" if partition else ""

    def windowspec_sql(self, expression: exp.WindowSpec) -> str:
        kind = self.sql(expression, "kind")
        start = csv(self.sql(expression, "start"), self.sql(expression, "start_side"), sep=" ")
        end = (
            csv(self.sql(expression, "end"), self.sql(expression, "end_side"), sep=" ")
            or "CURRENT ROW"
        )
        return f"{kind} BETWEEN {start} AND {end}"

    def withingroup_sql(self, expression: exp.WithinGroup) -> str:
        this = self.sql(expression, "this")
        expression_sql = self.sql(expression, "expression")[1:]  # order has a leading space
        return f"{this} WITHIN GROUP ({expression_sql})"

    def between_sql(self, expression: exp.Between) -> str:
        this = self.sql(expression, "this")
        low = self.sql(expression, "low")
        high = self.sql(expression, "high")
        return f"{this} BETWEEN {low} AND {high}"

    def bracket_sql(self, expression: exp.Bracket) -> str:
        expressions = apply_index_offset(expression.this, expression.expressions, self.index_offset)
        expressions_sql = ", ".join(self.sql(e) for e in expressions)

        return f"{self.sql(expression, 'this')}[{expressions_sql}]"

    def all_sql(self, expression: exp.All) -> str:
        return f"ALL {self.wrap(expression)}"

    def any_sql(self, expression: exp.Any) -> str:
        this = self.sql(expression, "this")
        if isinstance(expression.this, exp.Subqueryable):
            this = self.wrap(this)
        return f"ANY {this}"

    def exists_sql(self, expression: exp.Exists) -> str:
        return f"EXISTS{self.wrap(expression)}"

    def case_sql(self, expression: exp.Case) -> str:
        this = self.sql(expression, "this")
        statements = [f"CASE {this}" if this else "CASE"]

        for e in expression.args["ifs"]:
            statements.append(f"WHEN {self.sql(e, 'this')}")
            statements.append(f"THEN {self.sql(e, 'true')}")

        default = self.sql(expression, "default")

        if default:
            statements.append(f"ELSE {default}")

        statements.append("END")

        if self.pretty and self.text_width(statements) > self._max_text_width:
            return self.indent("\n".join(statements), skip_first=True, skip_last=True)

        return " ".join(statements)

    def constraint_sql(self, expression: exp.Constraint) -> str:
        this = self.sql(expression, "this")
        expressions = self.expressions(expression, flat=True)
        return f"CONSTRAINT {this} {expressions}"

    def nextvaluefor_sql(self, expression: exp.NextValueFor) -> str:
        order = expression.args.get("order")
        order = f" OVER ({self.order_sql(order, flat=True)})" if order else ""
        return f"NEXT VALUE FOR {self.sql(expression, 'this')}{order}"

    def extract_sql(self, expression: exp.Extract) -> str:
        this = self.sql(expression, "this")
        expression_sql = self.sql(expression, "expression")
        return f"EXTRACT({this} FROM {expression_sql})"

    def trim_sql(self, expression: exp.Trim) -> str:
        trim_type = self.sql(expression, "position")

        if trim_type == "LEADING":
            return self.func("LTRIM", expression.this)
        elif trim_type == "TRAILING":
            return self.func("RTRIM", expression.this)
        else:
            return self.func("TRIM", expression.this, expression.expression)

    def concat_sql(self, expression: exp.Concat) -> str:
        if len(expression.expressions) == 1:
            return self.sql(expression.expressions[0])
        return self.function_fallback_sql(expression)

    def check_sql(self, expression: exp.Check) -> str:
        this = self.sql(expression, key="this")
        return f"CHECK ({this})"

    def foreignkey_sql(self, expression: exp.ForeignKey) -> str:
        expressions = self.expressions(expression, flat=True)
        reference = self.sql(expression, "reference")
        reference = f" {reference}" if reference else ""
        delete = self.sql(expression, "delete")
        delete = f" ON DELETE {delete}" if delete else ""
        update = self.sql(expression, "update")
        update = f" ON UPDATE {update}" if update else ""
        return f"FOREIGN KEY ({expressions}){reference}{delete}{update}"

    def primarykey_sql(self, expression: exp.ForeignKey) -> str:
        expressions = self.expressions(expression, flat=True)
        options = self.expressions(expression, key="options", flat=True, sep=" ")
        options = f" {options}" if options else ""
        return f"PRIMARY KEY ({expressions}){options}"

    def unique_sql(self, expression: exp.Unique) -> str:
        columns = self.expressions(expression, key="expressions")
        return f"UNIQUE ({columns})"

    def if_sql(self, expression: exp.If) -> str:
        return self.case_sql(
            exp.Case(ifs=[expression.copy()], default=expression.args.get("false"))
        )

    def matchagainst_sql(self, expression: exp.MatchAgainst) -> str:
        modifier = expression.args.get("modifier")
        modifier = f" {modifier}" if modifier else ""
        return f"{self.func('MATCH', *expression.expressions)} AGAINST({self.sql(expression, 'this')}{modifier})"

    def jsonkeyvalue_sql(self, expression: exp.JSONKeyValue) -> str:
        return f"{self.sql(expression, 'this')}: {self.sql(expression, 'expression')}"

    def jsonobject_sql(self, expression: exp.JSONObject) -> str:
        expressions = self.expressions(expression)
        null_handling = expression.args.get("null_handling")
        null_handling = f" {null_handling}" if null_handling else ""
        unique_keys = expression.args.get("unique_keys")
        if unique_keys is not None:
            unique_keys = f" {'WITH' if unique_keys else 'WITHOUT'} UNIQUE KEYS"
        else:
            unique_keys = ""
        return_type = self.sql(expression, "return_type")
        return_type = f" RETURNING {return_type}" if return_type else ""
        format_json = " FORMAT JSON" if expression.args.get("format_json") else ""
        encoding = self.sql(expression, "encoding")
        encoding = f" ENCODING {encoding}" if encoding else ""
        return f"JSON_OBJECT({expressions}{null_handling}{unique_keys}{return_type}{format_json}{encoding})"

    def openjsoncolumndef_sql(self, expression: exp.OpenJSONColumnDef) -> str:
        this = self.sql(expression, "this")
        kind = self.sql(expression, "kind")
        path = self.sql(expression, "path")
        path = f" {path}" if path else ""
        as_json = " AS JSON" if expression.args.get("as_json") else ""
        return f"{this} {kind}{path}{as_json}"

    def openjson_sql(self, expression: exp.OpenJSON) -> str:
        this = self.sql(expression, "this")
        path = self.sql(expression, "path")
        path = f", {path}" if path else ""
        expressions = self.expressions(expression)
        with_ = (
            f" WITH ({self.seg(self.indent(expressions), sep='')}{self.seg(')', sep='')}"
            if expressions
            else ""
        )
        return f"OPENJSON({this}{path}){with_}"

    def in_sql(self, expression: exp.In) -> str:
        query = expression.args.get("query")
        unnest = expression.args.get("unnest")
        field = expression.args.get("field")
        is_global = " GLOBAL" if expression.args.get("is_global") else ""

        if query:
            in_sql = self.wrap(query)
        elif unnest:
            in_sql = self.in_unnest_op(unnest)
        elif field:
            in_sql = self.sql(field)
        else:
            in_sql = f"({self.expressions(expression, flat=True)})"

        return f"{self.sql(expression, 'this')}{is_global} IN {in_sql}"

    def in_unnest_op(self, unnest: exp.Unnest) -> str:
        return f"(SELECT {self.sql(unnest)})"

    def interval_sql(self, expression: exp.Interval) -> str:
        unit = self.sql(expression, "unit")
        if not self.INTERVAL_ALLOWS_PLURAL_FORM:
            unit = self.TIME_PART_SINGULARS.get(unit.lower(), unit)
        unit = f" {unit}" if unit else ""

        if self.SINGLE_STRING_INTERVAL:
            this = expression.this.name if expression.this else ""
            return f"INTERVAL '{this}{unit}'" if this else f"INTERVAL{unit}"

        this = self.sql(expression, "this")
        if this:
            unwrapped = isinstance(expression.this, self.UNWRAPPED_INTERVAL_VALUES)
            this = f" {this}" if unwrapped else f" ({this})"

        return f"INTERVAL{this}{unit}"

    def return_sql(self, expression: exp.Return) -> str:
        return f"RETURN {self.sql(expression, 'this')}"

    def reference_sql(self, expression: exp.Reference) -> str:
        this = self.sql(expression, "this")
        expressions = self.expressions(expression, flat=True)
        expressions = f"({expressions})" if expressions else ""
        options = self.expressions(expression, key="options", flat=True, sep=" ")
        options = f" {options}" if options else ""
        return f"REFERENCES {this}{expressions}{options}"

    def anonymous_sql(self, expression: exp.Anonymous) -> str:
        return self.func(expression.name, *expression.expressions)

    def paren_sql(self, expression: exp.Paren) -> str:
        if isinstance(expression.unnest(), exp.Select):
            sql = self.wrap(expression)
        else:
            sql = self.seg(self.indent(self.sql(expression, "this")), sep="")
            sql = f"({sql}{self.seg(')', sep='')}"

        return self.prepend_ctes(expression, sql)

    def neg_sql(self, expression: exp.Neg) -> str:
        # This makes sure we don't convert "- - 5" to "--5", which is a comment
        this_sql = self.sql(expression, "this")
        sep = " " if this_sql[0] == "-" else ""
        return f"-{sep}{this_sql}"

    def not_sql(self, expression: exp.Not) -> str:
        return f"NOT {self.sql(expression, 'this')}"

    def alias_sql(self, expression: exp.Alias) -> str:
        alias = self.sql(expression, "alias")
        alias = f" AS {alias}" if alias else ""
        return f"{self.sql(expression, 'this')}{alias}"

    def aliases_sql(self, expression: exp.Aliases) -> str:
        return f"{self.sql(expression, 'this')} AS ({self.expressions(expression, flat=True)})"

    def attimezone_sql(self, expression: exp.AtTimeZone) -> str:
        this = self.sql(expression, "this")
        zone = self.sql(expression, "zone")
        return f"{this} AT TIME ZONE {zone}"

    def add_sql(self, expression: exp.Add) -> str:
        return self.binary(expression, "+")

    def and_sql(self, expression: exp.And) -> str:
        return self.connector_sql(expression, "AND")

    def connector_sql(self, expression: exp.Connector, op: str) -> str:
        if not self.pretty:
            return self.binary(expression, op)

        sqls = tuple(
            self.maybe_comment(self.sql(e), e, e.parent.comments or []) if i != 1 else self.sql(e)
            for i, e in enumerate(expression.flatten(unnest=False))
        )

        sep = "\n" if self.text_width(sqls) > self._max_text_width else " "
        return f"{sep}{op} ".join(sqls)

    def bitwiseand_sql(self, expression: exp.BitwiseAnd) -> str:
        return self.binary(expression, "&")

    def bitwiseleftshift_sql(self, expression: exp.BitwiseLeftShift) -> str:
        return self.binary(expression, "<<")

    def bitwisenot_sql(self, expression: exp.BitwiseNot) -> str:
        return f"~{self.sql(expression, 'this')}"

    def bitwiseor_sql(self, expression: exp.BitwiseOr) -> str:
        return self.binary(expression, "|")

    def bitwiserightshift_sql(self, expression: exp.BitwiseRightShift) -> str:
        return self.binary(expression, ">>")

    def bitwisexor_sql(self, expression: exp.BitwiseXor) -> str:
        return self.binary(expression, "^")

    def cast_sql(self, expression: exp.Cast) -> str:
        return f"CAST({self.sql(expression, 'this')} AS {self.sql(expression, 'to')})"

    def currentdate_sql(self, expression: exp.CurrentDate) -> str:
        zone = self.sql(expression, "this")
        return f"CURRENT_DATE({zone})" if zone else "CURRENT_DATE"

    def collate_sql(self, expression: exp.Collate) -> str:
        return self.binary(expression, "COLLATE")

    def command_sql(self, expression: exp.Command) -> str:
        return f"{self.sql(expression, 'this').upper()} {expression.text('expression').strip()}"

    def comment_sql(self, expression: exp.Comment) -> str:
        this = self.sql(expression, "this")
        kind = expression.args["kind"]
        exists_sql = " IF EXISTS " if expression.args.get("exists") else " "
        expression_sql = self.sql(expression, "expression")
        return f"COMMENT{exists_sql}ON {kind} {this} IS {expression_sql}"

    def mergetreettlaction_sql(self, expression: exp.MergeTreeTTLAction) -> str:
        this = self.sql(expression, "this")
        delete = " DELETE" if expression.args.get("delete") else ""
        recompress = self.sql(expression, "recompress")
        recompress = f" RECOMPRESS {recompress}" if recompress else ""
        to_disk = self.sql(expression, "to_disk")
        to_disk = f" TO DISK {to_disk}" if to_disk else ""
        to_volume = self.sql(expression, "to_volume")
        to_volume = f" TO VOLUME {to_volume}" if to_volume else ""
        return f"{this}{delete}{recompress}{to_disk}{to_volume}"

    def mergetreettl_sql(self, expression: exp.MergeTreeTTL) -> str:
        where = self.sql(expression, "where")
        group = self.sql(expression, "group")
        aggregates = self.expressions(expression, key="aggregates")
        aggregates = self.seg("SET") + self.seg(aggregates) if aggregates else ""

        if not (where or group or aggregates) and len(expression.expressions) == 1:
            return f"TTL {self.expressions(expression, flat=True)}"

        return f"TTL{self.seg(self.expressions(expression))}{where}{group}{aggregates}"

    def transaction_sql(self, expression: exp.Transaction) -> str:
        return "BEGIN"

    def commit_sql(self, expression: exp.Commit) -> str:
        chain = expression.args.get("chain")
        if chain is not None:
            chain = " AND CHAIN" if chain else " AND NO CHAIN"

        return f"COMMIT{chain or ''}"

    def rollback_sql(self, expression: exp.Rollback) -> str:
        savepoint = expression.args.get("savepoint")
        savepoint = f" TO {savepoint}" if savepoint else ""
        return f"ROLLBACK{savepoint}"

    def altercolumn_sql(self, expression: exp.AlterColumn) -> str:
        this = self.sql(expression, "this")

        dtype = self.sql(expression, "dtype")
        if dtype:
            collate = self.sql(expression, "collate")
            collate = f" COLLATE {collate}" if collate else ""
            using = self.sql(expression, "using")
            using = f" USING {using}" if using else ""
            return f"ALTER COLUMN {this} TYPE {dtype}{collate}{using}"

        default = self.sql(expression, "default")
        if default:
            return f"ALTER COLUMN {this} SET DEFAULT {default}"

        if not expression.args.get("drop"):
            self.unsupported("Unsupported ALTER COLUMN syntax")

        return f"ALTER COLUMN {this} DROP DEFAULT"

    def renametable_sql(self, expression: exp.RenameTable) -> str:
        if not self.RENAME_TABLE_WITH_DB:
            # Remove db from tables
            expression = expression.transform(
                lambda n: exp.table_(n.this) if isinstance(n, exp.Table) else n
            )
        this = self.sql(expression, "this")
        return f"RENAME TO {this}"

    def altertable_sql(self, expression: exp.AlterTable) -> str:
        actions = expression.args["actions"]

        if isinstance(actions[0], exp.ColumnDef):
            actions = self.expressions(expression, key="actions", prefix="ADD COLUMN ")
        elif isinstance(actions[0], exp.Schema):
            actions = self.expressions(expression, key="actions", prefix="ADD COLUMNS ")
        elif isinstance(actions[0], exp.Delete):
            actions = self.expressions(expression, key="actions", flat=True)
        else:
            actions = self.expressions(expression, key="actions")

        exists = " IF EXISTS" if expression.args.get("exists") else ""
        return f"ALTER TABLE{exists} {self.sql(expression, 'this')} {actions}"

    def droppartition_sql(self, expression: exp.DropPartition) -> str:
        expressions = self.expressions(expression)
        exists = " IF EXISTS " if expression.args.get("exists") else " "
        return f"DROP{exists}{expressions}"

    def addconstraint_sql(self, expression: exp.AddConstraint) -> str:
        this = self.sql(expression, "this")
        expression_ = self.sql(expression, "expression")
        add_constraint = f"ADD CONSTRAINT {this}" if this else "ADD"

        enforced = expression.args.get("enforced")
        if enforced is not None:
            return f"{add_constraint} CHECK ({expression_}){' ENFORCED' if enforced else ''}"

        return f"{add_constraint} {expression_}"

    def distinct_sql(self, expression: exp.Distinct) -> str:
        this = self.expressions(expression, flat=True)
        this = f" {this}" if this else ""

        on = self.sql(expression, "on")
        on = f" ON {on}" if on else ""
        return f"DISTINCT{this}{on}"

    def ignorenulls_sql(self, expression: exp.IgnoreNulls) -> str:
        return f"{self.sql(expression, 'this')} IGNORE NULLS"

    def respectnulls_sql(self, expression: exp.RespectNulls) -> str:
        return f"{self.sql(expression, 'this')} RESPECT NULLS"

    def intdiv_sql(self, expression: exp.IntDiv) -> str:
        return self.sql(
            exp.Cast(
                this=exp.Div(this=expression.this, expression=expression.expression),
                to=exp.DataType(this=exp.DataType.Type.INT),
            )
        )

    def dpipe_sql(self, expression: exp.DPipe) -> str:
        return self.binary(expression, "||")

    def div_sql(self, expression: exp.Div) -> str:
        return self.binary(expression, "/")

    def overlaps_sql(self, expression: exp.Overlaps) -> str:
        return self.binary(expression, "OVERLAPS")

    def distance_sql(self, expression: exp.Distance) -> str:
        return self.binary(expression, "<->")

    def dot_sql(self, expression: exp.Dot) -> str:
        return f"{self.sql(expression, 'this')}.{self.sql(expression, 'expression')}"

    def eq_sql(self, expression: exp.EQ) -> str:
        return self.binary(expression, "=")

    def escape_sql(self, expression: exp.Escape) -> str:
        return self.binary(expression, "ESCAPE")

    def glob_sql(self, expression: exp.Glob) -> str:
        return self.binary(expression, "GLOB")

    def gt_sql(self, expression: exp.GT) -> str:
        return self.binary(expression, ">")

    def gte_sql(self, expression: exp.GTE) -> str:
        return self.binary(expression, ">=")

    def ilike_sql(self, expression: exp.ILike) -> str:
        return self.binary(expression, "ILIKE")

    def ilikeany_sql(self, expression: exp.ILikeAny) -> str:
        return self.binary(expression, "ILIKE ANY")

    def is_sql(self, expression: exp.Is) -> str:
        return self.binary(expression, "IS")

    def like_sql(self, expression: exp.Like) -> str:
        return self.binary(expression, "LIKE")

    def likeany_sql(self, expression: exp.LikeAny) -> str:
        return self.binary(expression, "LIKE ANY")

    def similarto_sql(self, expression: exp.SimilarTo) -> str:
        return self.binary(expression, "SIMILAR TO")

    def lt_sql(self, expression: exp.LT) -> str:
        return self.binary(expression, "<")

    def lte_sql(self, expression: exp.LTE) -> str:
        return self.binary(expression, "<=")

    def mod_sql(self, expression: exp.Mod) -> str:
        return self.binary(expression, "%")

    def mul_sql(self, expression: exp.Mul) -> str:
        return self.binary(expression, "*")

    def neq_sql(self, expression: exp.NEQ) -> str:
        return self.binary(expression, "<>")

    def nullsafeeq_sql(self, expression: exp.NullSafeEQ) -> str:
        return self.binary(expression, "IS NOT DISTINCT FROM")

    def nullsafeneq_sql(self, expression: exp.NullSafeNEQ) -> str:
        return self.binary(expression, "IS DISTINCT FROM")

    def or_sql(self, expression: exp.Or) -> str:
        return self.connector_sql(expression, "OR")

    def slice_sql(self, expression: exp.Slice) -> str:
        return self.binary(expression, ":")

    def sub_sql(self, expression: exp.Sub) -> str:
        return self.binary(expression, "-")

    def trycast_sql(self, expression: exp.TryCast) -> str:
        return f"TRY_CAST({self.sql(expression, 'this')} AS {self.sql(expression, 'to')})"

    def use_sql(self, expression: exp.Use) -> str:
        kind = self.sql(expression, "kind")
        kind = f" {kind}" if kind else ""
        this = self.sql(expression, "this")
        this = f" {this}" if this else ""
        return f"USE{kind}{this}"

    def binary(self, expression: exp.Binary, op: str) -> str:
        op = self.maybe_comment(op, comments=expression.comments)
        return f"{self.sql(expression, 'this')} {op} {self.sql(expression, 'expression')}"

    def function_fallback_sql(self, expression: exp.Func) -> str:
        args = []
        for arg_value in expression.args.values():
            if isinstance(arg_value, list):
                for value in arg_value:
                    args.append(value)
            else:
                args.append(arg_value)

        return self.func(expression.sql_name(), *args)

    def func(self, name: str, *args: t.Optional[exp.Expression | str]) -> str:
        return f"{self.normalize_func(name)}({self.format_args(*args)})"

    def format_args(self, *args: t.Optional[str | exp.Expression]) -> str:
        arg_sqls = tuple(self.sql(arg) for arg in args if arg is not None)
        if self.pretty and self.text_width(arg_sqls) > self._max_text_width:
            return self.indent("\n" + f",\n".join(arg_sqls) + "\n", skip_first=True, skip_last=True)
        return ", ".join(arg_sqls)

    def text_width(self, args: t.Iterable) -> int:
        return sum(len(arg) for arg in args)

    def format_time(self, expression: exp.Expression) -> t.Optional[str]:
        return format_time(self.sql(expression, "format"), self.time_mapping, self.time_trie)

    def expressions(
        self,
        expression: t.Optional[exp.Expression] = None,
        key: t.Optional[str] = None,
        sqls: t.Optional[t.List[str]] = None,
        flat: bool = False,
        indent: bool = True,
        sep: str = ", ",
        prefix: str = "",
    ) -> str:
        expressions = expression.args.get(key or "expressions") if expression else sqls

        if not expressions:
            return ""

        if flat:
            return sep.join(self.sql(e) for e in expressions)

        num_sqls = len(expressions)

        # These are calculated once in case we have the leading_comma / pretty option set, correspondingly
        pad = " " * self.pad
        stripped_sep = sep.strip()

        result_sqls = []
        for i, e in enumerate(expressions):
            sql = self.sql(e, comment=False)
            comments = self.maybe_comment("", e) if isinstance(e, exp.Expression) else ""

            if self.pretty:
                if self._leading_comma:
                    result_sqls.append(f"{sep if i > 0 else pad}{prefix}{sql}{comments}")
                else:
                    result_sqls.append(
                        f"{prefix}{sql}{stripped_sep if i + 1 < num_sqls else ''}{comments}"
                    )
            else:
                result_sqls.append(f"{prefix}{sql}{comments}{sep if i + 1 < num_sqls else ''}")

        result_sql = "\n".join(result_sqls) if self.pretty else "".join(result_sqls)
        return self.indent(result_sql, skip_first=False) if indent else result_sql

    def op_expressions(self, op: str, expression: exp.Expression, flat: bool = False) -> str:
        flat = flat or isinstance(expression.parent, exp.Properties)
        expressions_sql = self.expressions(expression, flat=flat)
        if flat:
            return f"{op} {expressions_sql}"
        return f"{self.seg(op)}{self.sep() if expressions_sql else ''}{expressions_sql}"

    def naked_property(self, expression: exp.Property) -> str:
        property_name = exp.Properties.PROPERTY_TO_NAME.get(expression.__class__)
        if not property_name:
            self.unsupported(f"Unsupported property {expression.__class__.__name__}")
        return f"{property_name} {self.sql(expression, 'this')}"

    def set_operation(self, expression: exp.Expression, op: str) -> str:
        this = self.sql(expression, "this")
        op = self.seg(op)
        return self.query_modifiers(
            expression, f"{this}{op}{self.sep()}{self.sql(expression, 'expression')}"
        )

    def tag_sql(self, expression: exp.Tag) -> str:
        return f"{expression.args.get('prefix')}{self.sql(expression.this)}{expression.args.get('postfix')}"

    def token_sql(self, token_type: TokenType) -> str:
        return self.TOKEN_MAPPING.get(token_type, token_type.name)

    def userdefinedfunction_sql(self, expression: exp.UserDefinedFunction) -> str:
        this = self.sql(expression, "this")
        expressions = self.no_identify(self.expressions, expression)
        expressions = (
            self.wrap(expressions) if expression.args.get("wrapped") else f" {expressions}"
        )
        return f"{this}{expressions}"

    def joinhint_sql(self, expression: exp.JoinHint) -> str:
        this = self.sql(expression, "this")
        expressions = self.expressions(expression, flat=True)
        return f"{this}({expressions})"

    def kwarg_sql(self, expression: exp.Kwarg) -> str:
        return self.binary(expression, "=>")

    def when_sql(self, expression: exp.When) -> str:
        matched = "MATCHED" if expression.args["matched"] else "NOT MATCHED"
        source = " BY SOURCE" if self.MATCHED_BY_SOURCE and expression.args.get("source") else ""
        condition = self.sql(expression, "condition")
        condition = f" AND {condition}" if condition else ""

        then_expression = expression.args.get("then")
        if isinstance(then_expression, exp.Insert):
            then = f"INSERT {self.sql(then_expression, 'this')}"
            if "expression" in then_expression.args:
                then += f" VALUES {self.sql(then_expression, 'expression')}"
        elif isinstance(then_expression, exp.Update):
            if isinstance(then_expression.args.get("expressions"), exp.Star):
                then = f"UPDATE {self.sql(then_expression, 'expressions')}"
            else:
                then = f"UPDATE SET {self.expressions(then_expression, flat=True)}"
        else:
            then = self.sql(then_expression)
        return f"WHEN {matched}{source}{condition} THEN {then}"

    def merge_sql(self, expression: exp.Merge) -> str:
        this = self.sql(expression, "this")
        using = f"USING {self.sql(expression, 'using')}"
        on = f"ON {self.sql(expression, 'on')}"
        return f"MERGE INTO {this} {using} {on} {self.expressions(expression, sep=' ')}"

    def tochar_sql(self, expression: exp.ToChar) -> str:
        if expression.args.get("format"):
            self.unsupported("Format argument unsupported for TO_CHAR/TO_VARCHAR function")

        return self.sql(exp.cast(expression.this, "text"))


def cached_generator(
    cache: t.Optional[t.Dict[int, str]] = None
) -> t.Callable[[exp.Expression], str]:
    """Returns a cached generator."""
    cache = {} if cache is None else cache
    generator = Generator(normalize=True, identify="safe")
    return lambda e: generator.generate(e, cache)
