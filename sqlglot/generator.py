from __future__ import annotations

import logging
import typing as t

from sqlglot import exp
from sqlglot.errors import ErrorLevel, UnsupportedError, concat_messages
from sqlglot.helper import apply_index_offset, csv
from sqlglot.time import format_time
from sqlglot.tokens import TokenType

logger = logging.getLogger("sqlglot")


class Generator:
    """
    Generator interprets the given syntax tree and produces a SQL string as an output.

    Args
        time_mapping (dict): the dictionary of custom time mappings in which the key
            represents a python time format and the output the target time format
        time_trie (trie): a trie of the time_mapping keys
        pretty (bool): if set to True the returned string will be formatted. Default: False.
        quote_start (str): specifies which starting character to use to delimit quotes. Default: '.
        quote_end (str): specifies which ending character to use to delimit quotes. Default: '.
        identifier_start (str): specifies which starting character to use to delimit identifiers. Default: ".
        identifier_end (str): specifies which ending character to use to delimit identifiers. Default: ".
        identify (bool): if set to True all identifiers will be delimited by the corresponding
            character.
        normalize (bool): if set to True all identifiers will lower cased
        escape (str): specifies an escape character. Default: '.
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
        comments: Whether or not to preserve comments in the ouput SQL code.
            Default: True
    """

    TRANSFORMS = {
        exp.DateAdd: lambda self, e: f"DATE_ADD({self.format_args(e.this, e.expression, e.args.get('unit'))})",
        exp.DateDiff: lambda self, e: f"DATEDIFF({self.format_args(e.this, e.expression)})",
        exp.TsOrDsAdd: lambda self, e: f"TS_OR_DS_ADD({self.format_args(e.this, e.expression, e.args.get('unit'))})",
        exp.VarMap: lambda self, e: f"MAP({self.format_args(e.args['keys'], e.args['values'])})",
        exp.CharacterSetProperty: lambda self, e: f"{'DEFAULT ' if e.args['default'] else ''}CHARACTER SET={self.sql(e, 'this')}",
        exp.LanguageProperty: lambda self, e: self.naked_property(e),
        exp.LocationProperty: lambda self, e: self.naked_property(e),
        exp.ReturnsProperty: lambda self, e: self.naked_property(e),
        exp.ExecuteAsProperty: lambda self, e: self.naked_property(e),
        exp.VolatilityProperty: lambda self, e: e.name,
    }

    # Whether 'CREATE ... TRANSIENT ... TABLE' is allowed
    CREATE_TRANSIENT = False

    # Whether or not null ordering is supported in order by
    NULL_ORDERING_SUPPORTED = True

    # Always do union distinct or union all
    EXPLICIT_UNION = False

    # Wrap derived values in parens, usually standard but spark doesn't support it
    WRAP_DERIVED_VALUES = True

    TYPE_MAPPING = {
        exp.DataType.Type.NCHAR: "CHAR",
        exp.DataType.Type.NVARCHAR: "VARCHAR",
    }

    TOKEN_MAPPING: t.Dict[TokenType, str] = {}

    STRUCT_DELIMITER = ("<", ">")

    ROOT_PROPERTIES = {
        exp.ReturnsProperty,
        exp.LanguageProperty,
        exp.DistStyleProperty,
        exp.DistKeyProperty,
        exp.SortKeyProperty,
        exp.LikeProperty,
    }

    WITH_PROPERTIES = {
        exp.Property,
        exp.FileFormatProperty,
        exp.PartitionedByProperty,
        exp.TableFormatProperty,
    }

    WITH_SEPARATED_COMMENTS = (exp.Select, exp.From, exp.Where, exp.Binary)

    __slots__ = (
        "time_mapping",
        "time_trie",
        "pretty",
        "configured_pretty",
        "quote_start",
        "quote_end",
        "identifier_start",
        "identifier_end",
        "identify",
        "normalize",
        "escape",
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
        "_replace_backslash",
        "_escaped_quote_end",
        "_leading_comma",
        "_max_text_width",
        "_comments",
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
        identify=False,
        normalize=False,
        escape=None,
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
        self.configured_pretty = self.pretty
        self.quote_start = quote_start or "'"
        self.quote_end = quote_end or "'"
        self.identifier_start = identifier_start or '"'
        self.identifier_end = identifier_end or '"'
        self.identify = identify
        self.normalize = normalize
        self.escape = escape or "'"
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
        self._replace_backslash = self.escape == "\\"
        self._escaped_quote_end = self.escape + self.quote_end
        self._leading_comma = leading_comma
        self._max_text_width = max_text_width
        self._comments = comments

    def generate(self, expression: t.Optional[exp.Expression]) -> str:
        """
        Generates a SQL string by interpreting the given syntax tree.

        Args
            expression: the syntax tree.

        Returns
            the SQL string.
        """
        self.unsupported_messages = []
        sql = self.sql(expression).strip()

        if self.unsupported_level == ErrorLevel.IGNORE:
            return sql

        if self.unsupported_level == ErrorLevel.WARN:
            for msg in self.unsupported_messages:
                logger.warning(msg)
        elif self.unsupported_level == ErrorLevel.RAISE and self.unsupported_messages:
            raise UnsupportedError(concat_messages(self.unsupported_messages, self.max_unsupported))

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

    def maybe_comment(self, sql: str, expression: exp.Expression) -> str:
        comments = expression.comments if self._comments else None

        if not comments:
            return sql

        sep = "\n" if self.pretty else " "
        comments = sep.join(f"/*{self.pad_comment(comment)}*/" for comment in comments)

        if isinstance(expression, self.WITH_SEPARATED_COMMENTS):
            return f"{comments}{self.sep()}{sql}"

        return f"{sql} {comments}"

    def wrap(self, expression: exp.Expression | str) -> str:
        this_sql = self.indent(
            self.sql(expression)
            if isinstance(expression, (exp.Select, exp.Union))
            else self.sql(expression, "this"),
            level=1,
            pad=0,
        )
        return f"({self.sep('')}{this_sql}{self.seg(')', sep='')}"

    def no_identify(self, func: t.Callable[[], str]) -> str:
        original = self.identify
        self.identify = False
        result = func()
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

        return self.maybe_comment(sql, expression) if self._comments and comment else sql

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
            part
            for part in [
                self.sql(expression, "db"),
                self.sql(expression, "table"),
                self.sql(expression, "this"),
            ]
            if part
        )

    def columndef_sql(self, expression: exp.ColumnDef) -> str:
        column = self.sql(expression, "this")
        kind = self.sql(expression, "kind")
        constraints = self.expressions(expression, key="constraints", sep=" ", flat=True)
        exists = "IF NOT EXISTS " if expression.args.get("exists") else ""

        if not constraints:
            return f"{exists}{column} {kind}"
        return f"{exists}{column} {kind} {constraints}"

    def columnconstraint_sql(self, expression: exp.ColumnConstraint) -> str:
        this = self.sql(expression, "this")
        kind_sql = self.sql(expression, "kind")
        return f"CONSTRAINT {this} {kind_sql}" if this else kind_sql

    def autoincrementcolumnconstraint_sql(self, _) -> str:
        return self.token_sql(TokenType.AUTO_INCREMENT)

    def checkcolumnconstraint_sql(self, expression: exp.CheckColumnConstraint) -> str:
        this = self.sql(expression, "this")
        return f"CHECK ({this})"

    def commentcolumnconstraint_sql(self, expression: exp.CommentColumnConstraint) -> str:
        comment = self.sql(expression, "this")
        return f"COMMENT {comment}"

    def collatecolumnconstraint_sql(self, expression: exp.CollateColumnConstraint) -> str:
        collate = self.sql(expression, "this")
        return f"COLLATE {collate}"

    def encodecolumnconstraint_sql(self, expression: exp.EncodeColumnConstraint) -> str:
        encode = self.sql(expression, "this")
        return f"ENCODE {encode}"

    def defaultcolumnconstraint_sql(self, expression: exp.DefaultColumnConstraint) -> str:
        default = self.sql(expression, "this")
        return f"DEFAULT {default}"

    def generatedasidentitycolumnconstraint_sql(
        self, expression: exp.GeneratedAsIdentityColumnConstraint
    ) -> str:
        return f"GENERATED {'ALWAYS' if expression.this else 'BY DEFAULT'} AS IDENTITY"

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
        this = self.sql(expression, "this")
        kind = self.sql(expression, "kind").upper()
        expression_sql = self.sql(expression, "expression")
        expression_sql = f"AS{self.sep()}{expression_sql}" if expression_sql else ""
        temporary = " TEMPORARY" if expression.args.get("temporary") else ""
        transient = (
            " TRANSIENT" if self.CREATE_TRANSIENT and expression.args.get("transient") else ""
        )
        external = " EXTERNAL" if expression.args.get("external") else ""
        replace = " OR REPLACE" if expression.args.get("replace") else ""
        exists_sql = " IF NOT EXISTS" if expression.args.get("exists") else ""
        unique = " UNIQUE" if expression.args.get("unique") else ""
        materialized = " MATERIALIZED" if expression.args.get("materialized") else ""
        properties = self.sql(expression, "properties")

        modifiers = "".join(
            (
                replace,
                temporary,
                transient,
                external,
                unique,
                materialized,
            )
        )
        expression_sql = f"CREATE{modifiers} {kind}{exists_sql} {this}{properties} {expression_sql}"
        return self.prepend_ctes(expression, expression_sql)

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
        return self.sql(expression, "this")

    def hexstring_sql(self, expression: exp.HexString) -> str:
        return self.sql(expression, "this")

    def datatype_sql(self, expression: exp.DataType) -> str:
        type_value = expression.this
        type_sql = self.TYPE_MAPPING.get(type_value, type_value.value)
        nested = ""
        interior = self.expressions(expression, flat=True)
        if interior:
            nested = (
                f"{self.STRUCT_DELIMITER[0]}{interior}{self.STRUCT_DELIMITER[1]}"
                if expression.args.get("nested")
                else f"({interior})"
            )
        return f"{type_sql}{nested}"

    def directory_sql(self, expression: exp.Directory) -> str:
        local = "LOCAL " if expression.args.get("local") else ""
        row_format = self.sql(expression, "row_format")
        row_format = f" {row_format}" if row_format else ""
        return f"{local}DIRECTORY {self.sql(expression, 'this')}{row_format}"

    def delete_sql(self, expression: exp.Delete) -> str:
        this = self.sql(expression, "this")
        using_sql = (
            f" USING {self.expressions(expression, 'using', sep=', USING ')}"
            if expression.args.get("using")
            else ""
        )
        where_sql = self.sql(expression, "where")
        sql = f"DELETE FROM {this}{using_sql}{where_sql}"
        return self.prepend_ctes(expression, sql)

    def drop_sql(self, expression: exp.Drop) -> str:
        this = self.sql(expression, "this")
        kind = expression.args["kind"]
        exists_sql = " IF EXISTS " if expression.args.get("exists") else " "
        temporary = " TEMPORARY" if expression.args.get("temporary") else ""
        materialized = " MATERIALIZED" if expression.args.get("materialized") else ""
        cascade = " CASCADE" if expression.args.get("cascade") else ""
        return f"DROP{temporary}{materialized} {kind}{exists_sql}{this}{cascade}"

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
        return f"{self.seg('FETCH')}{direction}{count} ROWS ONLY"

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
        text = text.lower() if self.normalize else text
        if expression.args.get("quoted") or self.identify:
            text = f"{self.identifier_start}{text}{self.identifier_end}"
        return text

    def partition_sql(self, expression: exp.Partition) -> str:
        keys = csv(
            *[
                f"""{prop.name}='{prop.text("value")}'""" if prop.text("value") else prop.name
                for prop in expression.this
            ]
        )
        return f"PARTITION({keys})"

    def properties_sql(self, expression: exp.Properties) -> str:
        root_properties = []
        with_properties = []

        for p in expression.expressions:
            p_class = p.__class__
            if p_class in self.WITH_PROPERTIES:
                with_properties.append(p)
            elif p_class in self.ROOT_PROPERTIES:
                root_properties.append(p)

        return self.root_properties(
            exp.Properties(expressions=root_properties)
        ) + self.with_properties(exp.Properties(expressions=with_properties))

    def root_properties(self, properties: exp.Properties) -> str:
        if properties.expressions:
            return self.sep() + self.expressions(properties, indent=False, sep=" ")
        return ""

    def properties(self, properties: exp.Properties, prefix: str = "", sep: str = ", ") -> str:
        if properties.expressions:
            expressions = self.expressions(properties, sep=sep, indent=False)
            return f"{prefix}{' ' if prefix else ''}{self.wrap(expressions)}"
        return ""

    def with_properties(self, properties: exp.Properties) -> str:
        return self.properties(properties, prefix=self.seg("WITH"))

    def property_sql(self, expression: exp.Property) -> str:
        property_cls = expression.__class__
        if property_cls == exp.Property:
            return f"{expression.name}={self.sql(expression, 'value')}"

        property_name = exp.Properties.PROPERTY_TO_NAME.get(property_cls)
        if not property_name:
            self.unsupported(f"Unsupported property {property_name}")

        return f"{property_name}={self.sql(expression, 'this')}"

    def likeproperty_sql(self, expression: exp.LikeProperty) -> str:
        options = " ".join(f"{e.name} {self.sql(e, 'value')}" for e in expression.expressions)
        options = f" {options}" if options else ""
        return f"LIKE {self.sql(expression, 'this')}{options}"

    def insert_sql(self, expression: exp.Insert) -> str:
        overwrite = expression.args.get("overwrite")

        if isinstance(expression.this, exp.Directory):
            this = "OVERWRITE " if overwrite else "INTO "
        else:
            this = "OVERWRITE TABLE " if overwrite else "INTO "

        this = f"{this}{self.sql(expression, 'this')}"
        exists = " IF EXISTS " if expression.args.get("exists") else " "
        partition_sql = (
            self.sql(expression, "partition") if expression.args.get("partition") else ""
        )
        expression_sql = self.sql(expression, "expression")
        sep = self.sep() if partition_sql else ""
        sql = f"INSERT {this}{exists}{partition_sql}{sep}{expression_sql}"
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
        laterals = self.expressions(expression, key="laterals", sep="")
        joins = self.expressions(expression, key="joins", sep="")
        pivots = self.expressions(expression, key="pivots", sep="")

        if alias and pivots:
            pivots = f"{pivots}{alias}"
            alias = ""

        return f"{table}{alias}{laterals}{joins}{pivots}"

    def tablesample_sql(self, expression: exp.TableSample) -> str:
        if self.alias_post_tablesample and expression.this.alias:
            this = self.sql(expression.this, "this")
            alias = f" AS {self.sql(expression.this, 'alias')}"
        else:
            this = self.sql(expression, "this")
            alias = ""
        method = self.sql(expression, "method")
        method = f" {method.upper()} " if method else ""
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
        seed = self.sql(expression, "seed")
        seed = f" SEED ({seed})" if seed else ""
        return f"{this} TABLESAMPLE{method}({bucket}{percent}{rows}{size}){seed}{alias}"

    def pivot_sql(self, expression: exp.Pivot) -> str:
        this = self.sql(expression, "this")
        unpivot = expression.args.get("unpivot")
        direction = "UNPIVOT" if unpivot else "PIVOT"
        expressions = self.expressions(expression, key="expressions")
        field = self.sql(expression, "field")
        return f"{this} {direction}({expressions} FOR {field})"

    def tuple_sql(self, expression: exp.Tuple) -> str:
        return f"({self.expressions(expression, flat=True)})"

    def update_sql(self, expression: exp.Update) -> str:
        this = self.sql(expression, "this")
        set_sql = self.expressions(expression, flat=True)
        from_sql = self.sql(expression, "from")
        where_sql = self.sql(expression, "where")
        sql = f"UPDATE {this} SET {set_sql}{from_sql}{where_sql}"
        return self.prepend_ctes(expression, sql)

    def values_sql(self, expression: exp.Values) -> str:
        alias = self.sql(expression, "alias")
        args = self.expressions(expression)
        if not alias:
            return f"VALUES{self.seg('')}{args}"
        alias = f" AS {alias}" if alias else alias
        if self.WRAP_DERIVED_VALUES:
            return f"(VALUES{self.seg('')}{args}){alias}"
        return f"VALUES{self.seg('')}{args}{alias}"

    def var_sql(self, expression: exp.Var) -> str:
        return self.sql(expression, "this")

    def into_sql(self, expression: exp.Into) -> str:
        temporary = " TEMPORARY" if expression.args.get("temporary") else ""
        unlogged = " UNLOGGED" if expression.args.get("unlogged") else ""
        return f"{self.seg('INTO')}{temporary or unlogged} {self.sql(expression, 'this')}"

    def from_sql(self, expression: exp.From) -> str:
        expressions = self.expressions(expression, flat=True)
        return f"{self.seg('FROM')} {expressions}"

    def group_sql(self, expression: exp.Group) -> str:
        group_by = self.op_expressions("GROUP BY", expression)
        grouping_sets = self.expressions(expression, key="grouping_sets", indent=False)
        grouping_sets = (
            f"{self.seg('GROUPING SETS')} {self.wrap(grouping_sets)}" if grouping_sets else ""
        )
        cube = self.expressions(expression, key="cube", indent=False)
        cube = f"{self.seg('CUBE')} {self.wrap(cube)}" if cube else ""
        rollup = self.expressions(expression, key="rollup", indent=False)
        rollup = f"{self.seg('ROLLUP')} {self.wrap(rollup)}" if rollup else ""
        return f"{group_by}{grouping_sets}{cube}{rollup}"

    def having_sql(self, expression: exp.Having) -> str:
        this = self.indent(self.sql(expression, "this"))
        return f"{self.seg('HAVING')}{self.sep()}{this}"

    def join_sql(self, expression: exp.Join) -> str:
        op_sql = self.seg(
            " ".join(
                op
                for op in (
                    "NATURAL" if expression.args.get("natural") else None,
                    expression.side,
                    expression.kind,
                    "JOIN",
                )
                if op
            )
        )
        on_sql = self.sql(expression, "on")
        using = expression.args.get("using")

        if not on_sql and using:
            on_sql = csv(*(self.sql(column) for column in using))

        if on_sql:
            on_sql = self.indent(on_sql, skip_first=True)
            space = self.seg(" " * self.pad) if self.pretty else " "
            if using:
                on_sql = f"{space}USING ({on_sql})"
            else:
                on_sql = f"{space}ON {on_sql}"

        expression_sql = self.sql(expression, "expression")
        this_sql = self.sql(expression, "this")
        return f"{expression_sql}{op_sql} {this_sql}{on_sql}"

    def lambda_sql(self, expression: exp.Lambda, arrow_sep: str = "->") -> str:
        args = self.expressions(expression, flat=True)
        args = f"({args})" if len(args.split(",")) > 1 else args
        return self.no_identify(lambda: f"{args} {arrow_sep} {self.sql(expression, 'this')}")

    def lateral_sql(self, expression: exp.Lateral) -> str:
        this = self.sql(expression, "this")

        if isinstance(expression.this, exp.Subquery):
            return f"LATERAL {this}"

        alias = expression.args["alias"]
        table = alias.name
        columns = self.expressions(alias, key="columns", flat=True)

        if expression.args.get("view"):
            table = f" {table}" if table else table
            columns = f" AS {columns}" if columns else ""
            op_sql = self.seg(f"LATERAL VIEW{' OUTER' if expression.args.get('outer') else ''}")
            return f"{op_sql}{self.sep()}{this}{table}{columns}"

        table = f" AS {table}" if table else table
        columns = f"({columns})" if columns else ""
        return f"LATERAL {this}{table}{columns}"

    def limit_sql(self, expression: exp.Limit) -> str:
        this = self.sql(expression, "this")
        return f"{this}{self.seg('LIMIT')} {self.sql(expression, 'expression')}"

    def offset_sql(self, expression: exp.Offset) -> str:
        this = self.sql(expression, "this")
        return f"{this}{self.seg('OFFSET')} {self.sql(expression, 'expression')}"

    def literal_sql(self, expression: exp.Literal) -> str:
        text = expression.this or ""
        if expression.is_string:
            if self._replace_backslash:
                text = text.replace("\\", "\\\\")
            text = text.replace(self.quote_end, self._escaped_quote_end)
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

    def query_modifiers(self, expression: exp.Expression, *sqls: str) -> str:
        return csv(
            *sqls,
            *[self.sql(sql) for sql in expression.args.get("joins", [])],
            *[self.sql(sql) for sql in expression.args.get("laterals", [])],
            self.sql(expression, "where"),
            self.sql(expression, "group"),
            self.sql(expression, "having"),
            self.sql(expression, "qualify"),
            self.sql(expression, "window"),
            self.sql(expression, "distribute"),
            self.sql(expression, "sort"),
            self.sql(expression, "cluster"),
            self.sql(expression, "order"),
            self.sql(expression, "limit"),
            self.sql(expression, "offset"),
            sep="",
        )

    def select_sql(self, expression: exp.Select) -> str:
        hint = self.sql(expression, "hint")
        distinct = self.sql(expression, "distinct")
        distinct = f" {distinct}" if distinct else ""
        expressions = self.expressions(expression)
        expressions = f"{self.sep()}{expressions}" if expressions else expressions
        sql = self.query_modifiers(
            expression,
            f"SELECT{hint}{distinct}{expressions}",
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
        except_ = f"{self.seg('EXCEPT')} ({except_})" if except_ else ""
        replace = self.expressions(expression, key="replace", flat=True)
        replace = f"{self.seg('REPLACE')} ({replace})" if replace else ""
        return f"*{except_}{replace}"

    def structkwarg_sql(self, expression: exp.StructKwarg) -> str:
        return f"{self.sql(expression, 'this')} {self.sql(expression, 'expression')}"

    def parameter_sql(self, expression: exp.Parameter) -> str:
        return f"@{self.sql(expression, 'this')}"

    def sessionparameter_sql(self, expression: exp.SessionParameter) -> str:
        this = self.sql(expression, "this")
        kind = expression.text("kind")
        if kind:
            kind = f"{kind}."
        return f"@@{kind}{this}"

    def placeholder_sql(self, expression: exp.Placeholder) -> str:
        return f":{expression.name}" if expression.name else "?"

    def subquery_sql(self, expression: exp.Subquery) -> str:
        alias = self.sql(expression, "alias")

        sql = self.query_modifiers(
            expression,
            self.wrap(expression),
            self.expressions(expression, key="pivots", sep=" "),
            f" AS {alias}" if alias else "",
        )

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
        return f"UNNEST({args}){ordinality}{alias}"

    def where_sql(self, expression: exp.Where) -> str:
        this = self.indent(self.sql(expression, "this"))
        return f"{self.seg('WHERE')}{self.sep()}{this}"

    def window_sql(self, expression: exp.Window) -> str:
        this = self.sql(expression, "this")

        partition = self.expressions(expression, key="partition_by", flat=True)
        partition = f"PARTITION BY {partition}" if partition else ""

        order = expression.args.get("order")
        order_sql = self.order_sql(order, flat=True) if order else ""

        partition_sql = partition + " " if partition and order else partition

        spec = expression.args.get("spec")
        spec_sql = " " + self.window_spec_sql(spec) if spec else ""

        alias = self.sql(expression, "alias")

        if expression.arg_key == "window":
            this = this = f"{self.seg('WINDOW')} {this} AS"
        else:
            this = f"{this} OVER"

        if not partition and not order and not spec and alias:
            return f"{this} {alias}"

        return f"{this} ({alias}{partition_sql}{order_sql}{spec_sql})"

    def window_spec_sql(self, expression: exp.WindowSpec) -> str:
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
        expressions = apply_index_offset(expression.expressions, self.index_offset)
        expressions_sql = ", ".join(self.sql(e) for e in expressions)

        return f"{self.sql(expression, 'this')}[{expressions_sql}]"

    def all_sql(self, expression: exp.All) -> str:
        return f"ALL {self.wrap(expression)}"

    def any_sql(self, expression: exp.Any) -> str:
        return f"ANY {self.wrap(expression)}"

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

    def extract_sql(self, expression: exp.Extract) -> str:
        this = self.sql(expression, "this")
        expression_sql = self.sql(expression, "expression")
        return f"EXTRACT({this} FROM {expression_sql})"

    def trim_sql(self, expression: exp.Trim) -> str:
        target = self.sql(expression, "this")
        trim_type = self.sql(expression, "position")

        if trim_type == "LEADING":
            return f"LTRIM({target})"
        elif trim_type == "TRAILING":
            return f"RTRIM({target})"
        else:
            return f"TRIM({target})"

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

    def unique_sql(self, expression: exp.Unique) -> str:
        columns = self.expressions(expression, key="expressions")
        return f"UNIQUE ({columns})"

    def if_sql(self, expression: exp.If) -> str:
        return self.case_sql(
            exp.Case(ifs=[expression.copy()], default=expression.args.get("false"))
        )

    def in_sql(self, expression: exp.In) -> str:
        query = expression.args.get("query")
        unnest = expression.args.get("unnest")
        field = expression.args.get("field")
        if query:
            in_sql = self.wrap(query)
        elif unnest:
            in_sql = self.in_unnest_op(unnest)
        elif field:
            in_sql = self.sql(field)
        else:
            in_sql = f"({self.expressions(expression, flat=True)})"
        return f"{self.sql(expression, 'this')} IN {in_sql}"

    def in_unnest_op(self, unnest: exp.Unnest) -> str:
        return f"(SELECT {self.sql(unnest)})"

    def interval_sql(self, expression: exp.Interval) -> str:
        unit = self.sql(expression, "unit")
        unit = f" {unit}" if unit else ""
        return f"INTERVAL {self.sql(expression, 'this')}{unit}"

    def reference_sql(self, expression: exp.Reference) -> str:
        this = self.sql(expression, "this")
        expressions = self.expressions(expression, flat=True)
        return f"REFERENCES {this}({expressions})"

    def anonymous_sql(self, expression: exp.Anonymous) -> str:
        args = self.format_args(*expression.expressions)
        return f"{self.normalize_func(self.sql(expression, 'this'))}({args})"

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
        to_sql = self.sql(expression, "alias")
        to_sql = f" AS {to_sql}" if to_sql else ""
        return f"{self.sql(expression, 'this')}{to_sql}"

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

        sqls = tuple(self.sql(e) for e in expression.flatten(unnest=False))
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

    def transaction_sql(self, *_) -> str:
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

    def altertable_sql(self, expression: exp.AlterTable) -> str:
        actions = expression.args["actions"]

        if isinstance(actions[0], exp.ColumnDef):
            actions = self.expressions(expression, "actions", prefix="ADD COLUMN ")
        elif isinstance(actions[0], exp.Schema):
            actions = self.expressions(expression, "actions", prefix="ADD COLUMNS ")
        elif isinstance(actions[0], exp.Drop):
            actions = self.expressions(expression, "actions")
        elif isinstance(actions[0], exp.AlterColumn):
            actions = self.sql(actions[0])
        else:
            self.unsupported(f"Unsupported ALTER TABLE action {actions[0].__class__.__name__}")

        exists = " IF EXISTS" if expression.args.get("exists") else ""
        return f"ALTER TABLE{exists} {self.sql(expression, 'this')} {actions}"

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

    def distance_sql(self, expression: exp.Distance) -> str:
        return self.binary(expression, "<->")

    def dot_sql(self, expression: exp.Dot) -> str:
        return f"{self.sql(expression, 'this')}.{self.sql(expression, 'expression')}"

    def eq_sql(self, expression: exp.EQ) -> str:
        return self.binary(expression, "=")

    def escape_sql(self, expression: exp.Escape) -> str:
        return self.binary(expression, "ESCAPE")

    def gt_sql(self, expression: exp.GT) -> str:
        return self.binary(expression, ">")

    def gte_sql(self, expression: exp.GTE) -> str:
        return self.binary(expression, ">=")

    def ilike_sql(self, expression: exp.ILike) -> str:
        return self.binary(expression, "ILIKE")

    def is_sql(self, expression: exp.Is) -> str:
        return self.binary(expression, "IS")

    def like_sql(self, expression: exp.Like) -> str:
        return self.binary(expression, "LIKE")

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

    def sub_sql(self, expression: exp.Sub) -> str:
        return self.binary(expression, "-")

    def trycast_sql(self, expression: exp.TryCast) -> str:
        return f"TRY_CAST({self.sql(expression, 'this')} AS {self.sql(expression, 'to')})"

    def use_sql(self, expression: exp.Use) -> str:
        return f"USE {self.sql(expression, 'this')}"

    def binary(self, expression: exp.Binary, op: str) -> str:
        return f"{self.sql(expression, 'this')} {op} {self.sql(expression, 'expression')}"

    def function_fallback_sql(self, expression: exp.Func) -> str:
        args = []
        for arg_value in expression.args.values():
            if isinstance(arg_value, list):
                for value in arg_value:
                    args.append(value)
            else:
                args.append(arg_value)

        return f"{self.normalize_func(expression.sql_name())}({self.format_args(*args)})"

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
        expression: exp.Expression,
        key: t.Optional[str] = None,
        flat: bool = False,
        indent: bool = True,
        sep: str = ", ",
        prefix: str = "",
    ) -> str:
        expressions = expression.args.get(key or "expressions")

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
            comments = self.maybe_comment("", e)

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

    def token_sql(self, token_type: TokenType) -> str:
        return self.TOKEN_MAPPING.get(token_type, token_type.name)

    def userdefinedfunction_sql(self, expression: exp.UserDefinedFunction) -> str:
        this = self.sql(expression, "this")
        expressions = self.no_identify(lambda: self.expressions(expression))
        return f"{this}({expressions})"

    def userdefinedfunctionkwarg_sql(self, expression: exp.UserDefinedFunctionKwarg) -> str:
        this = self.sql(expression, "this")
        kind = self.sql(expression, "kind")
        return f"{this} {kind}"

    def joinhint_sql(self, expression: exp.JoinHint) -> str:
        this = self.sql(expression, "this")
        expressions = self.expressions(expression, flat=True)
        return f"{this}({expressions})"

    def kwarg_sql(self, expression: exp.Kwarg) -> str:
        return self.binary(expression, "=>")

    def when_sql(self, expression: exp.When) -> str:
        this = self.sql(expression, "this")
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
        return f"WHEN {this} THEN {then}"

    def merge_sql(self, expression: exp.Merge) -> str:
        this = self.sql(expression, "this")
        using = f"USING {self.sql(expression, 'using')}"
        on = f"ON {self.sql(expression, 'on')}"
        return f"MERGE INTO {this} {using} {on} {self.expressions(expression, sep=' ')}"
