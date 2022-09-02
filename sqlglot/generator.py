import logging

from sqlglot import exp
from sqlglot.errors import ErrorLevel, UnsupportedError, concat_errors
from sqlglot.helper import apply_index_offset, csv, ensure_list
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
        identifier_start (str): specifies which starting character to use to delimit identifiers. Default: ".
        identifier_end (str): specifies which ending character to use to delimit identifiers. Default: ".
        identify (bool): if set to True all identifiers will be delimited by the corresponding
            character.
        normalize (bool): if set to True all identifiers will lower cased
        quote (str): specifies a character which should be treated as a quote (eg. to delimit
            literals). Default: '.
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
    """

    TRANSFORMS = {
        exp.AnonymousProperty: lambda self, e: self.property_sql(e),
        exp.AutoIncrementProperty: lambda self, e: f"AUTO_INCREMENT={self.sql(e, 'value')}",
        exp.CharacterSetProperty: lambda self, e: f"{'DEFAULT ' if e.args['default'] else ''}CHARACTER SET={self.sql(e, 'value')}",
        exp.CollateProperty: lambda self, e: f"COLLATE={self.sql(e, 'value')}",
        exp.DateAdd: lambda self, e: f"DATE_ADD({self.sql(e, 'this')}, {self.sql(e, 'expression')}, {self.sql(e, 'unit')})",
        exp.DateDiff: lambda self, e: f"DATE_DIFF({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
        exp.EngineProperty: lambda self, e: f"ENGINE={self.sql(e, 'value')}",
        exp.FileFormatProperty: lambda self, e: f"FORMAT={self.sql(e, 'value')}",
        exp.LocationProperty: lambda self, e: f"LOCATION {self.sql(e, 'value')}",
        exp.PartitionedByProperty: lambda self, e: f"PARTITIONED_BY={self.sql(e.args['value'])}",
        exp.SchemaCommentProperty: lambda self, e: f"COMMENT={self.sql(e, 'value')}",
        exp.TableFormatProperty: lambda self, e: f"TABLE_FORMAT={self.sql(e, 'value')}",
        exp.TsOrDsAdd: lambda self, e: f"TS_OR_DS_ADD({self.sql(e, 'this')}, {self.sql(e, 'expression')}, {self.sql(e, 'unit')})",
    }

    NULL_ORDERING_SUPPORTED = True

    TYPE_MAPPING = {
        exp.DataType.Type.NCHAR: "CHAR",
        exp.DataType.Type.NVARCHAR: "VARCHAR",
    }

    TOKEN_MAPPING = {}

    STRUCT_DELIMITER = ("<", ">")

    ROOT_PROPERTIES = [
        exp.AutoIncrementProperty,
        exp.CharacterSetProperty,
        exp.CollateProperty,
        exp.EngineProperty,
        exp.SchemaCommentProperty,
    ]
    WITH_PROPERTIES = [
        exp.AnonymousProperty,
        exp.FileFormatProperty,
        exp.PartitionedByProperty,
        exp.TableFormatProperty,
    ]

    __slots__ = (
        "time_mapping",
        "time_trie",
        "pretty",
        "configured_pretty",
        "identifier_start",
        "identifier_end",
        "identify",
        "normalize",
        "quote",
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
        "_escaped_quote",
    )

    def __init__(
        self,
        time_mapping=None,
        time_trie=None,
        pretty=None,
        identifier_start=None,
        identifier_end=None,
        identify=False,
        normalize=False,
        quote=None,
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
    ):
        # pylint: disable=too-many-arguments
        import sqlglot

        self.time_mapping = time_mapping or {}
        self.time_trie = time_trie
        self.pretty = pretty if pretty is not None else sqlglot.pretty
        self.configured_pretty = self.pretty
        self.identifier_start = identifier_start or '"'
        self.identifier_end = identifier_end or '"'
        self.identify = identify
        self.normalize = normalize
        self.quote = quote or "'"
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
        self._escaped_quote = self.escape + self.quote

    def generate(self, expression):
        """
        Generates a SQL string by interpreting the given syntax tree.

        Args
            expression (Expression): the syntax tree.

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
            raise UnsupportedError(
                concat_errors(self.unsupported_messages, self.max_unsupported)
            )

        return sql

    def unsupported(self, message):
        if self.unsupported_level == ErrorLevel.IMMEDIATE:
            raise UnsupportedError(message)
        self.unsupported_messages.append(message)

    def sep(self, sep=" "):
        return f"{sep.strip()}\n" if self.pretty else sep

    def seg(self, sql, sep=" "):
        return f"{self.sep(sep)}{sql}"

    def wrap(self, expression):
        this_sql = self.indent(
            self.sql(expression)
            if isinstance(expression, (exp.Select, exp.Union))
            else self.sql(expression, "this"),
            level=1,
            pad=0,
        )
        return f"({self.sep('')}{this_sql}{self.seg(')', sep='')}"

    def no_identify(self, func):
        original = self.identify
        self.identify = False
        result = func()
        self.identify = original
        return result

    def normalize_func(self, name):
        if self.normalize_functions == "upper":
            return name.upper()
        if self.normalize_functions == "lower":
            return name.lower()
        return name

    def indent(self, sql, level=0, pad=None, skip_first=False, skip_last=False):
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

    def sql(self, expression, key=None):
        if not expression:
            return ""

        if isinstance(expression, str):
            return expression

        if key:
            return self.sql(expression.args.get(key))

        transform = self.TRANSFORMS.get(expression.__class__)

        if callable(transform):
            return transform(self, expression)
        if transform:
            return transform

        if not isinstance(expression, exp.Expression):
            raise ValueError(
                f"Expected an Expression. Received {type(expression)}: {expression}"
            )

        exp_handler_name = f"{expression.key}_sql"
        if hasattr(self, exp_handler_name):
            return getattr(self, exp_handler_name)(expression)

        if isinstance(expression, exp.Func):
            return self.function_fallback_sql(expression)

        raise ValueError(f"Unsupported expression type {expression.__class__.__name__}")

    def annotation_sql(self, expression):
        return self.sql(expression, "expression")

    def uncache_sql(self, expression):
        table = self.sql(expression, "this")
        exists_sql = " IF EXISTS" if expression.args.get("exists") else ""
        return f"UNCACHE TABLE{exists_sql} {table}"

    def cache_sql(self, expression):
        lazy = " LAZY" if expression.args.get("lazy") else ""
        table = self.sql(expression, "this")
        options = expression.args.get("options")
        options = (
            f" OPTIONS({self.sql(options[0])} = {self.sql(options[1])})"
            if options
            else ""
        )
        sql = self.sql(expression, "expression")
        sql = f" AS{self.sep()}{sql}" if sql else ""
        sql = f"CACHE{lazy} TABLE {table}{options}{sql}"
        return self.prepend_ctes(expression, sql)

    def characterset_sql(self, expression):
        if isinstance(expression.parent, exp.Cast):
            return f"CHAR CHARACTER SET {self.sql(expression, 'this')}"
        default = "DEFAULT " if expression.args.get("default") else ""
        return f"{default}CHARACTER SET={self.sql(expression, 'this')}"

    def column_sql(self, expression):
        return ".".join(
            part
            for part in [
                self.sql(expression, "db"),
                self.sql(expression, "table"),
                self.sql(expression, "this"),
            ]
            if part
        )

    def columndef_sql(self, expression):
        column = self.sql(expression, "this")
        kind = self.sql(expression, "kind")
        not_null = " NOT NULL" if expression.args.get("not_null") else ""
        default = self.sql(expression, "default")
        default = f" DEFAULT {default}" if default else ""
        auto_increment = (
            " " + self.token_sql(TokenType.AUTO_INCREMENT)
            if expression.args.get("auto_increment")
            else ""
        )
        collate = self.sql(expression, "collate")
        collate = f" COLLATE {collate}" if collate else ""
        comment = self.sql(expression, "comment")
        comment = f" COMMENT {comment}" if comment else ""
        primary = " PRIMARY KEY" if expression.args.get("primary") else ""
        unique = " UNIQUE" if expression.args.get("unique") else ""
        return f"{column} {kind}{not_null}{default}{collate}{comment}{unique}{primary}{auto_increment}"

    def create_sql(self, expression):
        this = self.sql(expression, "this")
        kind = self.sql(expression, "kind").upper()
        expression_sql = self.sql(expression, "expression")
        expression_sql = f"AS{self.sep()}{expression_sql}" if expression_sql else ""
        temporary = " TEMPORARY" if expression.args.get("temporary") else ""
        replace = " OR REPLACE" if expression.args.get("replace") else ""
        exists_sql = " IF NOT EXISTS" if expression.args.get("exists") else ""
        properties = self.sql(expression, "properties")

        expression_sql = f"CREATE{replace}{temporary} {kind}{exists_sql} {this}{properties} {expression_sql}"
        return self.prepend_ctes(expression, expression_sql)

    def prepend_ctes(self, expression, sql):
        with_ = self.sql(expression, "with")
        if with_:
            sql = f"{with_}{self.sep()}{sql}"
        return sql

    def with_sql(self, expression):
        sql = self.expressions(expression, flat=True)
        recursive = "RECURSIVE " if expression.args.get("recursive") else ""

        return f"WITH {recursive}{sql}"

    def cte_sql(self, expression):
        alias = self.sql(expression, "alias")
        return f"{alias} AS {self.wrap(expression)}"

    def tablealias_sql(self, expression):
        alias = self.sql(expression, "this")
        columns = self.expressions(expression, key="columns", flat=True)
        columns = f"({columns})" if columns else ""
        return f"{alias}{columns}"

    def datatype_sql(self, expression):
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

    def delete_sql(self, expression):
        this = self.sql(expression, "this")
        where_sql = self.sql(expression, "where")
        sql = f"DELETE FROM {this}{where_sql}"
        return self.prepend_ctes(expression, sql)

    def drop_sql(self, expression):
        this = self.sql(expression, "this")
        kind = expression.args["kind"]
        exists_sql = " IF EXISTS " if expression.args.get("exists") else " "
        return f"DROP {kind}{exists_sql}{this}"

    def except_sql(self, expression):
        return self.prepend_ctes(
            expression,
            self.set_operation(expression, self.except_op(expression)),
        )

    def except_op(self, expression):
        return f"EXCEPT{'' if expression.args.get('distinct') else ' ALL'}"

    def fetch_sql(self, expression):
        direction = expression.args.get("direction")
        direction = f" {direction.upper()}" if direction else ""
        count = expression.args.get("count")
        count = f" {count}" if count else ""
        return f"{self.seg('FETCH')}{direction}{count} ROWS ONLY"

    def filter_sql(self, expression):
        this = self.sql(expression, "this")
        where = self.sql(expression, "expression")[1:]  # where has a leading space
        return f"{this} FILTER({where})"

    def hint_sql(self, expression):
        if self.sql(expression, "this"):
            self.unsupported("Hints are not supported")
        return ""

    def identifier_sql(self, expression):
        value = expression.name
        value = value.lower() if self.normalize else value
        if expression.args.get("quoted") or self.identify:
            return f"{self.identifier_start}{value}{self.identifier_end}"
        return value

    def partition_sql(self, expression):
        keys = csv(
            *[
                f"{k.args['this']}='{v.args['this']}'" if v else k.args["this"]
                for k, v in expression.args.get("this")
            ]
        )
        return f"PARTITION({keys})"

    def properties_sql(self, expression):
        root_properties = []
        with_properties = []

        for p in expression.expressions:
            p_class = p.__class__
            if p_class in self.ROOT_PROPERTIES:
                root_properties.append(p)
            elif p_class in self.WITH_PROPERTIES:
                with_properties.append(p)

        return self.root_properties(
            exp.Properties(expressions=root_properties)
        ) + self.with_properties(exp.Properties(expressions=with_properties))

    def root_properties(self, properties):
        if properties.expressions:
            return self.sep() + self.expressions(
                properties,
                indent=False,
                sep=" ",
            )
        return ""

    def properties(self, properties, prefix="", sep=", "):
        if properties.expressions:
            expressions = self.expressions(
                properties,
                sep=sep,
                indent=False,
            )
            return f"{self.seg(prefix)}{' ' if prefix else ''}{self.wrap(expressions)}"
        return ""

    def with_properties(self, properties):
        return self.properties(
            properties,
            prefix="WITH",
        )

    def property_sql(self, expression):
        key = expression.name
        value = self.sql(expression, "value")
        return f"{key} = {value}"

    def insert_sql(self, expression):
        kind = "OVERWRITE TABLE" if expression.args.get("overwrite") else "INTO"
        this = self.sql(expression, "this")
        exists = " IF EXISTS " if expression.args.get("exists") else " "
        partition_sql = (
            self.sql(expression, "partition")
            if expression.args.get("partition")
            else ""
        )
        expression_sql = self.sql(expression, "expression")
        sep = self.sep() if partition_sql else ""
        sql = f"INSERT {kind} {this}{exists}{partition_sql}{sep}{expression_sql}"
        return self.prepend_ctes(expression, sql)

    def intersect_sql(self, expression):
        return self.prepend_ctes(
            expression,
            self.set_operation(expression, self.intersect_op(expression)),
        )

    def intersect_op(self, expression):
        return f"INTERSECT{'' if expression.args.get('distinct') else ' ALL'}"

    def table_sql(self, expression):
        return ".".join(
            part
            for part in [
                self.sql(expression, "catalog"),
                self.sql(expression, "db"),
                self.sql(expression, "this"),
            ]
            if part
        )

    def tablesample_sql(self, expression):
        if self.alias_post_tablesample and isinstance(expression.this, exp.Alias):
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
        return f"{this} TABLESAMPLE{method}({bucket}{percent}{rows}{size}){alias}"

    def tuple_sql(self, expression):
        return f"({self.expressions(expression, flat=True)})"

    def update_sql(self, expression):
        this = self.sql(expression, "this")
        set_sql = self.expressions(expression, flat=True)
        from_sql = self.sql(expression, "from")
        where_sql = self.sql(expression, "where")
        sql = f"UPDATE {this} SET {set_sql}{from_sql}{where_sql}"
        return self.prepend_ctes(expression, sql)

    def values_sql(self, expression):
        return f"VALUES{self.seg('')}{self.expressions(expression)}"

    def var_sql(self, expression):
        return self.sql(expression, "this")

    def from_sql(self, expression):
        expressions = self.expressions(expression, flat=True)
        return f"{self.seg('FROM')} {expressions}"

    def group_sql(self, expression):
        group_by = self.op_expressions("GROUP BY", expression)
        grouping_sets = self.expressions(expression, key="grouping_sets", indent=False)
        grouping_sets = (
            f"{self.seg('GROUPING SETS')} {self.wrap(grouping_sets)}"
            if grouping_sets
            else ""
        )
        cube = self.expressions(expression, key="cube", indent=False)
        cube = f"{self.seg('CUBE')} {self.wrap(cube)}" if cube else ""
        rollup = self.expressions(expression, key="rollup", indent=False)
        rollup = f"{self.seg('ROLLUP')} {self.wrap(rollup)}" if rollup else ""
        return f"{group_by}{grouping_sets}{cube}{rollup}"

    def having_sql(self, expression):
        this = self.indent(self.sql(expression, "this"))
        return f"{self.seg('HAVING')}{self.sep()}{this}"

    def join_sql(self, expression):
        op_sql = self.seg(
            " ".join(op for op in (expression.side, expression.kind, "JOIN") if op)
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

    def lambda_sql(self, expression):
        args = self.expressions(expression, flat=True)
        args = f"({args})" if len(args.split(",")) > 1 else args
        return self.no_identify(lambda: f"{args} -> {self.sql(expression, 'this')}")

    def lateral_sql(self, expression):
        this = self.sql(expression, "this")
        op_sql = self.seg(
            f"LATERAL VIEW{' OUTER' if expression.args.get('outer') else ''}"
        )
        alias = expression.args["alias"]
        table = alias.name
        table = f" {table}" if table else table
        columns = self.expressions(alias, key="columns", flat=True)
        columns = f" AS {columns}" if columns else ""
        return f"{op_sql}{self.sep()}{this}{table}{columns}"

    def limit_sql(self, expression):
        this = self.sql(expression, "this")
        return f"{this}{self.seg('LIMIT')} {self.sql(expression, 'expression')}"

    def offset_sql(self, expression):
        this = self.sql(expression, "this")
        return f"{this}{self.seg('OFFSET')} {self.sql(expression, 'expression')}"

    def literal_sql(self, expression):
        text = expression.this or ""
        if expression.is_string:
            if self._replace_backslash:
                text = text.replace("\\", "\\\\")
            text = text.replace(self.quote, self._escaped_quote)
            return f"{self.quote}{text}{self.quote}"
        return text

    def null_sql(self, *_):
        return "NULL"

    def boolean_sql(self, expression):
        return "TRUE" if expression.this else "FALSE"

    def order_sql(self, expression, flat=False):
        this = self.sql(expression, "this")
        this = f"{this} " if this else this
        return self.op_expressions(f"{this}ORDER BY", expression, flat=this or flat)

    def cluster_sql(self, expression):
        return self.op_expressions("CLUSTER BY", expression)

    def distribute_sql(self, expression):
        return self.op_expressions("DISTRIBUTE BY", expression)

    def sort_sql(self, expression):
        return self.op_expressions("SORT BY", expression)

    def ordered_sql(self, expression):
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

    def query_modifiers(self, expression, *sqls):
        return csv(
            *sqls,
            *[self.sql(sql) for sql in expression.args.get("laterals", [])],
            *[self.sql(sql) for sql in expression.args.get("joins", [])],
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

    def select_sql(self, expression):
        hint = self.sql(expression, "hint")
        distinct = self.sql(expression, "distinct")
        distinct = f" {distinct}" if distinct else ""
        expressions = self.expressions(expression)
        expressions = f"{self.sep()}{expressions}" if expressions else expressions
        sql = self.query_modifiers(
            expression,
            f"SELECT{hint}{distinct}{expressions}",
            self.sql(expression, "from"),
        )
        return self.prepend_ctes(expression, sql)

    def schema_sql(self, expression):
        this = self.sql(expression, "this")
        this = f"{this} " if this else ""
        sql = f"({self.sep('')}{self.expressions(expression)}{self.seg(')', sep='')}"
        return f"{this}{sql}"

    def star_sql(self, expression):
        except_ = self.expressions(expression, key="except", flat=True)
        except_ = f"{self.seg('EXCEPT')} ({except_})" if except_ else ""
        replace = self.expressions(expression, key="replace", flat=True)
        replace = f"{self.seg('REPLACE')} ({replace})" if replace else ""
        return f"*{except_}{replace}"

    def structkwarg_sql(self, expression):
        return f"{self.sql(expression, 'this')} {self.sql(expression, 'expression')}"

    def placeholder_sql(self, *_):
        return "?"

    def subquery_sql(self, expression):
        alias = self.sql(expression, "alias")

        return self.query_modifiers(
            expression,
            self.wrap(expression),
            f" AS {alias}" if alias else "",
        )

    def qualify_sql(self, expression):
        this = self.indent(self.sql(expression, "this"))
        return f"{self.seg('QUALIFY')}{self.sep()}{this}"

    def union_sql(self, expression):
        return self.prepend_ctes(
            expression,
            self.set_operation(expression, self.union_op(expression)),
        )

    def union_op(self, expression):
        return f"UNION{'' if expression.args.get('distinct') else ' ALL'}"

    def unnest_sql(self, expression):
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

    def where_sql(self, expression):
        this = self.indent(self.sql(expression, "this"))
        return f"{self.seg('WHERE')}{self.sep()}{this}"

    def window_sql(self, expression):
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

    def window_spec_sql(self, expression):
        kind = self.sql(expression, "kind")
        start = csv(
            self.sql(expression, "start"), self.sql(expression, "start_side"), sep=" "
        )
        end = csv(
            self.sql(expression, "end"), self.sql(expression, "end_side"), sep=" "
        )
        return f"{kind} BETWEEN {start} AND {end}"

    def withingroup_sql(self, expression):
        this = self.sql(expression, "this")
        expression = self.sql(expression, "expression")[1:]  # order has a leading space
        return f"{this} WITHIN GROUP ({expression})"

    def between_sql(self, expression):
        this = self.sql(expression, "this")
        low = self.sql(expression, "low")
        high = self.sql(expression, "high")
        return f"{this} BETWEEN {low} AND {high}"

    def bracket_sql(self, expression):
        expressions = apply_index_offset(expression.expressions, self.index_offset)
        expressions = ", ".join(self.sql(e) for e in expressions)

        return f"{self.sql(expression, 'this')}[{expressions}]"

    def all_sql(self, expression):
        return f"ALL {self.wrap(expression)}"

    def any_sql(self, expression):
        return f"ANY {self.wrap(expression)}"

    def exists_sql(self, expression):
        return f"EXISTS{self.wrap(expression)}"

    def case_sql(self, expression):
        this = self.indent(self.sql(expression, "this"), skip_first=True)
        this = f" {this}" if this else ""
        ifs = []

        for e in expression.args["ifs"]:
            ifs.append(self.indent(f"WHEN {self.sql(e, 'this')}"))
            ifs.append(self.indent(f"THEN {self.sql(e, 'true')}"))

        if expression.args.get("default") is not None:
            ifs.append(self.indent(f"ELSE {self.sql(expression, 'default')}"))

        ifs = "".join(self.seg(self.indent(e, skip_first=True)) for e in ifs)
        statement = f"CASE{this}{ifs}{self.seg('END')}"
        return statement

    def constraint_sql(self, expression):
        this = self.sql(expression, "this")
        expressions = self.expressions(expression, flat=True)
        return f"CONSTRAINT {this} {expressions}"

    def extract_sql(self, expression):
        this = self.sql(expression, "this")
        expression_sql = self.sql(expression, "expression")
        return f"EXTRACT({this} FROM {expression_sql})"

    def foreignkey_sql(self, expression):
        expressions = self.expressions(expression, flat=True)
        reference = self.sql(expression, "reference")
        reference = f" {reference}" if reference else ""
        delete = self.sql(expression, "delete")
        delete = f" ON DELETE {delete}" if delete else ""
        update = self.sql(expression, "update")
        update = f" ON UPDATE {update}" if update else ""
        return f"FOREIGN KEY ({expressions}){reference}{delete}{update}"

    def if_sql(self, expression):
        return self.case_sql(
            exp.Case(ifs=[expression], default=expression.args.get("false"))
        )

    def in_sql(self, expression):
        query = expression.args.get("query")
        unnest = expression.args.get("unnest")
        if query:
            in_sql = self.wrap(query)
        elif unnest:
            in_sql = self.in_unnest_op(unnest)
        else:
            in_sql = f"({self.expressions(expression, flat=True)})"
        return f"{self.sql(expression, 'this')} IN {in_sql}"

    def in_unnest_op(self, unnest):
        return f"(SELECT {self.sql(unnest)})"

    def interval_sql(self, expression):
        return f"INTERVAL {self.sql(expression, 'this')} {self.sql(expression, 'unit')}"

    def reference_sql(self, expression):
        this = self.sql(expression, "this")
        expressions = self.expressions(expression, flat=True)
        return f"REFERENCES {this}({expressions})"

    def anonymous_sql(self, expression):
        args = self.indent(
            self.expressions(expression, flat=True), skip_first=True, skip_last=True
        )
        return f"{self.normalize_func(self.sql(expression, 'this'))}({args})"

    def paren_sql(self, expression):
        if isinstance(expression.unnest(), exp.Select):
            return self.wrap(expression)
        sql = self.seg(self.indent(self.sql(expression, "this")), sep="")
        return f"({sql}{self.seg(')', sep='')}"

    def neg_sql(self, expression):
        return f"-{self.sql(expression, 'this')}"

    def not_sql(self, expression):
        return f"NOT {self.sql(expression, 'this')}"

    def alias_sql(self, expression):
        to_sql = self.sql(expression, "alias")
        to_sql = f" AS {to_sql}" if to_sql else ""
        return f"{self.sql(expression, 'this')}{to_sql}"

    def aliases_sql(self, expression):
        return f"{self.sql(expression, 'this')} AS ({self.expressions(expression, flat=True)})"

    def attimezone_sql(self, expression):
        this = self.sql(expression, "this")
        zone = self.sql(expression, "zone")
        return f"{this} AT TIME ZONE {zone}"

    def add_sql(self, expression):
        return self.binary(expression, "+")

    def and_sql(self, expression):
        return self.connector_sql(expression, "AND")

    def connector_sql(self, expression, op):
        if not self.pretty:
            return self.binary(expression, op)

        return f"\n{op} ".join(self.sql(e) for e in expression.flatten(unnest=False))

    def bitwiseand_sql(self, expression):
        return self.binary(expression, "&")

    def bitwiseleftshift_sql(self, expression):
        return self.binary(expression, "<<")

    def bitwisenot_sql(self, expression):
        return f"~{self.sql(expression, 'this')}"

    def bitwiseor_sql(self, expression):
        return self.binary(expression, "|")

    def bitwiserightshift_sql(self, expression):
        return self.binary(expression, ">>")

    def bitwisexor_sql(self, expression):
        return self.binary(expression, "^")

    def cast_sql(self, expression):
        return f"CAST({self.sql(expression, 'this')} AS {self.sql(expression, 'to')})"

    def currentdate_sql(self, expression):
        zone = self.sql(expression, "this")
        return f"CURRENT_DATE({zone})" if zone else "CURRENT_DATE"

    def command_sql(self, expression):
        return f"{self.sql(expression, 'this').upper()} {expression.text('expression').strip()}"

    def distinct_sql(self, expression):
        this = self.sql(expression, "this")
        this = f" {this}" if this else ""

        on = self.sql(expression, "on")
        on = f" ON {on}" if on else ""
        return f"DISTINCT{this}{on}"

    def ignorenulls_sql(self, expression):
        return f"{self.sql(expression, 'this')} IGNORE NULLS"

    def intdiv_sql(self, expression):
        return self.sql(
            exp.Cast(
                this=exp.Div(
                    this=expression.args["this"],
                    expression=expression.args["expression"],
                ),
                to=exp.DataType(this=exp.DataType.Type.INT),
            )
        )

    def dpipe_sql(self, expression):
        return self.binary(expression, "||")

    def div_sql(self, expression):
        return self.binary(expression, "/")

    def dot_sql(self, expression):
        return f"{self.sql(expression, 'this')}.{self.sql(expression, 'expression')}"

    def eq_sql(self, expression):
        return self.binary(expression, "=")

    def escape_sql(self, expression):
        return self.binary(expression, "ESCAPE")

    def gt_sql(self, expression):
        return self.binary(expression, ">")

    def gte_sql(self, expression):
        return self.binary(expression, ">=")

    def ilike_sql(self, expression):
        return self.binary(expression, "ILIKE")

    def is_sql(self, expression):
        return self.binary(expression, "IS")

    def like_sql(self, expression):
        return self.binary(expression, "LIKE")

    def lt_sql(self, expression):
        return self.binary(expression, "<")

    def lte_sql(self, expression):
        return self.binary(expression, "<=")

    def mod_sql(self, expression):
        return self.binary(expression, "%")

    def mul_sql(self, expression):
        return self.binary(expression, "*")

    def neq_sql(self, expression):
        return self.binary(expression, "<>")

    def or_sql(self, expression):
        return self.connector_sql(expression, "OR")

    def sub_sql(self, expression):
        return self.binary(expression, "-")

    def trycast_sql(self, expression):
        return (
            f"TRY_CAST({self.sql(expression, 'this')} AS {self.sql(expression, 'to')})"
        )

    def binary(self, expression, op):
        return (
            f"{self.sql(expression, 'this')} {op} {self.sql(expression, 'expression')}"
        )

    def function_fallback_sql(self, expression):
        args = []
        for arg_key in expression.arg_types:
            arg_value = ensure_list(expression.args.get(arg_key) or [])
            for a in arg_value:
                args.append(self.sql(a))

        args_str = self.indent(", ".join(args), skip_first=True, skip_last=True)
        return f"{self.normalize_func(expression.sql_name())}({args_str})"

    def format_time(self, expression):
        return format_time(
            self.sql(expression, "format"), self.time_mapping, self.time_trie
        )

    def expressions(self, expression, key=None, flat=False, indent=True, sep=", "):
        # pylint: disable=cell-var-from-loop
        expressions = expression.args.get(key or "expressions")

        if not expressions:
            return ""

        if flat:
            return sep.join(self.sql(e) for e in expressions)

        expressions = self.sep(sep).join(self.sql(e) for e in expressions)
        if indent:
            return self.indent(expressions, skip_first=False)
        return expressions

    def op_expressions(self, op, expression, flat=False):
        expressions_sql = self.expressions(expression, flat=flat)
        if flat:
            return f"{op} {expressions_sql}"
        return f"{self.seg(op)}{self.sep() if expressions_sql else ''}{expressions_sql}"

    def set_operation(self, expression, op):
        this = self.sql(expression, "this")
        op = self.seg(op)
        return self.query_modifiers(
            expression, f"{this}{op}{self.sep()}{self.sql(expression, 'expression')}"
        )

    def token_sql(self, token_type):
        return self.TOKEN_MAPPING.get(token_type, token_type.name)
