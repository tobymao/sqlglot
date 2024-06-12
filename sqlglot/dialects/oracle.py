from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
    build_formatted_time,
    no_ilike_sql,
    rename_func,
    to_number_with_nls_param,
    trim_sql,
)
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType

if t.TYPE_CHECKING:
    from sqlglot._typing import E


def _build_timetostr_or_tochar(args: t.List) -> exp.TimeToStr | exp.ToChar:
    this = seq_get(args, 0)

    if this and not this.type:
        from sqlglot.optimizer.annotate_types import annotate_types

        annotate_types(this)
        if this.is_type(*exp.DataType.TEMPORAL_TYPES):
            return build_formatted_time(exp.TimeToStr, "oracle", default=True)(args)

    return exp.ToChar.from_arg_list(args)


def eliminate_join_marks(ast: exp.Expression) -> exp.Expression:
    from sqlglot.optimizer.scope import traverse_scope

    """Remove join marks from an expression
    
    SELECT * FROM a, b WHERE a.id = b.id(+)   
    becomes:
    SELECT * FROM a LEFT JOIN b ON a.id = b.id

    - for each scope
        - for each column with a join mark
            - find the predicate it belongs to
            - remove the predicate from the where clause
            - convert the predicate to a join with the (+) side as the left join table
            - replace the existing join with the new join

    Args:
        ast: The AST to remove join marks from

    Returns:
       The AST with join marks removed"""
    for scope in traverse_scope(ast):
        _eliminate_join_marks_from_scope(scope)
    return ast


def _update_from(
    select: exp.Select,
    new_join_dict: t.Dict[str, exp.Join],
    old_join_dict: t.Dict[str, exp.Join],
) -> None:
    """If the from clause needs to become a new join, find an appropriate table to use as the new from.
    updates select in place

    Args:
        select: The select statement to update
        new_join_dict: The dictionary of new joins
        old_join_dict: The dictionary of old joins
    """
    old_from = select.args["from"]
    if old_from.alias_or_name not in new_join_dict:
        return
    in_old_not_new = old_join_dict.keys() - new_join_dict.keys()
    if len(in_old_not_new) >= 1:
        new_from_name = list(old_join_dict.keys() - new_join_dict.keys())[0]
        new_from_this = old_join_dict[new_from_name].this
        new_from = exp.From(this=new_from_this)
        del old_join_dict[new_from_name]
        select.set("from", new_from)
    else:
        raise ValueError("Cannot determine which table to use as the new from")


def _has_join_mark(col: exp.Expression) -> bool:
    """Check if the column has a join mark

    Args:
        The column to check
    """
    return col.args.get("join_mark", False)


def _predicate_to_join(
    eq: exp.Binary, old_joins: t.Dict[str, exp.Join], old_from: exp.From
) -> t.Optional[exp.Join]:
    """Convert an equality predicate to a join if it contains a join mark

    Args:
        eq: The equality expression to convert to a join

    Returns:
        The join expression if the equality contains a join mark (otherwise None)
    """

    # if not (isinstance(eq.left, exp.Column) or isinstance(eq.right, exp.Column)):
    #     return None

    left_columns = [col for col in eq.left.find_all(exp.Column) if _has_join_mark(col)]
    right_columns = [col for col in eq.right.find_all(exp.Column) if _has_join_mark(col)]

    left_has_join_mark = len(left_columns) > 0
    right_has_join_mark = len(right_columns) > 0

    if left_has_join_mark:
        for col in left_columns:
            col.set("join_mark", False)
            join_on = col.table
    elif right_has_join_mark:
        for col in right_columns:
            col.set("join_mark", False)
            join_on = col.table
    else:
        return None

    join_this = old_joins.get(join_on, old_from).this
    return exp.Join(this=join_this, on=eq, kind="LEFT")


if t.TYPE_CHECKING:
    from sqlglot.optimizer.scope import Scope


def _eliminate_join_marks_from_scope(scope: Scope) -> None:
    """Remove join marks columns in scope's where clause.
    Converts them to left joins and replaces any existing joins.
    Updates scope in place.

    Args:
        scope: The scope to remove join marks from
    """
    select_scope = scope.expression
    where = select_scope.args.get("where")
    joins = select_scope.args.get("joins")
    if not where:
        return
    if not joins:
        return

    # dictionaries used to keep track of joins to be replaced
    old_joins = {join.alias_or_name: join for join in list(joins)}
    new_joins: t.Dict[str, exp.Join] = {}

    for node in scope.find_all(exp.Column):
        if _has_join_mark(node):
            predicate = node.find_ancestor(exp.Predicate)
            if not isinstance(predicate, exp.Binary):
                continue
            predicate_parent = predicate.parent

            join_on = predicate.pop()
            new_join = _predicate_to_join(
                join_on, old_joins=old_joins, old_from=select_scope.args["from"]
            )
            # upsert new_join into new_joins dictionary
            if new_join:
                if new_join.alias_or_name in new_joins:
                    new_joins[new_join.alias_or_name].set(
                        "on",
                        exp.and_(
                            new_joins[new_join.alias_or_name].args["on"],
                            new_join.args["on"],
                        ),
                    )
                else:
                    new_joins[new_join.alias_or_name] = new_join
            # If the parent is a binary node with only one child, promote the child to the parent
            if predicate_parent:
                if isinstance(predicate_parent, exp.Binary):
                    if predicate_parent.left is None:
                        predicate_parent.replace(predicate_parent.right)
                    elif predicate_parent.right is None:
                        predicate_parent.replace(predicate_parent.left)

    _update_from(select_scope, new_joins, old_joins)
    replacement_joins = [new_joins.get(join.alias_or_name, join) for join in old_joins.values()]
    select_scope.set("joins", replacement_joins)
    if not where.this:
        where.pop()


class Oracle(Dialect):
    ALIAS_POST_TABLESAMPLE = True
    LOCKING_READS_SUPPORTED = True
    TABLESAMPLE_SIZE_IS_PERCENT = True
    SUPPORTS_COLUMN_JOIN_MARKS = True

    # See section 8: https://docs.oracle.com/cd/A97630_01/server.920/a96540/sql_elements9a.htm
    NORMALIZATION_STRATEGY = NormalizationStrategy.UPPERCASE

    # https://docs.oracle.com/database/121/SQLRF/sql_elements004.htm#SQLRF00212
    # https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
    TIME_MAPPING = {
        "AM": "%p",  # Meridian indicator with or without periods
        "A.M.": "%p",  # Meridian indicator with or without periods
        "PM": "%p",  # Meridian indicator with or without periods
        "P.M.": "%p",  # Meridian indicator with or without periods
        "D": "%u",  # Day of week (1-7)
        "DAY": "%A",  # name of day
        "DD": "%d",  # day of month (1-31)
        "DDD": "%j",  # day of year (1-366)
        "DY": "%a",  # abbreviated name of day
        "HH": "%I",  # Hour of day (1-12)
        "HH12": "%I",  # alias for HH
        "HH24": "%H",  # Hour of day (0-23)
        "IW": "%V",  # Calendar week of year (1-52 or 1-53), as defined by the ISO 8601 standard
        "MI": "%M",  # Minute (0-59)
        "MM": "%m",  # Month (01-12; January = 01)
        "MON": "%b",  # Abbreviated name of month
        "MONTH": "%B",  # Name of month
        "SS": "%S",  # Second (0-59)
        "WW": "%W",  # Week of year (1-53)
        "YY": "%y",  # 15
        "YYYY": "%Y",  # 2015
        "FF6": "%f",  # only 6 digits are supported in python formats
    }

    class Tokenizer(tokens.Tokenizer):
        VAR_SINGLE_TOKENS = {"@", "$", "#"}

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "(+)": TokenType.JOIN_MARKER,
            "BINARY_DOUBLE": TokenType.DOUBLE,
            "BINARY_FLOAT": TokenType.FLOAT,
            "COLUMNS": TokenType.COLUMN,
            "MATCH_RECOGNIZE": TokenType.MATCH_RECOGNIZE,
            "MINUS": TokenType.EXCEPT,
            "NVARCHAR2": TokenType.NVARCHAR,
            "ORDER SIBLINGS BY": TokenType.ORDER_SIBLINGS_BY,
            "SAMPLE": TokenType.TABLE_SAMPLE,
            "START": TokenType.BEGIN,
            "SYSDATE": TokenType.CURRENT_TIMESTAMP,
            "TOP": TokenType.TOP,
            "VARCHAR2": TokenType.VARCHAR,
        }

    class Parser(parser.Parser):
        ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN = False
        WINDOW_BEFORE_PAREN_TOKENS = {TokenType.OVER, TokenType.KEEP}
        VALUES_FOLLOWED_BY_PAREN = False

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "SQUARE": lambda args: exp.Pow(this=seq_get(args, 0), expression=exp.Literal.number(2)),
            "TO_CHAR": _build_timetostr_or_tochar,
            "TO_TIMESTAMP": build_formatted_time(exp.StrToTime, "oracle"),
            "TO_DATE": build_formatted_time(exp.StrToDate, "oracle"),
        }

        FUNCTION_PARSERS: t.Dict[str, t.Callable] = {
            **parser.Parser.FUNCTION_PARSERS,
            "JSON_ARRAY": lambda self: self._parse_json_array(
                exp.JSONArray,
                expressions=self._parse_csv(lambda: self._parse_format_json(self._parse_bitwise())),
            ),
            "JSON_ARRAYAGG": lambda self: self._parse_json_array(
                exp.JSONArrayAgg,
                this=self._parse_format_json(self._parse_bitwise()),
                order=self._parse_order(),
            ),
            "XMLTABLE": lambda self: self._parse_xml_table(),
        }

        NO_PAREN_FUNCTION_PARSERS = {
            **parser.Parser.NO_PAREN_FUNCTION_PARSERS,
            "CONNECT_BY_ROOT": lambda self: self.expression(
                exp.ConnectByRoot, this=self._parse_column()
            ),
        }

        PROPERTY_PARSERS = {
            **parser.Parser.PROPERTY_PARSERS,
            "GLOBAL": lambda self: self._match_text_seq("TEMPORARY")
            and self.expression(exp.TemporaryProperty, this="GLOBAL"),
            "PRIVATE": lambda self: self._match_text_seq("TEMPORARY")
            and self.expression(exp.TemporaryProperty, this="PRIVATE"),
        }

        QUERY_MODIFIER_PARSERS = {
            **parser.Parser.QUERY_MODIFIER_PARSERS,
            TokenType.ORDER_SIBLINGS_BY: lambda self: ("order", self._parse_order()),
        }

        TYPE_LITERAL_PARSERS = {
            exp.DataType.Type.DATE: lambda self, this, _: self.expression(
                exp.DateStrToDate, this=this
            )
        }

        # SELECT UNIQUE .. is old-style Oracle syntax for SELECT DISTINCT ..
        # Reference: https://stackoverflow.com/a/336455
        DISTINCT_TOKENS = {TokenType.DISTINCT, TokenType.UNIQUE}

        def _parse_xml_table(self) -> exp.XMLTable:
            this = self._parse_string()

            passing = None
            columns = None

            if self._match_text_seq("PASSING"):
                # The BY VALUE keywords are optional and are provided for semantic clarity
                self._match_text_seq("BY", "VALUE")
                passing = self._parse_csv(self._parse_column)

            by_ref = self._match_text_seq("RETURNING", "SEQUENCE", "BY", "REF")

            if self._match_text_seq("COLUMNS"):
                columns = self._parse_csv(self._parse_field_def)

            return self.expression(
                exp.XMLTable, this=this, passing=passing, columns=columns, by_ref=by_ref
            )

        def _parse_json_array(self, expr_type: t.Type[E], **kwargs) -> E:
            return self.expression(
                expr_type,
                null_handling=self._parse_on_handling("NULL", "NULL", "ABSENT"),
                return_type=self._match_text_seq("RETURNING") and self._parse_type(),
                strict=self._match_text_seq("STRICT"),
                **kwargs,
            )

        def _parse_hint(self) -> t.Optional[exp.Hint]:
            if self._match(TokenType.HINT):
                start = self._curr
                while self._curr and not self._match_pair(TokenType.STAR, TokenType.SLASH):
                    self._advance()

                if not self._curr:
                    self.raise_error("Expected */ after HINT")

                end = self._tokens[self._index - 3]
                return exp.Hint(expressions=[self._find_sql(start, end)])

            return None

    class Generator(generator.Generator):
        LOCKING_READS_SUPPORTED = True
        JOIN_HINTS = False
        TABLE_HINTS = False
        DATA_TYPE_SPECIFIERS_ALLOWED = True
        ALTER_TABLE_INCLUDE_COLUMN_KEYWORD = False
        LIMIT_FETCH = "FETCH"
        TABLESAMPLE_KEYWORDS = "SAMPLE"
        LAST_DAY_SUPPORTS_DATE_PART = False
        SUPPORTS_SELECT_INTO = True
        TZ_TO_WITH_TIME_ZONE = True

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.TINYINT: "NUMBER",
            exp.DataType.Type.SMALLINT: "NUMBER",
            exp.DataType.Type.INT: "NUMBER",
            exp.DataType.Type.BIGINT: "NUMBER",
            exp.DataType.Type.DECIMAL: "NUMBER",
            exp.DataType.Type.DOUBLE: "DOUBLE PRECISION",
            exp.DataType.Type.VARCHAR: "VARCHAR2",
            exp.DataType.Type.NVARCHAR: "NVARCHAR2",
            exp.DataType.Type.NCHAR: "NCHAR",
            exp.DataType.Type.TEXT: "CLOB",
            exp.DataType.Type.TIMETZ: "TIME",
            exp.DataType.Type.TIMESTAMPTZ: "TIMESTAMP",
            exp.DataType.Type.BINARY: "BLOB",
            exp.DataType.Type.VARBINARY: "BLOB",
            exp.DataType.Type.ROWVERSION: "BLOB",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.ConnectByRoot: lambda self, e: f"CONNECT_BY_ROOT {self.sql(e, 'this')}",
            exp.DateStrToDate: lambda self, e: self.func(
                "TO_DATE", e.this, exp.Literal.string("YYYY-MM-DD")
            ),
            exp.Group: transforms.preprocess([transforms.unalias_group]),
            exp.ILike: no_ilike_sql,
            exp.Select: transforms.preprocess(
                [
                    transforms.eliminate_distinct_on,
                    transforms.eliminate_qualify,
                ]
            ),
            exp.StrToTime: lambda self, e: self.func("TO_TIMESTAMP", e.this, self.format_time(e)),
            exp.StrToDate: lambda self, e: self.func("TO_DATE", e.this, self.format_time(e)),
            exp.Subquery: lambda self, e: self.subquery_sql(e, sep=" "),
            exp.Substring: rename_func("SUBSTR"),
            exp.Table: lambda self, e: self.table_sql(e, sep=" "),
            exp.TableSample: lambda self, e: self.tablesample_sql(e, sep=" "),
            exp.TemporaryProperty: lambda _, e: f"{e.name or 'GLOBAL'} TEMPORARY",
            exp.TimeToStr: lambda self, e: self.func("TO_CHAR", e.this, self.format_time(e)),
            exp.ToChar: lambda self, e: self.function_fallback_sql(e),
            exp.ToNumber: to_number_with_nls_param,
            exp.Trim: trim_sql,
            exp.UnixToTime: lambda self,
            e: f"TO_DATE('1970-01-01', 'YYYY-MM-DD') + ({self.sql(e, 'this')} / 86400)",
        }

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        }

        def currenttimestamp_sql(self, expression: exp.CurrentTimestamp) -> str:
            this = expression.this
            return self.func("CURRENT_TIMESTAMP", this) if this else "CURRENT_TIMESTAMP"

        def offset_sql(self, expression: exp.Offset) -> str:
            return f"{super().offset_sql(expression)} ROWS"

        def xmltable_sql(self, expression: exp.XMLTable) -> str:
            this = self.sql(expression, "this")
            passing = self.expressions(expression, key="passing")
            passing = f"{self.sep()}PASSING{self.seg(passing)}" if passing else ""
            columns = self.expressions(expression, key="columns")
            columns = f"{self.sep()}COLUMNS{self.seg(columns)}" if columns else ""
            by_ref = (
                f"{self.sep()}RETURNING SEQUENCE BY REF" if expression.args.get("by_ref") else ""
            )
            return f"XMLTABLE({self.sep('')}{self.indent(this + passing + by_ref + columns)}{self.seg(')', sep='')}"

        def add_column_sql(self, expression: exp.AlterTable) -> str:
            actions = self.expressions(expression, key="actions", flat=True)
            if len(expression.args.get("actions", [])) > 1:
                return f"ADD ({actions})"
            return f"ADD {actions}"
