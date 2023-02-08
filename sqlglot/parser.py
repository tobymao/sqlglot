from __future__ import annotations

import logging
import typing as t

from sqlglot import exp
from sqlglot.errors import ErrorLevel, ParseError, concat_messages, merge_errors
from sqlglot.helper import (
    apply_index_offset,
    count_params,
    ensure_collection,
    ensure_list,
    seq_get,
)
from sqlglot.tokens import Token, Tokenizer, TokenType
from sqlglot.trie import in_trie, new_trie

logger = logging.getLogger("sqlglot")


def parse_var_map(args):
    keys = []
    values = []
    for i in range(0, len(args), 2):
        keys.append(args[i])
        values.append(args[i + 1])
    return exp.VarMap(
        keys=exp.Array(expressions=keys),
        values=exp.Array(expressions=values),
    )


class _Parser(type):
    def __new__(cls, clsname, bases, attrs):
        klass = super().__new__(cls, clsname, bases, attrs)
        klass._show_trie = new_trie(key.split(" ") for key in klass.SHOW_PARSERS)
        klass._set_trie = new_trie(key.split(" ") for key in klass.SET_PARSERS)
        return klass


class Parser(metaclass=_Parser):
    """
    Parser consumes a list of tokens produced by the `sqlglot.tokens.Tokenizer` and produces
    a parsed syntax tree.

    Args:
        error_level: the desired error level.
            Default: ErrorLevel.RAISE
        error_message_context: determines the amount of context to capture from a
            query string when displaying the error message (in number of characters).
            Default: 50.
        index_offset: Index offset for arrays eg ARRAY[0] vs ARRAY[1] as the head of a list.
            Default: 0
        alias_post_tablesample: If the table alias comes after tablesample.
            Default: False
        max_errors: Maximum number of error messages to include in a raised ParseError.
            This is only relevant if error_level is ErrorLevel.RAISE.
            Default: 3
        null_ordering: Indicates the default null ordering method to use if not explicitly set.
            Options are "nulls_are_small", "nulls_are_large", "nulls_are_last".
            Default: "nulls_are_small"
    """

    FUNCTIONS: t.Dict[str, t.Callable] = {
        **{name: f.from_arg_list for f in exp.ALL_FUNCTIONS for name in f.sql_names()},
        "DATE_TO_DATE_STR": lambda args: exp.Cast(
            this=seq_get(args, 0),
            to=exp.DataType(this=exp.DataType.Type.TEXT),
        ),
        "TIME_TO_TIME_STR": lambda args: exp.Cast(
            this=seq_get(args, 0),
            to=exp.DataType(this=exp.DataType.Type.TEXT),
        ),
        "TS_OR_DS_TO_DATE_STR": lambda args: exp.Substring(
            this=exp.Cast(
                this=seq_get(args, 0),
                to=exp.DataType(this=exp.DataType.Type.TEXT),
            ),
            start=exp.Literal.number(1),
            length=exp.Literal.number(10),
        ),
        "VAR_MAP": parse_var_map,
        "IFNULL": exp.Coalesce.from_arg_list,
    }

    NO_PAREN_FUNCTIONS = {
        TokenType.CURRENT_DATE: exp.CurrentDate,
        TokenType.CURRENT_DATETIME: exp.CurrentDate,
        TokenType.CURRENT_TIMESTAMP: exp.CurrentTimestamp,
    }

    NESTED_TYPE_TOKENS = {
        TokenType.ARRAY,
        TokenType.MAP,
        TokenType.STRUCT,
        TokenType.NULLABLE,
    }

    TYPE_TOKENS = {
        TokenType.BOOLEAN,
        TokenType.TINYINT,
        TokenType.SMALLINT,
        TokenType.INT,
        TokenType.BIGINT,
        TokenType.FLOAT,
        TokenType.DOUBLE,
        TokenType.CHAR,
        TokenType.NCHAR,
        TokenType.VARCHAR,
        TokenType.NVARCHAR,
        TokenType.TEXT,
        TokenType.MEDIUMTEXT,
        TokenType.LONGTEXT,
        TokenType.MEDIUMBLOB,
        TokenType.LONGBLOB,
        TokenType.BINARY,
        TokenType.VARBINARY,
        TokenType.JSON,
        TokenType.JSONB,
        TokenType.INTERVAL,
        TokenType.TIME,
        TokenType.TIMESTAMP,
        TokenType.TIMESTAMPTZ,
        TokenType.TIMESTAMPLTZ,
        TokenType.DATETIME,
        TokenType.DATE,
        TokenType.DECIMAL,
        TokenType.UUID,
        TokenType.GEOGRAPHY,
        TokenType.GEOMETRY,
        TokenType.HLLSKETCH,
        TokenType.HSTORE,
        TokenType.PSEUDO_TYPE,
        TokenType.SUPER,
        TokenType.SERIAL,
        TokenType.SMALLSERIAL,
        TokenType.BIGSERIAL,
        TokenType.XML,
        TokenType.UNIQUEIDENTIFIER,
        TokenType.MONEY,
        TokenType.SMALLMONEY,
        TokenType.ROWVERSION,
        TokenType.IMAGE,
        TokenType.VARIANT,
        TokenType.OBJECT,
        *NESTED_TYPE_TOKENS,
    }

    SUBQUERY_PREDICATES = {
        TokenType.ANY: exp.Any,
        TokenType.ALL: exp.All,
        TokenType.EXISTS: exp.Exists,
        TokenType.SOME: exp.Any,
    }

    RESERVED_KEYWORDS = {*Tokenizer.SINGLE_TOKENS.values(), TokenType.SELECT}

    ID_VAR_TOKENS = {
        TokenType.VAR,
        TokenType.ALWAYS,
        TokenType.ANTI,
        TokenType.APPLY,
        TokenType.AUTO_INCREMENT,
        TokenType.BEGIN,
        TokenType.BOTH,
        TokenType.BUCKET,
        TokenType.CACHE,
        TokenType.CASCADE,
        TokenType.COLLATE,
        TokenType.COLUMN,
        TokenType.COMMAND,
        TokenType.COMMIT,
        TokenType.COMPOUND,
        TokenType.CONSTRAINT,
        TokenType.CURRENT_TIME,
        TokenType.DEFAULT,
        TokenType.DELETE,
        TokenType.DESCRIBE,
        TokenType.DIV,
        TokenType.END,
        TokenType.EXECUTE,
        TokenType.ESCAPE,
        TokenType.FALSE,
        TokenType.FIRST,
        TokenType.FILTER,
        TokenType.FOLLOWING,
        TokenType.FORMAT,
        TokenType.FUNCTION,
        TokenType.GENERATED,
        TokenType.IDENTITY,
        TokenType.IF,
        TokenType.INDEX,
        TokenType.ISNULL,
        TokenType.INTERVAL,
        TokenType.LAZY,
        TokenType.LEADING,
        TokenType.LEFT,
        TokenType.LOCAL,
        TokenType.MATERIALIZED,
        TokenType.MERGE,
        TokenType.NATURAL,
        TokenType.NEXT,
        TokenType.OFFSET,
        TokenType.ONLY,
        TokenType.OPTIONS,
        TokenType.ORDINALITY,
        TokenType.PERCENT,
        TokenType.PIVOT,
        TokenType.PRECEDING,
        TokenType.RANGE,
        TokenType.REFERENCES,
        TokenType.RIGHT,
        TokenType.ROW,
        TokenType.ROWS,
        TokenType.SCHEMA,
        TokenType.SCHEMA_COMMENT,
        TokenType.SEED,
        TokenType.SEMI,
        TokenType.SET,
        TokenType.SHOW,
        TokenType.SORTKEY,
        TokenType.TABLE,
        TokenType.TEMPORARY,
        TokenType.TOP,
        TokenType.TRAILING,
        TokenType.TRUE,
        TokenType.UNBOUNDED,
        TokenType.UNIQUE,
        TokenType.UNLOGGED,
        TokenType.UNPIVOT,
        TokenType.PROCEDURE,
        TokenType.VIEW,
        TokenType.VOLATILE,
        TokenType.WINDOW,
        *SUBQUERY_PREDICATES,
        *TYPE_TOKENS,
        *NO_PAREN_FUNCTIONS,
    }

    TABLE_ALIAS_TOKENS = ID_VAR_TOKENS - {
        TokenType.APPLY,
        TokenType.LEFT,
        TokenType.NATURAL,
        TokenType.OFFSET,
        TokenType.RIGHT,
        TokenType.WINDOW,
    }

    UPDATE_ALIAS_TOKENS = TABLE_ALIAS_TOKENS - {TokenType.SET}

    TRIM_TYPES = {TokenType.LEADING, TokenType.TRAILING, TokenType.BOTH}

    FUNC_TOKENS = {
        TokenType.COMMAND,
        TokenType.CURRENT_DATE,
        TokenType.CURRENT_DATETIME,
        TokenType.CURRENT_TIMESTAMP,
        TokenType.CURRENT_TIME,
        TokenType.FILTER,
        TokenType.FIRST,
        TokenType.FORMAT,
        TokenType.IDENTIFIER,
        TokenType.INDEX,
        TokenType.ISNULL,
        TokenType.MERGE,
        TokenType.OFFSET,
        TokenType.PRIMARY_KEY,
        TokenType.REPLACE,
        TokenType.ROW,
        TokenType.UNNEST,
        TokenType.VAR,
        TokenType.LEFT,
        TokenType.RIGHT,
        TokenType.DATE,
        TokenType.DATETIME,
        TokenType.TABLE,
        TokenType.TIMESTAMP,
        TokenType.TIMESTAMPTZ,
        TokenType.WINDOW,
        *TYPE_TOKENS,
        *SUBQUERY_PREDICATES,
    }

    CONJUNCTION = {
        TokenType.AND: exp.And,
        TokenType.OR: exp.Or,
    }

    EQUALITY = {
        TokenType.EQ: exp.EQ,
        TokenType.NEQ: exp.NEQ,
        TokenType.NULLSAFE_EQ: exp.NullSafeEQ,
    }

    COMPARISON = {
        TokenType.GT: exp.GT,
        TokenType.GTE: exp.GTE,
        TokenType.LT: exp.LT,
        TokenType.LTE: exp.LTE,
    }

    BITWISE = {
        TokenType.AMP: exp.BitwiseAnd,
        TokenType.CARET: exp.BitwiseXor,
        TokenType.PIPE: exp.BitwiseOr,
        TokenType.DPIPE: exp.DPipe,
    }

    TERM = {
        TokenType.DASH: exp.Sub,
        TokenType.PLUS: exp.Add,
        TokenType.MOD: exp.Mod,
        TokenType.COLLATE: exp.Collate,
    }

    FACTOR = {
        TokenType.DIV: exp.IntDiv,
        TokenType.LR_ARROW: exp.Distance,
        TokenType.SLASH: exp.Div,
        TokenType.STAR: exp.Mul,
    }

    TIMESTAMPS = {
        TokenType.TIME,
        TokenType.TIMESTAMP,
        TokenType.TIMESTAMPTZ,
        TokenType.TIMESTAMPLTZ,
    }

    SET_OPERATIONS = {
        TokenType.UNION,
        TokenType.INTERSECT,
        TokenType.EXCEPT,
    }

    JOIN_SIDES = {
        TokenType.LEFT,
        TokenType.RIGHT,
        TokenType.FULL,
    }

    JOIN_KINDS = {
        TokenType.INNER,
        TokenType.OUTER,
        TokenType.CROSS,
        TokenType.SEMI,
        TokenType.ANTI,
    }

    LAMBDAS = {
        TokenType.ARROW: lambda self, expressions: self.expression(
            exp.Lambda,
            this=self._parse_conjunction().transform(
                self._replace_lambda, {node.name for node in expressions}
            ),
            expressions=expressions,
        ),
        TokenType.FARROW: lambda self, expressions: self.expression(
            exp.Kwarg,
            this=exp.Var(this=expressions[0].name),
            expression=self._parse_conjunction(),
        ),
    }

    COLUMN_OPERATORS = {
        TokenType.DOT: None,
        TokenType.DCOLON: lambda self, this, to: self.expression(
            exp.Cast,
            this=this,
            to=to,
        ),
        TokenType.ARROW: lambda self, this, path: self.expression(
            exp.JSONExtract,
            this=this,
            expression=path,
        ),
        TokenType.DARROW: lambda self, this, path: self.expression(
            exp.JSONExtractScalar,
            this=this,
            expression=path,
        ),
        TokenType.HASH_ARROW: lambda self, this, path: self.expression(
            exp.JSONBExtract,
            this=this,
            expression=path,
        ),
        TokenType.DHASH_ARROW: lambda self, this, path: self.expression(
            exp.JSONBExtractScalar,
            this=this,
            expression=path,
        ),
        TokenType.PLACEHOLDER: lambda self, this, key: self.expression(
            exp.JSONBContains,
            this=this,
            expression=key,
        ),
    }

    EXPRESSION_PARSERS = {
        exp.Column: lambda self: self._parse_column(),
        exp.DataType: lambda self: self._parse_types(),
        exp.From: lambda self: self._parse_from(),
        exp.Group: lambda self: self._parse_group(),
        exp.Identifier: lambda self: self._parse_id_var(),
        exp.Lateral: lambda self: self._parse_lateral(),
        exp.Join: lambda self: self._parse_join(),
        exp.Order: lambda self: self._parse_order(),
        exp.Cluster: lambda self: self._parse_sort(TokenType.CLUSTER_BY, exp.Cluster),
        exp.Sort: lambda self: self._parse_sort(TokenType.SORT_BY, exp.Sort),
        exp.Lambda: lambda self: self._parse_lambda(),
        exp.Limit: lambda self: self._parse_limit(),
        exp.Offset: lambda self: self._parse_offset(),
        exp.TableAlias: lambda self: self._parse_table_alias(),
        exp.Table: lambda self: self._parse_table(),
        exp.Condition: lambda self: self._parse_conjunction(),
        exp.Expression: lambda self: self._parse_statement(),
        exp.Properties: lambda self: self._parse_properties(),
        exp.Where: lambda self: self._parse_where(),
        exp.Ordered: lambda self: self._parse_ordered(),
        exp.Having: lambda self: self._parse_having(),
        exp.With: lambda self: self._parse_with(),
        exp.Window: lambda self: self._parse_named_window(),
        "JOIN_TYPE": lambda self: self._parse_join_side_and_kind(),
    }

    STATEMENT_PARSERS = {
        TokenType.ALTER: lambda self: self._parse_alter(),
        TokenType.BEGIN: lambda self: self._parse_transaction(),
        TokenType.CACHE: lambda self: self._parse_cache(),
        TokenType.COMMIT: lambda self: self._parse_commit_or_rollback(),
        TokenType.CREATE: lambda self: self._parse_create(),
        TokenType.DELETE: lambda self: self._parse_delete(),
        TokenType.DESC: lambda self: self._parse_describe(),
        TokenType.DESCRIBE: lambda self: self._parse_describe(),
        TokenType.DROP: lambda self: self._parse_drop(),
        TokenType.END: lambda self: self._parse_commit_or_rollback(),
        TokenType.INSERT: lambda self: self._parse_insert(),
        TokenType.LOAD_DATA: lambda self: self._parse_load_data(),
        TokenType.MERGE: lambda self: self._parse_merge(),
        TokenType.ROLLBACK: lambda self: self._parse_commit_or_rollback(),
        TokenType.UNCACHE: lambda self: self._parse_uncache(),
        TokenType.UPDATE: lambda self: self._parse_update(),
        TokenType.USE: lambda self: self.expression(
            exp.Use,
            kind=self._match_texts(("ROLE", "WAREHOUSE", "DATABASE", "SCHEMA"))
            and exp.Var(this=self._prev.text),
            this=self._parse_table(schema=False),
        ),
    }

    UNARY_PARSERS = {
        TokenType.PLUS: lambda self: self._parse_unary(),  # Unary + is handled as a no-op
        TokenType.NOT: lambda self: self.expression(exp.Not, this=self._parse_equality()),
        TokenType.TILDA: lambda self: self.expression(exp.BitwiseNot, this=self._parse_unary()),
        TokenType.DASH: lambda self: self.expression(exp.Neg, this=self._parse_unary()),
    }

    PRIMARY_PARSERS = {
        TokenType.STRING: lambda self, token: self.expression(
            exp.Literal, this=token.text, is_string=True
        ),
        TokenType.NUMBER: lambda self, token: self.expression(
            exp.Literal, this=token.text, is_string=False
        ),
        TokenType.STAR: lambda self, _: self.expression(
            exp.Star,
            **{"except": self._parse_except(), "replace": self._parse_replace()},
        ),
        TokenType.NULL: lambda self, _: self.expression(exp.Null),
        TokenType.TRUE: lambda self, _: self.expression(exp.Boolean, this=True),
        TokenType.FALSE: lambda self, _: self.expression(exp.Boolean, this=False),
        TokenType.BIT_STRING: lambda self, token: self.expression(exp.BitString, this=token.text),
        TokenType.HEX_STRING: lambda self, token: self.expression(exp.HexString, this=token.text),
        TokenType.BYTE_STRING: lambda self, token: self.expression(exp.ByteString, this=token.text),
        TokenType.INTRODUCER: lambda self, token: self._parse_introducer(token),
        TokenType.NATIONAL: lambda self, token: self._parse_national(token),
        TokenType.SESSION_PARAMETER: lambda self, _: self._parse_session_parameter(),
    }

    PLACEHOLDER_PARSERS = {
        TokenType.PLACEHOLDER: lambda self: self.expression(exp.Placeholder),
        TokenType.PARAMETER: lambda self: self.expression(
            exp.Parameter, this=self._parse_var() or self._parse_primary()
        ),
        TokenType.COLON: lambda self: self.expression(exp.Placeholder, this=self._prev.text)
        if self._match_set((TokenType.NUMBER, TokenType.VAR))
        else None,
    }

    RANGE_PARSERS = {
        TokenType.BETWEEN: lambda self, this: self._parse_between(this),
        TokenType.GLOB: lambda self, this: self._parse_escape(
            self.expression(exp.Glob, this=this, expression=self._parse_bitwise())
        ),
        TokenType.IN: lambda self, this: self._parse_in(this),
        TokenType.IS: lambda self, this: self._parse_is(this),
        TokenType.LIKE: lambda self, this: self._parse_escape(
            self.expression(exp.Like, this=this, expression=self._parse_bitwise())
        ),
        TokenType.ILIKE: lambda self, this: self._parse_escape(
            self.expression(exp.ILike, this=this, expression=self._parse_bitwise())
        ),
        TokenType.IRLIKE: lambda self, this: self.expression(
            exp.RegexpILike, this=this, expression=self._parse_bitwise()
        ),
        TokenType.RLIKE: lambda self, this: self.expression(
            exp.RegexpLike, this=this, expression=self._parse_bitwise()
        ),
        TokenType.SIMILAR_TO: lambda self, this: self.expression(
            exp.SimilarTo, this=this, expression=self._parse_bitwise()
        ),
    }

    PROPERTY_PARSERS = {
        "AUTO_INCREMENT": lambda self: self._parse_property_assignment(exp.AutoIncrementProperty),
        "CHARACTER SET": lambda self: self._parse_character_set(),
        "LOCATION": lambda self: self._parse_property_assignment(exp.LocationProperty),
        "PARTITION BY": lambda self: self._parse_partitioned_by(),
        "PARTITIONED BY": lambda self: self._parse_partitioned_by(),
        "PARTITIONED_BY": lambda self: self._parse_partitioned_by(),
        "COMMENT": lambda self: self._parse_property_assignment(exp.SchemaCommentProperty),
        "STORED": lambda self: self._parse_property_assignment(exp.FileFormatProperty),
        "DISTKEY": lambda self: self._parse_distkey(),
        "DISTSTYLE": lambda self: self._parse_property_assignment(exp.DistStyleProperty),
        "SORTKEY": lambda self: self._parse_sortkey(),
        "LIKE": lambda self: self._parse_create_like(),
        "RETURNS": lambda self: self._parse_returns(),
        "ROW": lambda self: self._parse_row(),
        "COLLATE": lambda self: self._parse_property_assignment(exp.CollateProperty),
        "FORMAT": lambda self: self._parse_property_assignment(exp.FileFormatProperty),
        "TABLE_FORMAT": lambda self: self._parse_property_assignment(exp.TableFormatProperty),
        "USING": lambda self: self._parse_property_assignment(exp.TableFormatProperty),
        "LANGUAGE": lambda self: self._parse_property_assignment(exp.LanguageProperty),
        "EXECUTE": lambda self: self._parse_property_assignment(exp.ExecuteAsProperty),
        "DETERMINISTIC": lambda self: self.expression(
            exp.VolatilityProperty, this=exp.Literal.string("IMMUTABLE")
        ),
        "IMMUTABLE": lambda self: self.expression(
            exp.VolatilityProperty, this=exp.Literal.string("IMMUTABLE")
        ),
        "STABLE": lambda self: self.expression(
            exp.VolatilityProperty, this=exp.Literal.string("STABLE")
        ),
        "VOLATILE": lambda self: self.expression(
            exp.VolatilityProperty, this=exp.Literal.string("VOLATILE")
        ),
        "WITH": lambda self: self._parse_with_property(),
        "TBLPROPERTIES": lambda self: self._parse_wrapped_csv(self._parse_property),
        "FALLBACK": lambda self: self._parse_fallback(no=self._prev.text.upper() == "NO"),
        "LOG": lambda self: self._parse_log(no=self._prev.text.upper() == "NO"),
        "BEFORE": lambda self: self._parse_journal(
            no=self._prev.text.upper() == "NO", dual=self._prev.text.upper() == "DUAL"
        ),
        "JOURNAL": lambda self: self._parse_journal(
            no=self._prev.text.upper() == "NO", dual=self._prev.text.upper() == "DUAL"
        ),
        "AFTER": lambda self: self._parse_afterjournal(
            no=self._prev.text.upper() == "NO", dual=self._prev.text.upper() == "DUAL"
        ),
        "LOCAL": lambda self: self._parse_afterjournal(no=False, dual=False, local=True),
        "NOT": lambda self: self._parse_afterjournal(no=False, dual=False, local=False),
        "CHECKSUM": lambda self: self._parse_checksum(),
        "FREESPACE": lambda self: self._parse_freespace(),
        "MERGEBLOCKRATIO": lambda self: self._parse_mergeblockratio(
            no=self._prev.text.upper() == "NO", default=self._prev.text.upper() == "DEFAULT"
        ),
        "MIN": lambda self: self._parse_datablocksize(),
        "MINIMUM": lambda self: self._parse_datablocksize(),
        "MAX": lambda self: self._parse_datablocksize(),
        "MAXIMUM": lambda self: self._parse_datablocksize(),
        "DATABLOCKSIZE": lambda self: self._parse_datablocksize(
            default=self._prev.text.upper() == "DEFAULT"
        ),
        "BLOCKCOMPRESSION": lambda self: self._parse_blockcompression(),
        "ALGORITHM": lambda self: self._parse_property_assignment(exp.AlgorithmProperty),
        "DEFINER": lambda self: self._parse_definer(),
    }

    CONSTRAINT_PARSERS = {
        TokenType.CHECK: lambda self: self.expression(
            exp.Check, this=self._parse_wrapped(self._parse_conjunction)
        ),
        TokenType.FOREIGN_KEY: lambda self: self._parse_foreign_key(),
        TokenType.UNIQUE: lambda self: self._parse_unique(),
        TokenType.LIKE: lambda self: self._parse_create_like(),
    }

    NO_PAREN_FUNCTION_PARSERS = {
        TokenType.CASE: lambda self: self._parse_case(),
        TokenType.IF: lambda self: self._parse_if(),
    }

    FUNCTION_PARSERS: t.Dict[str, t.Callable] = {
        "CONVERT": lambda self: self._parse_convert(self.STRICT_CAST),
        "TRY_CONVERT": lambda self: self._parse_convert(False),
        "EXTRACT": lambda self: self._parse_extract(),
        "POSITION": lambda self: self._parse_position(),
        "SUBSTRING": lambda self: self._parse_substring(),
        "TRIM": lambda self: self._parse_trim(),
        "CAST": lambda self: self._parse_cast(self.STRICT_CAST),
        "TRY_CAST": lambda self: self._parse_cast(False),
        "STRING_AGG": lambda self: self._parse_string_agg(),
    }

    QUERY_MODIFIER_PARSERS = {
        "match": lambda self: self._parse_match_recognize(),
        "where": lambda self: self._parse_where(),
        "group": lambda self: self._parse_group(),
        "having": lambda self: self._parse_having(),
        "qualify": lambda self: self._parse_qualify(),
        "windows": lambda self: self._parse_window_clause(),
        "distribute": lambda self: self._parse_sort(TokenType.DISTRIBUTE_BY, exp.Distribute),
        "sort": lambda self: self._parse_sort(TokenType.SORT_BY, exp.Sort),
        "cluster": lambda self: self._parse_sort(TokenType.CLUSTER_BY, exp.Cluster),
        "order": lambda self: self._parse_order(),
        "limit": lambda self: self._parse_limit(),
        "offset": lambda self: self._parse_offset(),
        "lock": lambda self: self._parse_lock(),
    }

    SHOW_PARSERS: t.Dict[str, t.Callable] = {}
    SET_PARSERS: t.Dict[str, t.Callable] = {}

    MODIFIABLES = (exp.Subquery, exp.Subqueryable, exp.Table)

    CREATABLES = {
        TokenType.COLUMN,
        TokenType.FUNCTION,
        TokenType.INDEX,
        TokenType.PROCEDURE,
        TokenType.SCHEMA,
        TokenType.TABLE,
        TokenType.VIEW,
    }

    TRANSACTION_KIND = {"DEFERRED", "IMMEDIATE", "EXCLUSIVE"}

    WINDOW_ALIAS_TOKENS = ID_VAR_TOKENS - {TokenType.ROWS}

    ADD_CONSTRAINT_TOKENS = {TokenType.CONSTRAINT, TokenType.PRIMARY_KEY, TokenType.FOREIGN_KEY}

    STRICT_CAST = True

    __slots__ = (
        "error_level",
        "error_message_context",
        "sql",
        "errors",
        "index_offset",
        "unnest_column_only",
        "alias_post_tablesample",
        "max_errors",
        "null_ordering",
        "_tokens",
        "_index",
        "_curr",
        "_next",
        "_prev",
        "_prev_comments",
        "_show_trie",
        "_set_trie",
    )

    def __init__(
        self,
        error_level: t.Optional[ErrorLevel] = None,
        error_message_context: int = 100,
        index_offset: int = 0,
        unnest_column_only: bool = False,
        alias_post_tablesample: bool = False,
        max_errors: int = 3,
        null_ordering: t.Optional[str] = None,
    ):
        self.error_level = error_level or ErrorLevel.IMMEDIATE
        self.error_message_context = error_message_context
        self.index_offset = index_offset
        self.unnest_column_only = unnest_column_only
        self.alias_post_tablesample = alias_post_tablesample
        self.max_errors = max_errors
        self.null_ordering = null_ordering
        self.reset()

    def reset(self):
        self.sql = ""
        self.errors = []
        self._tokens = []
        self._index = 0
        self._curr = None
        self._next = None
        self._prev = None
        self._prev_comments = None

    def parse(
        self, raw_tokens: t.List[Token], sql: t.Optional[str] = None
    ) -> t.List[t.Optional[exp.Expression]]:
        """
        Parses a list of tokens and returns a list of syntax trees, one tree
        per parsed SQL statement.

        Args:
            raw_tokens: the list of tokens.
            sql: the original SQL string, used to produce helpful debug messages.

        Returns:
            The list of syntax trees.
        """
        return self._parse(
            parse_method=self.__class__._parse_statement, raw_tokens=raw_tokens, sql=sql
        )

    def parse_into(
        self,
        expression_types: exp.IntoType,
        raw_tokens: t.List[Token],
        sql: t.Optional[str] = None,
    ) -> t.List[t.Optional[exp.Expression]]:
        """
        Parses a list of tokens into a given Expression type. If a collection of Expression
        types is given instead, this method will try to parse the token list into each one
        of them, stopping at the first for which the parsing succeeds.

        Args:
            expression_types: the expression type(s) to try and parse the token list into.
            raw_tokens: the list of tokens.
            sql: the original SQL string, used to produce helpful debug messages.

        Returns:
            The target Expression.
        """
        errors = []
        for expression_type in ensure_collection(expression_types):
            parser = self.EXPRESSION_PARSERS.get(expression_type)
            if not parser:
                raise TypeError(f"No parser registered for {expression_type}")
            try:
                return self._parse(parser, raw_tokens, sql)
            except ParseError as e:
                e.errors[0]["into_expression"] = expression_type
                errors.append(e)
        raise ParseError(
            f"Failed to parse into {expression_types}",
            errors=merge_errors(errors),
        ) from errors[-1]

    def _parse(
        self,
        parse_method: t.Callable[[Parser], t.Optional[exp.Expression]],
        raw_tokens: t.List[Token],
        sql: t.Optional[str] = None,
    ) -> t.List[t.Optional[exp.Expression]]:
        self.reset()
        self.sql = sql or ""
        total = len(raw_tokens)
        chunks: t.List[t.List[Token]] = [[]]

        for i, token in enumerate(raw_tokens):
            if token.token_type == TokenType.SEMICOLON:
                if i < total - 1:
                    chunks.append([])
            else:
                chunks[-1].append(token)

        expressions = []

        for tokens in chunks:
            self._index = -1
            self._tokens = tokens
            self._advance()

            expressions.append(parse_method(self))

            if self._index < len(self._tokens):
                self.raise_error("Invalid expression / Unexpected token")

            self.check_errors()

        return expressions

    def check_errors(self) -> None:
        """
        Logs or raises any found errors, depending on the chosen error level setting.
        """
        if self.error_level == ErrorLevel.WARN:
            for error in self.errors:
                logger.error(str(error))
        elif self.error_level == ErrorLevel.RAISE and self.errors:
            raise ParseError(
                concat_messages(self.errors, self.max_errors),
                errors=merge_errors(self.errors),
            )

    def raise_error(self, message: str, token: t.Optional[Token] = None) -> None:
        """
        Appends an error in the list of recorded errors or raises it, depending on the chosen
        error level setting.
        """
        token = token or self._curr or self._prev or Token.string("")
        start = self._find_token(token)
        end = start + len(token.text)
        start_context = self.sql[max(start - self.error_message_context, 0) : start]
        highlight = self.sql[start:end]
        end_context = self.sql[end : end + self.error_message_context]

        error = ParseError.new(
            f"{message}. Line {token.line}, Col: {token.col}.\n"
            f"  {start_context}\033[4m{highlight}\033[0m{end_context}",
            description=message,
            line=token.line,
            col=token.col,
            start_context=start_context,
            highlight=highlight,
            end_context=end_context,
        )

        if self.error_level == ErrorLevel.IMMEDIATE:
            raise error

        self.errors.append(error)

    def expression(
        self, exp_class: t.Type[exp.Expression], comments: t.Optional[t.List[str]] = None, **kwargs
    ) -> exp.Expression:
        """
        Creates a new, validated Expression.

        Args:
            exp_class: the expression class to instantiate.
            comments: an optional list of comments to attach to the expression.
            kwargs: the arguments to set for the expression along with their respective values.

        Returns:
            The target expression.
        """
        instance = exp_class(**kwargs)
        if self._prev_comments:
            instance.comments = self._prev_comments
            self._prev_comments = None
        if comments:
            instance.comments = comments
        self.validate_expression(instance)
        return instance

    def validate_expression(
        self, expression: exp.Expression, args: t.Optional[t.List] = None
    ) -> None:
        """
        Validates an already instantiated expression, making sure that all its mandatory arguments
        are set.

        Args:
            expression: the expression to validate.
            args: an optional list of items that was used to instantiate the expression, if it's a Func.
        """
        if self.error_level == ErrorLevel.IGNORE:
            return

        for error_message in expression.error_messages(args):
            self.raise_error(error_message)

    def _find_sql(self, start: Token, end: Token) -> str:
        return self.sql[self._find_token(start) : self._find_token(end) + len(end.text)]

    def _find_token(self, token: Token) -> int:
        line = 1
        col = 1
        index = 0

        while line < token.line or col < token.col:
            if Tokenizer.WHITE_SPACE.get(self.sql[index]) == TokenType.BREAK:
                line += 1
                col = 1
            else:
                col += 1
            index += 1

        return index

    def _advance(self, times: int = 1) -> None:
        self._index += times
        self._curr = seq_get(self._tokens, self._index)
        self._next = seq_get(self._tokens, self._index + 1)
        if self._index > 0:
            self._prev = self._tokens[self._index - 1]
            self._prev_comments = self._prev.comments
        else:
            self._prev = None
            self._prev_comments = None

    def _retreat(self, index: int) -> None:
        self._advance(index - self._index)

    def _parse_command(self) -> exp.Expression:
        return self.expression(exp.Command, this=self._prev.text, expression=self._parse_string())

    def _parse_statement(self) -> t.Optional[exp.Expression]:
        if self._curr is None:
            return None

        if self._match_set(self.STATEMENT_PARSERS):
            return self.STATEMENT_PARSERS[self._prev.token_type](self)

        if self._match_set(Tokenizer.COMMANDS):
            return self._parse_command()

        expression = self._parse_expression()
        expression = self._parse_set_operations(expression) if expression else self._parse_select()

        self._parse_query_modifiers(expression)
        return expression

    def _parse_drop(self, default_kind: t.Optional[str] = None) -> t.Optional[exp.Expression]:
        start = self._prev
        temporary = self._match(TokenType.TEMPORARY)
        materialized = self._match(TokenType.MATERIALIZED)
        kind = self._match_set(self.CREATABLES) and self._prev.text
        if not kind:
            if default_kind:
                kind = default_kind
            else:
                return self._parse_as_command(start)

        return self.expression(
            exp.Drop,
            exists=self._parse_exists(),
            this=self._parse_table(schema=True),
            kind=kind,
            temporary=temporary,
            materialized=materialized,
            cascade=self._match(TokenType.CASCADE),
        )

    def _parse_exists(self, not_: bool = False) -> t.Optional[bool]:
        return (
            self._match(TokenType.IF)
            and (not not_ or self._match(TokenType.NOT))
            and self._match(TokenType.EXISTS)
        )

    def _parse_create(self) -> t.Optional[exp.Expression]:
        start = self._prev
        replace = self._match_pair(TokenType.OR, TokenType.REPLACE)
        set_ = self._match(TokenType.SET)  # Teradata
        multiset = self._match_text_seq("MULTISET")  # Teradata
        global_temporary = self._match_text_seq("GLOBAL", "TEMPORARY")  # Teradata
        volatile = self._match(TokenType.VOLATILE)  # Teradata
        temporary = self._match(TokenType.TEMPORARY)
        transient = self._match_text_seq("TRANSIENT")
        external = self._match_text_seq("EXTERNAL")
        unique = self._match(TokenType.UNIQUE)
        materialized = self._match(TokenType.MATERIALIZED)

        if self._match_pair(TokenType.TABLE, TokenType.FUNCTION, advance=False):
            self._match(TokenType.TABLE)

        properties = None
        create_token = self._match_set(self.CREATABLES) and self._prev

        if not create_token:
            properties = self._parse_properties()
            create_token = self._match_set(self.CREATABLES) and self._prev

            if not properties or not create_token:
                return self._parse_as_command(start)

        exists = self._parse_exists(not_=True)
        this = None
        expression = None
        data = None
        statistics = None
        no_primary_index = None
        indexes = None
        no_schema_binding = None
        begin = None

        if create_token.token_type in (TokenType.FUNCTION, TokenType.PROCEDURE):
            this = self._parse_user_defined_function(kind=create_token.token_type)
            properties = self._parse_properties()
            if self._match(TokenType.ALIAS):
                begin = self._match(TokenType.BEGIN)
                return_ = self._match_text_seq("RETURN")
                expression = self._parse_statement()

                if return_:
                    expression = self.expression(exp.Return, this=expression)
        elif create_token.token_type == TokenType.INDEX:
            this = self._parse_index()
        elif create_token.token_type in (
            TokenType.TABLE,
            TokenType.VIEW,
            TokenType.SCHEMA,
        ):
            table_parts = self._parse_table_parts(schema=True)

            if self._match(TokenType.COMMA):  # comma-separated properties before schema definition
                properties = self._parse_properties(before=True)

            this = self._parse_schema(this=table_parts)

            if not properties:  # properties after schema definition
                properties = self._parse_properties()

            self._match(TokenType.ALIAS)
            expression = self._parse_ddl_select()

            if create_token.token_type == TokenType.TABLE:
                if self._match_text_seq("WITH", "DATA"):
                    data = True
                elif self._match_text_seq("WITH", "NO", "DATA"):
                    data = False

                if self._match_text_seq("AND", "STATISTICS"):
                    statistics = True
                elif self._match_text_seq("AND", "NO", "STATISTICS"):
                    statistics = False

                no_primary_index = self._match_text_seq("NO", "PRIMARY", "INDEX")

                indexes = []
                while True:
                    index = self._parse_create_table_index()

                    # post index PARTITION BY property
                    if self._match(TokenType.PARTITION_BY, advance=False):
                        if properties:
                            properties.expressions.append(self._parse_property())
                        else:
                            properties = self._parse_properties()

                    if not index:
                        break
                    else:
                        indexes.append(index)
            elif create_token.token_type == TokenType.VIEW:
                if self._match_text_seq("WITH", "NO", "SCHEMA", "BINDING"):
                    no_schema_binding = True

        return self.expression(
            exp.Create,
            this=this,
            kind=create_token.text,
            expression=expression,
            set=set_,
            multiset=multiset,
            global_temporary=global_temporary,
            volatile=volatile,
            exists=exists,
            properties=properties,
            temporary=temporary,
            transient=transient,
            external=external,
            replace=replace,
            unique=unique,
            materialized=materialized,
            data=data,
            statistics=statistics,
            no_primary_index=no_primary_index,
            indexes=indexes,
            no_schema_binding=no_schema_binding,
            begin=begin,
        )

    def _parse_property_before(self) -> t.Optional[exp.Expression]:
        self._match(TokenType.COMMA)

        # parsers look to _prev for no/dual/default, so need to consume first
        self._match_text_seq("NO")
        self._match_text_seq("DUAL")
        self._match_text_seq("DEFAULT")

        if self.PROPERTY_PARSERS.get(self._curr.text.upper()):
            return self.PROPERTY_PARSERS[self._curr.text.upper()](self)

        return None

    def _parse_property(self) -> t.Optional[exp.Expression]:
        if self._match_texts(self.PROPERTY_PARSERS):
            return self.PROPERTY_PARSERS[self._prev.text.upper()](self)

        if self._match_pair(TokenType.DEFAULT, TokenType.CHARACTER_SET):
            return self._parse_character_set(True)

        if self._match_pair(TokenType.COMPOUND, TokenType.SORTKEY):
            return self._parse_sortkey(compound=True)

        if self._match_text_seq("SQL", "SECURITY"):
            return self.expression(exp.SqlSecurityProperty, definer=self._match_text_seq("DEFINER"))

        assignment = self._match_pair(
            TokenType.VAR, TokenType.EQ, advance=False
        ) or self._match_pair(TokenType.STRING, TokenType.EQ, advance=False)

        if assignment:
            key = self._parse_var_or_string()
            self._match(TokenType.EQ)
            return self.expression(exp.Property, this=key, value=self._parse_column())

        return None

    def _parse_property_assignment(self, exp_class: t.Type[exp.Expression]) -> exp.Expression:
        self._match(TokenType.EQ)
        self._match(TokenType.ALIAS)
        return self.expression(
            exp_class,
            this=self._parse_var_or_string() or self._parse_number() or self._parse_id_var(),
        )

    def _parse_properties(self, before=None) -> t.Optional[exp.Expression]:
        properties = []

        while True:
            if before:
                identified_property = self._parse_property_before()
            else:
                identified_property = self._parse_property()

            if not identified_property:
                break
            for p in ensure_collection(identified_property):
                properties.append(p)

        if properties:
            return self.expression(exp.Properties, expressions=properties)

        return None

    def _parse_fallback(self, no=False) -> exp.Expression:
        self._match_text_seq("FALLBACK")
        return self.expression(
            exp.FallbackProperty, no=no, protection=self._match_text_seq("PROTECTION")
        )

    def _parse_with_property(
        self,
    ) -> t.Union[t.Optional[exp.Expression], t.List[t.Optional[exp.Expression]]]:
        if self._match(TokenType.L_PAREN, advance=False):
            return self._parse_wrapped_csv(self._parse_property)

        if not self._next:
            return None

        if self._next.text.upper() == "JOURNAL":
            return self._parse_withjournaltable()

        return self._parse_withisolatedloading()

    # https://dev.mysql.com/doc/refman/8.0/en/create-view.html
    def _parse_definer(self) -> t.Optional[exp.Expression]:
        self._match(TokenType.EQ)

        user = self._parse_id_var()
        self._match(TokenType.PARAMETER)
        host = self._parse_id_var() or (self._match(TokenType.MOD) and self._prev.text)

        if not user or not host:
            return None

        return exp.DefinerProperty(this=f"{user}@{host}")

    def _parse_withjournaltable(self) -> exp.Expression:
        self._match_text_seq("WITH", "JOURNAL", "TABLE")
        self._match(TokenType.EQ)
        return self.expression(exp.WithJournalTableProperty, this=self._parse_table_parts())

    def _parse_log(self, no=False) -> exp.Expression:
        self._match_text_seq("LOG")
        return self.expression(exp.LogProperty, no=no)

    def _parse_journal(self, no=False, dual=False) -> exp.Expression:
        before = self._match_text_seq("BEFORE")
        self._match_text_seq("JOURNAL")
        return self.expression(exp.JournalProperty, no=no, dual=dual, before=before)

    def _parse_afterjournal(self, no=False, dual=False, local=None) -> exp.Expression:
        self._match_text_seq("NOT")
        self._match_text_seq("LOCAL")
        self._match_text_seq("AFTER", "JOURNAL")
        return self.expression(exp.AfterJournalProperty, no=no, dual=dual, local=local)

    def _parse_checksum(self) -> exp.Expression:
        self._match_text_seq("CHECKSUM")
        self._match(TokenType.EQ)

        on = None
        if self._match(TokenType.ON):
            on = True
        elif self._match_text_seq("OFF"):
            on = False
        default = self._match(TokenType.DEFAULT)

        return self.expression(
            exp.ChecksumProperty,
            on=on,
            default=default,
        )

    def _parse_freespace(self) -> exp.Expression:
        self._match_text_seq("FREESPACE")
        self._match(TokenType.EQ)
        return self.expression(
            exp.FreespaceProperty, this=self._parse_number(), percent=self._match(TokenType.PERCENT)
        )

    def _parse_mergeblockratio(self, no=False, default=False) -> exp.Expression:
        self._match_text_seq("MERGEBLOCKRATIO")
        if self._match(TokenType.EQ):
            return self.expression(
                exp.MergeBlockRatioProperty,
                this=self._parse_number(),
                percent=self._match(TokenType.PERCENT),
            )
        else:
            return self.expression(
                exp.MergeBlockRatioProperty,
                no=no,
                default=default,
            )

    def _parse_datablocksize(self, default=None) -> exp.Expression:
        if default:
            self._match_text_seq("DATABLOCKSIZE")
            return self.expression(exp.DataBlocksizeProperty, default=True)
        elif self._match_texts(("MIN", "MINIMUM")):
            self._match_text_seq("DATABLOCKSIZE")
            return self.expression(exp.DataBlocksizeProperty, min=True)
        elif self._match_texts(("MAX", "MAXIMUM")):
            self._match_text_seq("DATABLOCKSIZE")
            return self.expression(exp.DataBlocksizeProperty, min=False)

        self._match_text_seq("DATABLOCKSIZE")
        self._match(TokenType.EQ)
        size = self._parse_number()
        units = None
        if self._match_texts(("BYTES", "KBYTES", "KILOBYTES")):
            units = self._prev.text
        return self.expression(exp.DataBlocksizeProperty, size=size, units=units)

    def _parse_blockcompression(self) -> exp.Expression:
        self._match_text_seq("BLOCKCOMPRESSION")
        self._match(TokenType.EQ)
        always = self._match(TokenType.ALWAYS)
        manual = self._match_text_seq("MANUAL")
        never = self._match_text_seq("NEVER")
        default = self._match_text_seq("DEFAULT")
        autotemp = None
        if self._match_text_seq("AUTOTEMP"):
            autotemp = self._parse_schema()

        return self.expression(
            exp.BlockCompressionProperty,
            always=always,
            manual=manual,
            never=never,
            default=default,
            autotemp=autotemp,
        )

    def _parse_withisolatedloading(self) -> exp.Expression:
        self._match(TokenType.WITH)
        no = self._match_text_seq("NO")
        concurrent = self._match_text_seq("CONCURRENT")
        self._match_text_seq("ISOLATED", "LOADING")
        for_all = self._match_text_seq("FOR", "ALL")
        for_insert = self._match_text_seq("FOR", "INSERT")
        for_none = self._match_text_seq("FOR", "NONE")
        return self.expression(
            exp.IsolatedLoadingProperty,
            no=no,
            concurrent=concurrent,
            for_all=for_all,
            for_insert=for_insert,
            for_none=for_none,
        )

    def _parse_partition_by(self) -> t.List[t.Optional[exp.Expression]]:
        if self._match(TokenType.PARTITION_BY):
            return self._parse_csv(self._parse_conjunction)
        return []

    def _parse_partitioned_by(self) -> exp.Expression:
        self._match(TokenType.EQ)
        return self.expression(
            exp.PartitionedByProperty,
            this=self._parse_schema() or self._parse_bracket(self._parse_field()),
        )

    def _parse_distkey(self) -> exp.Expression:
        return self.expression(exp.DistKeyProperty, this=self._parse_wrapped(self._parse_id_var))

    def _parse_create_like(self) -> t.Optional[exp.Expression]:
        table = self._parse_table(schema=True)
        options = []
        while self._match_texts(("INCLUDING", "EXCLUDING")):
            this = self._prev.text.upper()
            id_var = self._parse_id_var()

            if not id_var:
                return None

            options.append(
                self.expression(
                    exp.Property,
                    this=this,
                    value=exp.Var(this=id_var.this.upper()),
                )
            )
        return self.expression(exp.LikeProperty, this=table, expressions=options)

    def _parse_sortkey(self, compound: bool = False) -> exp.Expression:
        return self.expression(
            exp.SortKeyProperty, this=self._parse_wrapped_csv(self._parse_id_var), compound=compound
        )

    def _parse_character_set(self, default: bool = False) -> exp.Expression:
        self._match(TokenType.EQ)
        return self.expression(
            exp.CharacterSetProperty, this=self._parse_var_or_string(), default=default
        )

    def _parse_returns(self) -> exp.Expression:
        value: t.Optional[exp.Expression]
        is_table = self._match(TokenType.TABLE)

        if is_table:
            if self._match(TokenType.LT):
                value = self.expression(
                    exp.Schema,
                    this="TABLE",
                    expressions=self._parse_csv(self._parse_struct_kwargs),
                )
                if not self._match(TokenType.GT):
                    self.raise_error("Expecting >")
            else:
                value = self._parse_schema(exp.Var(this="TABLE"))
        else:
            value = self._parse_types()

        return self.expression(exp.ReturnsProperty, this=value, is_table=is_table)

    def _parse_describe(self) -> exp.Expression:
        kind = self._match_set(self.CREATABLES) and self._prev.text
        this = self._parse_table()

        return self.expression(exp.Describe, this=this, kind=kind)

    def _parse_insert(self) -> exp.Expression:
        overwrite = self._match(TokenType.OVERWRITE)
        local = self._match(TokenType.LOCAL)

        this: t.Optional[exp.Expression]

        if self._match_text_seq("DIRECTORY"):
            this = self.expression(
                exp.Directory,
                this=self._parse_var_or_string(),
                local=local,
                row_format=self._parse_row_format(match_row=True),
            )
        else:
            self._match(TokenType.INTO)
            self._match(TokenType.TABLE)
            this = self._parse_table(schema=True)

        return self.expression(
            exp.Insert,
            this=this,
            exists=self._parse_exists(),
            partition=self._parse_partition(),
            expression=self._parse_ddl_select(),
            overwrite=overwrite,
        )

    def _parse_row(self) -> t.Optional[exp.Expression]:
        if not self._match(TokenType.FORMAT):
            return None
        return self._parse_row_format()

    def _parse_row_format(self, match_row: bool = False) -> t.Optional[exp.Expression]:
        if match_row and not self._match_pair(TokenType.ROW, TokenType.FORMAT):
            return None

        if self._match_text_seq("SERDE"):
            return self.expression(exp.RowFormatSerdeProperty, this=self._parse_string())

        self._match_text_seq("DELIMITED")

        kwargs = {}

        if self._match_text_seq("FIELDS", "TERMINATED", "BY"):
            kwargs["fields"] = self._parse_string()
            if self._match_text_seq("ESCAPED", "BY"):
                kwargs["escaped"] = self._parse_string()
        if self._match_text_seq("COLLECTION", "ITEMS", "TERMINATED", "BY"):
            kwargs["collection_items"] = self._parse_string()
        if self._match_text_seq("MAP", "KEYS", "TERMINATED", "BY"):
            kwargs["map_keys"] = self._parse_string()
        if self._match_text_seq("LINES", "TERMINATED", "BY"):
            kwargs["lines"] = self._parse_string()
        if self._match_text_seq("NULL", "DEFINED", "AS"):
            kwargs["null"] = self._parse_string()

        return self.expression(exp.RowFormatDelimitedProperty, **kwargs)  # type: ignore

    def _parse_load_data(self) -> exp.Expression:
        local = self._match(TokenType.LOCAL)
        self._match_text_seq("INPATH")
        inpath = self._parse_string()
        overwrite = self._match(TokenType.OVERWRITE)
        self._match_pair(TokenType.INTO, TokenType.TABLE)

        return self.expression(
            exp.LoadData,
            this=self._parse_table(schema=True),
            local=local,
            overwrite=overwrite,
            inpath=inpath,
            partition=self._parse_partition(),
            input_format=self._match_text_seq("INPUTFORMAT") and self._parse_string(),
            serde=self._match_text_seq("SERDE") and self._parse_string(),
        )

    def _parse_delete(self) -> exp.Expression:
        self._match(TokenType.FROM)

        return self.expression(
            exp.Delete,
            this=self._parse_table(schema=True),
            using=self._parse_csv(lambda: self._match(TokenType.USING) and self._parse_table()),
            where=self._parse_where(),
        )

    def _parse_update(self) -> exp.Expression:
        return self.expression(
            exp.Update,
            **{  # type: ignore
                "this": self._parse_table(alias_tokens=self.UPDATE_ALIAS_TOKENS),
                "expressions": self._match(TokenType.SET) and self._parse_csv(self._parse_equality),
                "from": self._parse_from(),
                "where": self._parse_where(),
            },
        )

    def _parse_uncache(self) -> exp.Expression:
        if not self._match(TokenType.TABLE):
            self.raise_error("Expecting TABLE after UNCACHE")

        return self.expression(
            exp.Uncache,
            exists=self._parse_exists(),
            this=self._parse_table(schema=True),
        )

    def _parse_cache(self) -> exp.Expression:
        lazy = self._match(TokenType.LAZY)
        self._match(TokenType.TABLE)
        table = self._parse_table(schema=True)
        options = []

        if self._match(TokenType.OPTIONS):
            self._match_l_paren()
            k = self._parse_string()
            self._match(TokenType.EQ)
            v = self._parse_string()
            options = [k, v]
            self._match_r_paren()

        self._match(TokenType.ALIAS)
        return self.expression(
            exp.Cache,
            this=table,
            lazy=lazy,
            options=options,
            expression=self._parse_select(nested=True),
        )

    def _parse_partition(self) -> t.Optional[exp.Expression]:
        if not self._match(TokenType.PARTITION):
            return None

        return self.expression(
            exp.Partition, expressions=self._parse_wrapped_csv(self._parse_conjunction)
        )

    def _parse_value(self) -> exp.Expression:
        if self._match(TokenType.L_PAREN):
            expressions = self._parse_csv(self._parse_conjunction)
            self._match_r_paren()
            return self.expression(exp.Tuple, expressions=expressions)

        # In presto we can have VALUES 1, 2 which results in 1 column & 2 rows.
        # Source: https://prestodb.io/docs/current/sql/values.html
        return self.expression(exp.Tuple, expressions=[self._parse_conjunction()])

    def _parse_select(
        self, nested: bool = False, table: bool = False, parse_subquery_alias: bool = True
    ) -> t.Optional[exp.Expression]:
        cte = self._parse_with()
        if cte:
            this = self._parse_statement()

            if not this:
                self.raise_error("Failed to parse any statement following CTE")
                return cte

            if "with" in this.arg_types:
                this.set("with", cte)
            else:
                self.raise_error(f"{this.key} does not support CTE")
                this = cte
        elif self._match(TokenType.SELECT):
            comments = self._prev_comments

            hint = self._parse_hint()
            all_ = self._match(TokenType.ALL)
            distinct = self._match(TokenType.DISTINCT)

            if distinct:
                distinct = self.expression(
                    exp.Distinct,
                    on=self._parse_value() if self._match(TokenType.ON) else None,
                )

            if all_ and distinct:
                self.raise_error("Cannot specify both ALL and DISTINCT after SELECT")

            limit = self._parse_limit(top=True)
            expressions = self._parse_csv(self._parse_expression)

            this = self.expression(
                exp.Select,
                hint=hint,
                distinct=distinct,
                expressions=expressions,
                limit=limit,
            )
            this.comments = comments

            into = self._parse_into()
            if into:
                this.set("into", into)

            from_ = self._parse_from()
            if from_:
                this.set("from", from_)

            self._parse_query_modifiers(this)
        elif (table or nested) and self._match(TokenType.L_PAREN):
            this = self._parse_table() if table else self._parse_select(nested=True)
            self._parse_query_modifiers(this)
            this = self._parse_set_operations(this)
            self._match_r_paren()

            # early return so that subquery unions aren't parsed again
            # SELECT * FROM (SELECT 1) UNION ALL SELECT 1
            # Union ALL should be a property of the top select node, not the subquery
            return self._parse_subquery(this, parse_alias=parse_subquery_alias)
        elif self._match(TokenType.VALUES):
            this = self.expression(
                exp.Values,
                expressions=self._parse_csv(self._parse_value),
                alias=self._parse_table_alias(),
            )
        else:
            this = None

        return self._parse_set_operations(this)

    def _parse_with(self, skip_with_token: bool = False) -> t.Optional[exp.Expression]:
        if not skip_with_token and not self._match(TokenType.WITH):
            return None

        recursive = self._match(TokenType.RECURSIVE)

        expressions = []
        while True:
            expressions.append(self._parse_cte())

            if not self._match(TokenType.COMMA) and not self._match(TokenType.WITH):
                break
            else:
                self._match(TokenType.WITH)

        return self.expression(exp.With, expressions=expressions, recursive=recursive)

    def _parse_cte(self) -> exp.Expression:
        alias = self._parse_table_alias()
        if not alias or not alias.this:
            self.raise_error("Expected CTE to have alias")

        self._match(TokenType.ALIAS)

        return self.expression(
            exp.CTE,
            this=self._parse_wrapped(self._parse_statement),
            alias=alias,
        )

    def _parse_table_alias(
        self, alias_tokens: t.Optional[t.Collection[TokenType]] = None
    ) -> t.Optional[exp.Expression]:
        any_token = self._match(TokenType.ALIAS)
        alias = self._parse_id_var(
            any_token=any_token, tokens=alias_tokens or self.TABLE_ALIAS_TOKENS
        )
        index = self._index

        if self._match(TokenType.L_PAREN):
            columns = self._parse_csv(lambda: self._parse_column_def(self._parse_id_var()))
            self._match_r_paren() if columns else self._retreat(index)
        else:
            columns = None

        if not alias and not columns:
            return None

        return self.expression(exp.TableAlias, this=alias, columns=columns)

    def _parse_subquery(
        self, this: t.Optional[exp.Expression], parse_alias: bool = True
    ) -> exp.Expression:
        return self.expression(
            exp.Subquery,
            this=this,
            pivots=self._parse_pivots(),
            alias=self._parse_table_alias() if parse_alias else None,
        )

    def _parse_query_modifiers(self, this: t.Optional[exp.Expression]) -> None:
        if not isinstance(this, self.MODIFIABLES):
            return

        table = isinstance(this, exp.Table)

        while True:
            lateral = self._parse_lateral()
            join = self._parse_join()
            comma = None if table else self._match(TokenType.COMMA)
            if lateral:
                this.append("laterals", lateral)
            if join:
                this.append("joins", join)
            if comma:
                this.args["from"].append("expressions", self._parse_table())
            if not (lateral or join or comma):
                break

        for key, parser in self.QUERY_MODIFIER_PARSERS.items():
            expression = parser(self)

            if expression:
                this.set(key, expression)

    def _parse_hint(self) -> t.Optional[exp.Expression]:
        if self._match(TokenType.HINT):
            hints = self._parse_csv(self._parse_function)
            if not self._match_pair(TokenType.STAR, TokenType.SLASH):
                self.raise_error("Expected */ after HINT")
            return self.expression(exp.Hint, expressions=hints)

        return None

    def _parse_into(self) -> t.Optional[exp.Expression]:
        if not self._match(TokenType.INTO):
            return None

        temp = self._match(TokenType.TEMPORARY)
        unlogged = self._match(TokenType.UNLOGGED)
        self._match(TokenType.TABLE)

        return self.expression(
            exp.Into, this=self._parse_table(schema=True), temporary=temp, unlogged=unlogged
        )

    def _parse_from(self) -> t.Optional[exp.Expression]:
        if not self._match(TokenType.FROM):
            return None

        return self.expression(
            exp.From, comments=self._prev_comments, expressions=self._parse_csv(self._parse_table)
        )

    def _parse_match_recognize(self) -> t.Optional[exp.Expression]:
        if not self._match(TokenType.MATCH_RECOGNIZE):
            return None
        self._match_l_paren()

        partition = self._parse_partition_by()
        order = self._parse_order()
        measures = (
            self._parse_alias(self._parse_conjunction())
            if self._match_text_seq("MEASURES")
            else None
        )

        if self._match_text_seq("ONE", "ROW", "PER", "MATCH"):
            rows = exp.Var(this="ONE ROW PER MATCH")
        elif self._match_text_seq("ALL", "ROWS", "PER", "MATCH"):
            text = "ALL ROWS PER MATCH"
            if self._match_text_seq("SHOW", "EMPTY", "MATCHES"):
                text += f" SHOW EMPTY MATCHES"
            elif self._match_text_seq("OMIT", "EMPTY", "MATCHES"):
                text += f" OMIT EMPTY MATCHES"
            elif self._match_text_seq("WITH", "UNMATCHED", "ROWS"):
                text += f" WITH UNMATCHED ROWS"
            rows = exp.Var(this=text)
        else:
            rows = None

        if self._match_text_seq("AFTER", "MATCH", "SKIP"):
            text = "AFTER MATCH SKIP"
            if self._match_text_seq("PAST", "LAST", "ROW"):
                text += f" PAST LAST ROW"
            elif self._match_text_seq("TO", "NEXT", "ROW"):
                text += f" TO NEXT ROW"
            elif self._match_text_seq("TO", "FIRST"):
                text += f" TO FIRST {self._advance_any().text}"  # type: ignore
            elif self._match_text_seq("TO", "LAST"):
                text += f" TO LAST {self._advance_any().text}"  # type: ignore
            after = exp.Var(this=text)
        else:
            after = None

        if self._match_text_seq("PATTERN"):
            self._match_l_paren()

            if not self._curr:
                self.raise_error("Expecting )", self._curr)

            paren = 1
            start = self._curr

            while self._curr and paren > 0:
                if self._curr.token_type == TokenType.L_PAREN:
                    paren += 1
                if self._curr.token_type == TokenType.R_PAREN:
                    paren -= 1
                end = self._prev
                self._advance()
            if paren > 0:
                self.raise_error("Expecting )", self._curr)
            pattern = exp.Var(this=self._find_sql(start, end))
        else:
            pattern = None

        define = (
            self._parse_alias(self._parse_conjunction()) if self._match_text_seq("DEFINE") else None
        )
        self._match_r_paren()

        return self.expression(
            exp.MatchRecognize,
            partition_by=partition,
            order=order,
            measures=measures,
            rows=rows,
            after=after,
            pattern=pattern,
            define=define,
        )

    def _parse_lateral(self) -> t.Optional[exp.Expression]:
        outer_apply = self._match_pair(TokenType.OUTER, TokenType.APPLY)
        cross_apply = self._match_pair(TokenType.CROSS, TokenType.APPLY)

        if outer_apply or cross_apply:
            this = self._parse_select(table=True)
            view = None
            outer = not cross_apply
        elif self._match(TokenType.LATERAL):
            this = self._parse_select(table=True)
            view = self._match(TokenType.VIEW)
            outer = self._match(TokenType.OUTER)
        else:
            return None

        if not this:
            this = self._parse_function() or self._parse_id_var(any_token=False)
            while self._match(TokenType.DOT):
                this = exp.Dot(
                    this=this,
                    expression=self._parse_function() or self._parse_id_var(any_token=False),
                )

        table_alias: t.Optional[exp.Expression]

        if view:
            table = self._parse_id_var(any_token=False)
            columns = self._parse_csv(self._parse_id_var) if self._match(TokenType.ALIAS) else []
            table_alias = self.expression(exp.TableAlias, this=table, columns=columns)
        else:
            table_alias = self._parse_table_alias()

        expression = self.expression(
            exp.Lateral,
            this=this,
            view=view,
            outer=outer,
            alias=table_alias,
        )

        if outer_apply or cross_apply:
            return self.expression(exp.Join, this=expression, side=None if cross_apply else "LEFT")

        return expression

    def _parse_join_side_and_kind(
        self,
    ) -> t.Tuple[t.Optional[Token], t.Optional[Token], t.Optional[Token]]:
        return (
            self._match(TokenType.NATURAL) and self._prev,
            self._match_set(self.JOIN_SIDES) and self._prev,
            self._match_set(self.JOIN_KINDS) and self._prev,
        )

    def _parse_join(self, skip_join_token: bool = False) -> t.Optional[exp.Expression]:
        natural, side, kind = self._parse_join_side_and_kind()

        if not skip_join_token and not self._match(TokenType.JOIN):
            return None

        kwargs: t.Dict[
            str, t.Optional[exp.Expression] | bool | str | t.List[t.Optional[exp.Expression]]
        ] = {"this": self._parse_table()}

        if natural:
            kwargs["natural"] = True
        if side:
            kwargs["side"] = side.text
        if kind:
            kwargs["kind"] = kind.text

        if self._match(TokenType.ON):
            kwargs["on"] = self._parse_conjunction()
        elif self._match(TokenType.USING):
            kwargs["using"] = self._parse_wrapped_id_vars()

        return self.expression(exp.Join, **kwargs)  # type: ignore

    def _parse_index(self) -> exp.Expression:
        index = self._parse_id_var()
        self._match(TokenType.ON)
        self._match(TokenType.TABLE)  # hive

        return self.expression(
            exp.Index,
            this=index,
            table=self.expression(exp.Table, this=self._parse_id_var()),
            columns=self._parse_expression(),
        )

    def _parse_create_table_index(self) -> t.Optional[exp.Expression]:
        unique = self._match(TokenType.UNIQUE)
        primary = self._match_text_seq("PRIMARY")
        amp = self._match_text_seq("AMP")
        if not self._match(TokenType.INDEX):
            return None
        index = self._parse_id_var()
        columns = None
        if self._match(TokenType.L_PAREN, advance=False):
            columns = self._parse_wrapped_csv(self._parse_column)
        return self.expression(
            exp.Index,
            this=index,
            columns=columns,
            unique=unique,
            primary=primary,
            amp=amp,
        )

    def _parse_table_parts(self, schema: bool = False) -> exp.Expression:
        catalog = None
        db = None
        table = (not schema and self._parse_function()) or self._parse_id_var(any_token=False)

        while self._match(TokenType.DOT):
            if catalog:
                # This allows nesting the table in arbitrarily many dot expressions if needed
                table = self.expression(exp.Dot, this=table, expression=self._parse_id_var())
            else:
                catalog = db
                db = table
                table = self._parse_id_var()

        if not table:
            self.raise_error(f"Expected table name but got {self._curr}")

        return self.expression(
            exp.Table, this=table, db=db, catalog=catalog, pivots=self._parse_pivots()
        )

    def _parse_table(
        self, schema: bool = False, alias_tokens: t.Optional[t.Collection[TokenType]] = None
    ) -> t.Optional[exp.Expression]:
        lateral = self._parse_lateral()

        if lateral:
            return lateral

        unnest = self._parse_unnest()

        if unnest:
            return unnest

        values = self._parse_derived_table_values()

        if values:
            return values

        subquery = self._parse_select(table=True)

        if subquery:
            return subquery

        this = self._parse_table_parts(schema=schema)

        if schema:
            return self._parse_schema(this=this)

        if self.alias_post_tablesample:
            table_sample = self._parse_table_sample()

        alias = self._parse_table_alias(alias_tokens=alias_tokens or self.TABLE_ALIAS_TOKENS)

        if alias:
            this.set("alias", alias)

        if self._match_pair(TokenType.WITH, TokenType.L_PAREN):
            this.set(
                "hints",
                self._parse_csv(lambda: self._parse_function() or self._parse_var(any_token=True)),
            )
            self._match_r_paren()

        if not self.alias_post_tablesample:
            table_sample = self._parse_table_sample()

        if table_sample:
            table_sample.set("this", this)
            this = table_sample

        return this

    def _parse_unnest(self) -> t.Optional[exp.Expression]:
        if not self._match(TokenType.UNNEST):
            return None

        expressions = self._parse_wrapped_csv(self._parse_column)
        ordinality = bool(self._match(TokenType.WITH) and self._match(TokenType.ORDINALITY))
        alias = self._parse_table_alias()

        if alias and self.unnest_column_only:
            if alias.args.get("columns"):
                self.raise_error("Unexpected extra column alias in unnest.")
            alias.set("columns", [alias.this])
            alias.set("this", None)

        offset = None
        if self._match_pair(TokenType.WITH, TokenType.OFFSET):
            self._match(TokenType.ALIAS)
            offset = self._parse_conjunction()

        return self.expression(
            exp.Unnest,
            expressions=expressions,
            ordinality=ordinality,
            alias=alias,
            offset=offset,
        )

    def _parse_derived_table_values(self) -> t.Optional[exp.Expression]:
        is_derived = self._match_pair(TokenType.L_PAREN, TokenType.VALUES)
        if not is_derived and not self._match(TokenType.VALUES):
            return None

        expressions = self._parse_csv(self._parse_value)

        if is_derived:
            self._match_r_paren()

        return self.expression(exp.Values, expressions=expressions, alias=self._parse_table_alias())

    def _parse_table_sample(self) -> t.Optional[exp.Expression]:
        if not self._match(TokenType.TABLE_SAMPLE):
            return None

        method = self._parse_var()
        bucket_numerator = None
        bucket_denominator = None
        bucket_field = None
        percent = None
        rows = None
        size = None
        seed = None

        self._match_l_paren()

        if self._match(TokenType.BUCKET):
            bucket_numerator = self._parse_number()
            self._match(TokenType.OUT_OF)
            bucket_denominator = bucket_denominator = self._parse_number()
            self._match(TokenType.ON)
            bucket_field = self._parse_field()
        else:
            num = self._parse_number()

            if self._match(TokenType.PERCENT):
                percent = num
            elif self._match(TokenType.ROWS):
                rows = num
            else:
                size = num

        self._match_r_paren()

        if self._match(TokenType.SEED):
            seed = self._parse_wrapped(self._parse_number)

        return self.expression(
            exp.TableSample,
            method=method,
            bucket_numerator=bucket_numerator,
            bucket_denominator=bucket_denominator,
            bucket_field=bucket_field,
            percent=percent,
            rows=rows,
            size=size,
            seed=seed,
        )

    def _parse_pivots(self) -> t.List[t.Optional[exp.Expression]]:
        return list(iter(self._parse_pivot, None))

    def _parse_pivot(self) -> t.Optional[exp.Expression]:
        index = self._index

        if self._match(TokenType.PIVOT):
            unpivot = False
        elif self._match(TokenType.UNPIVOT):
            unpivot = True
        else:
            return None

        expressions = []
        field = None

        if not self._match(TokenType.L_PAREN):
            self._retreat(index)
            return None

        if unpivot:
            expressions = self._parse_csv(self._parse_column)
        else:
            expressions = self._parse_csv(lambda: self._parse_alias(self._parse_function()))

        if not self._match(TokenType.FOR):
            self.raise_error("Expecting FOR")

        value = self._parse_column()

        if not self._match(TokenType.IN):
            self.raise_error("Expecting IN")

        field = self._parse_in(value)

        self._match_r_paren()

        return self.expression(exp.Pivot, expressions=expressions, field=field, unpivot=unpivot)

    def _parse_where(self, skip_where_token: bool = False) -> t.Optional[exp.Expression]:
        if not skip_where_token and not self._match(TokenType.WHERE):
            return None

        return self.expression(
            exp.Where, comments=self._prev_comments, this=self._parse_conjunction()
        )

    def _parse_group(self, skip_group_by_token: bool = False) -> t.Optional[exp.Expression]:
        if not skip_group_by_token and not self._match(TokenType.GROUP_BY):
            return None

        expressions = self._parse_csv(self._parse_conjunction)
        grouping_sets = self._parse_grouping_sets()

        self._match(TokenType.COMMA)
        with_ = self._match(TokenType.WITH)
        cube = self._match(TokenType.CUBE) and (
            with_ or self._parse_wrapped_csv(self._parse_column)
        )

        self._match(TokenType.COMMA)
        rollup = self._match(TokenType.ROLLUP) and (
            with_ or self._parse_wrapped_csv(self._parse_column)
        )

        return self.expression(
            exp.Group,
            expressions=expressions,
            grouping_sets=grouping_sets,
            cube=cube,
            rollup=rollup,
        )

    def _parse_grouping_sets(self) -> t.Optional[t.List[t.Optional[exp.Expression]]]:
        if not self._match(TokenType.GROUPING_SETS):
            return None

        return self._parse_wrapped_csv(self._parse_grouping_set)

    def _parse_grouping_set(self) -> t.Optional[exp.Expression]:
        if self._match(TokenType.L_PAREN):
            grouping_set = self._parse_csv(self._parse_column)
            self._match_r_paren()
            return self.expression(exp.Tuple, expressions=grouping_set)

        return self._parse_column()

    def _parse_having(self, skip_having_token: bool = False) -> t.Optional[exp.Expression]:
        if not skip_having_token and not self._match(TokenType.HAVING):
            return None
        return self.expression(exp.Having, this=self._parse_conjunction())

    def _parse_qualify(self) -> t.Optional[exp.Expression]:
        if not self._match(TokenType.QUALIFY):
            return None
        return self.expression(exp.Qualify, this=self._parse_conjunction())

    def _parse_order(
        self, this: t.Optional[exp.Expression] = None, skip_order_token: bool = False
    ) -> t.Optional[exp.Expression]:
        if not skip_order_token and not self._match(TokenType.ORDER_BY):
            return this

        return self.expression(
            exp.Order, this=this, expressions=self._parse_csv(self._parse_ordered)
        )

    def _parse_sort(
        self, token_type: TokenType, exp_class: t.Type[exp.Expression]
    ) -> t.Optional[exp.Expression]:
        if not self._match(token_type):
            return None
        return self.expression(exp_class, expressions=self._parse_csv(self._parse_ordered))

    def _parse_ordered(self) -> exp.Expression:
        this = self._parse_conjunction()
        self._match(TokenType.ASC)
        is_desc = self._match(TokenType.DESC)
        is_nulls_first = self._match(TokenType.NULLS_FIRST)
        is_nulls_last = self._match(TokenType.NULLS_LAST)
        desc = is_desc or False
        asc = not desc
        nulls_first = is_nulls_first or False
        explicitly_null_ordered = is_nulls_first or is_nulls_last
        if (
            not explicitly_null_ordered
            and (
                (asc and self.null_ordering == "nulls_are_small")
                or (desc and self.null_ordering != "nulls_are_small")
            )
            and self.null_ordering != "nulls_are_last"
        ):
            nulls_first = True

        return self.expression(exp.Ordered, this=this, desc=desc, nulls_first=nulls_first)

    def _parse_limit(
        self, this: t.Optional[exp.Expression] = None, top: bool = False
    ) -> t.Optional[exp.Expression]:
        if self._match(TokenType.TOP if top else TokenType.LIMIT):
            limit_paren = self._match(TokenType.L_PAREN)
            limit_exp = self.expression(
                exp.Limit, this=this, expression=self._parse_number() if top else self._parse_term()
            )

            if limit_paren:
                self._match_r_paren()

            return limit_exp

        if self._match(TokenType.FETCH):
            direction = self._match_set((TokenType.FIRST, TokenType.NEXT))
            direction = self._prev.text if direction else "FIRST"
            count = self._parse_number()
            self._match_set((TokenType.ROW, TokenType.ROWS))
            self._match(TokenType.ONLY)
            return self.expression(exp.Fetch, direction=direction, count=count)

        return this

    def _parse_offset(self, this: t.Optional[exp.Expression] = None) -> t.Optional[exp.Expression]:
        if not self._match_set((TokenType.OFFSET, TokenType.COMMA)):
            return this

        count = self._parse_number()
        self._match_set((TokenType.ROW, TokenType.ROWS))
        return self.expression(exp.Offset, this=this, expression=count)

    def _parse_lock(self) -> t.Optional[exp.Expression]:
        if self._match_text_seq("FOR", "UPDATE"):
            return self.expression(exp.Lock, update=True)
        if self._match_text_seq("FOR", "SHARE"):
            return self.expression(exp.Lock, update=False)

        return None

    def _parse_set_operations(self, this: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
        if not self._match_set(self.SET_OPERATIONS):
            return this

        token_type = self._prev.token_type

        if token_type == TokenType.UNION:
            expression = exp.Union
        elif token_type == TokenType.EXCEPT:
            expression = exp.Except
        else:
            expression = exp.Intersect

        return self.expression(
            expression,
            this=this,
            distinct=self._match(TokenType.DISTINCT) or not self._match(TokenType.ALL),
            expression=self._parse_set_operations(self._parse_select(nested=True)),
        )

    def _parse_expression(self) -> t.Optional[exp.Expression]:
        return self._parse_alias(self._parse_conjunction())

    def _parse_conjunction(self) -> t.Optional[exp.Expression]:
        return self._parse_tokens(self._parse_equality, self.CONJUNCTION)

    def _parse_equality(self) -> t.Optional[exp.Expression]:
        return self._parse_tokens(self._parse_comparison, self.EQUALITY)

    def _parse_comparison(self) -> t.Optional[exp.Expression]:
        return self._parse_tokens(self._parse_range, self.COMPARISON)

    def _parse_range(self) -> t.Optional[exp.Expression]:
        this = self._parse_bitwise()
        negate = self._match(TokenType.NOT)

        if self._match_set(self.RANGE_PARSERS):
            this = self.RANGE_PARSERS[self._prev.token_type](self, this)
        elif self._match(TokenType.ISNULL):
            this = self.expression(exp.Is, this=this, expression=exp.Null())

        # Postgres supports ISNULL and NOTNULL for conditions.
        # https://blog.andreiavram.ro/postgresql-null-composite-type/
        if self._match(TokenType.NOTNULL):
            this = self.expression(exp.Is, this=this, expression=exp.Null())
            this = self.expression(exp.Not, this=this)

        if negate:
            this = self.expression(exp.Not, this=this)

        if self._match(TokenType.IS):
            this = self._parse_is(this)

        return this

    def _parse_is(self, this: t.Optional[exp.Expression]) -> exp.Expression:
        negate = self._match(TokenType.NOT)
        if self._match(TokenType.DISTINCT_FROM):
            klass = exp.NullSafeEQ if negate else exp.NullSafeNEQ
            return self.expression(klass, this=this, expression=self._parse_expression())

        this = self.expression(
            exp.Is,
            this=this,
            expression=self._parse_null() or self._parse_boolean(),
        )
        return self.expression(exp.Not, this=this) if negate else this

    def _parse_in(self, this: t.Optional[exp.Expression]) -> exp.Expression:
        unnest = self._parse_unnest()
        if unnest:
            this = self.expression(exp.In, this=this, unnest=unnest)
        elif self._match(TokenType.L_PAREN):
            expressions = self._parse_csv(self._parse_select_or_expression)

            if len(expressions) == 1 and isinstance(expressions[0], exp.Subqueryable):
                this = self.expression(exp.In, this=this, query=expressions[0])
            else:
                this = self.expression(exp.In, this=this, expressions=expressions)

            self._match_r_paren()
        else:
            this = self.expression(exp.In, this=this, field=self._parse_field())

        return this

    def _parse_between(self, this: exp.Expression) -> exp.Expression:
        low = self._parse_bitwise()
        self._match(TokenType.AND)
        high = self._parse_bitwise()
        return self.expression(exp.Between, this=this, low=low, high=high)

    def _parse_escape(self, this: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
        if not self._match(TokenType.ESCAPE):
            return this
        return self.expression(exp.Escape, this=this, expression=self._parse_string())

    def _parse_bitwise(self) -> t.Optional[exp.Expression]:
        this = self._parse_term()

        while True:
            if self._match_set(self.BITWISE):
                this = self.expression(
                    self.BITWISE[self._prev.token_type],
                    this=this,
                    expression=self._parse_term(),
                )
            elif self._match_pair(TokenType.LT, TokenType.LT):
                this = self.expression(
                    exp.BitwiseLeftShift, this=this, expression=self._parse_term()
                )
            elif self._match_pair(TokenType.GT, TokenType.GT):
                this = self.expression(
                    exp.BitwiseRightShift, this=this, expression=self._parse_term()
                )
            else:
                break

        return this

    def _parse_term(self) -> t.Optional[exp.Expression]:
        return self._parse_tokens(self._parse_factor, self.TERM)

    def _parse_factor(self) -> t.Optional[exp.Expression]:
        return self._parse_tokens(self._parse_unary, self.FACTOR)

    def _parse_unary(self) -> t.Optional[exp.Expression]:
        if self._match_set(self.UNARY_PARSERS):
            return self.UNARY_PARSERS[self._prev.token_type](self)
        return self._parse_at_time_zone(self._parse_type())

    def _parse_type(self) -> t.Optional[exp.Expression]:
        if self._match(TokenType.INTERVAL):
            return self.expression(exp.Interval, this=self._parse_term(), unit=self._parse_var())

        index = self._index
        type_token = self._parse_types(check_func=True)
        this = self._parse_column()

        if type_token:
            if this and not isinstance(this, exp.Star):
                return self.expression(exp.Cast, this=this, to=type_token)
            if not type_token.args.get("expressions"):
                self._retreat(index)
                return self._parse_column()
            return type_token

        return this

    def _parse_types(self, check_func: bool = False) -> t.Optional[exp.Expression]:
        index = self._index

        if not self._match_set(self.TYPE_TOKENS):
            return None

        type_token = self._prev.token_type

        if type_token == TokenType.PSEUDO_TYPE:
            return self.expression(exp.PseudoType, this=self._prev.text)

        nested = type_token in self.NESTED_TYPE_TOKENS
        is_struct = type_token == TokenType.STRUCT
        expressions = None
        maybe_func = False

        if self._match(TokenType.L_PAREN):
            if is_struct:
                expressions = self._parse_csv(self._parse_struct_kwargs)
            elif nested:
                expressions = self._parse_csv(self._parse_types)
            else:
                expressions = self._parse_csv(self._parse_conjunction)

            if not expressions:
                self._retreat(index)
                return None

            self._match_r_paren()
            maybe_func = True

        if not nested and self._match_pair(TokenType.L_BRACKET, TokenType.R_BRACKET):
            this = exp.DataType(
                this=exp.DataType.Type.ARRAY,
                expressions=[exp.DataType.build(type_token.value, expressions=expressions)],
                nested=True,
            )

            while self._match_pair(TokenType.L_BRACKET, TokenType.R_BRACKET):
                this = exp.DataType(
                    this=exp.DataType.Type.ARRAY,
                    expressions=[this],
                    nested=True,
                )

            return this

        if self._match(TokenType.L_BRACKET):
            self._retreat(index)
            return None

        values: t.Optional[t.List[t.Optional[exp.Expression]]] = None
        if nested and self._match(TokenType.LT):
            if is_struct:
                expressions = self._parse_csv(self._parse_struct_kwargs)
            else:
                expressions = self._parse_csv(self._parse_types)

            if not self._match(TokenType.GT):
                self.raise_error("Expecting >")

            if self._match_set((TokenType.L_BRACKET, TokenType.L_PAREN)):
                values = self._parse_csv(self._parse_conjunction)
                self._match_set((TokenType.R_BRACKET, TokenType.R_PAREN))

        value: t.Optional[exp.Expression] = None
        if type_token in self.TIMESTAMPS:
            if self._match(TokenType.WITH_TIME_ZONE) or type_token == TokenType.TIMESTAMPTZ:
                value = exp.DataType(this=exp.DataType.Type.TIMESTAMPTZ, expressions=expressions)
            elif (
                self._match(TokenType.WITH_LOCAL_TIME_ZONE) or type_token == TokenType.TIMESTAMPLTZ
            ):
                value = exp.DataType(this=exp.DataType.Type.TIMESTAMPLTZ, expressions=expressions)
            elif self._match(TokenType.WITHOUT_TIME_ZONE):
                if type_token == TokenType.TIME:
                    value = exp.DataType(this=exp.DataType.Type.TIME, expressions=expressions)
                else:
                    value = exp.DataType(this=exp.DataType.Type.TIMESTAMP, expressions=expressions)

            maybe_func = maybe_func and value is None

            if value is None:
                value = exp.DataType(this=exp.DataType.Type.TIMESTAMP, expressions=expressions)
        elif type_token == TokenType.INTERVAL:
            value = self.expression(exp.Interval, unit=self._parse_var())

        if maybe_func and check_func:
            index2 = self._index
            peek = self._parse_string()

            if not peek:
                self._retreat(index)
                return None

            self._retreat(index2)

        if value:
            return value

        return exp.DataType(
            this=exp.DataType.Type[type_token.value.upper()],
            expressions=expressions,
            nested=nested,
            values=values,
        )

    def _parse_struct_kwargs(self) -> t.Optional[exp.Expression]:
        if self._curr and self._curr.token_type in self.TYPE_TOKENS:
            return self._parse_types()

        this = self._parse_id_var()
        self._match(TokenType.COLON)
        data_type = self._parse_types()

        if not data_type:
            return None
        return self.expression(exp.StructKwarg, this=this, expression=data_type)

    def _parse_at_time_zone(self, this: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
        if not self._match(TokenType.AT_TIME_ZONE):
            return this
        return self.expression(exp.AtTimeZone, this=this, zone=self._parse_unary())

    def _parse_column(self) -> t.Optional[exp.Expression]:
        this = self._parse_field()
        if isinstance(this, exp.Identifier):
            this = self.expression(exp.Column, this=this)
        elif not this:
            return self._parse_bracket(this)
        this = self._parse_bracket(this)

        while self._match_set(self.COLUMN_OPERATORS):
            op_token = self._prev.token_type
            op = self.COLUMN_OPERATORS.get(op_token)

            if op_token == TokenType.DCOLON:
                field = self._parse_types()
                if not field:
                    self.raise_error("Expected type")
            elif op:
                self._advance()
                value = self._prev.text
                field = (
                    exp.Literal.number(value)
                    if self._prev.token_type == TokenType.NUMBER
                    else exp.Literal.string(value)
                )
            else:
                field = self._parse_star() or self._parse_function() or self._parse_id_var()

            if isinstance(field, exp.Func):
                # bigquery allows function calls like x.y.count(...)
                # SAFE.SUBSTR(...)
                # https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-reference#function_call_rules
                this = self._replace_columns_with_dots(this)

            if op:
                this = op(self, this, field)
            elif isinstance(this, exp.Column) and not this.table:
                this = self.expression(exp.Column, this=field, table=this.this)
            else:
                this = self.expression(exp.Dot, this=this, expression=field)
            this = self._parse_bracket(this)

        return this

    def _parse_primary(self) -> t.Optional[exp.Expression]:
        if self._match_set(self.PRIMARY_PARSERS):
            token_type = self._prev.token_type
            primary = self.PRIMARY_PARSERS[token_type](self, self._prev)

            if token_type == TokenType.STRING:
                expressions = [primary]
                while self._match(TokenType.STRING):
                    expressions.append(exp.Literal.string(self._prev.text))
                if len(expressions) > 1:
                    return self.expression(exp.Concat, expressions=expressions)
            return primary

        if self._match_pair(TokenType.DOT, TokenType.NUMBER):
            return exp.Literal.number(f"0.{self._prev.text}")

        if self._match(TokenType.L_PAREN):
            comments = self._prev_comments
            query = self._parse_select()

            if query:
                expressions = [query]
            else:
                expressions = self._parse_csv(
                    lambda: self._parse_alias(self._parse_conjunction(), explicit=True)
                )

            this = seq_get(expressions, 0)
            self._parse_query_modifiers(this)
            self._match_r_paren()

            if isinstance(this, exp.Subqueryable):
                this = self._parse_set_operations(
                    self._parse_subquery(this=this, parse_alias=False)
                )
            elif len(expressions) > 1:
                this = self.expression(exp.Tuple, expressions=expressions)
            else:
                this = self.expression(exp.Paren, this=this)

            if this and comments:
                this.comments = comments

            return this

        return None

    def _parse_field(self, any_token: bool = False) -> t.Optional[exp.Expression]:
        return self._parse_primary() or self._parse_function() or self._parse_id_var(any_token)

    def _parse_function(
        self, functions: t.Optional[t.Dict[str, t.Callable]] = None
    ) -> t.Optional[exp.Expression]:
        if not self._curr:
            return None

        token_type = self._curr.token_type

        if self._match_set(self.NO_PAREN_FUNCTION_PARSERS):
            return self.NO_PAREN_FUNCTION_PARSERS[token_type](self)

        if not self._next or self._next.token_type != TokenType.L_PAREN:
            if token_type in self.NO_PAREN_FUNCTIONS:
                self._advance()
                return self.expression(self.NO_PAREN_FUNCTIONS[token_type])

            return None

        if token_type not in self.FUNC_TOKENS:
            return None

        this = self._curr.text
        upper = this.upper()
        self._advance(2)

        parser = self.FUNCTION_PARSERS.get(upper)

        if parser:
            this = parser(self)
        else:
            subquery_predicate = self.SUBQUERY_PREDICATES.get(token_type)

            if subquery_predicate and self._curr.token_type in (TokenType.SELECT, TokenType.WITH):
                this = self.expression(subquery_predicate, this=self._parse_select())
                self._match_r_paren()
                return this

            if functions is None:
                functions = self.FUNCTIONS

            function = functions.get(upper)
            args = self._parse_csv(self._parse_lambda)

            if function:
                # Clickhouse supports function calls like foo(x, y)(z), so for these we need to also parse the
                # second parameter list (i.e. "(z)") and the corresponding function will receive both arg lists.
                if count_params(function) == 2:
                    params = None
                    if self._match_pair(TokenType.R_PAREN, TokenType.L_PAREN):
                        params = self._parse_csv(self._parse_lambda)

                    this = function(args, params)
                else:
                    this = function(args)

                self.validate_expression(this, args)
            else:
                this = self.expression(exp.Anonymous, this=this, expressions=args)

        self._match_r_paren(this)
        return self._parse_window(this)

    def _parse_user_defined_function(
        self, kind: t.Optional[TokenType] = None
    ) -> t.Optional[exp.Expression]:
        this = self._parse_id_var()

        while self._match(TokenType.DOT):
            this = self.expression(exp.Dot, this=this, expression=self._parse_id_var())

        if not self._match(TokenType.L_PAREN):
            return this

        expressions = self._parse_csv(self._parse_udf_kwarg)
        self._match_r_paren()
        return self.expression(
            exp.UserDefinedFunction, this=this, expressions=expressions, wrapped=True
        )

    def _parse_introducer(self, token: Token) -> t.Optional[exp.Expression]:
        literal = self._parse_primary()
        if literal:
            return self.expression(exp.Introducer, this=token.text, expression=literal)

        return self.expression(exp.Identifier, this=token.text)

    def _parse_national(self, token: Token) -> exp.Expression:
        return self.expression(exp.National, this=exp.Literal.string(token.text))

    def _parse_session_parameter(self) -> exp.Expression:
        kind = None
        this = self._parse_id_var() or self._parse_primary()

        if this and self._match(TokenType.DOT):
            kind = this.name
            this = self._parse_var() or self._parse_primary()

        return self.expression(exp.SessionParameter, this=this, kind=kind)

    def _parse_udf_kwarg(self) -> t.Optional[exp.Expression]:
        this = self._parse_id_var()
        kind = self._parse_types()

        if not kind:
            return this

        return self.expression(exp.UserDefinedFunctionKwarg, this=this, kind=kind)

    def _parse_lambda(self) -> t.Optional[exp.Expression]:
        index = self._index

        if self._match(TokenType.L_PAREN):
            expressions = self._parse_csv(self._parse_id_var)

            if not self._match(TokenType.R_PAREN):
                self._retreat(index)
        else:
            expressions = [self._parse_id_var()]

        if self._match_set(self.LAMBDAS):
            return self.LAMBDAS[self._prev.token_type](self, expressions)

        self._retreat(index)

        this: t.Optional[exp.Expression]

        if self._match(TokenType.DISTINCT):
            this = self.expression(
                exp.Distinct, expressions=self._parse_csv(self._parse_conjunction)
            )
        else:
            this = self._parse_select_or_expression()

        if self._match(TokenType.IGNORE_NULLS):
            this = self.expression(exp.IgnoreNulls, this=this)
        else:
            self._match(TokenType.RESPECT_NULLS)

        return self._parse_limit(self._parse_order(this))

    def _parse_schema(self, this: t.Optional[exp.Expression] = None) -> t.Optional[exp.Expression]:
        index = self._index
        if not self._match(TokenType.L_PAREN) or self._match(TokenType.SELECT):
            self._retreat(index)
            return this

        args = self._parse_csv(
            lambda: self._parse_constraint()
            or self._parse_column_def(self._parse_field(any_token=True))
        )
        self._match_r_paren()
        return self.expression(exp.Schema, this=this, expressions=args)

    def _parse_column_def(self, this: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
        kind = self._parse_types()

        constraints = []
        while True:
            constraint = self._parse_column_constraint()
            if not constraint:
                break
            constraints.append(constraint)

        if not kind and not constraints:
            return this

        return self.expression(exp.ColumnDef, this=this, kind=kind, constraints=constraints)

    def _parse_column_constraint(self) -> t.Optional[exp.Expression]:
        this = self._parse_references()

        if this:
            return this

        if self._match(TokenType.CONSTRAINT):
            this = self._parse_id_var()

        kind: exp.Expression

        if self._match(TokenType.AUTO_INCREMENT):
            kind = exp.AutoIncrementColumnConstraint()
        elif self._match(TokenType.CHECK):
            constraint = self._parse_wrapped(self._parse_conjunction)
            kind = self.expression(exp.CheckColumnConstraint, this=constraint)
        elif self._match(TokenType.COLLATE):
            kind = self.expression(exp.CollateColumnConstraint, this=self._parse_var())
        elif self._match(TokenType.ENCODE):
            kind = self.expression(exp.EncodeColumnConstraint, this=self._parse_var())
        elif self._match(TokenType.DEFAULT):
            kind = self.expression(exp.DefaultColumnConstraint, this=self._parse_bitwise())
        elif self._match_pair(TokenType.NOT, TokenType.NULL):
            kind = exp.NotNullColumnConstraint()
        elif self._match(TokenType.NULL):
            kind = exp.NotNullColumnConstraint(allow_null=True)
        elif self._match(TokenType.SCHEMA_COMMENT):
            kind = self.expression(exp.CommentColumnConstraint, this=self._parse_string())
        elif self._match(TokenType.PRIMARY_KEY):
            desc = None
            if self._match(TokenType.ASC) or self._match(TokenType.DESC):
                desc = self._prev.token_type == TokenType.DESC
            kind = exp.PrimaryKeyColumnConstraint(desc=desc)
        elif self._match(TokenType.UNIQUE):
            kind = exp.UniqueColumnConstraint()
        elif self._match(TokenType.GENERATED):
            if self._match(TokenType.BY_DEFAULT):
                kind = self.expression(exp.GeneratedAsIdentityColumnConstraint, this=False)
            else:
                self._match(TokenType.ALWAYS)
                kind = self.expression(exp.GeneratedAsIdentityColumnConstraint, this=True)
            self._match_pair(TokenType.ALIAS, TokenType.IDENTITY)

            if self._match(TokenType.L_PAREN):
                if self._match_text_seq("START", "WITH"):
                    kind.set("start", self._parse_bitwise())
                if self._match_text_seq("INCREMENT", "BY"):
                    kind.set("increment", self._parse_bitwise())

                self._match_r_paren()
        else:
            return this

        return self.expression(exp.ColumnConstraint, this=this, kind=kind)

    def _parse_constraint(self) -> t.Optional[exp.Expression]:
        if not self._match(TokenType.CONSTRAINT):
            return self._parse_unnamed_constraint()

        this = self._parse_id_var()
        expressions = []

        while True:
            constraint = self._parse_unnamed_constraint() or self._parse_function()
            if not constraint:
                break
            expressions.append(constraint)

        return self.expression(exp.Constraint, this=this, expressions=expressions)

    def _parse_unnamed_constraint(self) -> t.Optional[exp.Expression]:
        if not self._match_set(self.CONSTRAINT_PARSERS):
            return None
        return self.CONSTRAINT_PARSERS[self._prev.token_type](self)

    def _parse_unique(self) -> exp.Expression:
        return self.expression(exp.Unique, expressions=self._parse_wrapped_id_vars())

    def _parse_key_constraint_options(self) -> t.List[str]:
        options = []
        while True:
            if not self._curr:
                break

            if self._match(TokenType.ON):
                action = None
                on = self._advance_any() and self._prev.text

                if self._match(TokenType.NO_ACTION):
                    action = "NO ACTION"
                elif self._match(TokenType.CASCADE):
                    action = "CASCADE"
                elif self._match_pair(TokenType.SET, TokenType.NULL):
                    action = "SET NULL"
                elif self._match_pair(TokenType.SET, TokenType.DEFAULT):
                    action = "SET DEFAULT"
                else:
                    self.raise_error("Invalid key constraint")

                options.append(f"ON {on} {action}")
            elif self._match_text_seq("NOT", "ENFORCED"):
                options.append("NOT ENFORCED")
            elif self._match_text_seq("DEFERRABLE"):
                options.append("DEFERRABLE")
            elif self._match_text_seq("INITIALLY", "DEFERRED"):
                options.append("INITIALLY DEFERRED")
            elif self._match_text_seq("NORELY"):
                options.append("NORELY")
            elif self._match_text_seq("MATCH", "FULL"):
                options.append("MATCH FULL")
            else:
                break

        return options

    def _parse_references(self) -> t.Optional[exp.Expression]:
        if not self._match(TokenType.REFERENCES):
            return None

        expressions = None
        this = self._parse_id_var()

        if self._match(TokenType.L_PAREN, advance=False):
            expressions = self._parse_wrapped_id_vars()

        options = self._parse_key_constraint_options()
        return self.expression(exp.Reference, this=this, expressions=expressions, options=options)

    def _parse_foreign_key(self) -> exp.Expression:
        expressions = self._parse_wrapped_id_vars()
        reference = self._parse_references()
        options = {}

        while self._match(TokenType.ON):
            if not self._match_set((TokenType.DELETE, TokenType.UPDATE)):
                self.raise_error("Expected DELETE or UPDATE")

            kind = self._prev.text.lower()

            if self._match(TokenType.NO_ACTION):
                action = "NO ACTION"
            elif self._match(TokenType.SET):
                self._match_set((TokenType.NULL, TokenType.DEFAULT))
                action = "SET " + self._prev.text.upper()
            else:
                self._advance()
                action = self._prev.text.upper()

            options[kind] = action

        return self.expression(
            exp.ForeignKey, expressions=expressions, reference=reference, **options  # type: ignore
        )

    def _parse_primary_key(self) -> exp.Expression:
        expressions = self._parse_wrapped_id_vars()
        options = self._parse_key_constraint_options()
        return self.expression(exp.PrimaryKey, expressions=expressions, options=options)

    def _parse_bracket(self, this: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
        if not self._match_set((TokenType.L_BRACKET, TokenType.L_BRACE)):
            return this

        bracket_kind = self._prev.token_type
        expressions: t.List[t.Optional[exp.Expression]]

        if self._match(TokenType.COLON):
            expressions = [self.expression(exp.Slice, expression=self._parse_conjunction())]
        else:
            expressions = self._parse_csv(lambda: self._parse_slice(self._parse_conjunction()))

        # https://duckdb.org/docs/sql/data_types/struct.html#creating-structs
        if bracket_kind == TokenType.L_BRACE:
            this = self.expression(exp.Struct, expressions=expressions)
        elif not this or this.name.upper() == "ARRAY":
            this = self.expression(exp.Array, expressions=expressions)
        else:
            expressions = apply_index_offset(expressions, -self.index_offset)
            this = self.expression(exp.Bracket, this=this, expressions=expressions)

        if not self._match(TokenType.R_BRACKET) and bracket_kind == TokenType.L_BRACKET:
            self.raise_error("Expected ]")
        elif not self._match(TokenType.R_BRACE) and bracket_kind == TokenType.L_BRACE:
            self.raise_error("Expected }")

        this.comments = self._prev_comments
        return self._parse_bracket(this)

    def _parse_slice(self, this: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
        if self._match(TokenType.COLON):
            return self.expression(exp.Slice, this=this, expression=self._parse_conjunction())
        return this

    def _parse_case(self) -> t.Optional[exp.Expression]:
        ifs = []
        default = None

        expression = self._parse_conjunction()

        while self._match(TokenType.WHEN):
            this = self._parse_conjunction()
            self._match(TokenType.THEN)
            then = self._parse_conjunction()
            ifs.append(self.expression(exp.If, this=this, true=then))

        if self._match(TokenType.ELSE):
            default = self._parse_conjunction()

        if not self._match(TokenType.END):
            self.raise_error("Expected END after CASE", self._prev)

        return self._parse_window(
            self.expression(exp.Case, this=expression, ifs=ifs, default=default)
        )

    def _parse_if(self) -> t.Optional[exp.Expression]:
        if self._match(TokenType.L_PAREN):
            args = self._parse_csv(self._parse_conjunction)
            this = exp.If.from_arg_list(args)
            self.validate_expression(this, args)
            self._match_r_paren()
        else:
            condition = self._parse_conjunction()
            self._match(TokenType.THEN)
            true = self._parse_conjunction()
            false = self._parse_conjunction() if self._match(TokenType.ELSE) else None
            self._match(TokenType.END)
            this = self.expression(exp.If, this=condition, true=true, false=false)

        return self._parse_window(this)

    def _parse_extract(self) -> exp.Expression:
        this = self._parse_function() or self._parse_var() or self._parse_type()

        if self._match(TokenType.FROM):
            return self.expression(exp.Extract, this=this, expression=self._parse_bitwise())

        if not self._match(TokenType.COMMA):
            self.raise_error("Expected FROM or comma after EXTRACT", self._prev)

        return self.expression(exp.Extract, this=this, expression=self._parse_bitwise())

    def _parse_cast(self, strict: bool) -> exp.Expression:
        this = self._parse_conjunction()

        if not self._match(TokenType.ALIAS):
            self.raise_error("Expected AS after CAST")

        to = self._parse_types()

        if not to:
            self.raise_error("Expected TYPE after CAST")
        elif to.this == exp.DataType.Type.CHAR:
            if self._match(TokenType.CHARACTER_SET):
                to = self.expression(exp.CharacterSet, this=self._parse_var_or_string())

        return self.expression(exp.Cast if strict else exp.TryCast, this=this, to=to)

    def _parse_string_agg(self) -> exp.Expression:
        expression: t.Optional[exp.Expression]

        if self._match(TokenType.DISTINCT):
            args = self._parse_csv(self._parse_conjunction)
            expression = self.expression(exp.Distinct, expressions=[seq_get(args, 0)])
        else:
            args = self._parse_csv(self._parse_conjunction)
            expression = seq_get(args, 0)

        index = self._index
        if not self._match(TokenType.R_PAREN):
            # postgres: STRING_AGG([DISTINCT] expression, separator [ORDER BY expression1 {ASC | DESC} [, ...]])
            order = self._parse_order(this=expression)
            return self.expression(exp.GroupConcat, this=order, separator=seq_get(args, 1))

        # Checks if we can parse an order clause: WITHIN GROUP (ORDER BY <order_by_expression_list> [ASC | DESC]).
        # This is done "manually", instead of letting _parse_window parse it into an exp.WithinGroup node, so that
        # the STRING_AGG call is parsed like in MySQL / SQLite and can thus be transpiled more easily to them.
        if not self._match(TokenType.WITHIN_GROUP):
            self._retreat(index)
            this = exp.GroupConcat.from_arg_list(args)
            self.validate_expression(this, args)
            return this

        self._match_l_paren()  # The corresponding match_r_paren will be called in parse_function (caller)
        order = self._parse_order(this=expression)
        return self.expression(exp.GroupConcat, this=order, separator=seq_get(args, 1))

    def _parse_convert(self, strict: bool) -> t.Optional[exp.Expression]:
        to: t.Optional[exp.Expression]
        this = self._parse_column()

        if self._match(TokenType.USING):
            to = self.expression(exp.CharacterSet, this=self._parse_var())
        elif self._match(TokenType.COMMA):
            to = self._parse_types()
        else:
            to = None

        return self.expression(exp.Cast if strict else exp.TryCast, this=this, to=to)

    def _parse_position(self, haystack_first: bool = False) -> exp.Expression:
        args = self._parse_csv(self._parse_bitwise)

        if self._match(TokenType.IN):
            return self.expression(
                exp.StrPosition, this=self._parse_bitwise(), substr=seq_get(args, 0)
            )

        if haystack_first:
            haystack = seq_get(args, 0)
            needle = seq_get(args, 1)
        else:
            needle = seq_get(args, 0)
            haystack = seq_get(args, 1)

        this = exp.StrPosition(this=haystack, substr=needle, position=seq_get(args, 2))

        self.validate_expression(this, args)

        return this

    def _parse_join_hint(self, func_name: str) -> exp.Expression:
        args = self._parse_csv(self._parse_table)
        return exp.JoinHint(this=func_name.upper(), expressions=args)

    def _parse_substring(self) -> exp.Expression:
        # Postgres supports the form: substring(string [from int] [for int])
        # https://www.postgresql.org/docs/9.1/functions-string.html @ Table 9-6

        args = self._parse_csv(self._parse_bitwise)

        if self._match(TokenType.FROM):
            args.append(self._parse_bitwise())
            if self._match(TokenType.FOR):
                args.append(self._parse_bitwise())

        this = exp.Substring.from_arg_list(args)
        self.validate_expression(this, args)

        return this

    def _parse_trim(self) -> exp.Expression:
        # https://www.w3resource.com/sql/character-functions/trim.php
        # https://docs.oracle.com/javadb/10.8.3.0/ref/rreftrimfunc.html

        position = None
        collation = None

        if self._match_set(self.TRIM_TYPES):
            position = self._prev.text.upper()

        expression = self._parse_term()
        if self._match_set((TokenType.FROM, TokenType.COMMA)):
            this = self._parse_term()
        else:
            this = expression
            expression = None

        if self._match(TokenType.COLLATE):
            collation = self._parse_term()

        return self.expression(
            exp.Trim,
            this=this,
            position=position,
            expression=expression,
            collation=collation,
        )

    def _parse_window_clause(self) -> t.Optional[t.List[t.Optional[exp.Expression]]]:
        return self._match(TokenType.WINDOW) and self._parse_csv(self._parse_named_window)

    def _parse_named_window(self) -> t.Optional[exp.Expression]:
        return self._parse_window(self._parse_id_var(), alias=True)

    def _parse_window(
        self, this: t.Optional[exp.Expression], alias: bool = False
    ) -> t.Optional[exp.Expression]:
        if self._match(TokenType.FILTER):
            where = self._parse_wrapped(self._parse_where)
            this = self.expression(exp.Filter, this=this, expression=where)

        # T-SQL allows the OVER (...) syntax after WITHIN GROUP.
        # https://learn.microsoft.com/en-us/sql/t-sql/functions/percentile-disc-transact-sql?view=sql-server-ver16
        if self._match(TokenType.WITHIN_GROUP):
            order = self._parse_wrapped(self._parse_order)
            this = self.expression(exp.WithinGroup, this=this, expression=order)

        # SQL spec defines an optional [ { IGNORE | RESPECT } NULLS ] OVER
        # Some dialects choose to implement and some do not.
        # https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html

        # There is some code above in _parse_lambda that handles
        #   SELECT FIRST_VALUE(TABLE.COLUMN IGNORE|RESPECT NULLS) OVER ...

        # The below changes handle
        #   SELECT FIRST_VALUE(TABLE.COLUMN) IGNORE|RESPECT NULLS OVER ...

        # Oracle allows both formats
        #   (https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/img_text/first_value.html)
        #   and Snowflake chose to do the same for familiarity
        #   https://docs.snowflake.com/en/sql-reference/functions/first_value.html#usage-notes
        if self._match(TokenType.IGNORE_NULLS):
            this = self.expression(exp.IgnoreNulls, this=this)
        elif self._match(TokenType.RESPECT_NULLS):
            this = self.expression(exp.RespectNulls, this=this)

        # bigquery select from window x AS (partition by ...)
        if alias:
            self._match(TokenType.ALIAS)
        elif not self._match(TokenType.OVER):
            return this

        if not self._match(TokenType.L_PAREN):
            return self.expression(exp.Window, this=this, alias=self._parse_id_var(False))

        window_alias = self._parse_id_var(any_token=False, tokens=self.WINDOW_ALIAS_TOKENS)
        partition = self._parse_partition_by()
        order = self._parse_order()
        kind = self._match_set((TokenType.ROWS, TokenType.RANGE)) and self._prev.text

        if kind:
            self._match(TokenType.BETWEEN)
            start = self._parse_window_spec()
            self._match(TokenType.AND)
            end = self._parse_window_spec()

            spec = self.expression(
                exp.WindowSpec,
                kind=kind,
                start=start["value"],
                start_side=start["side"],
                end=end["value"],
                end_side=end["side"],
            )
        else:
            spec = None

        self._match_r_paren()

        return self.expression(
            exp.Window,
            this=this,
            partition_by=partition,
            order=order,
            spec=spec,
            alias=window_alias,
        )

    def _parse_window_spec(self) -> t.Dict[str, t.Optional[str | exp.Expression]]:
        self._match(TokenType.BETWEEN)

        return {
            "value": (
                self._match_set((TokenType.UNBOUNDED, TokenType.CURRENT_ROW)) and self._prev.text
            )
            or self._parse_bitwise(),
            "side": self._match_set((TokenType.PRECEDING, TokenType.FOLLOWING)) and self._prev.text,
        }

    def _parse_alias(
        self, this: t.Optional[exp.Expression], explicit: bool = False
    ) -> t.Optional[exp.Expression]:
        any_token = self._match(TokenType.ALIAS)

        if explicit and not any_token:
            return this

        if self._match(TokenType.L_PAREN):
            aliases = self.expression(
                exp.Aliases,
                this=this,
                expressions=self._parse_csv(lambda: self._parse_id_var(any_token)),
            )
            self._match_r_paren(aliases)
            return aliases

        alias = self._parse_id_var(any_token)

        if alias:
            return self.expression(exp.Alias, this=this, alias=alias)

        return this

    def _parse_id_var(
        self,
        any_token: bool = True,
        tokens: t.Optional[t.Collection[TokenType]] = None,
        prefix_tokens: t.Optional[t.Collection[TokenType]] = None,
    ) -> t.Optional[exp.Expression]:
        identifier = self._parse_identifier()

        if identifier:
            return identifier

        prefix = ""

        if prefix_tokens:
            while self._match_set(prefix_tokens):
                prefix += self._prev.text

        if (any_token and self._advance_any()) or self._match_set(tokens or self.ID_VAR_TOKENS):
            quoted = self._prev.token_type == TokenType.STRING
            return exp.Identifier(this=prefix + self._prev.text, quoted=quoted)

        return None

    def _parse_string(self) -> t.Optional[exp.Expression]:
        if self._match(TokenType.STRING):
            return self.PRIMARY_PARSERS[TokenType.STRING](self, self._prev)
        return self._parse_placeholder()

    def _parse_number(self) -> t.Optional[exp.Expression]:
        if self._match(TokenType.NUMBER):
            return self.PRIMARY_PARSERS[TokenType.NUMBER](self, self._prev)
        return self._parse_placeholder()

    def _parse_identifier(self) -> t.Optional[exp.Expression]:
        if self._match(TokenType.IDENTIFIER):
            return self.expression(exp.Identifier, this=self._prev.text, quoted=True)
        return self._parse_placeholder()

    def _parse_var(self, any_token: bool = False) -> t.Optional[exp.Expression]:
        if (any_token and self._advance_any()) or self._match(TokenType.VAR):
            return self.expression(exp.Var, this=self._prev.text)
        return self._parse_placeholder()

    def _advance_any(self) -> t.Optional[Token]:
        if self._curr and self._curr.token_type not in self.RESERVED_KEYWORDS:
            self._advance()
            return self._prev
        return None

    def _parse_var_or_string(self) -> t.Optional[exp.Expression]:
        return self._parse_var() or self._parse_string()

    def _parse_null(self) -> t.Optional[exp.Expression]:
        if self._match(TokenType.NULL):
            return self.PRIMARY_PARSERS[TokenType.NULL](self, self._prev)
        return None

    def _parse_boolean(self) -> t.Optional[exp.Expression]:
        if self._match(TokenType.TRUE):
            return self.PRIMARY_PARSERS[TokenType.TRUE](self, self._prev)
        if self._match(TokenType.FALSE):
            return self.PRIMARY_PARSERS[TokenType.FALSE](self, self._prev)
        return None

    def _parse_star(self) -> t.Optional[exp.Expression]:
        if self._match(TokenType.STAR):
            return self.PRIMARY_PARSERS[TokenType.STAR](self, self._prev)
        return None

    def _parse_placeholder(self) -> t.Optional[exp.Expression]:
        if self._match_set(self.PLACEHOLDER_PARSERS):
            placeholder = self.PLACEHOLDER_PARSERS[self._prev.token_type](self)
            if placeholder:
                return placeholder
            self._advance(-1)
        return None

    def _parse_except(self) -> t.Optional[t.List[t.Optional[exp.Expression]]]:
        if not self._match(TokenType.EXCEPT):
            return None
        if self._match(TokenType.L_PAREN, advance=False):
            return self._parse_wrapped_csv(self._parse_column)
        return self._parse_csv(self._parse_column)

    def _parse_replace(self) -> t.Optional[t.List[t.Optional[exp.Expression]]]:
        if not self._match(TokenType.REPLACE):
            return None
        if self._match(TokenType.L_PAREN, advance=False):
            return self._parse_wrapped_csv(self._parse_expression)
        return self._parse_csv(self._parse_expression)

    def _parse_csv(
        self, parse_method: t.Callable, sep: TokenType = TokenType.COMMA
    ) -> t.List[t.Optional[exp.Expression]]:
        parse_result = parse_method()
        items = [parse_result] if parse_result is not None else []

        while self._match(sep):
            if parse_result and self._prev_comments:
                parse_result.comments = self._prev_comments

            parse_result = parse_method()
            if parse_result is not None:
                items.append(parse_result)

        return items

    def _parse_tokens(
        self, parse_method: t.Callable, expressions: t.Dict
    ) -> t.Optional[exp.Expression]:
        this = parse_method()

        while self._match_set(expressions):
            this = self.expression(
                expressions[self._prev.token_type],
                this=this,
                comments=self._prev_comments,
                expression=parse_method(),
            )

        return this

    def _parse_wrapped_id_vars(self) -> t.List[t.Optional[exp.Expression]]:
        return self._parse_wrapped_csv(self._parse_id_var)

    def _parse_wrapped_csv(
        self, parse_method: t.Callable, sep: TokenType = TokenType.COMMA
    ) -> t.List[t.Optional[exp.Expression]]:
        return self._parse_wrapped(lambda: self._parse_csv(parse_method, sep=sep))

    def _parse_wrapped(self, parse_method: t.Callable) -> t.Any:
        self._match_l_paren()
        parse_result = parse_method()
        self._match_r_paren()
        return parse_result

    def _parse_select_or_expression(self) -> t.Optional[exp.Expression]:
        return self._parse_select() or self._parse_expression()

    def _parse_ddl_select(self) -> t.Optional[exp.Expression]:
        return self._parse_set_operations(
            self._parse_select(nested=True, parse_subquery_alias=False)
        )

    def _parse_transaction(self) -> exp.Expression:
        this = None
        if self._match_texts(self.TRANSACTION_KIND):
            this = self._prev.text

        self._match_texts({"TRANSACTION", "WORK"})

        modes = []
        while True:
            mode = []
            while self._match(TokenType.VAR):
                mode.append(self._prev.text)

            if mode:
                modes.append(" ".join(mode))
            if not self._match(TokenType.COMMA):
                break

        return self.expression(exp.Transaction, this=this, modes=modes)

    def _parse_commit_or_rollback(self) -> exp.Expression:
        chain = None
        savepoint = None
        is_rollback = self._prev.token_type == TokenType.ROLLBACK

        self._match_texts({"TRANSACTION", "WORK"})

        if self._match_text_seq("TO"):
            self._match_text_seq("SAVEPOINT")
            savepoint = self._parse_id_var()

        if self._match(TokenType.AND):
            chain = not self._match_text_seq("NO")
            self._match_text_seq("CHAIN")

        if is_rollback:
            return self.expression(exp.Rollback, savepoint=savepoint)
        return self.expression(exp.Commit, chain=chain)

    def _parse_add_column(self) -> t.Optional[exp.Expression]:
        if not self._match_text_seq("ADD"):
            return None

        self._match(TokenType.COLUMN)
        exists_column = self._parse_exists(not_=True)
        expression = self._parse_column_def(self._parse_field(any_token=True))

        if expression:
            expression.set("exists", exists_column)

        return expression

    def _parse_drop_column(self) -> t.Optional[exp.Expression]:
        return self._match(TokenType.DROP) and self._parse_drop(default_kind="COLUMN")

    # https://docs.aws.amazon.com/athena/latest/ug/alter-table-drop-partition.html
    def _parse_drop_partition(self, exists: t.Optional[bool] = None) -> exp.Expression:
        return self.expression(
            exp.DropPartition, expressions=self._parse_csv(self._parse_partition), exists=exists
        )

    def _parse_add_constraint(self) -> t.Optional[exp.Expression]:
        this = None
        kind = self._prev.token_type

        if kind == TokenType.CONSTRAINT:
            this = self._parse_id_var()

            if self._match(TokenType.CHECK):
                expression = self._parse_wrapped(self._parse_conjunction)
                enforced = self._match_text_seq("ENFORCED")

                return self.expression(
                    exp.AddConstraint, this=this, expression=expression, enforced=enforced
                )

        if kind == TokenType.FOREIGN_KEY or self._match(TokenType.FOREIGN_KEY):
            expression = self._parse_foreign_key()
        elif kind == TokenType.PRIMARY_KEY or self._match(TokenType.PRIMARY_KEY):
            expression = self._parse_primary_key()

        return self.expression(exp.AddConstraint, this=this, expression=expression)

    def _parse_alter(self) -> t.Optional[exp.Expression]:
        if not self._match(TokenType.TABLE):
            return None

        exists = self._parse_exists()
        this = self._parse_table(schema=True)

        actions: t.Optional[exp.Expression | t.List[t.Optional[exp.Expression]]] = None

        index = self._index
        if self._match(TokenType.DELETE):
            actions = [self.expression(exp.Delete, where=self._parse_where())]
        elif self._match_text_seq("ADD"):
            if self._match_set(self.ADD_CONSTRAINT_TOKENS):
                actions = self._parse_csv(self._parse_add_constraint)
            else:
                self._retreat(index)
                actions = self._parse_csv(self._parse_add_column)
        elif self._match_text_seq("DROP"):
            partition_exists = self._parse_exists()

            if self._match(TokenType.PARTITION, advance=False):
                actions = self._parse_csv(
                    lambda: self._parse_drop_partition(exists=partition_exists)
                )
            else:
                self._retreat(index)
                actions = self._parse_csv(self._parse_drop_column)
        elif self._match_text_seq("RENAME", "TO"):
            actions = self.expression(exp.RenameTable, this=self._parse_table(schema=True))
        elif self._match_text_seq("ALTER"):
            self._match(TokenType.COLUMN)
            column = self._parse_field(any_token=True)

            if self._match_pair(TokenType.DROP, TokenType.DEFAULT):
                actions = self.expression(exp.AlterColumn, this=column, drop=True)
            elif self._match_pair(TokenType.SET, TokenType.DEFAULT):
                actions = self.expression(
                    exp.AlterColumn, this=column, default=self._parse_conjunction()
                )
            else:
                self._match_text_seq("SET", "DATA")
                actions = self.expression(
                    exp.AlterColumn,
                    this=column,
                    dtype=self._match_text_seq("TYPE") and self._parse_types(),
                    collate=self._match(TokenType.COLLATE) and self._parse_term(),
                    using=self._match(TokenType.USING) and self._parse_conjunction(),
                )

        actions = ensure_list(actions)
        return self.expression(exp.AlterTable, this=this, exists=exists, actions=actions)

    def _parse_show(self) -> t.Optional[exp.Expression]:
        parser = self._find_parser(self.SHOW_PARSERS, self._show_trie)  # type: ignore
        if parser:
            return parser(self)
        self._advance()
        return self.expression(exp.Show, this=self._prev.text.upper())

    def _default_parse_set_item(self) -> exp.Expression:
        return self.expression(
            exp.SetItem,
            this=self._parse_statement(),
        )

    def _parse_set_item(self) -> t.Optional[exp.Expression]:
        parser = self._find_parser(self.SET_PARSERS, self._set_trie)  # type: ignore
        return parser(self) if parser else self._default_parse_set_item()

    def _parse_merge(self) -> exp.Expression:
        self._match(TokenType.INTO)
        target = self._parse_table()

        self._match(TokenType.USING)
        using = self._parse_table()

        self._match(TokenType.ON)
        on = self._parse_conjunction()

        whens = []
        while self._match(TokenType.WHEN):
            this = self._parse_conjunction()
            self._match(TokenType.THEN)

            if self._match(TokenType.INSERT):
                _this = self._parse_star()
                if _this:
                    then = self.expression(exp.Insert, this=_this)
                else:
                    then = self.expression(
                        exp.Insert,
                        this=self._parse_value(),
                        expression=self._match(TokenType.VALUES) and self._parse_value(),
                    )
            elif self._match(TokenType.UPDATE):
                expressions = self._parse_star()
                if expressions:
                    then = self.expression(exp.Update, expressions=expressions)
                else:
                    then = self.expression(
                        exp.Update,
                        expressions=self._match(TokenType.SET)
                        and self._parse_csv(self._parse_equality),
                    )
            elif self._match(TokenType.DELETE):
                then = self.expression(exp.Var, this=self._prev.text)

            whens.append(self.expression(exp.When, this=this, then=then))

        return self.expression(
            exp.Merge,
            this=target,
            using=using,
            on=on,
            expressions=whens,
        )

    def _parse_set(self) -> exp.Expression:
        return self.expression(exp.Set, expressions=self._parse_csv(self._parse_set_item))

    def _parse_as_command(self, start: Token) -> exp.Command:
        while self._curr:
            self._advance()
        return exp.Command(this=self._find_sql(start, self._prev))

    def _find_parser(
        self, parsers: t.Dict[str, t.Callable], trie: t.Dict
    ) -> t.Optional[t.Callable]:
        index = self._index
        this = []
        while True:
            # The current token might be multiple words
            curr = self._curr.text.upper()
            key = curr.split(" ")
            this.append(curr)
            self._advance()
            result, trie = in_trie(trie, key)
            if result == 0:
                break
            if result == 2:
                subparser = parsers[" ".join(this)]
                return subparser
        self._retreat(index)
        return None

    def _match(self, token_type, advance=True):
        if not self._curr:
            return None

        if self._curr.token_type == token_type:
            if advance:
                self._advance()
            return True

        return None

    def _match_set(self, types):
        if not self._curr:
            return None

        if self._curr.token_type in types:
            self._advance()
            return True

        return None

    def _match_pair(self, token_type_a, token_type_b, advance=True):
        if not self._curr or not self._next:
            return None

        if self._curr.token_type == token_type_a and self._next.token_type == token_type_b:
            if advance:
                self._advance(2)
            return True

        return None

    def _match_l_paren(self, expression=None):
        if not self._match(TokenType.L_PAREN):
            self.raise_error("Expecting (")
        if expression and self._prev_comments:
            expression.comments = self._prev_comments

    def _match_r_paren(self, expression=None):
        if not self._match(TokenType.R_PAREN):
            self.raise_error("Expecting )")
        if expression and self._prev_comments:
            expression.comments = self._prev_comments

    def _match_texts(self, texts):
        if self._curr and self._curr.text.upper() in texts:
            self._advance()
            return True
        return False

    def _match_text_seq(self, *texts, advance=True):
        index = self._index
        for text in texts:
            if self._curr and self._curr.text.upper() == text:
                self._advance()
            else:
                self._retreat(index)
                return False

        if not advance:
            self._retreat(index)

        return True

    def _replace_columns_with_dots(self, this):
        if isinstance(this, exp.Dot):
            exp.replace_children(this, self._replace_columns_with_dots)
        elif isinstance(this, exp.Column):
            exp.replace_children(this, self._replace_columns_with_dots)
            table = this.args.get("table")
            this = (
                self.expression(exp.Dot, this=table, expression=this.this)
                if table
                else self.expression(exp.Var, this=this.name)
            )
        elif isinstance(this, exp.Identifier):
            this = self.expression(exp.Var, this=this.name)
        return this

    def _replace_lambda(self, node, lambda_variables):
        if isinstance(node, exp.Column):
            if node.name in lambda_variables:
                return node.this
        return node
