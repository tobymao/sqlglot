import logging

from sqlglot import exp
from sqlglot.errors import ErrorLevel, ParseError, concat_errors
from sqlglot.helper import apply_index_offset, ensure_list, list_get
from sqlglot.tokens import Token, Tokenizer, TokenType

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


class Parser:
    """
    Parser consumes a list of tokens produced by the :class:`~sqlglot.tokens.Tokenizer`
    and produces a parsed syntax tree.

    Args
        error_level (ErrorLevel): the desired error level. Default: ErrorLevel.RAISE.
        error_message_context (int): determines the amount of context to capture from
            a query string when displaying the error message (in number of characters).
            Default: 50.
        index_offset (int): Index offset for arrays eg ARRAY[0] vs ARRAY[1] as the head of a list
            Default: 0
        alias_post_tablesample (bool): If the table alias comes after tablesample
            Default: False
        max_errors (int): Maximum number of error messages to include in a raised ParseError.
            This is only relevant if error_level is ErrorLevel.RAISE.
            Default: 3
        null_ordering (str): Indicates the default null ordering method to use if not explicitly set.
            Options are "nulls_are_small", "nulls_are_large", "nulls_are_last".
            Default: "nulls_are_small"
    """

    FUNCTIONS = {
        **{name: f.from_arg_list for f in exp.ALL_FUNCTIONS for name in f.sql_names()},
        "DATE_TO_DATE_STR": lambda args: exp.Cast(
            this=list_get(args, 0),
            to=exp.DataType(this=exp.DataType.Type.TEXT),
        ),
        "TIME_TO_TIME_STR": lambda args: exp.Cast(
            this=list_get(args, 0),
            to=exp.DataType(this=exp.DataType.Type.TEXT),
        ),
        "TS_OR_DS_TO_DATE_STR": lambda args: exp.Substring(
            this=exp.Cast(
                this=list_get(args, 0),
                to=exp.DataType(this=exp.DataType.Type.TEXT),
            ),
            start=exp.Literal.number(1),
            length=exp.Literal.number(10),
        ),
        "VAR_MAP": parse_var_map,
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
        TokenType.BINARY,
        TokenType.JSON,
        TokenType.INTERVAL,
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
        TokenType.ALTER,
        TokenType.ALWAYS,
        TokenType.ANTI,
        TokenType.BEGIN,
        TokenType.BOTH,
        TokenType.BUCKET,
        TokenType.CACHE,
        TokenType.CALL,
        TokenType.COLLATE,
        TokenType.COMMIT,
        TokenType.CONSTRAINT,
        TokenType.DEFAULT,
        TokenType.DELETE,
        TokenType.DESCRIBE,
        TokenType.DETERMINISTIC,
        TokenType.EXECUTE,
        TokenType.ENGINE,
        TokenType.ESCAPE,
        TokenType.EXPLAIN,
        TokenType.FALSE,
        TokenType.FIRST,
        TokenType.FOLLOWING,
        TokenType.FORMAT,
        TokenType.FUNCTION,
        TokenType.GENERATED,
        TokenType.IDENTITY,
        TokenType.IF,
        TokenType.INDEX,
        TokenType.ISNULL,
        TokenType.IMMUTABLE,
        TokenType.INTERVAL,
        TokenType.LAZY,
        TokenType.LANGUAGE,
        TokenType.LEADING,
        TokenType.LOCAL,
        TokenType.LOCATION,
        TokenType.MATERIALIZED,
        TokenType.NATURAL,
        TokenType.NEXT,
        TokenType.ONLY,
        TokenType.OPTIMIZE,
        TokenType.OPTIONS,
        TokenType.ORDINALITY,
        TokenType.PARTITIONED_BY,
        TokenType.PERCENT,
        TokenType.PIVOT,
        TokenType.PRECEDING,
        TokenType.RANGE,
        TokenType.REFERENCES,
        TokenType.RETURNS,
        TokenType.ROWS,
        TokenType.SCHEMA,
        TokenType.SCHEMA_COMMENT,
        TokenType.SEED,
        TokenType.SEMI,
        TokenType.SET,
        TokenType.SHOW,
        TokenType.STABLE,
        TokenType.STORED,
        TokenType.TABLE,
        TokenType.TABLE_FORMAT,
        TokenType.TEMPORARY,
        TokenType.TOP,
        TokenType.TRAILING,
        TokenType.TRUNCATE,
        TokenType.TRUE,
        TokenType.UNBOUNDED,
        TokenType.UNIQUE,
        TokenType.UNPIVOT,
        TokenType.PROPERTIES,
        TokenType.PROCEDURE,
        TokenType.VOLATILE,
        *SUBQUERY_PREDICATES,
        *TYPE_TOKENS,
    }

    TABLE_ALIAS_TOKENS = ID_VAR_TOKENS - {TokenType.NATURAL}

    TRIM_TYPES = {TokenType.LEADING, TokenType.TRAILING, TokenType.BOTH}

    FUNC_TOKENS = {
        TokenType.CURRENT_DATE,
        TokenType.CURRENT_DATETIME,
        TokenType.CURRENT_TIMESTAMP,
        TokenType.CURRENT_TIME,
        TokenType.FILTER,
        TokenType.FIRST,
        TokenType.FORMAT,
        TokenType.ISNULL,
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
        TokenType.TIMESTAMP,
        TokenType.TIMESTAMPTZ,
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
    }

    FACTOR = {
        TokenType.DIV: exp.IntDiv,
        TokenType.LR_ARROW: exp.Distance,
        TokenType.SLASH: exp.Div,
        TokenType.STAR: exp.Mul,
    }

    TIMESTAMPS = {
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
            path=path,
        ),
        TokenType.DARROW: lambda self, this, path: self.expression(
            exp.JSONExtractScalar,
            this=this,
            path=path,
        ),
        TokenType.HASH_ARROW: lambda self, this, path: self.expression(
            exp.JSONBExtract,
            this=this,
            path=path,
        ),
        TokenType.DHASH_ARROW: lambda self, this, path: self.expression(
            exp.JSONBExtractScalar,
            this=this,
            path=path,
        ),
    }

    EXPRESSION_PARSERS = {
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
        "JOIN_TYPE": lambda self: self._parse_join_side_and_kind(),
    }

    STATEMENT_PARSERS = {
        TokenType.CREATE: lambda self: self._parse_create(),
        TokenType.DESCRIBE: lambda self: self._parse_describe(),
        TokenType.DROP: lambda self: self._parse_drop(),
        TokenType.INSERT: lambda self: self._parse_insert(),
        TokenType.LOAD_DATA: lambda self: self._parse_load_data(),
        TokenType.UPDATE: lambda self: self._parse_update(),
        TokenType.DELETE: lambda self: self._parse_delete(),
        TokenType.CACHE: lambda self: self._parse_cache(),
        TokenType.UNCACHE: lambda self: self._parse_uncache(),
    }

    PRIMARY_PARSERS = {
        TokenType.STRING: lambda _, token: exp.Literal.string(token.text),
        TokenType.NUMBER: lambda _, token: exp.Literal.number(token.text),
        TokenType.STAR: lambda self, _: exp.Star(**{"except": self._parse_except(), "replace": self._parse_replace()}),
        TokenType.NULL: lambda *_: exp.Null(),
        TokenType.TRUE: lambda *_: exp.Boolean(this=True),
        TokenType.FALSE: lambda *_: exp.Boolean(this=False),
        TokenType.PARAMETER: lambda self, _: exp.Parameter(this=self._parse_var() or self._parse_primary()),
        TokenType.BIT_STRING: lambda _, token: exp.BitString(this=token.text),
        TokenType.HEX_STRING: lambda _, token: exp.HexString(this=token.text),
        TokenType.BYTE_STRING: lambda _, token: exp.ByteString(this=token.text),
        TokenType.INTRODUCER: lambda self, token: self._parse_introducer(token),
    }

    RANGE_PARSERS = {
        TokenType.BETWEEN: lambda self, this: self._parse_between(this),
        TokenType.IN: lambda self, this: self._parse_in(this),
        TokenType.IS: lambda self, this: self._parse_is(this),
        TokenType.LIKE: lambda self, this: self._parse_escape(
            self.expression(exp.Like, this=this, expression=self._parse_bitwise())
        ),
        TokenType.ILIKE: lambda self, this: self._parse_escape(
            self.expression(exp.ILike, this=this, expression=self._parse_bitwise())
        ),
        TokenType.RLIKE: lambda self, this: self.expression(
            exp.RegexpLike, this=this, expression=self._parse_bitwise()
        ),
        TokenType.SIMILAR_TO: lambda self, this: self.expression(
            exp.SimilarTo, this=this, expression=self._parse_bitwise()
        ),
    }

    PROPERTY_PARSERS = {
        TokenType.AUTO_INCREMENT: lambda self: self._parse_auto_increment(),
        TokenType.CHARACTER_SET: lambda self: self._parse_character_set(),
        TokenType.LOCATION: lambda self: self.expression(
            exp.LocationProperty,
            this=exp.Literal.string("LOCATION"),
            value=self._parse_string(),
        ),
        TokenType.PARTITIONED_BY: lambda self: self._parse_partitioned_by(),
        TokenType.SCHEMA_COMMENT: lambda self: self._parse_schema_comment(),
        TokenType.STORED: lambda self: self._parse_stored(),
        TokenType.RETURNS: lambda self: self._parse_returns(),
        TokenType.COLLATE: lambda self: self._parse_property_assignment(exp.CollateProperty),
        TokenType.COMMENT: lambda self: self._parse_property_assignment(exp.SchemaCommentProperty),
        TokenType.FORMAT: lambda self: self._parse_property_assignment(exp.FileFormatProperty),
        TokenType.TABLE_FORMAT: lambda self: self._parse_property_assignment(exp.TableFormatProperty),
        TokenType.USING: lambda self: self._parse_property_assignment(exp.TableFormatProperty),
        TokenType.LANGUAGE: lambda self: self._parse_property_assignment(exp.LanguageProperty),
        TokenType.EXECUTE: lambda self: self._parse_execute_as(),
        TokenType.DETERMINISTIC: lambda self: self.expression(
            exp.VolatilityProperty, this=exp.Literal.string("IMMUTABLE")
        ),
        TokenType.IMMUTABLE: lambda self: self.expression(exp.VolatilityProperty, this=exp.Literal.string("IMMUTABLE")),
        TokenType.STABLE: lambda self: self.expression(exp.VolatilityProperty, this=exp.Literal.string("STABLE")),
        TokenType.VOLATILE: lambda self: self.expression(exp.VolatilityProperty, this=exp.Literal.string("VOLATILE")),
    }

    CONSTRAINT_PARSERS = {
        TokenType.CHECK: lambda self: self._parse_check(),
        TokenType.FOREIGN_KEY: lambda self: self._parse_foreign_key(),
        TokenType.UNIQUE: lambda self: self._parse_unique(),
    }

    NO_PAREN_FUNCTION_PARSERS = {
        TokenType.CASE: lambda self: self._parse_case(),
        TokenType.IF: lambda self: self._parse_if(),
    }

    FUNCTION_PARSERS = {
        "CONVERT": lambda self: self._parse_convert(self.STRICT_CAST),
        "TRY_CONVERT": lambda self: self._parse_convert(False),
        "EXTRACT": lambda self: self._parse_extract(),
        "POSITION": lambda self: self._parse_position(),
        "SUBSTRING": lambda self: self._parse_substring(),
        "TRIM": lambda self: self._parse_trim(),
        "CAST": lambda self: self._parse_cast(self.STRICT_CAST),
        "TRY_CAST": lambda self: self._parse_cast(False),
    }

    QUERY_MODIFIER_PARSERS = {
        "where": lambda self: self._parse_where(),
        "group": lambda self: self._parse_group(),
        "having": lambda self: self._parse_having(),
        "qualify": lambda self: self._parse_qualify(),
        "window": lambda self: self._match(TokenType.WINDOW) and self._parse_window(self._parse_id_var(), alias=True),
        "distribute": lambda self: self._parse_sort(TokenType.DISTRIBUTE_BY, exp.Distribute),
        "sort": lambda self: self._parse_sort(TokenType.SORT_BY, exp.Sort),
        "cluster": lambda self: self._parse_sort(TokenType.CLUSTER_BY, exp.Cluster),
        "order": lambda self: self._parse_order(),
        "limit": lambda self: self._parse_limit(),
        "offset": lambda self: self._parse_offset(),
    }

    MODIFIABLES = (exp.Subquery, exp.Subqueryable, exp.Table)

    CREATABLES = {
        TokenType.TABLE,
        TokenType.VIEW,
        TokenType.FUNCTION,
        TokenType.INDEX,
        TokenType.PROCEDURE,
        TokenType.SCHEMA,
    }

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
        "_chunks",
        "_index",
        "_curr",
        "_next",
        "_prev",
        "_greedy_subqueries",
    )

    def __init__(
        self,
        error_level=None,
        error_message_context=100,
        index_offset=0,
        unnest_column_only=False,
        alias_post_tablesample=False,
        max_errors=3,
        null_ordering=None,
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
        self._chunks = [[]]
        self._index = 0
        self._curr = None
        self._next = None
        self._prev = None
        self._greedy_subqueries = False

    def parse(self, raw_tokens, sql=None):
        """
        Parses the given list of tokens and returns a list of syntax trees, one tree
        per parsed SQL statement.

        Args
            raw_tokens (list): the list of tokens (:class:`~sqlglot.tokens.Token`).
            sql (str): the original SQL string. Used to produce helpful debug messages.

        Returns
            the list of syntax trees (:class:`~sqlglot.expressions.Expression`).
        """
        return self._parse(parse_method=self.__class__._parse_statement, raw_tokens=raw_tokens, sql=sql)

    def parse_into(self, expression_types, raw_tokens, sql=None):
        for expression_type in ensure_list(expression_types):
            parser = self.EXPRESSION_PARSERS.get(expression_type)
            if not parser:
                raise TypeError(f"No parser registered for {expression_type}")
            try:
                return self._parse(parser, raw_tokens, sql)
            except ParseError as e:
                error = e
        raise ParseError(f"Failed to parse into {expression_types}") from error

    def _parse(self, parse_method, raw_tokens, sql=None):
        self.reset()
        self.sql = sql or ""
        total = len(raw_tokens)

        for i, token in enumerate(raw_tokens):
            if token.token_type == TokenType.SEMICOLON:
                if i < total - 1:
                    self._chunks.append([])
            else:
                self._chunks[-1].append(token)

        expressions = []

        for tokens in self._chunks:
            self._index = -1
            self._tokens = tokens
            self._advance()
            expressions.append(parse_method(self))

            if self._index < len(self._tokens):
                self.raise_error("Invalid expression / Unexpected token")

            self.check_errors()

        return expressions

    def check_errors(self):
        if self.error_level == ErrorLevel.WARN:
            for error in self.errors:
                logger.error(str(error))
        elif self.error_level == ErrorLevel.RAISE and self.errors:
            raise ParseError(concat_errors(self.errors, self.max_errors))

    def raise_error(self, message, token=None):
        token = token or self._curr or self._prev or Token.string("")
        start = self._find_token(token, self.sql)
        end = start + len(token.text)
        start_context = self.sql[max(start - self.error_message_context, 0) : start]
        highlight = self.sql[start:end]
        end_context = self.sql[end : end + self.error_message_context]
        error = ParseError(
            f"{message}. Line {token.line}, Col: {token.col}.\n"
            f"  {start_context}\033[4m{highlight}\033[0m{end_context}"
        )
        if self.error_level == ErrorLevel.IMMEDIATE:
            raise error
        self.errors.append(error)

    def expression(self, exp_class, **kwargs):
        instance = exp_class(**kwargs)
        self.validate_expression(instance)
        return instance

    def validate_expression(self, expression, args=None):
        if self.error_level == ErrorLevel.IGNORE:
            return

        for k in expression.args:
            if k not in expression.arg_types:
                self.raise_error(f"Unexpected keyword: '{k}' for {expression.__class__}")
        for k, mandatory in expression.arg_types.items():
            v = expression.args.get(k)
            if mandatory and (v is None or (isinstance(v, list) and not v)):
                self.raise_error(f"Required keyword: '{k}' missing for {expression.__class__}")

        if args and len(args) > len(expression.arg_types) and not expression.is_var_len_args:
            self.raise_error(
                f"The number of provided arguments ({len(args)}) is greater than "
                f"the maximum number of supported arguments ({len(expression.arg_types)})"
            )

    def _find_token(self, token, sql):
        line = 1
        col = 1
        index = 0

        while line < token.line or col < token.col:
            if Tokenizer.WHITE_SPACE.get(sql[index]) == TokenType.BREAK:
                line += 1
                col = 1
            else:
                col += 1
            index += 1

        return index

    def _get_token(self, index):
        return list_get(self._tokens, index)

    def _advance(self, times=1):
        self._index += times
        self._curr = self._get_token(self._index)
        self._next = self._get_token(self._index + 1)
        self._prev = self._get_token(self._index - 1) if self._index > 0 else None

    def _retreat(self, index):
        self._advance(index - self._index)

    def _parse_statement(self):
        if self._curr is None:
            return None

        if self._match_set(self.STATEMENT_PARSERS):
            return self.STATEMENT_PARSERS[self._prev.token_type](self)

        if self._match_set(Tokenizer.COMMANDS):
            return self.expression(
                exp.Command,
                this=self._prev.text,
                expression=self._parse_string(),
            )

        expression = self._parse_expression()
        expression = self._parse_set_operations(expression) if expression else self._parse_select()
        self._parse_query_modifiers(expression)
        return expression

    def _parse_drop(self):
        temporary = self._match(TokenType.TEMPORARY)
        materialized = self._match(TokenType.MATERIALIZED)
        kind = self._match_set(self.CREATABLES) and self._prev.text
        if not kind:
            self.raise_error(f"Expected {self.CREATABLES}")
            return

        return self.expression(
            exp.Drop,
            exists=self._parse_exists(),
            this=self._parse_table(schema=True),
            kind=kind,
            temporary=temporary,
            materialized=materialized,
        )

    def _parse_exists(self, not_=False):
        return self._match(TokenType.IF) and (not not_ or self._match(TokenType.NOT)) and self._match(TokenType.EXISTS)

    def _parse_create(self):
        replace = self._match(TokenType.OR) and self._match(TokenType.REPLACE)
        temporary = self._match(TokenType.TEMPORARY)
        unique = self._match(TokenType.UNIQUE)
        materialized = self._match(TokenType.MATERIALIZED)

        if self._match_pair(TokenType.TABLE, TokenType.FUNCTION, advance=False):
            self._match(TokenType.TABLE)

        create_token = self._match_set(self.CREATABLES) and self._prev

        if not create_token:
            self.raise_error(f"Expected {self.CREATABLES}")
            return

        exists = self._parse_exists(not_=True)
        this = None
        expression = None
        properties = None

        if create_token.token_type in (TokenType.FUNCTION, TokenType.PROCEDURE):
            this = self._parse_user_defined_function()
            properties = self._parse_properties()
            if self._match(TokenType.ALIAS):
                expression = self._parse_select_or_expression()
        elif create_token.token_type == TokenType.INDEX:
            this = self._parse_index()
        elif create_token.token_type in (TokenType.TABLE, TokenType.VIEW, TokenType.SCHEMA):
            this = self._parse_table(schema=True)
            properties = self._parse_properties()
            if self._match(TokenType.ALIAS):
                expression = self._parse_select(nested=True)

        return self.expression(
            exp.Create,
            this=this,
            kind=create_token.text,
            expression=expression,
            exists=exists,
            properties=properties,
            temporary=temporary,
            replace=replace,
            unique=unique,
            materialized=materialized,
        )

    def _parse_property(self):
        if self._match_set(self.PROPERTY_PARSERS):
            return self.PROPERTY_PARSERS[self._prev.token_type](self)
        if self._match_pair(TokenType.DEFAULT, TokenType.CHARACTER_SET):
            return self._parse_character_set(True)

        if self._match_pair(TokenType.VAR, TokenType.EQ, advance=False):
            key = self._parse_var().this
            self._match(TokenType.EQ)

            return self.expression(
                exp.AnonymousProperty,
                this=exp.Literal.string(key),
                value=self._parse_column(),
            )

        return None

    def _parse_property_assignment(self, exp_class):
        prop = self._prev.text
        self._match(TokenType.EQ)
        return self.expression(exp_class, this=prop, value=self._parse_var_or_string())

    def _parse_partitioned_by(self):
        self._match(TokenType.EQ)
        return self.expression(
            exp.PartitionedByProperty,
            this=exp.Literal.string("PARTITIONED_BY"),
            value=self._parse_schema() or self._parse_bracket(self._parse_field()),
        )

    def _parse_stored(self):
        self._match(TokenType.ALIAS)
        self._match(TokenType.EQ)
        return self.expression(
            exp.FileFormatProperty,
            this=exp.Literal.string("FORMAT"),
            value=exp.Literal.string(self._parse_var().name),
        )

    def _parse_auto_increment(self):
        self._match(TokenType.EQ)
        return self.expression(
            exp.AutoIncrementProperty,
            this=exp.Literal.string("AUTO_INCREMENT"),
            value=self._parse_var() or self._parse_number(),
        )

    def _parse_schema_comment(self):
        self._match(TokenType.EQ)
        return self.expression(
            exp.SchemaCommentProperty,
            this=exp.Literal.string("COMMENT"),
            value=self._parse_string(),
        )

    def _parse_character_set(self, default=False):
        self._match(TokenType.EQ)
        return self.expression(
            exp.CharacterSetProperty,
            this=exp.Literal.string("CHARACTER_SET"),
            value=self._parse_var_or_string(),
            default=default,
        )

    def _parse_returns(self):
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
                value = self._parse_schema("TABLE")
        else:
            value = self._parse_types()

        return self.expression(
            exp.ReturnsProperty,
            this=exp.Literal.string("RETURNS"),
            value=value,
            is_table=is_table,
        )

    def _parse_execute_as(self):
        self._match(TokenType.ALIAS)
        return self.expression(
            exp.ExecuteAsProperty,
            this=exp.Literal.string("EXECUTE AS"),
            value=self._parse_var(),
        )

    def _parse_properties(self):
        properties = []

        while True:
            if self._match(TokenType.WITH):
                self._match_l_paren()
                properties.extend(self._parse_csv(lambda: self._parse_property()))
                self._match_r_paren()
            elif self._match(TokenType.PROPERTIES):
                self._match_l_paren()
                properties.extend(
                    self._parse_csv(
                        lambda: self.expression(
                            exp.AnonymousProperty,
                            this=self._parse_string(),
                            value=self._match(TokenType.EQ) and self._parse_string(),
                        )
                    )
                )
                self._match_r_paren()
            else:
                identified_property = self._parse_property()
                if not identified_property:
                    break
                properties.append(identified_property)
        if properties:
            return self.expression(exp.Properties, expressions=properties)
        return None

    def _parse_describe(self):
        self._match(TokenType.TABLE)

        return self.expression(exp.Describe, this=self._parse_id_var())

    def _parse_insert(self):
        overwrite = self._match(TokenType.OVERWRITE)
        local = self._match(TokenType.LOCAL)
        if self._match_text("DIRECTORY"):
            this = self.expression(
                exp.Directory,
                this=self._parse_var_or_string(),
                local=local,
                row_format=self._parse_row_format(),
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
            expression=self._parse_select(nested=True),
            overwrite=overwrite,
        )

    def _parse_row_format(self):
        if not self._match_pair(TokenType.ROW, TokenType.FORMAT):
            return None

        self._match_text("DELIMITED")

        kwargs = {}

        if self._match_text("FIELDS", "TERMINATED", "BY"):
            kwargs["fields"] = self._parse_string()
            if self._match_text("ESCAPED", "BY"):
                kwargs["escaped"] = self._parse_string()
        if self._match_text("COLLECTION", "ITEMS", "TERMINATED", "BY"):
            kwargs["collection_items"] = self._parse_string()
        if self._match_text("MAP", "KEYS", "TERMINATED", "BY"):
            kwargs["map_keys"] = self._parse_string()
        if self._match_text("LINES", "TERMINATED", "BY"):
            kwargs["lines"] = self._parse_string()
        if self._match_text("NULL", "DEFINED", "AS"):
            kwargs["null"] = self._parse_string()
        return self.expression(exp.RowFormat, **kwargs)

    def _parse_load_data(self):
        local = self._match(TokenType.LOCAL)
        self._match_text("INPATH")
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
            input_format=self._match_text("INPUTFORMAT") and self._parse_string(),
            serde=self._match_text("SERDE") and self._parse_string(),
        )

    def _parse_delete(self):
        self._match(TokenType.FROM)

        return self.expression(
            exp.Delete,
            this=self._parse_table(schema=True),
            using=self._parse_csv(lambda: self._match(TokenType.USING) and self._parse_table(schema=True)),
            where=self._parse_where(),
        )

    def _parse_update(self):
        return self.expression(
            exp.Update,
            **{
                "this": self._parse_table(schema=True),
                "expressions": self._match(TokenType.SET) and self._parse_csv(self._parse_equality),
                "from": self._parse_from(),
                "where": self._parse_where(),
            },
        )

    def _parse_uncache(self):
        if not self._match(TokenType.TABLE):
            self.raise_error("Expecting TABLE after UNCACHE")
        return self.expression(
            exp.Uncache,
            exists=self._parse_exists(),
            this=self._parse_table(schema=True),
        )

    def _parse_cache(self):
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

    def _parse_partition(self):
        if not self._match(TokenType.PARTITION):
            return None

        def parse_values():
            k = self._parse_var()
            if self._match(TokenType.EQ):
                v = self._parse_string()
                return (k, v)
            return (k, None)

        self._match_l_paren()
        values = self._parse_csv(parse_values)
        self._match_r_paren()

        return self.expression(
            exp.Partition,
            this=values,
        )

    def _parse_value(self):
        self._match_l_paren()
        expressions = self._parse_csv(self._parse_conjunction)
        self._match_r_paren()
        return self.expression(exp.Tuple, expressions=expressions)

    def _parse_select(self, nested=False, table=False):
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
            expressions = self._parse_csv(lambda: self._parse_annotation(self._parse_expression()))

            this = self.expression(
                exp.Select,
                hint=hint,
                distinct=distinct,
                expressions=expressions,
                limit=limit,
            )
            from_ = self._parse_from()
            if from_:
                this.set("from", from_)
            self._parse_query_modifiers(this)
        elif (table or nested) and self._match(TokenType.L_PAREN):
            this = self._parse_table() if table else self._parse_select(nested=True)
            self._parse_query_modifiers(this)
            self._match_r_paren()
            this = self._parse_subquery(this)
        elif self._match(TokenType.VALUES):
            this = self.expression(exp.Values, expressions=self._parse_csv(self._parse_value))
            alias = self._parse_table_alias()
            if alias:
                this = self.expression(exp.Subquery, this=this, alias=alias)
        else:
            this = None

        return self._parse_set_operations(this) if this else None

    def _parse_with(self):
        if not self._match(TokenType.WITH):
            return None

        recursive = self._match(TokenType.RECURSIVE)

        expressions = []

        while True:
            expressions.append(self._parse_cte())

            if not self._match(TokenType.COMMA):
                break

        return self.expression(
            exp.With,
            expressions=expressions,
            recursive=recursive,
        )

    def _parse_cte(self):
        alias = self._parse_table_alias()
        if not alias or not alias.this:
            self.raise_error("Expected CTE to have alias")

        if not self._match(TokenType.ALIAS):
            self.raise_error("Expected AS in CTE")

        self._match_l_paren()
        expression = self._parse_statement()
        self._match_r_paren()

        return self.expression(
            exp.CTE,
            this=expression,
            alias=alias,
        )

    def _parse_table_alias(self):
        any_token = self._match(TokenType.ALIAS)
        alias = self._parse_id_var(any_token=any_token, tokens=self.TABLE_ALIAS_TOKENS)
        columns = None

        if self._match(TokenType.L_PAREN):
            columns = self._parse_csv(lambda: self._parse_id_var(any_token))
            self._match_r_paren()

        if not alias and not columns:
            return None

        return self.expression(
            exp.TableAlias,
            this=alias,
            columns=columns,
        )

    def _parse_subquery(self, this):
        return self.expression(
            exp.Subquery,
            this=this,
            pivots=self._parse_pivots(),
            alias=self._parse_table_alias(),
        )

    def _parse_query_modifiers(self, this):
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

    def _parse_annotation(self, expression):
        if self._match(TokenType.ANNOTATION):
            return self.expression(exp.Annotation, this=self._prev.text.strip(), expression=expression)

        return expression

    def _parse_hint(self):
        if self._match(TokenType.HINT):
            hints = self._parse_csv(self._parse_function)
            if not self._match(TokenType.HINT):
                self.raise_error("Expected */ after HINT")
            return self.expression(exp.Hint, expressions=hints)
        return None

    def _parse_from(self):
        if not self._match(TokenType.FROM):
            return None

        return self.expression(exp.From, expressions=self._parse_csv(self._parse_table))

    def _parse_lateral(self):
        if not self._match(TokenType.LATERAL):
            return None

        subquery = self._parse_select(table=True)

        if subquery:
            return self.expression(exp.Lateral, this=subquery)

        self._match(TokenType.VIEW)
        outer = self._match(TokenType.OUTER)

        return self.expression(
            exp.Lateral,
            this=self._parse_function(),
            outer=outer,
            alias=self.expression(
                exp.TableAlias,
                this=self._parse_id_var(any_token=False),
                columns=(self._parse_csv(self._parse_id_var) if self._match(TokenType.ALIAS) else None),
            ),
        )

    def _parse_join_side_and_kind(self):
        return (
            self._match(TokenType.NATURAL) and self._prev,
            self._match_set(self.JOIN_SIDES) and self._prev,
            self._match_set(self.JOIN_KINDS) and self._prev,
        )

    def _parse_join(self):
        natural, side, kind = self._parse_join_side_and_kind()

        if not self._match(TokenType.JOIN):
            return None

        kwargs = {"this": self._parse_table()}

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

        return self.expression(exp.Join, **kwargs)

    def _parse_index(self):
        index = self._parse_id_var()
        self._match(TokenType.ON)
        self._match(TokenType.TABLE)  # hive
        return self.expression(
            exp.Index,
            this=index,
            table=self.expression(exp.Table, this=self._parse_id_var()),
            columns=self._parse_expression(),
        )

    def _parse_table(self, schema=False):
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

        catalog = None
        db = None
        table = (not schema and self._parse_function()) or self._parse_id_var(False)

        while self._match(TokenType.DOT):
            if catalog:
                # This allows nesting the table in arbitrarily many dot expressions if needed
                table = self.expression(exp.Dot, this=table, expression=self._parse_id_var())
            else:
                catalog = db
                db = table
                table = self._parse_id_var()

        if not table:
            self.raise_error("Expected table name")

        this = self.expression(exp.Table, this=table, db=db, catalog=catalog, pivots=self._parse_pivots())

        if schema:
            return self._parse_schema(this=this)

        if self.alias_post_tablesample:
            table_sample = self._parse_table_sample()

        alias = self._parse_table_alias()

        if alias:
            this.set("alias", alias)

        if not self.alias_post_tablesample:
            table_sample = self._parse_table_sample()

        if table_sample:
            table_sample.set("this", this)
            this = table_sample

        return this

    def _parse_unnest(self):
        if not self._match(TokenType.UNNEST):
            return None

        self._match_l_paren()
        expressions = self._parse_csv(self._parse_column)
        self._match_r_paren()

        ordinality = bool(self._match(TokenType.WITH) and self._match(TokenType.ORDINALITY))

        alias = self._parse_table_alias()

        if alias and self.unnest_column_only:
            if alias.args.get("columns"):
                self.raise_error("Unexpected extra column alias in unnest.")
            alias.set("columns", [alias.this])
            alias.set("this", None)

        return self.expression(
            exp.Unnest,
            expressions=expressions,
            ordinality=ordinality,
            alias=alias,
        )

    def _parse_derived_table_values(self):
        is_derived = self._match_pair(TokenType.L_PAREN, TokenType.VALUES)
        if not is_derived and not self._match(TokenType.VALUES):
            return None

        expressions = self._parse_csv(self._parse_value)

        if is_derived:
            self._match_r_paren()

        alias = self._parse_table_alias()

        return self.expression(
            exp.Values,
            expressions=expressions,
            alias=alias,
        )

    def _parse_table_sample(self):
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
            self._match_l_paren()
            seed = self._parse_number()
            self._match_r_paren()

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

    def _parse_pivots(self):
        return list(iter(self._parse_pivot, None))

    def _parse_pivot(self):
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

        return self.expression(
            exp.Pivot,
            expressions=expressions,
            field=field,
            unpivot=unpivot,
        )

    def _parse_where(self):
        if not self._match(TokenType.WHERE):
            return None
        return self.expression(exp.Where, this=self._parse_conjunction())

    def _parse_group(self):
        if not self._match(TokenType.GROUP_BY):
            return None
        return self.expression(
            exp.Group,
            expressions=self._parse_csv(self._parse_conjunction),
            grouping_sets=self._parse_grouping_sets(),
            cube=self._match(TokenType.CUBE) and self._parse_wrapped_id_vars(),
            rollup=self._match(TokenType.ROLLUP) and self._parse_wrapped_id_vars(),
        )

    def _parse_grouping_sets(self):
        if not self._match(TokenType.GROUPING_SETS):
            return None

        self._match_l_paren()
        grouping_sets = self._parse_csv(self._parse_grouping_set)
        self._match_r_paren()
        return grouping_sets

    def _parse_grouping_set(self):
        if self._match(TokenType.L_PAREN):
            grouping_set = self._parse_csv(self._parse_id_var)
            self._match_r_paren()
            return self.expression(exp.Tuple, expressions=grouping_set)
        return self._parse_id_var()

    def _parse_having(self):
        if not self._match(TokenType.HAVING):
            return None
        return self.expression(exp.Having, this=self._parse_conjunction())

    def _parse_qualify(self):
        if not self._match(TokenType.QUALIFY):
            return None
        return self.expression(exp.Qualify, this=self._parse_conjunction())

    def _parse_order(self, this=None):
        if not self._match(TokenType.ORDER_BY):
            return this

        return self.expression(exp.Order, this=this, expressions=self._parse_csv(self._parse_ordered))

    def _parse_sort(self, token_type, exp_class):
        if not self._match(token_type):
            return None

        return self.expression(exp_class, expressions=self._parse_csv(self._parse_ordered))

    def _parse_ordered(self):
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
                (asc and self.null_ordering == "nulls_are_small") or (desc and self.null_ordering != "nulls_are_small")
            )
            and self.null_ordering != "nulls_are_last"
        ):
            nulls_first = True

        return self.expression(exp.Ordered, this=this, desc=desc, nulls_first=nulls_first)

    def _parse_limit(self, this=None, top=False):
        if self._match(TokenType.TOP if top else TokenType.LIMIT):
            return self.expression(exp.Limit, this=this, expression=self._parse_number())
        if self._match(TokenType.FETCH):
            direction = self._match_set((TokenType.FIRST, TokenType.NEXT))
            direction = self._prev.text if direction else "FIRST"
            count = self._parse_number()
            self._match_set((TokenType.ROW, TokenType.ROWS))
            self._match(TokenType.ONLY)
            return self.expression(exp.Fetch, direction=direction, count=count)
        return this

    def _parse_offset(self, this=None):
        if not self._match_set((TokenType.OFFSET, TokenType.COMMA)):
            return this
        count = self._parse_number()
        self._match_set((TokenType.ROW, TokenType.ROWS))
        return self.expression(exp.Offset, this=this, expression=count)

    def _parse_set_operations(self, this):
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
            expression=self._parse_select(nested=True),
        )

    def _parse_expression(self):
        return self._parse_alias(self._parse_conjunction())

    def _parse_conjunction(self):
        return self._parse_tokens(self._parse_equality, self.CONJUNCTION)

    def _parse_equality(self):
        return self._parse_tokens(self._parse_comparison, self.EQUALITY)

    def _parse_comparison(self):
        return self._parse_tokens(self._parse_range, self.COMPARISON)

    def _parse_range(self):
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

        return this

    def _parse_is(self, this):
        negate = self._match(TokenType.NOT)
        this = self.expression(
            exp.Is,
            this=this,
            expression=self._parse_null() or self._parse_boolean(),
        )
        return self.expression(exp.Not, this=this) if negate else this

    def _parse_in(self, this):
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

    def _parse_between(self, this):
        low = self._parse_bitwise()
        self._match(TokenType.AND)
        high = self._parse_bitwise()
        return self.expression(exp.Between, this=this, low=low, high=high)

    def _parse_escape(self, this):
        if not self._match(TokenType.ESCAPE):
            return this
        return self.expression(exp.Escape, this=this, expression=self._parse_string())

    def _parse_bitwise(self):
        this = self._parse_term()

        while True:
            if self._match_set(self.BITWISE):
                this = self.expression(
                    self.BITWISE[self._prev.token_type],
                    this=this,
                    expression=self._parse_term(),
                )
            elif self._match_pair(TokenType.LT, TokenType.LT):
                this = self.expression(exp.BitwiseLeftShift, this=this, expression=self._parse_term())
            elif self._match_pair(TokenType.GT, TokenType.GT):
                this = self.expression(exp.BitwiseRightShift, this=this, expression=self._parse_term())
            else:
                break

        return this

    def _parse_term(self):
        return self._parse_tokens(self._parse_factor, self.TERM)

    def _parse_factor(self):
        return self._parse_tokens(self._parse_unary, self.FACTOR)

    def _parse_unary(self):
        if self._match(TokenType.NOT):
            return self.expression(exp.Not, this=self._parse_equality())
        if self._match(TokenType.TILDA):
            return self.expression(exp.BitwiseNot, this=self._parse_unary())
        if self._match(TokenType.DASH):
            return self.expression(exp.Neg, this=self._parse_unary())
        return self._parse_at_time_zone(self._parse_type())

    def _parse_type(self):
        if self._match(TokenType.INTERVAL):
            return self.expression(
                exp.Interval,
                this=self._parse_term(),
                unit=self._parse_var(),
            )

        index = self._index
        type_token = self._parse_types()
        this = self._parse_column()

        if type_token:
            if this:
                return self.expression(exp.Cast, this=this, to=type_token)
            if not type_token.args.get("expressions"):
                self._retreat(index)
                return self._parse_column()
            return type_token

        return this

    def _parse_types(self):
        index = self._index

        if not self._match_set(self.TYPE_TOKENS):
            return None

        type_token = self._prev.token_type
        nested = type_token in self.NESTED_TYPE_TOKENS
        is_struct = type_token == TokenType.STRUCT
        expressions = None

        if not nested and self._match_pair(TokenType.L_BRACKET, TokenType.R_BRACKET):
            return exp.DataType(
                this=exp.DataType.Type.ARRAY, expressions=[exp.DataType.build(type_token.value)], nested=True
            )

        if self._match(TokenType.L_BRACKET):
            self._retreat(index)
            return None

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

        if nested and self._match(TokenType.LT):
            if is_struct:
                expressions = self._parse_csv(self._parse_struct_kwargs)
            else:
                expressions = self._parse_csv(self._parse_types)

            if not self._match(TokenType.GT):
                self.raise_error("Expecting >")

        if type_token in self.TIMESTAMPS:
            tz = self._match(TokenType.WITH_TIME_ZONE) or type_token == TokenType.TIMESTAMPTZ
            if tz:
                return exp.DataType(
                    this=exp.DataType.Type.TIMESTAMPTZ,
                    expressions=expressions,
                )
            ltz = self._match(TokenType.WITH_LOCAL_TIME_ZONE) or type_token == TokenType.TIMESTAMPLTZ
            if ltz:
                return exp.DataType(
                    this=exp.DataType.Type.TIMESTAMPLTZ,
                    expressions=expressions,
                )
            self._match(TokenType.WITHOUT_TIME_ZONE)

            return exp.DataType(
                this=exp.DataType.Type.TIMESTAMP,
                expressions=expressions,
            )

        return exp.DataType(
            this=exp.DataType.Type[type_token.value.upper()],
            expressions=expressions,
            nested=nested,
        )

    def _parse_struct_kwargs(self):
        this = self._parse_id_var()
        self._match(TokenType.COLON)
        data_type = self._parse_types()
        if not data_type:
            return None
        return self.expression(exp.StructKwarg, this=this, expression=data_type)

    def _parse_at_time_zone(self, this):
        if not self._match(TokenType.AT_TIME_ZONE):
            return this

        return self.expression(exp.AtTimeZone, this=this, zone=self._parse_unary())

    def _parse_column(self):
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
                field = exp.Literal.string(self._advance() or self._prev.text)
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

    def _parse_primary(self):
        if self._match_set(self.PRIMARY_PARSERS):
            return self.PRIMARY_PARSERS[self._prev.token_type](self, self._prev)

        if self._match_pair(TokenType.DOT, TokenType.NUMBER):
            return exp.Literal.number(f"0.{self._prev.text}")

        if self._match(TokenType.L_PAREN):
            query = self._parse_select()

            if query:
                expressions = [query]
            else:
                expressions = self._parse_csv(lambda: self._parse_alias(self._parse_conjunction(), explicit=True))

            this = list_get(expressions, 0)
            self._parse_query_modifiers(this)
            self._match_r_paren()

            if isinstance(this, exp.Subqueryable):
                return self._parse_set_operations(self._parse_subquery(this))
            if len(expressions) > 1:
                return self.expression(exp.Tuple, expressions=expressions)
            return self.expression(exp.Paren, this=this)

        return None

    def _parse_field(self, any_token=False):
        return self._parse_primary() or self._parse_function() or self._parse_id_var(any_token)

    def _parse_function(self, functions=None):
        if not self._curr:
            return None

        token_type = self._curr.token_type

        if self._match_set(self.NO_PAREN_FUNCTION_PARSERS):
            return self.NO_PAREN_FUNCTION_PARSERS[token_type](self)

        if not self._next or self._next.token_type != TokenType.L_PAREN:
            if token_type in self.NO_PAREN_FUNCTIONS:
                return self.expression(self._advance() or self.NO_PAREN_FUNCTIONS[token_type])
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

            if subquery_predicate and self._curr.token_type in (
                TokenType.SELECT,
                TokenType.WITH,
            ):
                this = self.expression(subquery_predicate, this=self._parse_select())
                self._match_r_paren()
                return this

            if functions is None:
                functions = self.FUNCTIONS
            function = functions.get(upper)
            args = self._parse_csv(self._parse_lambda)

            if function:
                this = function(args)
                self.validate_expression(this, args)
            else:
                this = self.expression(exp.Anonymous, this=this, expressions=args)
        self._match_r_paren()
        return self._parse_window(this)

    def _parse_user_defined_function(self):
        this = self._parse_id_var()

        while self._match(TokenType.DOT):
            this = self.expression(exp.Dot, this=this, expression=self._parse_id_var())

        if not self._match(TokenType.L_PAREN):
            return this
        expressions = self._parse_csv(self._parse_udf_kwarg)
        self._match_r_paren()
        return self.expression(exp.UserDefinedFunction, this=this, expressions=expressions)

    def _parse_introducer(self, token):
        literal = self._parse_primary()
        if literal:
            return self.expression(
                exp.Introducer,
                this=token.text,
                expression=literal,
            )

        return self.expression(exp.Identifier, this=token.text)

    def _parse_udf_kwarg(self):
        this = self._parse_id_var()
        kind = self._parse_types()

        if not kind:
            return this

        return self.expression(exp.UserDefinedFunctionKwarg, this=this, kind=kind)

    def _parse_lambda(self):
        index = self._index

        if self._match(TokenType.L_PAREN):
            expressions = self._parse_csv(self._parse_id_var)
            self._match(TokenType.R_PAREN)
        else:
            expressions = [self._parse_id_var()]

        if not self._match(TokenType.ARROW):
            self._retreat(index)

            if self._match(TokenType.DISTINCT):
                this = self.expression(exp.Distinct, expressions=self._parse_csv(self._parse_conjunction))
            else:
                this = self._parse_conjunction()

            if self._match(TokenType.IGNORE_NULLS):
                this = self.expression(exp.IgnoreNulls, this=this)
            else:
                self._match(TokenType.RESPECT_NULLS)

            return self._parse_alias(self._parse_limit(self._parse_order(this)))

        conjunction = self._parse_conjunction().transform(self._replace_lambda, {node.name for node in expressions})
        return self.expression(
            exp.Lambda,
            this=conjunction,
            expressions=expressions,
        )

    def _parse_schema(self, this=None):
        index = self._index
        if not self._match(TokenType.L_PAREN) or self._match(TokenType.SELECT):
            self._retreat(index)
            return this

        args = self._parse_csv(lambda: self._parse_constraint() or self._parse_column_def(self._parse_field(True)))
        self._match_r_paren()
        return self.expression(exp.Schema, this=this, expressions=args)

    def _parse_column_def(self, this):
        kind = self._parse_types()

        if not kind:
            return this

        constraints = []
        while True:
            constraint = self._parse_column_constraint()
            if not constraint:
                break
            constraints.append(constraint)

        return self.expression(exp.ColumnDef, this=this, kind=kind, constraints=constraints)

    def _parse_column_constraint(self):
        this = None

        if self._match(TokenType.CONSTRAINT):
            this = self._parse_id_var()

        if self._match(TokenType.AUTO_INCREMENT):
            kind = exp.AutoIncrementColumnConstraint()
        elif self._match(TokenType.CHECK):
            self._match_l_paren()
            kind = self.expression(exp.CheckColumnConstraint, this=self._parse_conjunction())
            self._match_r_paren()
        elif self._match(TokenType.COLLATE):
            kind = self.expression(exp.CollateColumnConstraint, this=self._parse_var())
        elif self._match(TokenType.DEFAULT):
            kind = self.expression(exp.DefaultColumnConstraint, this=self._parse_field())
        elif self._match_pair(TokenType.NOT, TokenType.NULL):
            kind = exp.NotNullColumnConstraint()
        elif self._match(TokenType.SCHEMA_COMMENT):
            kind = self.expression(exp.CommentColumnConstraint, this=self._parse_string())
        elif self._match(TokenType.PRIMARY_KEY):
            kind = exp.PrimaryKeyColumnConstraint()
        elif self._match(TokenType.UNIQUE):
            kind = exp.UniqueColumnConstraint()
        elif self._match(TokenType.GENERATED):
            if self._match(TokenType.BY_DEFAULT):
                kind = self.expression(exp.GeneratedAsIdentityColumnConstraint, this=False)
            else:
                self._match(TokenType.ALWAYS)
                kind = self.expression(exp.GeneratedAsIdentityColumnConstraint, this=True)
            self._match_pair(TokenType.ALIAS, TokenType.IDENTITY)
        else:
            return None

        return self.expression(exp.ColumnConstraint, this=this, kind=kind)

    def _parse_constraint(self):
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

    def _parse_unnamed_constraint(self):
        if not self._match_set(self.CONSTRAINT_PARSERS):
            return None

        return self.CONSTRAINT_PARSERS[self._prev.token_type](self)

    def _parse_check(self):
        self._match(TokenType.CHECK)
        self._match_l_paren()
        expression = self._parse_conjunction()
        self._match_r_paren()

        return self.expression(exp.Check, this=expression)

    def _parse_unique(self):
        self._match(TokenType.UNIQUE)
        columns = self._parse_wrapped_id_vars()

        return self.expression(exp.Unique, expressions=columns)

    def _parse_foreign_key(self):
        self._match(TokenType.FOREIGN_KEY)

        expressions = self._parse_wrapped_id_vars()
        reference = self._match(TokenType.REFERENCES) and self.expression(
            exp.Reference,
            this=self._parse_id_var(),
            expressions=self._parse_wrapped_id_vars(),
        )
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
            exp.ForeignKey,
            expressions=expressions,
            reference=reference,
            **options,
        )

    def _parse_bracket(self, this):
        if not self._match(TokenType.L_BRACKET):
            return this

        expressions = self._parse_csv(self._parse_conjunction)

        if not this or this.name.upper() == "ARRAY":
            this = self.expression(exp.Array, expressions=expressions)
        else:
            expressions = apply_index_offset(expressions, -self.index_offset)
            this = self.expression(exp.Bracket, this=this, expressions=expressions)

        if not self._match(TokenType.R_BRACKET):
            self.raise_error("Expected ]")

        return self._parse_bracket(this)

    def _parse_case(self):
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

        return self._parse_window(self.expression(exp.Case, this=expression, ifs=ifs, default=default))

    def _parse_if(self):
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

    def _parse_extract(self):
        this = self._parse_var() or self._parse_type()

        if self._match(TokenType.FROM):
            return self.expression(exp.Extract, this=this, expression=self._parse_bitwise())

        if not self._match(TokenType.COMMA):
            self.raise_error("Expected FROM or comma after EXTRACT", self._prev)

        return self.expression(exp.Extract, this=this, expression=self._parse_bitwise())

    def _parse_cast(self, strict):
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

    def _parse_convert(self, strict):
        this = self._parse_field()
        if self._match(TokenType.USING):
            to = self.expression(exp.CharacterSet, this=self._parse_var())
        elif self._match(TokenType.COMMA):
            to = self._parse_types()
        else:
            to = None
        return self.expression(exp.Cast if strict else exp.TryCast, this=this, to=to)

    def _parse_position(self):
        args = self._parse_csv(self._parse_bitwise)

        if self._match(TokenType.IN):
            args.append(self._parse_bitwise())

        # Note: we're parsing in order needle, haystack, position
        this = exp.StrPosition.from_arg_list(args)
        self.validate_expression(this, args)

        return this

    def _parse_join_hint(self, func_name):
        args = self._parse_csv(self._parse_table)
        return exp.JoinHint(this=func_name.upper(), expressions=args)

    def _parse_substring(self):
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

    def _parse_trim(self):
        # https://www.w3resource.com/sql/character-functions/trim.php
        # https://docs.oracle.com/javadb/10.8.3.0/ref/rreftrimfunc.html

        position = None
        collation = None

        if self._match_set(self.TRIM_TYPES):
            position = self._prev.text.upper()

        expression = self._parse_term()
        if self._match(TokenType.FROM):
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

    def _parse_window(self, this, alias=False):
        if self._match(TokenType.FILTER):
            self._match_l_paren()
            this = self.expression(exp.Filter, this=this, expression=self._parse_where())
            self._match_r_paren()

        # T-SQL allows the OVER (...) syntax after WITHIN GROUP.
        # https://learn.microsoft.com/en-us/sql/t-sql/functions/percentile-disc-transact-sql?view=sql-server-ver16
        if self._match(TokenType.WITHIN_GROUP):
            self._match_l_paren()
            this = self.expression(
                exp.WithinGroup,
                this=this,
                expression=self._parse_order(),
            )
            self._match_r_paren()

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
            alias = self._parse_id_var(False)

            return self.expression(
                exp.Window,
                this=this,
                alias=alias,
            )

        partition = None

        alias = self._parse_id_var(False)

        if self._match(TokenType.PARTITION_BY):
            partition = self._parse_csv(self._parse_conjunction)

        order = self._parse_order()

        spec = None
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

        self._match_r_paren()

        return self.expression(
            exp.Window,
            this=this,
            partition_by=partition,
            order=order,
            spec=spec,
            alias=alias,
        )

    def _parse_window_spec(self):
        self._match(TokenType.BETWEEN)

        return {
            "value": (self._match_set((TokenType.UNBOUNDED, TokenType.CURRENT_ROW)) and self._prev.text)
            or self._parse_bitwise(),
            "side": self._match_set((TokenType.PRECEDING, TokenType.FOLLOWING)) and self._prev.text,
        }

    def _parse_alias(self, this, explicit=False):
        any_token = self._match(TokenType.ALIAS)

        if explicit and not any_token:
            return this

        if self._match(TokenType.L_PAREN):
            aliases = self.expression(
                exp.Aliases,
                this=this,
                expressions=self._parse_csv(lambda: self._parse_id_var(any_token)),
            )
            self._match_r_paren()
            return aliases

        alias = self._parse_id_var(any_token)

        if alias:
            return self.expression(exp.Alias, this=this, alias=alias)

        return this

    def _parse_id_var(self, any_token=True, tokens=None):
        identifier = self._parse_identifier()

        if identifier:
            return identifier

        if any_token and self._curr and self._curr.token_type not in self.RESERVED_KEYWORDS:
            return self._advance() or exp.Identifier(this=self._prev.text, quoted=False)

        return self._match_set(tokens or self.ID_VAR_TOKENS) and exp.Identifier(this=self._prev.text, quoted=False)

    def _parse_string(self):
        if self._match(TokenType.STRING):
            return exp.Literal.string(self._prev.text)
        return self._parse_placeholder()

    def _parse_number(self):
        if self._match(TokenType.NUMBER):
            return exp.Literal.number(self._prev.text)
        return self._parse_placeholder()

    def _parse_identifier(self):
        if self._match(TokenType.IDENTIFIER):
            return exp.Identifier(this=self._prev.text, quoted=True)
        return self._parse_placeholder()

    def _parse_var(self):
        if self._match(TokenType.VAR):
            return exp.Var(this=self._prev.text)
        return self._parse_placeholder()

    def _parse_var_or_string(self):
        return self._parse_var() or self._parse_string()

    def _parse_null(self):
        if self._match(TokenType.NULL):
            return exp.Null()
        return None

    def _parse_boolean(self):
        if self._match(TokenType.TRUE):
            return exp.Boolean(this=True)
        if self._match(TokenType.FALSE):
            return exp.Boolean(this=False)
        return None

    def _parse_star(self):
        if self._match(TokenType.STAR):
            return exp.Star(**{"except": self._parse_except(), "replace": self._parse_replace()})
        return None

    def _parse_placeholder(self):
        if self._match(TokenType.PLACEHOLDER):
            return exp.Placeholder()
        elif self._match(TokenType.COLON):
            self._advance()
            return exp.Placeholder(this=self._prev.text)
        return None

    def _parse_except(self):
        if not self._match(TokenType.EXCEPT):
            return None

        return self._parse_wrapped_id_vars()

    def _parse_replace(self):
        if not self._match(TokenType.REPLACE):
            return None

        self._match_l_paren()
        columns = self._parse_csv(lambda: self._parse_alias(self._parse_expression()))
        self._match_r_paren()
        return columns

    def _parse_csv(self, parse):
        parse_result = parse()
        items = [parse_result] if parse_result is not None else []

        while self._match(TokenType.COMMA):
            parse_result = parse()
            if parse_result is not None:
                items.append(parse_result)

        return items

    def _parse_tokens(self, parse, expressions):
        this = parse()

        while self._match_set(expressions):
            this = self.expression(expressions[self._prev.token_type], this=this, expression=parse())

        return this

    def _parse_wrapped_id_vars(self):
        self._match_l_paren()
        expressions = self._parse_csv(self._parse_id_var)
        self._match_r_paren()
        return expressions

    def _parse_select_or_expression(self):
        return self._parse_select() or self._parse_expression()

    def _match(self, token_type):
        if not self._curr:
            return None

        if self._curr.token_type == token_type:
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

    def _match_l_paren(self):
        if not self._match(TokenType.L_PAREN):
            self.raise_error("Expecting (")

    def _match_r_paren(self):
        if not self._match(TokenType.R_PAREN):
            self.raise_error("Expecting )")

    def _match_text(self, *texts):
        index = self._index
        for text in texts:
            if self._curr and self._curr.text.upper() == text:
                self._advance()
            else:
                self._retreat(index)
                return False
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
