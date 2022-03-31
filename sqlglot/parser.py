import logging

import sqlglot.constants as c
from sqlglot.errors import ErrorLevel, ParseError
from sqlglot.helper import apply_index_offset, list_get
from sqlglot.tokens import Token, Tokenizer, TokenType
import sqlglot.expressions as exp


logger = logging.getLogger("sqlglot")


class Parser:
    """
    Parser consumes a list of tokens produced by the :class:`~sqlglot.tokens.Tokenizer`
    and produces a parsed syntax tree.

    Args
        functions (dict): the dictionary of additional functions in which the key
            represents a function's SQL name and the value is a function which constructs
            the function instance from a list of arguments.
        error_level (ErrorLevel): the desired error level. Default: ErrorLevel.RAISE.
        error_message_context (int): determines the amount of context to capture from
            a query string when displaying the error message (in number of characters).
            Default: 50.
        index_offset (int): Index offset for arrays eg ARRAY[0] vs ARRAY[1] as the head of a list
            Default: 0
        strict_cast (boolean): if true, cast is expected to raise an error on failure
            Default: True
    """

    def _parse_decimal(args):
        size = len(args)
        precision = args[0] if size > 0 else None
        scale = args[1] if size > 1 else None
        return exp.Decimal(precision=precision, scale=scale)

    FUNCTIONS = {
        **{name: f.from_arg_list for f in exp.ALL_FUNCTIONS for name in f.sql_names()},
        "DECIMAL": _parse_decimal,
        "NUMERIC": _parse_decimal,
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
        TokenType.VARCHAR,
        TokenType.TEXT,
        TokenType.BINARY,
        TokenType.JSON,
        TokenType.TIMESTAMP,
        TokenType.TIMESTAMPTZ,
        TokenType.DATE,
        TokenType.ARRAY,
        TokenType.DECIMAL,
        TokenType.MAP,
        TokenType.UUID,
    }

    NESTED_TYPE_TOKENS = {
        TokenType.ARRAY,
        TokenType.DATE,
        TokenType.MAP,
    }

    ID_VAR_TOKENS = {
        TokenType.VAR,
        TokenType.ALL,
        TokenType.ASC,
        TokenType.ALTER,
        TokenType.BUCKET,
        TokenType.CACHE,
        TokenType.COLLATE,
        TokenType.COUNT,
        TokenType.DEFAULT,
        TokenType.DELETE,
        TokenType.DESC,
        TokenType.ENGINE,
        TokenType.EXPLAIN,
        TokenType.FALSE,
        TokenType.FOLLOWING,
        TokenType.IF,
        TokenType.INTERVAL,
        TokenType.LAZY,
        TokenType.OPTIMIZE,
        TokenType.OPTIONS,
        TokenType.ORDINALITY,
        TokenType.OVER,
        TokenType.PERCENT,
        TokenType.PRECEDING,
        TokenType.RANGE,
        TokenType.ROWS,
        TokenType.SCHEMA_COMMENT,
        TokenType.SET,
        TokenType.SHOW,
        TokenType.TABLE_SAMPLE,
        TokenType.TEMPORARY,
        TokenType.TRUNCATE,
        TokenType.TRUE,
        TokenType.UNBOUNDED,
        *TYPE_TOKENS,
    }

    CASTS = {
        TokenType.CAST,
        TokenType.TRY_CAST,
    }

    FUNC_TOKENS = {
        TokenType.COUNT,
        TokenType.EXISTS,
        TokenType.EXTRACT,
        TokenType.IF,
        TokenType.PRIMARY_KEY,
        TokenType.REPLACE,
        TokenType.UNNEST,
        TokenType.VAR,
        TokenType.LEFT,
        TokenType.RIGHT,
        *CASTS,
        *NESTED_TYPE_TOKENS,
    }

    CONJUNCTION = {
        TokenType.AND: exp.And,
        TokenType.OR: exp.Or,
    }

    EQUALITY = {
        TokenType.EQ: exp.EQ,
        TokenType.NEQ: exp.NEQ,
        TokenType.IS: exp.Is,
    }

    COMPARISON = {
        TokenType.GT: exp.GT,
        TokenType.GTE: exp.GTE,
        TokenType.LT: exp.LT,
        TokenType.LTE: exp.LTE,
    }

    BITWISE = {
        TokenType.LSHIFT: exp.BitwiseLeftShift,
        TokenType.RSHIFT: exp.BitwiseRightShift,
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
        TokenType.SLASH: exp.Div,
        TokenType.STAR: exp.Mul,
    }

    TIMESTAMPS = {
        TokenType.TIMESTAMP,
        TokenType.TIMESTAMPTZ,
    }

    SET_OPERATIONS = {
        TokenType.UNION,
        TokenType.INTERSECT,
        TokenType.EXCEPT,
    }

    __slots__ = (
        "functions",
        "error_level",
        "error_message_context",
        "code",
        "errors",
        "index_offset",
        "strict_cast",
        "_tokens",
        "_chunks",
        "_index",
        "_curr",
        "_next",
        "_prev",
    )

    def __init__(
        self,
        functions=None,
        error_level=None,
        error_message_context=100,
        index_offset=0,
        strict_cast=True,
    ):
        self.functions = {**self.FUNCTIONS, **(functions or {})}
        self.error_level = error_level or ErrorLevel.RAISE
        self.error_message_context = error_message_context
        self.index_offset = index_offset
        self.strict_cast = strict_cast
        self.reset()

    def reset(self):
        self.code = ""
        self.errors = []
        self._tokens = []
        self._chunks = [[]]
        self._index = 0
        self._curr = None
        self._next = None
        self._prev = None

    def parse(self, raw_tokens, code=None):
        """
        Parses the given list of tokens and returns a list of syntax trees, one tree
        per parsed SQL statement.

        Args
            raw_tokens (list): the list of tokens (:class:`~sqlglot.tokens.Token`).
            code (str): the original SQL string. Used to produce helpful debug messages.

        Returns
            the list of syntax trees (:class:`~sqlglot.expressions.Expression`).
        """
        self.reset()
        self.code = code or ""
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
            expressions.append(self._parse_statement())

            if self._index < len(self._tokens):
                self.raise_error("Invalid expression / Unexpected token")

            self.check_errors()
            self.set_parents(expressions)

        return expressions

    def check_errors(self):
        for error in self.errors:
            if self.error_level == ErrorLevel.RAISE:
                raise error
            if self.error_level == ErrorLevel.WARN:
                logger.error(error)

    def set_parents(self, expressions):
        for expression in expressions:
            if not expression:
                continue
            for node, parent, key in expression.walk():
                if isinstance(node, exp.Expression) and parent:
                    node.parent = parent
                    node.arg_key = key

    def raise_error(self, message, token=None):
        token = token or self._curr or self._prev or Token.string("")
        start = self._find_token(token, self.code)
        end = start + len(token.text)
        start_context = self.code[max(start - self.error_message_context, 0) : start]
        highlight = self.code[start:end]
        end_context = self.code[end : end + self.error_message_context]
        self.errors.append(
            ParseError(
                f"{message}. Line {token.line}, Col: {token.col}.\n"
                f"  {start_context}\033[4m{highlight}\033[0m{end_context}"
            )
        )

    def expression(self, exp_class, **kwargs):
        instance = exp_class(**kwargs)
        self.validate_expression(instance)
        return instance

    def validate_expression(self, expression):
        if self.error_level == ErrorLevel.IGNORE:
            return

        for k in expression.args:
            if k not in expression.arg_types:
                self.raise_error(
                    f"Unexpected keyword: '{k}' for {expression.__class__}"
                )
        for k, mandatory in expression.arg_types.items():
            v = expression.args.get(k)
            if mandatory and (v is None or v == []):
                self.raise_error(
                    f"Required keyword: '{k}' missing for {expression.__class__}"
                )

    def _find_token(self, token, code):
        line = 1
        col = 1
        index = 0

        while line < token.line or col < token.col:
            if code[index] == "\n":
                line += 1
                col = 1
            else:
                col += 1
            index += 1

        return index

    def _advance(self, times=1):
        self._index += times
        self._curr = list_get(self._tokens, self._index)
        self._next = list_get(self._tokens, self._index + 1)
        self._prev = (
            list_get(self._tokens, self._index - 1) if self._index > 0 else None
        )

    def _retreat(self, index):
        self._advance(index - self._index)

    def _parse_statement(self):
        if self._curr is None:
            return None

        if self._match(TokenType.CREATE):
            return self._parse_create()

        if self._match(TokenType.DROP):
            return self._parse_drop()

        if self._match(TokenType.INSERT):
            return self._parse_insert()

        if self._match(TokenType.UPDATE):
            return self._parse_update()

        if self._match(TokenType.DELETE):
            return self._parse_delete()

        if self._match(TokenType.CACHE):
            return self._parse_cache()

        if self._match(TokenType.UNCACHE):
            return self._parse_uncache()

        if self._match_set(Tokenizer.COMMANDS):
            return self.expression(
                exp.Command,
                this=self._prev.text,
                expression=self._parse_string(),
            )

        return self._parse_set_operations(self._parse_expression()) or self._parse_cte()

    def _parse_drop(self):
        if self._match(TokenType.TABLE):
            kind = "table"
        elif self._match(TokenType.VIEW):
            kind = "view"
        else:
            self.raise_error("Expected TABLE or View")

        return self.expression(
            exp.Drop,
            exists=self._parse_exists(),
            this=self._parse_table(None, schema=True),
            kind=kind,
        )

    def _parse_exists(self, not_=False):
        return (
            self._match(TokenType.IF)
            and (not not_ or self._match(TokenType.NOT))
            and self._match(TokenType.EXISTS)
        )

    def _parse_create(self):
        replace = self._match(TokenType.OR) and self._match(TokenType.REPLACE)
        temporary = self._match(TokenType.TEMPORARY)

        create_token = self._match_set((TokenType.TABLE, TokenType.VIEW)) and self._prev

        if not create_token:
            self.raise_error("Expected TABLE or View")

        exists = self._parse_exists(not_=True)
        this = self._parse_table(alias=None, schema=True)
        expression = None
        properties = None

        if create_token.token_type == TokenType.TABLE:
            properties = self._parse_properties(
                this if isinstance(this, exp.Schema) else None
            )

        if self._match(TokenType.ALIAS):
            expression = self._parse_cte()

        options = {
            "engine": None,
            "auto_increment": None,
            "character_set": None,
            "collate": None,
            "comment": None,
            "parsed": True,
        }

        def parse_option(option, token, option_lambda):
            if not options[option] and self._match(token):
                self._match(TokenType.EQ)
                options[option] = option_lambda()
                options["parsed"] = True

        while options["parsed"]:
            options["parsed"] = False

            parse_option("engine", TokenType.ENGINE, self._parse_var)
            parse_option("auto_increment", TokenType.AUTO_INCREMENT, self._parse_number)
            parse_option("collate", TokenType.COLLATE, self._parse_var)
            parse_option("comment", TokenType.SCHEMA_COMMENT, self._parse_string)

            if not options["character_set"]:
                default = self._match(TokenType.DEFAULT)
                parse_option(
                    "character_set",
                    TokenType.CHARACTER_SET,
                    lambda: self.expression(
                        exp.CharacterSet,
                        this=self._parse_var(),
                        default=default,
                    ),
                )

        options.pop("parsed")

        return self.expression(
            exp.Create,
            this=this,
            kind=create_token.text,
            expression=expression,
            exists=exists,
            properties=properties,
            temporary=temporary,
            replace=replace,
            **options,
        )

    def _parse_property(self, schema):
        key = self._parse_var().this
        self._match(TokenType.EQ)

        if key.upper() == c.PARTITIONED_BY:
            value = self._parse_schema() or self._parse_field()

            if schema and not isinstance(value, exp.Schema):
                columns = {v.text("this").upper() for v in value.args["expressions"]}
                partitions = [
                    expression
                    for expression in schema.args["expressions"]
                    if expression.this.text("this").upper() in columns
                ]
                schema.args["expressions"] = [
                    e for e in schema.args["expressions"] if e not in partitions
                ]
                value = self.expression(exp.Schema, expressions=partitions)
        else:
            value = self._parse_string()

        return self.expression(
            exp.Property,
            this=exp.Literal.string(key),
            value=value,
        )

    def _parse_properties(self, schema):
        properties = []

        if self._match(TokenType.WITH):
            self._match_l_paren()
            properties.extend(self._parse_csv(lambda: self._parse_property(schema)))
            self._match_r_paren()
        else:
            if self._match_by(TokenType.PARTITION):
                properties.append(
                    self.expression(
                        exp.Property,
                        this=exp.Literal.string(c.PARTITIONED_BY),
                        value=self._parse_schema(),
                    )
                )

            if self._match(TokenType.STORED):
                self._match(TokenType.ALIAS)
                properties.append(
                    self.expression(
                        exp.Property,
                        this=exp.Literal.string(c.FORMAT),
                        value=exp.Literal.string(self._parse_var().text("this")),
                    )
                )

            if self._match(TokenType.PROPERTIES):
                self._match_l_paren()
                properties.extend(
                    self._parse_csv(
                        lambda: self.expression(
                            exp.Property,
                            this=self._parse_string(),
                            value=self._match(TokenType.EQ) and self._parse_string(),
                        )
                    )
                )
                self._match_r_paren()
        if properties:
            return self.expression(exp.Properties, expressions=properties)
        return None

    def _parse_insert(self):
        overwrite = self._match(TokenType.OVERWRITE)
        self._match(TokenType.INTO)
        self._match(TokenType.TABLE)

        return self.expression(
            exp.Insert,
            this=self._parse_table(alias=None, schema=True),
            exists=self._parse_exists(),
            partition=self._parse_partition(),
            expression=self._parse_select(),
            overwrite=overwrite,
        )

    def _parse_delete(self):
        self._match(TokenType.FROM)

        return self.expression(
            exp.Delete,
            this=self._parse_table(alias=None, schema=True),
            where=self._parse_where(),
        )

    def _parse_update(self):
        return self.expression(
            exp.Update,
            **{
                "this": self._parse_table(alias=None, schema=True),
                "expressions": self._match(TokenType.SET)
                and self._parse_csv(self._parse_equality),
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
        table = self._parse_table(alias=None, schema=True)
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
            expression=self._parse_cte(),
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

    def _parse_values(self):
        if not self._match(TokenType.VALUES):
            return None

        return self.expression(
            exp.Values, expressions=self._parse_csv(self._parse_value)
        )

    def _parse_value(self):
        self._match_l_paren()
        expressions = self._parse_csv(self._parse_conjunction)
        self._match_r_paren()
        return self.expression(exp.Tuple, expressions=expressions)

    def _parse_cte(self):
        if not self._match(TokenType.WITH):
            return self._parse_select()

        expressions = []

        while True:
            recursive = self._match(TokenType.RECURSIVE)
            alias = self._parse_function() or self._parse_id_var()

            if not alias:
                self.raise_error("Expected alias after WITH")

            if not self._match(TokenType.ALIAS):
                self.raise_error("Expected AS after WITH")

            expressions.append(self._parse_table(alias=alias))

            if not self._match(TokenType.COMMA):
                break

        return self.expression(
            exp.CTE,
            this=self._parse_statement(),
            expressions=expressions,
            recursive=recursive,
        )

    def _parse_select(self):
        this = self._parse_values()

        if self._match(TokenType.L_PAREN):
            this = self._parse_select()
            self._match_r_paren()
            this = self._parse_alias(this)

        if isinstance(this, exp.Alias) or self._match(TokenType.SELECT):
            this = self.expression(
                exp.Select,
                hint=self._parse_hint(),
                distinct=self._match(TokenType.DISTINCT),
                expressions=self._parse_csv(
                    lambda: self._parse_annotation(self._parse_expression())
                ),
                **{
                    "from": this or self._parse_from(),
                    "laterals": self._parse_laterals(),
                    "joins": self._parse_joins(),
                    "where": self._parse_where(),
                    "group": self._parse_group(),
                    "having": self._parse_having(),
                    "order": self._parse_order(),
                    "limit": self._parse_limit(),
                    "offset": self._parse_offset(),
                },
            )

        return self._parse_set_operations(this)

    def _parse_annotation(self, expression):
        if self._match(TokenType.ANNOTATION):
            return self.expression(
                exp.Annotation, this=self._prev.text, expression=expression
            )

        return expression

    def _parse_hint(self):
        if self._match(TokenType.HINT):
            hints = self._parse_csv(self._parse_function)
            if not self._match(TokenType.COMMENT_END):
                self.raise_error("Expected */ after HINT")
            return self.expression(exp.Hint, expressions=hints)
        return None

    def _parse_from(self):
        if not self._match(TokenType.FROM):
            return None

        return self.expression(exp.From, expressions=self._parse_csv(self._parse_table))

    def _parse_laterals(self):
        laterals = []

        while True:
            if not self._match(TokenType.LATERAL):
                return laterals

            if not self._match(TokenType.VIEW):
                self.raise_error("Expected VIEW afteral LATERAL")

            outer = self._match(TokenType.OUTER)
            this = self._parse_function()
            table = self._parse_id_var()
            columns = (
                self._parse_csv(self._parse_id_var)
                if self._match(TokenType.ALIAS)
                else None
            )

            laterals.append(
                self.expression(
                    exp.Lateral,
                    this=this,
                    outer=outer,
                    table=self.expression(exp.Table, this=table),
                    columns=columns,
                )
            )

    def _parse_joins(self):
        joins = []

        while True:
            side = (
                self._match_set((TokenType.LEFT, TokenType.RIGHT, TokenType.FULL))
                and self._prev
            )
            kind = (
                self._match_set((TokenType.INNER, TokenType.OUTER, TokenType.CROSS))
                and self._prev
            )

            if not self._match(TokenType.JOIN):
                return joins

            joins.append(
                self.expression(
                    exp.Join,
                    this=self._parse_table(),
                    side=side.text if side else None,
                    kind=kind.text if kind else None,
                    on=self._parse_conjunction() if self._match(TokenType.ON) else None,
                )
            )

    def _parse_table(self, alias=False, schema=False):
        unnest = self._parse_unnest()

        if unnest:
            return unnest

        if self._match(TokenType.L_PAREN):
            expression = self._parse_cte()
            self._match_r_paren()
        else:
            db = None
            table = (not schema and self._parse_function()) or self._parse_id_var()

            if self._match(TokenType.DOT):
                db = table
                table = self._parse_id_var()
                if not table:
                    self.raise_error("Expected table name")

            expression = self.expression(exp.Table, this=table, db=db)

        expression = self._parse_table_sample(expression)

        if alias is None:
            this = expression
        elif alias:
            this = self.expression(exp.Alias, this=expression, alias=alias)
        else:
            this = self._parse_alias(expression)

        if not isinstance(this, (exp.Alias, exp.Table)):
            this = self.expression(exp.Alias, this=this, alias=None)

        if schema:
            return self._parse_schema(this=expression)
        return this

    def _parse_unnest(self):
        if not self._match(TokenType.UNNEST):
            return None

        self._match_l_paren()
        expressions = self._parse_csv(self._parse_table)
        self._match_r_paren()

        ordinality = self._match(TokenType.WITH) and self._match(TokenType.ORDINALITY)
        self._match(TokenType.ALIAS)
        table = self._parse_id_var()

        if not self._match(TokenType.L_PAREN):
            return self.expression(
                exp.Unnest, expressions=expressions, ordinality=ordinality, table=table
            )

        columns = self._parse_csv(self._parse_id_var)
        unnest = self.expression(
            exp.Unnest,
            expressions=expressions,
            ordinality=bool(ordinality),
            table=table,
            columns=columns,
        )
        self._match_r_paren()

        return unnest

    def _parse_table_sample(self, this):
        if not self._match(TokenType.TABLE_SAMPLE):
            return this

        bucket_numerator = None
        bucket_denominator = None
        bucket_field = None
        percent = None
        rows = None
        size = None

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

        return self.expression(
            exp.TableSample,
            this=this,
            bucket_numerator=bucket_numerator,
            bucket_denominator=bucket_denominator,
            bucket_field=bucket_field,
            percent=percent,
            rows=rows,
            size=size,
        )

    def _parse_where(self):
        if not self._match(TokenType.WHERE):
            return None

        index = self._index
        not_exists = self._match(TokenType.NOT)

        if self._match(TokenType.EXISTS):
            this = self.expression(
                exp.Exists, **{"this": self._parse_select(), "not": not_exists}
            )
        else:
            if not_exists:
                self._retreat(index)
            this = self._parse_conjunction()

        return self.expression(exp.Where, this=this)

    def _parse_group(self):
        if not self._match_by(TokenType.GROUP):
            return None
        return self.expression(
            exp.Group, expressions=self._parse_csv(self._parse_conjunction)
        )

    def _parse_having(self):
        if not self._match(TokenType.HAVING):
            return None
        return self.expression(exp.Having, this=self._parse_conjunction())

    def _parse_order(self):
        if not self._match_by(TokenType.ORDER):
            return None

        return self.expression(
            exp.Order, expressions=self._parse_csv(self._parse_ordered)
        )

    def _parse_ordered(self):
        this = self._parse_bitwise()
        self._match(TokenType.ASC)
        return self.expression(exp.Ordered, this=this, desc=self._match(TokenType.DESC))

    def _parse_limit(self):
        if not self._match(TokenType.LIMIT):
            return None
        return self.expression(exp.Limit, this=self._parse_number())

    def _parse_offset(self):
        if not self._match(TokenType.OFFSET):
            return None
        return self.expression(exp.Offset, this=self._parse_number())

    def _parse_set_operations(self, this):
        if not self._match_set(self.SET_OPERATIONS):
            return this

        token_type = self._prev.token_type

        if token_type == TokenType.UNION:
            return self.expression(
                exp.Union,
                this=this,
                distinct=self._match(TokenType.DISTINCT)
                or not self._match(TokenType.ALL),
                expression=self._parse_expression() or self._parse_select(),
            )

        return self.expression(
            exp.Except if token_type == TokenType.EXCEPT else exp.Intersect,
            this=this,
            distinct=self._match(TokenType.DISTINCT),
            expression=self._parse_expression() or self._parse_select(),
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

        if self._match(TokenType.LIKE):
            this = self.expression(exp.Like, this=this, expression=self._parse_term())
        elif self._match(TokenType.ILIKE):
            this = self.expression(exp.ILike, this=this, expression=self._parse_term())
        elif self._match(TokenType.RLIKE):
            this = self.expression(
                exp.RegexpLike, this=this, expression=self._parse_term()
            )
        elif self._match(TokenType.IN):
            self._match_l_paren()
            query = self._parse_select()

            if query:
                this = self.expression(exp.In, this=this, query=query)
            else:
                this = self.expression(
                    exp.In, this=this, expressions=self._parse_csv(self._parse_term)
                )

            self._match_r_paren()
        elif self._match(TokenType.BETWEEN):
            low = self._parse_term()
            self._match(TokenType.AND)
            high = self._parse_term()
            this = self.expression(exp.Between, this=this, low=low, high=high)

        if negate:
            this = self.expression(exp.Not, this=this)

        return this

    def _parse_bitwise(self):
        return self._parse_tokens(self._parse_term, self.BITWISE)

    def _parse_term(self):
        return self._parse_tokens(self._parse_factor, self.TERM)

    def _parse_factor(self):
        return self._parse_tokens(self._parse_unary, self.FACTOR)

    def _parse_unary(self):
        if self._match(TokenType.NOT):
            return self.expression(exp.Not, this=self._parse_unary())
        if self._match(TokenType.TILDA):
            return self.expression(exp.BitwiseNot, this=self._parse_unary())
        if self._match(TokenType.DASH):
            return self.expression(exp.Neg, this=self._parse_unary())
        return self._parse_type()

    def _parse_type(self):
        if self._match(TokenType.INTERVAL):
            return self.expression(
                exp.Interval,
                this=self._parse_string() or self._parse_number(),
                unit=self._parse_var(),
            )

        type_token = self._parse_types()
        this = self._parse_column()

        if type_token:
            if this:
                return self.expression(exp.Cast, this=this, to=type_token)
            return type_token

        if self._match(TokenType.DCOLON):
            type_token = self._parse_types()
            if not type_token:
                self.raise_error("Expected type")
            return self.expression(exp.Cast, this=this, to=type_token)

        return this

    def _parse_types(self):
        index = self._index

        if not self._match_set(self.TYPE_TOKENS):
            return None

        type_token = self._prev.token_type
        nested = type_token in self.NESTED_TYPE_TOKENS
        expressions = None

        if self._match(TokenType.L_BRACKET):
            self._retreat(index)
            return None

        if self._match(TokenType.L_PAREN):
            expressions = self._parse_csv(
                self._parse_types if nested else self._parse_number
            )

            if nested and not expressions:
                self._retreat(index)
                return None

            self._match_r_paren()

        if nested and self._match(TokenType.LT):
            expressions = self._parse_csv(self._parse_types)

            if not self._match(TokenType.GT):
                self.raise_error("Expecting >")

        if type_token in self.TIMESTAMPS:
            tz = self._match(TokenType.WITH)
            self._match(TokenType.WITHOUT)
            self._match(TokenType.TIME)
            self._match(TokenType.ZONE)
            if tz:
                return exp.DataType(
                    this=exp.DataType.Type.TIMESTAMPTZ,
                    expressions=expressions,
                    nested=nested,
                )
            return exp.DataType(
                this=exp.DataType.Type.TIMESTAMP,
                expressions=expressions,
                nested=nested,
            )

        return exp.DataType(
            this=exp.DataType.Type[type_token.value.upper()],
            expressions=expressions,
            nested=nested,
        )

    def _parse_column(self):
        this = self._parse_field()
        table = None
        db = None
        fields = None

        while self._match(TokenType.DOT):
            if db:
                fields = fields if fields else [db, table, this]
                fields.append(self._parse_field())
                continue
            if table:
                db = table
            table = this
            this = self._parse_field()

        if fields:
            return self.expression(exp.Column, fields=fields)
        if any(
            isinstance(field, (exp.Identifier, exp.Star)) for field in (this, table, db)
        ):
            return self.expression(exp.Column, this=this, table=table, db=db)
        return this

    def _parse_primary(self):
        this = (
            self._parse_string()
            or self._parse_number()
            or self._parse_star()
            or self._parse_null()
            or self._parse_boolean()
        )

        if this:
            return this

        if self._match(TokenType.L_PAREN):
            this = self._parse_conjunction() or self._parse_select()
            self._match_r_paren()
            return self.expression(exp.Paren, this=this)

        return None

    def _parse_field(self):
        return self._parse_bracket(
            self._parse_primary() or self._parse_function() or self._parse_id_var()
        )

    def _parse_function(self):
        if self._match(TokenType.CASE):
            return self._parse_case()

        if (
            not self._curr
            or self._curr.token_type not in self.FUNC_TOKENS
            or not self._next
            or self._next.token_type != TokenType.L_PAREN
        ):
            return None

        if self._match_set(self.CASTS):
            strict = self.strict_cast and self._prev.token_type == TokenType.CAST
            self._advance()
            this = self._parse_cast(strict)
        elif self._match(TokenType.COUNT):
            self._advance()
            this = self._parse_count()
        elif self._match(TokenType.EXTRACT):
            self._advance()
            this = self._parse_extract()
        else:
            this = self._curr.text
            self._advance(2)

            function = self.functions.get(this.upper())
            args = self._parse_csv(self._parse_lambda)

            if not callable(function):
                this = self.expression(exp.Anonymous, this=this, expressions=args)
            else:
                this = function(args)
                self.validate_expression(this)
                if len(args) > len(this.arg_types) and not this.is_var_len_args:
                    self.raise_error(
                        f"The number of provided arguments ({len(args)}) is greater than "
                        f"the maximum number of supported arguments ({len(this.arg_types)})"
                    )
        self._match_r_paren()
        return self._parse_window(this)

    def _parse_lambda(self):
        index = self._index

        if self._match(TokenType.L_PAREN):
            expressions = self._parse_csv(self._parse_id_var)
            self._match(TokenType.R_PAREN)
        else:
            expressions = [self._parse_id_var()]

        if not self._match(TokenType.LAMBDA):
            self._retreat(index)
            return self._parse_conjunction()

        return self.expression(
            exp.Lambda,
            this=self._parse_conjunction(),
            expressions=expressions,
        )

    def _parse_schema(self, this=None):
        if not self._match(TokenType.L_PAREN):
            return this

        args = self._parse_csv(lambda: self._parse_column_def(self._parse_field()))
        self._match_r_paren()
        return self.expression(exp.Schema, this=this, expressions=args)

    def _parse_column_def(self, this):
        kind = self._parse_types()

        if not kind:
            return this

        options = {
            "not_null": None,
            "auto_increment": None,
            "collate": None,
            "comment": None,
            "default": None,
            "primary": None,
            "parsed": True,
        }

        def parse_option(option, option_lambda):
            if not options[option]:
                options[option] = option_lambda()

                if options[option]:
                    options["parsed"] = True

        while options["parsed"]:
            options["parsed"] = False
            parse_option(
                "auto_increment", lambda: self._match(TokenType.AUTO_INCREMENT)
            )
            parse_option(
                "collate",
                lambda: self._match(TokenType.COLLATE) and self._parse_var(),
            )
            parse_option(
                "default",
                lambda: self._match(TokenType.DEFAULT) and self._parse_primary(),
            )
            parse_option(
                "not_null",
                lambda: self._match(TokenType.NOT) and self._match(TokenType.NULL),
            )
            parse_option(
                "comment",
                lambda: self._match(TokenType.SCHEMA_COMMENT) and self._parse_string(),
            )
            parse_option(
                "primary",
                lambda: self._match(TokenType.PRIMARY_KEY),
            )

        options.pop("parsed")
        return self.expression(exp.ColumnDef, this=this, kind=kind, **options)

    def _parse_bracket(self, this):
        while self._match(TokenType.L_BRACKET):
            expressions = self._parse_csv(self._parse_conjunction)

            if isinstance(this, exp.Identifier) and this.this.upper() == "ARRAY":
                this = self.expression(exp.Array, expressions=expressions)
            else:
                expressions = apply_index_offset(expressions, -self.index_offset)
                this = self.expression(exp.Bracket, this=this, expressions=expressions)

            if not self._match(TokenType.R_BRACKET):
                self.raise_error("Expected ]")

        return this

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

        return self.expression(exp.Case, this=expression, ifs=ifs, default=default)

    def _parse_count(self):
        return self.expression(
            exp.Count,
            distinct=self._match(TokenType.DISTINCT),
            this=self._parse_conjunction(),
        )

    def _parse_extract(self):
        this = self._parse_var()

        if not self._match(TokenType.FROM):
            self.raise_error("Expected FROM after EXTRACT", self._prev)

        return self.expression(exp.Extract, this=this, expression=self._parse_type())

    def _parse_cast(self, strict):
        this = self._parse_conjunction()

        if not self._match(TokenType.ALIAS):
            self.raise_error("Expected AS after CAST")

        to = self._parse_types()

        if not to:
            self.raise_error("Expected TYPE after CAST")

        return self.expression(exp.Cast if strict else exp.TryCast, this=this, to=to)

    def _parse_window(self, this):
        if not self._match(TokenType.OVER):
            return this

        self._match_l_paren()
        partition = None

        if self._match_by(TokenType.PARTITION):
            partition = self._parse_csv(self._parse_type)

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
            exp.Window, this=this, partition_by=partition, order=order, spec=spec
        )

    def _parse_window_spec(self):
        self._match(TokenType.BETWEEN)

        return {
            "value": (
                self._match_set((TokenType.UNBOUNDED, TokenType.CURRENT_ROW))
                and self._prev.text
            )
            or self._parse_bitwise(),
            "side": self._match_set((TokenType.PRECEDING, TokenType.FOLLOWING))
            and self._prev.text,
        }

    def _parse_alias(self, this):
        self._match(TokenType.ALIAS)

        if self._match(TokenType.L_PAREN):
            aliases = self.expression(
                exp.Aliases,
                this=this,
                expressions=self._parse_csv(self._parse_id_var),
            )
            self._match_r_paren()
            return aliases

        alias = self._parse_id_var()
        if alias:
            return self.expression(exp.Alias, this=this, alias=alias)

        return this

    def _parse_id_var(self):
        return self._parse_identifier() or (
            self._match_set(self.ID_VAR_TOKENS)
            and exp.Identifier(this=self._prev.text, quoted=False)
        )

    def _parse_string(self):
        if self._match(TokenType.STRING):
            return exp.Literal.string(self._prev.text)
        return None

    def _parse_number(self):
        if self._match(TokenType.NUMBER):
            return exp.Literal.number(self._prev.text)
        return None

    def _parse_identifier(self):
        if self._match(TokenType.IDENTIFIER):
            return exp.Identifier(this=self._prev.text, quoted=True)
        return None

    def _parse_var(self):
        if self._match(TokenType.VAR):
            return exp.Identifier(this=self._prev.text, quoted=False)
        return None

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
            return exp.Star()
        return None

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
            this = self.expression(
                expressions[self._prev.token_type], this=this, expression=parse()
            )

        return this

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

    def _match_l_paren(self):
        if not self._match(TokenType.L_PAREN):
            self.raise_error("Expecting (")

    def _match_r_paren(self):
        if not self._match(TokenType.R_PAREN):
            self.raise_error("Expecting )")

    def _match_by(self, token_type):
        if self._match(token_type):
            if not self._match(TokenType.BY):
                self.raise_error("Expecting BY")
            return True
        return False
