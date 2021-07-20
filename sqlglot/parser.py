import logging
import os

from sqlglot.errors import ErrorLevel, ParseError
from sqlglot.helper import list_get
from sqlglot.tokens import Token, TokenType
import sqlglot.expressions as exp

os.system('')


class Parser:
    def _parse_decimal(args):
        size = len(args)
        precision = args[0] if size > 0 else None
        scale = args[1] if size > 1 else None
        return exp.Decimal(precision=precision, scale=scale)

    FUNCTIONS = {
        'DECIMAL': _parse_decimal,
        'NUMERIC': _parse_decimal,
        'ARRAY': lambda args: exp.Array(expressions=args),
        'COLLECT_LIST': lambda args: exp.ArrayAgg(this=args[0]),
        'ARRAY_AGG': lambda args: exp.ArrayAgg(this=args[0]),
        'ARRAY_CONTAINS': lambda args: exp.ArrayContains(this=args[0], expression=args[1]),
        'ARRAY_SIZE': lambda args: exp.ArraySize(this=args[0]),
        'DATE_ADD': lambda args: exp.DateAdd(this=args[0], expression=args[1]),
        'DATE_DIFF': lambda args: exp.DateDiff(this=args[0], expression=args[1]),
        'DATE_STR_TO_DATE': lambda args: exp.DateStrToDate(this=args[0]),
        'IF': lambda args: exp.If(this=args[0], true=args[1], false=list_get(args, 2)),
        'INITCAP': lambda args: exp.Initcap(this=args[0]),
        'JSON_PATH': lambda args: exp.JSONPath(this=args[0], path=args[1]),
        'STR_POSITION': lambda args: exp.StrPosition(this=args[0], substr=args[1], position=list_get(args, 2)),
        'STR_TO_TIME': lambda args: exp.StrToTime(this=args[0], format=args[1]),
        'STR_TO_UNIX': lambda args: exp.StrToUnix(this=args[0], format=args[1]),
        'TIME_STR_TO_DATE': lambda args: exp.TimeStrToDate(this=args[0]),
        'TIME_STR_TO_TIME': lambda args: exp.TimeStrToTime(this=args[0]),
        'TIME_STR_TO_UNIX': lambda args: exp.TimeStrToUnix(this=args[0]),
        'TIME_TO_STR': lambda args: exp.TimeToStr(this=args[0], format=args[1]),
        'TIME_TO_TIME_STR': lambda args: exp.TimeToTimeStr(this=args[0]),
        'TIME_TO_UNIX': lambda args: exp.TimeToUnix(this=args[0]),
        'TS_OR_DS_TO_DATE_STR': lambda args: exp.TsOrDsToDateStr(this=args[0]),
        'UNIX_TO_STR': lambda args: exp.UnixToStr(this=args[0], format=args[1]),
        'UNIX_TO_TIME': lambda args: exp.UnixToTime(this=args[0]),
        'UNIX_TO_TIME_STR': lambda args: exp.UnixToTimeStr(this=args[0]),
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
        TokenType.DATE,
        TokenType.ARRAY,
        TokenType.DECIMAL,
        TokenType.MAP,
    }

    ID_VAR_TOKENS = {
        TokenType.IDENTIFIER,
        TokenType.VAR,
        TokenType.ALL,
        TokenType.ASC,
        TokenType.COUNT,
        TokenType.DESC,
        TokenType.FOLLOWING,
        TokenType.FORMAT,
        TokenType.IF,
        TokenType.INTERVAL,
        TokenType.ORDINALITY,
        TokenType.OVER,
        TokenType.PRECEDING,
        TokenType.RANGE,
        TokenType.ROWS,
        TokenType.UNBOUNDED,
        *TYPE_TOKENS,
    }

    PRIMARY_TOKENS = {
        TokenType.STRING,
        TokenType.NUMBER,
        TokenType.STAR,
        TokenType.NULL,
    }

    COLUMN_TOKENS = {
        TokenType.VAR,
        TokenType.IDENTIFIER,
        TokenType.STAR,
    }

    def __init__(self, **opts):
        self.functions = {**self.FUNCTIONS, **(opts.get('functions') or {})}
        self.error_level = opts.get('error_level', ErrorLevel.RAISE)
        self.error_message_context = opts.get('error_message_context', 50)
        self.reset()

    def reset(self):
        self.error = None
        self._tokens = []
        self._chunks = [[]]
        self._index = 0

    def parse(self, raw_tokens, code=None):
        self.code = code or ''
        self.reset()

        for token in raw_tokens:
            if token.token_type == TokenType.SEMICOLON:
                self._chunks.append([])
            else:
                self._chunks[-1].append(token)

        expressions = []

        for tokens in self._chunks:
            self._index = -1
            self._advance()
            self._tokens = tokens

            try:
                expressions.append(self._parse_statement())
                if self._index < len(self._tokens):
                    self.raise_error('Invalid expression / Unexpected token')
            except ParseError as e:
                if self.error_level == ErrorLevel.WARN:
                    logging.error(e)
                elif self.error_level == ErrorLevel.RAISE:
                    raise e

        for expression in expressions:
            if not isinstance(expression, exp.Expression):
                continue
            for node, parent, key in expression.walk():
                if hasattr(node, 'parent') and parent:
                    node.parent = parent
                    node.arg_key = key

        return expressions

    def raise_error(self, message, token=None):
        token = token or self._curr or self._prev
        start = self._find_token(token, self.code)
        end = start + len(token.text)
        start_context = self.code[max(start - self.error_message_context, 0):start]
        highlight = self.code[start:end]
        end_context = self.code[end:end + self.error_message_context]
        self.error = ParseError(f"{message}\n  {start_context}\033[4m{highlight}\033[0m{end_context}")
        raise self.error

    def _find_token(self, token, code):
        line = 0
        col = 0
        index = 0

        while line < token.line or col < token.col:
            if code[index] == '\n':
                line += 1
                col = 0
            else:
                col += 1
            index += 1

        return index

    def _advance(self):
        self._index += 1

    @property
    def _prev(self):
        return self._safe_get(self._index - 1)

    @property
    def _curr(self):
        return self._safe_get(self._index)

    @property
    def _next(self):
        return self._safe_get(self._index + 1)

    def _safe_get(self, index):
        try:
            return self._tokens[index]
        except IndexError:
            return None

    def _parse_statement(self):
        if self._curr is None:
            return None

        if self._match(TokenType.CREATE):
            return self._parse_create()

        if self._match(TokenType.DROP):
            return self._parse_drop()

        cte = self._parse_cte()

        if cte:
            return cte

        return self._parse_expression()

    def _parse_drop(self):
        if self._match(TokenType.TABLE):
            kind = 'table'
        elif self._match(TokenType.VIEW):
            kind = 'view'
        else:
            self.raise_error('Expected TABLE or View')

        this = self._parse_table()

        if self._match(TokenType.IF):
            self._match(TokenType.EXISTS)
            exists = True
        else:
            exists = False

        return exp.Drop(this=this, exists=exists, kind=kind)

    def _parse_create(self):
        if not self._match(TokenType.TABLE, TokenType.VIEW):
            self.raise_error('Expected TABLE or View')

        create_token = self._prev

        if self._match(TokenType.IF):
            self._match(TokenType.NOT)
            self._match(TokenType.EXISTS)
            exists = True
        else:
            exists = False

        this = self._parse_table()

        if create_token.token_type == TokenType.TABLE:
            if self._match(TokenType.STORED):
                self._match(TokenType.ALIAS)
                file_format = exp.FileFormat(this=self._parse_id_var())
            elif self._match(TokenType.WITH):
                self._match(TokenType.L_PAREN)
                self._match(TokenType.FORMAT)
                self._match(TokenType.EQ)
                file_format = exp.FileFormat(this=self._parse_primary())
                if not self._match(TokenType.R_PAREN):
                    self.raise_error('Expected ) after format')
            else:
                file_format = None

            self._match(TokenType.ALIAS)

            return exp.Create(
                this=this,
                kind='table',
                expression=self._parse_select(),
                exists=exists,
                file_format=file_format,
            )

        if create_token.token_type == TokenType.VIEW:
            self._match(TokenType.ALIAS)

            return exp.Create(
                this=this,
                kind='view',
                expression=self._parse_select(),
                exists=exists,
            )
        return None

    def _parse_cte(self):
        if not self._match(TokenType.WITH):
            return self._parse_select()

        expressions = []

        while True:
            if not self._match(TokenType.IDENTIFIER, TokenType.VAR):
                self.raise_error('Expected alias after WITH')

            alias = self._prev

            if not self._match(TokenType.ALIAS):
                self.raise_error('Expected AS after WITH')

            expressions.append(self._parse_table(alias=alias))

            if not self._match(TokenType.COMMA):
                break

        return exp.CTE(this=self._parse_select(), expressions=expressions)

    def _parse_select(self):
        if not self._match(TokenType.SELECT):
            return None

        this = exp.Select(
            hint=self._parse_hint(),
            distinct=self._match(TokenType.DISTINCT),
            expressions=self._parse_csv(self._parse_expression),
            **{
                'from': self._parse_from(),
                'laterals': self._parse_laterals(),
                'joins': self._parse_joins(),
                'where': self._parse_where(),
                'group': self._parse_group(),
                'having': self._parse_having(),
                'order': self._parse_order(),
                'limit': self._parse_limit(),
            },
        )

        return self._parse_union(this)

    def _parse_hint(self):
        if self._match(TokenType.HINT):
            hint = self._parse_primary()
            if not self._match(TokenType.COMMENT_END):
                self.raise_error('Expected */ after HINT')
            return exp.Hint(this=hint)
        return None

    def _parse_from(self):
        if not self._match(TokenType.FROM):
            return None

        return exp.From(this=self._parse_table())

    def _parse_laterals(self):
        laterals = []

        while True:
            if not self._match(TokenType.LATERAL):
                return laterals

            if not self._match(TokenType.VIEW):
                self.raise_error('Expected VIEW afteral LATERAL')

            outer = self._match(TokenType.OUTER)
            this = self._parse_primary()
            table = self._parse_id_var()

            if self._match(TokenType.ALIAS):
                columns = self._parse_csv(self._parse_id_var)

            laterals.append(exp.Lateral(
                this=this,
                outer=outer,
                table=table,
                columns=columns,
            ))

    def _parse_joins(self):
        joins = []

        while True:
            side = self._match(TokenType.LEFT, TokenType.RIGHT, TokenType.FULL)
            kind = self._match(TokenType.INNER, TokenType.OUTER, TokenType.CROSS)

            if not self._match(TokenType.JOIN):
                return joins

            joins.append(exp.Join(
                this=self._parse_table(),
                side=side,
                kind=kind,
                on=self._parse_expression() if self._match(TokenType.ON) else None,
            ))

    def _parse_table(self, alias=None):
        unnest = self._parse_unnest()

        if unnest:
            return unnest

        if self._match(TokenType.L_PAREN):
            expression = self._parse_cte()

            if not self._match(TokenType.R_PAREN):
                self.raise_error('Expecting )')
        else:
            db = None
            table = self._match(TokenType.VAR, TokenType.IDENTIFIER)

            if self._match(TokenType.DOT):
                db = table
                if not self._match(TokenType.VAR, TokenType.IDENTIFIER):
                    self.raise_error('Expected table name')
                table = self._prev

            expression = exp.Table(this=table, db=db)

        if alias:
            this = exp.Alias(this=expression, alias=alias)
        else:
            this = self._parse_alias(expression)

        # some dialects allow not having an alias after a nested sql
        if this.token_type != TokenType.ALIAS:
            this = exp.Alias(this=this, alias=None)

        return this

    def _parse_unnest(self):
        if not self._match(TokenType.UNNEST):
            return None

        if not self._match(TokenType.L_PAREN):
            self.raise_error('Expecting ( after unnest')

        expressions = self._parse_csv(self._parse_id_var)

        if not self._match(TokenType.R_PAREN):
            self.raise_error('Expecting )')

        ordinality = self._match(TokenType.WITH) and self._match(TokenType.ORDINALITY)
        self._match(TokenType.ALIAS)
        table = self._parse_id_var()

        if not self._match(TokenType.L_PAREN):
            return exp.Unnest(expressions=expressions, ordinality=ordinality, table=table)

        columns = self._parse_csv(self._parse_id_var)
        unnest = exp.Unnest(expressions=expressions, ordinality=ordinality, table=table, columns=columns)

        if not self._match(TokenType.R_PAREN):
            self.raise_error('Expecting )')

        return unnest

    def _parse_where(self):
        if not self._match(TokenType.WHERE):
            return None
        return exp.Where(this=self._parse_conjunction())

    def _parse_group(self):
        if not self._match(TokenType.GROUP):
            return None

        return exp.Group(expressions=self._parse_csv(self._parse_primary))

    def _parse_having(self):
        if not self._match(TokenType.HAVING):
            return None
        return exp.Having(this=self._parse_conjunction())

    def _parse_order(self):
        if not self._match(TokenType.ORDER):
            return None

        return exp.Order(expressions=self._parse_csv(self._parse_primary), desc=self._match(TokenType.DESC))

    def _parse_limit(self):
        if not self._match(TokenType.LIMIT):
            return None

        return exp.Limit(this=self._match(TokenType.NUMBER))

    def _parse_union(self, this):
        if not self._match(TokenType.UNION):
            return this

        distinct = not self._match(TokenType.ALL)

        return exp.Union(this=this, expression=self._parse_select(), distinct=distinct)

    def _parse_expression(self):
        return self._parse_alias(self._parse_window(self._parse_conjunction()))

    def _parse_conjunction(self):
        return self._parse_tokens(self._parse_equality, exp.And, exp.Or)

    def _parse_equality(self):
        return self._parse_tokens(self._parse_comparison, exp.EQ, exp.NEQ, exp.Is)

    def _parse_comparison(self):
        return self._parse_tokens(self._parse_range, exp.GT, exp.GTE, exp.LT, exp.LTE)

    def _parse_range(self):
        this = self._parse_bitwise()

        negate = self._match(TokenType.NOT)

        if self._match(TokenType.LIKE):
            this = exp.Like(this=this, expression=self._parse_term())
        elif self._match(TokenType.RLIKE):
            this = exp.RegexLike(this=this, expression=self._parse_term())
        elif self._match(TokenType.IN):
            if not self._match(TokenType.L_PAREN):
                self.raise_error('Expected ( after IN', self._prev)
            expressions = self._parse_csv(self._parse_term)
            if not self._match(TokenType.R_PAREN):
                self.raise_error('Expected ) after IN')
            this = exp.In(this=this, expressions=expressions)
        elif self._match(TokenType.BETWEEN):
            low = self._parse_term()
            self._match(TokenType.AND)
            high = self._parse_term()
            this = exp.Between(this=this, low=low, high=high)

        if negate:
            this = exp.Not(this=this)

        return this

    def _parse_bitwise(self):
        return self._parse_tokens(
            self._parse_term,
            exp.BitwiseLeftShift,
            exp.BitwiseRightShift,
            exp.BitwiseAnd,
            exp.BitwiseXor,
            exp.BitwiseOr,
        )

    def _parse_term(self):
        return self._parse_tokens(self._parse_factor, exp.Minus, exp.Plus, exp.Mod)

    def _parse_factor(self):
        return self._parse_tokens(self._parse_unary, exp.Slash, exp.Star)

    def _parse_unary(self):
        if self._match(TokenType.NOT):
            return exp.Not(this=self._parse_unary())
        if self._match(TokenType.TILDA):
            return exp.BitwiseNot(this=self._parse_unary())
        if self._match(TokenType.DASH):
            return exp.Neg(this=self._parse_unary())
        return self._parse_special()

    def _parse_special(self):
        if self._match(TokenType.CAST):
            return self._parse_cast()
        if self._match(TokenType.CASE):
            return self._parse_case()
        if self._match(TokenType.COUNT):
            return self._parse_count()
        return self._parse_primary()

    def _parse_case(self):
        ifs = []
        default = None

        while self._match(TokenType.WHEN):
            this = self._parse_expression()
            self._match(TokenType.THEN)
            then = self._parse_expression()
            ifs.append(exp.If(this=this, true=then))

        if self._match(TokenType.ELSE):
            default = self._parse_expression()

        if not self._match(TokenType.END):
            self.raise_error('Expected END after CASE', self._prev)

        return exp.Case(ifs=ifs, default=default)

    def _parse_count(self):
        if not self._match(TokenType.L_PAREN):
            self.raise_error("Expected ( after COUNT", self._prev)

        distinct = self._match(TokenType.DISTINCT)
        this = self._parse_conjunction()

        if not self._match(TokenType.R_PAREN):
            self.raise_error("Expected ) after COUNT")

        return exp.Count(this=this, distinct=distinct)

    def _parse_cast(self):
        if not self._match(TokenType.L_PAREN):
            self.raise_error("Expected ( after CAST", self._prev)

        this = self._parse_conjunction()

        if not self._match(TokenType.ALIAS):
            self.raise_error("Expected AS after CAST")

        if not self._match(*self.TYPE_TOKENS):
            self.raise_error("Expected type after CAST")

        to = self._parse_function(self._parse_brackets(self._prev))

        if not self._match(TokenType.R_PAREN):
            self.raise_error("Expected ) after CAST")

        return exp.Cast(this=this, to=to)

    def _parse_function(self, this):
        if not self._match(TokenType.L_PAREN):
            return this

        args = self._parse_csv(self._parse_expression)
        function = self.functions.get(this.text.upper())

        if not self._match(TokenType.R_PAREN):
            self.raise_error('Expected )')

        if not callable(function):
            return exp.Anonymous(this=this, expressions=args)

        return function(args)

    def _parse_primary(self):
        if self._match(*self.PRIMARY_TOKENS):
            return self._prev

        interval = self._parse_interval()

        if interval:
            return interval

        if self._match(TokenType.L_PAREN):
            paren = self._prev
            this = self._parse_expression()

            if not self._match(TokenType.R_PAREN):
                self.raise_error('Expecting )', paren)
            return exp.Paren(this=this)

        return self._parse_column()

    def _parse_column(self):
        db = None
        table = None

        if self._curr.token_type in (TokenType.OVER, TokenType.R_PAREN, TokenType.R_BRACKET):
            return None

        self._advance()
        this = self._parse_function(self._prev)

        if self._match(TokenType.DOT):
            table = this
            if not self._match(*self.COLUMN_TOKENS):
                self.raise_error('Expected column name')
            this = self._prev

            if self._match(TokenType.DOT):
                db = table
                table = this
                if not self._match(*self.COLUMN_TOKENS):
                    self.raise_error('Expected column name')
                this = self._prev

        if not isinstance(this, exp.Func) and this.token_type not in (TokenType.ARRAY, TokenType.MAP):
            this = exp.Column(this=this, db=db, table=table)

        return self._parse_brackets(this)

    def _parse_brackets(self, this):
        if not self._match(TokenType.L_BRACKET):
            return this

        expressions = self._parse_csv(self._parse_primary)

        if isinstance(this, Token) and this.token_type == TokenType.ARRAY:
            bracket = exp.Array(expressions=expressions)
        else:
            bracket = exp.Bracket(this=this, expressions=expressions)

        if not self._match(TokenType.R_BRACKET):
            self.raise_error('Expected ]')

        return self._parse_brackets(bracket)

    def _parse_window(self, this):
        if not self._match(TokenType.OVER):
            return this

        if not self._match(TokenType.L_PAREN):
            self.raise_error('Expecting ( after OVER')

        partition = None

        if self._match(TokenType.PARTITION):
            partition = self._parse_csv(self._parse_primary)

        order = self._parse_order()

        spec = None
        kind = self._match(TokenType.ROWS, TokenType.RANGE)

        if kind:
            self._match(TokenType.BETWEEN)
            start = self._parse_window_spec()
            self._match(TokenType.AND)
            end = self._parse_window_spec()

            spec = exp.WindowSpec(
                kind=kind,
                start=start['value'],
                start_side=start['side'],
                end=end['value'],
                end_side=end['side'],
            )

        if not self._match(TokenType.R_PAREN):
            self.raise_error('Expecting )')

        return exp.Window(this=this, partition=partition, order=order, spec=spec)

    def _parse_window_spec(self):
        self._match(TokenType.BETWEEN)

        return {
            'value': self._match(TokenType.UNBOUNDED, TokenType.CURRENT_ROW) or self._parse_primary(),
            'side': self._match(TokenType.PRECEDING, TokenType.FOLLOWING),
        }

    def _parse_alias(self, this):
        self._match(TokenType.ALIAS)

        alias = self._parse_id_var()
        if alias:
            return exp.Alias(this=this, alias=alias)

        return this

    def _parse_id_var(self):
        return self._match(*self.ID_VAR_TOKENS)

    def _parse_interval(self):
        if self._match(TokenType.INTERVAL):
            this = self._match(TokenType.STRING, TokenType.NUMBER)
            unit = self._match(TokenType.VAR)
            return exp.Interval(this=this, unit=unit)
        return None

    def _parse_csv(self, parse):
        items = [parse()]

        while self._match(TokenType.COMMA):
            items.append(parse())

        return items

    def _parse_tokens(self, parse, *expressions):
        this = parse()

        expressions = {expression.token_type: expression for expression in expressions}

        while self._match(*expressions):
            this = expressions[self._prev.token_type](this=this, expression=parse())

        return this

    def _match(self, *types):
        if not self._curr:
            return None

        for token_type in types:
            if self._curr.token_type == token_type:
                self._advance()
                return self._prev

        return None
