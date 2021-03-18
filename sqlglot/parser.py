import logging
import os

from sqlglot.errors import ErrorLevel, ParseError
from sqlglot.tokens import TokenType
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
        'IF': lambda args: exp.If(condition=args[0], true=args[1], false=args[2] if len(args) > 2 else None),
    }

    NESTED_TYPE_TOKENS = {
        TokenType.ARRAY,
        TokenType.DECIMAL,
        TokenType.MAP,
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
        *NESTED_TYPE_TOKENS,
    }

    PRIMARY_TOKENS = {
        TokenType.STRING,
        TokenType.NUMBER,
        TokenType.STAR,
        TokenType.NULL,
        *(TYPE_TOKENS - NESTED_TYPE_TOKENS),
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
                    self.raise_error('Invalid expression', self._prev)
            except ParseError as e:
                if self.error_level == ErrorLevel.WARN:
                    logging.error(e)
                elif self.error_level == ErrorLevel.RAISE:
                    raise e

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
        if not self._match(TokenType.WITH):
            return self._parse_select()

        expressions = self._parse_csv(self._parse_cte)
        return exp.CTE(this=self._parse_select(), expressions=expressions)

    def _parse_cte(self):
        if not self._match(TokenType.IDENTIFIER, TokenType.VAR):
            self.raise_error('Expected alias after WITH')

        alias = self._prev

        if not self._match(TokenType.ALIAS):
            self.raise_error('Expected AS after WITH')

        return self._parse_table(alias=alias)

    def _parse_select(self):
        if not self._match(TokenType.SELECT):
            return None

        this = exp.Select(expressions=self._parse_csv(self._parse_expression))
        this = self._parse_from(this)
        this = self._parse_join(this)
        this = self._parse_where(this)
        this = self._parse_group(this)
        this = self._parse_having(this)
        this = self._parse_order(this)
        this = self._parse_union(this)

        return this

    def _parse_from(self, this):
        if not self._match(TokenType.FROM):
            return this

        return exp.From(this=self._parse_table(), expression=this)

    def _parse_join(self, this):
        side = None
        kind = None

        if self._match(TokenType.LEFT, TokenType.RIGHT, TokenType.FULL):
            side = self._prev

        if self._match(TokenType.INNER, TokenType.OUTER, TokenType.CROSS):
            kind = self._prev

        if self._match(TokenType.JOIN):
            on = None
            expression = self._parse_table()

            if self._match(TokenType.ON):
                on = self._parse_expression()

            return self._parse_join(exp.Join(this=expression, expression=this, side=side, kind=kind, on=on))

        return this

    def _parse_table(self, alias=None):
        if self._match(TokenType.L_PAREN):
            nested = self._parse_select()

            if not self._match(TokenType.R_PAREN):
                self.raise_error('Expecting )')
            expression = nested
        else:
            db = None
            table = None

            if self._match(TokenType.VAR, TokenType.IDENTIFIER):
                table = self._prev

            if self._match(TokenType.DOT):
                db = table
                if not self._match(TokenType.VAR, TokenType.IDENTIFIER):
                    self.raise_error('Expected table name')
                table = self._prev

            expression = exp.Table(this=table, db=db)

        if alias:
            this = exp.Alias(this=expression, to=alias)
        else:
            this = self._parse_alias(expression)

        # some dialects allow not having an alias after a nested sql
        if this.token_type != TokenType.ALIAS:
            this = exp.Alias(this=this, to=None)

        return this

    def _parse_where(self, this):
        if not self._match(TokenType.WHERE):
            return this
        return exp.Where(this=this, expression=self._parse_conjunction())

    def _parse_group(self, this):
        if not self._match(TokenType.GROUP):
            return this

        if not self._match(TokenType.BY):
            self.raise_error('Expecting BY')

        return exp.Group(this=this, expressions=self._parse_csv(self._parse_primary))

    def _parse_having(self, this):
        if not self._match(TokenType.HAVING):
            return this
        return exp.Having(this=this, expression=self._parse_conjunction())

    def _parse_order(self, this):
        if not self._match(TokenType.ORDER):
            return this

        if not self._match(TokenType.BY):
            self.raise_error('Expecting BY')

        return exp.Order(this=this, expressions=self._parse_csv(self._parse_primary), desc=self._match(TokenType.DESC))

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
        this = self._parse_term()

        if self._match(TokenType.IN):
            if not self._match(TokenType.L_PAREN):
                self.raise_error('Expected ( after IN', self._prev)
            expressions = self._parse_csv(self._parse_primary)
            if not self._match(TokenType.R_PAREN):
                self.raise_error('Expected ) after IN')
            return exp.In(this=this, expressions=expressions)

        if self._match(TokenType.BETWEEN):
            low = self._parse_primary()
            self._match(TokenType.AND)
            high = self._parse_primary()
            return exp.Between(this=this, low=low, high=high)

        return this

    def _parse_term(self):
        return self._parse_tokens(self._parse_factor, exp.Minus, exp.Plus)

    def _parse_factor(self):
        return self._parse_tokens(self._parse_unary, exp.Slash, exp.Star)

    def _parse_unary(self):
        if self._match(TokenType.NOT):
            return exp.Not(this=self._parse_unary())
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
            condition = self._parse_expression()
            self._match(TokenType.THEN)
            then = self._parse_expression()
            ifs.append(exp.If(condition=condition, true=then))

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
            return exp.Func(this=this, expressions=args)

        return function(args)

    def _parse_primary(self):
        if self._match(*self.PRIMARY_TOKENS):
            return self._prev

        if self._match(*self.NESTED_TYPE_TOKENS):
            return self._parse_function(self._parse_brackets(self._prev))

        if self._match(TokenType.L_PAREN):
            paren = self._prev
            this = self._parse_expression()

            if not self._match(TokenType.R_PAREN):
                self.raise_error('Expecting )', paren)
            return exp.Paren(this=this)

        return self._parse_column()

    def _parse_column(self):
        if not self._match(TokenType.VAR, TokenType.IDENTIFIER):
            return None

        db = None
        table = None
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

        return self._parse_brackets(exp.Column(this=this, db=db, table=table))

    def _parse_brackets(self, this):
        if not self._match(TokenType.L_BRACKET):
            return this

        bracket = exp.Bracket(this=this, expressions=self._parse_csv(self._parse_primary))

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
            if not self._match(TokenType.BY):
                self.raise_error('Expecting BY after PARTITION')
            partition = self._parse_csv(self._parse_primary)

        order = self._parse_order(None)

        if not self._match(TokenType.R_PAREN):
            self.raise_error('Expecting )')

        return exp.Window(this=this, partition=partition, order=order)

    def _parse_alias(self, this):
        self._match(TokenType.ALIAS)

        if self._match(TokenType.IDENTIFIER, TokenType.VAR):
            return exp.Alias(this=this, to=self._prev)

        return this

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
            return False

        for token_type in types:
            if self._curr.token_type == token_type:
                self._advance()
                return True

        return False
