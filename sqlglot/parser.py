from sqlglot.tokens import TokenType
import sqlglot.expressions as exp

class Parser:
    FUNCTIONS = {
        'AVG': lambda args: exp.Avg(this=args[0]),
        'COALESCE': lambda args: exp.Coalesce(expressions=args),
        'FIRST': lambda args: exp.First(this=args[0]),
        'LAST': lambda args: exp.Last(this=args[0]),
        'IF': lambda args: exp.If(condition=args[0], true=args[1], false=args[2]),
        'LN': lambda args: exp.LN(this=args[0]),
        'MAX': lambda args: exp.Max(this=args[0]),
        'MIN': lambda args: exp.Min(this=args[0]),
        'SUM': lambda args: exp.Sum(this=args[0]),
    }

    TYPE_TOKENS = {
        TokenType.BOOLEAN,
        TokenType.TINYINT,
        TokenType.SMALLINT,
        TokenType.INT,
        TokenType.BIGINT,
        TokenType.REAL,
        TokenType.DOUBLE,
        TokenType.DECIMAL,
        TokenType.CHAR,
        TokenType.VARCHAR,
        TokenType.TEXT,
        TokenType.BINARY,
        TokenType.JSON,
    }

    def __init__(self, tokens, **kwargs):
        self.raw_tokens = tokens
        self.functions = {**self.FUNCTIONS, **kwargs.get('functions', {})}
        self._tokens = []
        self._chunks = [[]]
        self._index = 0

        for token in tokens:
            if token.token_type == TokenType.SEMICOLON:
                self._chunks.append([])
            self._chunks[-1].append(token)

    def parse(self):
        expressions = []
        for tokens in self._chunks:
            self._reset()
            self._tokens = tokens
            expressions.append(self._parse_statement())
            if self._index < len(self._tokens):
                raise ValueError(f"Invalid expression {self._curr}")
        return expressions

    def _reset(self):
        self._index = -1
        self._advance()

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
        return self._parse_select()

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

        return this

    def _parse_from(self, this):
        if not self._match(TokenType.FROM):
            return this

        return exp.From(this=self._parse_table(), expression=this)

    def _parse_join(self, this):
        joiner = None

        if self._match(TokenType.INNER, TokenType.LEFT, TokenType.RIGHT, TokenType.CROSS):
            joiner = self._prev

        if self._match(TokenType.JOIN):
            on = None
            expression = self._parse_table()

            if self._match(TokenType.ON):
                on = self._parse_expression()

            return exp.Join(this=expression, expression=this, joiner=joiner, on=on)

        return this

    def _parse_table(self):
        if self._match(TokenType.L_PAREN):
            nested = self._parse_select()

            if not self._match(TokenType.R_PAREN):
                raise ValueError('Expecting )')
            expression = nested
        else:
            db = None
            table = None

            if self._match(TokenType.VAR, TokenType.IDENTIFIER):
                table = self._prev

            if self._match(TokenType.DOT):
                db = table
                if not self._match(TokenType.VAR, TokenType.IDENTIFIER):
                    raise ValueError('Expected table name')
                table = self._prev

            expression = exp.Table(this=table, db=db)

        return self._parse_alias(expression)

    def _parse_where(self, this):
        if not self._match(TokenType.WHERE):
            return this
        return exp.Where(this=this, expression=self._parse_union())

    def _parse_group(self, this):
        if not self._match(TokenType.GROUP):
            return this

        if not self._match(TokenType.BY):
            raise ValueError('Expecting BY')

        return exp.Group(this=this, expressions=self._parse_csv(self._parse_primary))

    def _parse_having(self, this):
        if not self._match(TokenType.HAVING):
            return this
        return exp.Having(this=this, expression=self._parse_union())

    def _parse_order(self, this):
        if not self._match(TokenType.ORDER):
            return this

        if not self._match(TokenType.BY):
            raise ValueError('Expecting BY')

        return exp.Order(this=this, expressions=self._parse_csv(self._parse_primary), desc=self._match(TokenType.DESC))

    def _parse_expression(self):
        return self._parse_alias(self._parse_union())

    def _parse_union(self):
        return self._parse_tokens(self._parse_equality, exp.And, exp.Or)

    def _parse_equality(self):
        return self._parse_tokens(self._parse_comparison, exp.EQ, exp.NEQ, exp.Is)

    def _parse_comparison(self):
        return self._parse_tokens(self._parse_range, exp.GT, exp.GTE, exp.LT, exp.LTE)

    def _parse_range(self):
        this = self._parse_term()

        if self._match(TokenType.IN):
            if not self._match(TokenType.L_PAREN):
                raise ValueError('Expected ( after IN')
            expressions = self._parse_csv(self._parse_primary)
            if not self._match(TokenType.R_PAREN):
                raise ValueError('Expected ) after IN')
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
            raise ValueError('Expected END after CASE')

        return exp.Case(ifs=ifs, default=default)

    def _parse_count(self):
        if not self._match(TokenType.L_PAREN):
            raise ValueError("Expected ( after COUNT")

        distinct = self._match(TokenType.DISTINCT)
        this = self._parse_union()

        if not self._match(TokenType.R_PAREN):
            raise ValueError("Expected ) after COUNT")

        return exp.Count(this=this, distinct=distinct)

    def _parse_cast(self):
        if not self._match(TokenType.L_PAREN):
            raise ValueError("Expected ( after CAST")

        this = self._parse_union()

        if not self._match(TokenType.ALIAS):
            raise ValueError("Expected AS after CAST")

        if not self._match(*self.TYPE_TOKENS):
            raise ValueError("Expected type after CAST")

        to = self._prev

        if not self._match(TokenType.R_PAREN):
            raise ValueError("Expected ) after CAST")

        return exp.Cast(this=this, to=to)

    def _parse_type(self):
        if not self._match(TokenType.L_PAREN):
            raise ValueError("Expected ( after CAST")

        this = self._parse_primary()

        if not self._match(TokenType.ALIAS):
            raise ValueError("Expected AS after CAST")

        expression = self._parse_primary()

        if not self._match(TokenType.R_PAREN):
            raise ValueError("Expected ) after CAST")

        return exp.Cast(this=this, expression=expression)

    def _parse_primary(self):
        this = self._curr

        if self._match(TokenType.STRING, TokenType.NUMBER, TokenType.STAR, TokenType.NULL):
            return this

        if self._match(TokenType.VAR) or self._match(TokenType.IDENTIFIER):
            if not self._match(TokenType.L_PAREN):
                db = None
                table = None
                this = self._prev

                if self._match(TokenType.DOT):
                    table = this
                    if not self._match(TokenType.VAR, TokenType.IDENTIFIER):
                        raise ValueError('Expected column name')
                    this = self._prev

                    if self._match(TokenType.DOT):
                        db = table
                        table = this
                        if not self._match(TokenType.VAR, TokenType.IDENTIFIER):
                            raise ValueError('Expected column name')
                        this = self._prev

                return exp.Column(this=this, db=db, table=table)

            if this.token_type == TokenType.IDENTIFIER:
                raise ValueError('Unexpected (')

            function = self.functions.get(this.text.upper())
            if not function:
                raise ValueError(f"Unrecognized function name {this}")
            function = function(self._parse_csv(self._parse_expression))
            if not self._match(TokenType.R_PAREN):
                raise ValueError(f"Expected ) after function {this}")
            return function

        if self._match(TokenType.L_PAREN):
            this = self._parse_expression()

            if not self._match(TokenType.R_PAREN):
                raise ValueError('Expecting )')
            return exp.Paren(this=this)

        return None

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

    def _parse_function_args(self, parse):
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
