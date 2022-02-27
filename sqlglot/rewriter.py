from copy import deepcopy

import sqlglot.expressions as exp
from sqlglot import parse_one


class chainable:
    def __init__(self, func):
        self.func = func

    def __set_name__(self, owner, name):
        def wrapper(rewriter, *args, **kwargs):
            expression = self.func(rewriter, *args, **kwargs)
            return Rewriter(expression, rewriter.copy)

        setattr(owner, name, wrapper)


class Rewriter:
    def __init__(self, expression, copy=True):
        self.copy = copy
        self.expression = deepcopy(expression) if copy else expression

    @chainable
    def ctas(self, table, db=None, **properties):
        if self.expression.find(exp.Create):
            raise ValueError("Expression already a CTAS")

        return exp.Create(
            this=exp.Table(this=table, db=db),
            kind="table",
            expression=self.expression,
            properties=exp.Properties(
                expressions=[
                    exp.Property(
                        this=exp.Literal.string(k),
                        value=exp.Literal.string(v),
                    )
                    for k, v in properties.items()
                ]
            ),
        )

    @chainable
    def add_selects(self, *selects, read=None):
        select = self.expression.find(exp.Select)
        for sql in selects:
            select.args["expressions"].append(parse_one(sql, read=read))
        return self.expression
