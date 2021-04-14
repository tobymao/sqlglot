from copy import deepcopy

import sqlglot.expressions as exp
from sqlglot import parse


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
    def ctas(self, table, db=None, file_format=None):
        create = self.expression.find(exp.Create)

        if create:
            create.args['db'] = db
            create.args['this'] = table
            create.args['file_format'] = exp.FileFormat(this=file_format)
        else:
            create = exp.Create(
                this=exp.Table(this=table, db=db),
                kind='table',
                expression=self.expression,
                file_format = exp.FileFormat(this=file_format),
            )

        return create

    @chainable
    def add_selects(self, *selects, read=None):
        select = self.expression.find(exp.Select)
        for sql in selects:
            select.args['expressions'].append(parse(sql, read=read)[0])
        return self.expression
