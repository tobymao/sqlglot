from copy import deepcopy

import sqlglot.expressions as exp


class Rewriter:
    def __init__(self, expression, copy=True):
        self.copy = copy
        self.expression = deepcopy(expression) if copy else expression

    def ctas(self, table, db=None, file_format=None):
        create = self.expression.find(exp.Create)

        if create:
            create.args['db'] = db
            create.args['table'] = table
            create.args['file_format'] = file_format
        else:
            create = exp.Create(
                this=self.expression,
                table=exp.Table(this=table, db=db),
                file_format = file_format,
            )

        return Rewriter(create, self.copy)
