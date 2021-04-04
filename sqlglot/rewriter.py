from copy import deepcopy

import sqlglot.expressions as exp


class Rewriter:
    def __init__(self, expression, copy=True):
        self.copy = copy
        self.expression = deepcopy(expression) if copy else expression

    def ctas(self, table, db=None, file_format=None):
        create = self.expression.find(exp.Create)

        if create:
            create = create[0]
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

        return Rewriter(create, self.copy)
