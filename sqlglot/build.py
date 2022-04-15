from sqlglot import expressions as exp


def select(*codes, dialect=None, parser_opts=None):
    expression = exp.Select()
    for code in codes:
        expression = expression.select(code, dialect=dialect, parser_opts=parser_opts)
    return expression
