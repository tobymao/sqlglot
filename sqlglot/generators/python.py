from __future__ import annotations

import itertools

from sqlglot import exp, generator
from sqlglot.dialects.dialect import inline_array_sql
from sqlglot.helper import subclasses


def _ordered_py(self, expression):
    this = self.sql(expression, "this")
    desc = "True" if expression.args.get("desc") else "False"
    nulls_first = "True" if expression.args.get("nulls_first") else "False"
    return f"ORDERED({this}, {desc}, {nulls_first})"


def _rename(self, e):
    try:
        values = list(e.args.values())

        if len(values) == 1:
            values = values[0]
            if not isinstance(values, list):
                return self.func(e.key, values)
            return self.func(e.key, *values)

        if isinstance(e, exp.Func) and e.is_var_len_args:
            args = itertools.chain.from_iterable(x if isinstance(x, list) else [x] for x in values)
            return self.func(e.key, *args)

        return self.func(e.key, *values)
    except Exception as ex:
        raise Exception(f"Could not rename {repr(e)}") from ex


def _case_sql(self, expression):
    this = self.sql(expression, "this")
    chain = self.sql(expression, "default") or "None"

    for e in reversed(expression.args["ifs"]):
        true = self.sql(e, "true")
        condition = self.sql(e, "this")
        condition = f"{this} = ({condition})" if this else condition
        chain = f"{true} if {condition} else ({chain})"

    return chain


def _lambda_sql(self, e: exp.Lambda) -> str:
    names = {e.name.lower() for e in e.expressions}

    e = e.transform(
        lambda n: (
            exp.var(n.name) if isinstance(n, exp.Identifier) and n.name.lower() in names else n
        )
    ).assert_is(exp.Lambda)

    return f"lambda {self.expressions(e, flat=True)}: {self.sql(e, 'this')}"


def _like_sql(self: generator.Generator, e: exp.Like | exp.ILike) -> str:
    sql = self.func(e.key, e.this, e.expression)

    if e.args.get("negate"):
        sql = f"NOT({sql})"

    return sql


def _div_sql(self: generator.Generator, e: exp.Div) -> str:
    denominator = self.sql(e, "expression")

    if e.args.get("safe"):
        denominator += " or None"

    sql = f"DIV({self.sql(e, 'this')}, {denominator})"

    if e.args.get("typed") and not (
        e.this.is_type(*exp.DataType.REAL_TYPES) or e.expression.is_type(*exp.DataType.REAL_TYPES)
    ):
        sql = f"int({sql})"

    return sql


def _dpipe_sql(self: generator.Generator, e: exp.DPipe) -> str:
    if e.this.is_type(exp.DataType.Type.ARRAY) or e.expression.is_type(exp.DataType.Type.ARRAY):
        return self.func("ARRAYCONCAT", e.this, e.expression)

    return self.func("SAFECONCAT" if e.args.get("safe") else "CONCAT", e.this, e.expression)


class PythonGenerator(generator.Generator):
    TRANSFORMS = {
        **{klass: _rename for klass in subclasses(exp.__name__, exp.Binary)},
        **{klass: _rename for klass in exp.ALL_FUNCTIONS},
        exp.Case: _case_sql,
        exp.Alias: lambda self, e: self.sql(e.this),
        exp.Array: inline_array_sql,
        exp.And: lambda self, e: f"AND(lambda: {self.sql(e.left)}, lambda: {self.sql(e.right)})",
        exp.Between: _rename,
        exp.Boolean: lambda self, e: "True" if e.this else "False",
        exp.Cast: lambda self, e: f"CAST({self.sql(e.this)}, exp.DType.{e.args['to']})",
        exp.Column: lambda self, e: f"scope[{self.sql(e, 'table') or None}][{self.sql(e.this)}]",
        exp.Concat: lambda self, e: self.func(
            "SAFECONCAT" if e.args.get("safe") else "CONCAT", *e.expressions
        ),
        exp.Distinct: lambda self, e: f"set({self.sql(e, 'this')})",
        exp.Div: _div_sql,
        exp.DPipe: _dpipe_sql,
        exp.Extract: lambda self, e: f"EXTRACT('{e.name.lower()}', {self.sql(e, 'expression')})",
        exp.ILike: _like_sql,
        exp.In: lambda self, e: self.func("IN", e.this, *e.expressions),
        exp.Interval: lambda self, e: f"INTERVAL({self.sql(e.this)}, '{self.sql(e.unit)}')",
        exp.Is: lambda self, e: (
            self.binary(e, "!=" if e.args.get("negate") else "==")
            if isinstance(e.this, exp.Literal)
            else self.binary(e, "is not" if e.args.get("negate") else "is")
        ),
        exp.JSONExtract: lambda self, e: self.func(e.key, e.this, e.expression, *e.expressions),
        exp.JSONPath: lambda self, e: f"[{','.join(self.sql(p) for p in e.expressions[1:])}]",
        exp.JSONPathKey: lambda self, e: f"'{self.sql(e.this)}'",
        exp.JSONPathSubscript: lambda self, e: f"'{e.this}'",
        exp.Lambda: _lambda_sql,
        exp.Like: _like_sql,
        exp.Not: lambda self, e: self.func("NOT", e.this),
        exp.Null: lambda *_: "None",
        exp.Or: lambda self, e: f"OR(lambda: {self.sql(e.left)}, lambda: {self.sql(e.right)})",
        exp.Ordered: _ordered_py,
        exp.Star: lambda *_: "1",
    }
