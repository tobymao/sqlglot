from functools import wraps

import sqlglot.expressions as exp


def builder(func):
    @wraps(func)
    def builder_method(self, *args, **kwargs):
        query = Query()
        query.node = self.node.copy()

        rv = func(query, *args, **kwargs)
        if rv is not None:
            return rv
        return query

    return builder_method


class Query:
    def __init__(self):
        self.node = exp.Select()

    def to_node(self, obj, spec=None, prefix=None):
        if isinstance(obj, exp.Expression):
            return obj
        if isinstance(obj, Query):
            return sql.node

        return sqlglot.parse_one(
            f"{prefix} {obj}" if prefix else str(obj),
            spec=spec,
        )

    @builder
    def select(self, *selects):
        self.node.args["expressions"].extend(self.to_node(select) for select in selects)

    @builder
    def from_(self, obj):
        self.node.args["from"] = self._parse_from(obj)

    @builder
    def group_by(self, expression):
        self.node.args["group"] = self.to_node(
            expression, spec=lambda parser: parser._parse_group, prefix="GROUP BY"
        )

    def _parse_from(self, obj):
        return self.to_node(obj, lambda parser: parser._parse_from, prefix="FROM")

    def sql(self, **kwargs):
        return self.node.sql(**kwargs)


class from_(Query):
    def __init__(self, obj):
        self.node = exp.Select(
            **{
                "from": self._parse_from(obj),
                "expressions": [],
                "joins": [],
            }
        )


class select(Query):
    def __init__(self, *selects):
        self.node = exp.Select(
            **{
                "expressions": [self.to_node(select) for select in selects],
                "joins": [],
            }
        )
