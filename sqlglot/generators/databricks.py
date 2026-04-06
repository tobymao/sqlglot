from __future__ import annotations


from sqlglot import exp, transforms
from sqlglot.dialects.dialect import (
    date_delta_sql,
    timestamptrunc_sql,
    groupconcat_sql,
)
from sqlglot.generators.spark import SparkGenerator


def _jsonextract_sql(
    self: DatabricksGenerator, expression: exp.JSONExtract | exp.JSONExtractScalar
) -> str:
    this = self.sql(expression, "this")
    expr = self.sql(expression, "expression")
    return f"{this}:{expr}"


class DatabricksGenerator(SparkGenerator):
    TABLESAMPLE_SEED_KEYWORD = "REPEATABLE"
    COPY_PARAMS_ARE_WRAPPED = False
    COPY_PARAMS_EQ_REQUIRED = True
    JSON_PATH_SINGLE_QUOTE_ESCAPE = False
    SAFE_JSON_PATH_KEY_RE = exp.SAFE_IDENTIFIER_RE
    QUOTE_JSON_PATH = False
    PARSE_JSON_NAME: str | None = "PARSE_JSON"

    TRANSFORMS = {
        k: v
        for k, v in {
            **SparkGenerator.TRANSFORMS,
            exp.CurrentVersion: lambda *_: "CURRENT_VERSION()",
            exp.DateAdd: date_delta_sql("DATEADD"),
            exp.DateDiff: date_delta_sql("DATEDIFF"),
            exp.DatetimeAdd: lambda self, e: self.func(
                "TIMESTAMPADD", e.unit, e.expression, e.this
            ),
            exp.DatetimeSub: lambda self, e: self.func(
                "TIMESTAMPADD",
                e.unit,
                exp.Mul(this=e.expression, expression=exp.Literal.number(-1)),
                e.this,
            ),
            exp.DatetimeTrunc: timestamptrunc_sql(),
            exp.GroupConcat: groupconcat_sql,
            exp.Select: transforms.preprocess(
                [
                    transforms.eliminate_distinct_on,
                    transforms.unnest_to_explode,
                    transforms.any_to_exists,
                ]
            ),
            exp.JSONExtract: _jsonextract_sql,
            exp.JSONExtractScalar: _jsonextract_sql,
            exp.JSONPathRoot: lambda *_: "",
            exp.ToChar: lambda self, e: (
                self.cast_sql(exp.Cast(this=e.this, to=exp.DataType(this="STRING")))
                if e.args.get("is_numeric")
                else self.function_fallback_sql(e)
            ),
            exp.CurrentCatalog: lambda *_: "CURRENT_CATALOG()",
            exp.RegexpLike: None,
            exp.TryCast: None,
        }.items()
        if v is not None
    }

    TYPE_MAPPING = {
        **SparkGenerator.TYPE_MAPPING,
        exp.DType.NULL: "VOID",
    }

    def create_sql(self, expression: exp.Create) -> str:
        body = expression.expression
        if (
            body
            and not isinstance(body, exp.Return)
            and expression.kind == "FUNCTION"
            and any(p.args.get("is_table") for p in expression.find_all(exp.ReturnsProperty))
        ):
            expression.set("expression", exp.Return(this=body))
        return super().create_sql(expression)

    def columndef_sql(self, expression: exp.ColumnDef, sep: str = " ") -> str:
        constraint = expression.find(exp.GeneratedAsIdentityColumnConstraint)
        kind = expression.kind
        if (
            constraint
            and isinstance(kind, exp.DataType)
            and kind.this in exp.DataType.INTEGER_TYPES
        ):
            # only BIGINT generated identity constraints are supported
            expression.set("kind", exp.DType.BIGINT.into_expr())

        return super().columndef_sql(expression, sep)

    def jsonpath_sql(self, expression: exp.JSONPath) -> str:
        expression.set("escape", None)
        return super().jsonpath_sql(expression)

    def uniform_sql(self, expression: exp.Uniform) -> str:
        gen = expression.args.get("gen")
        seed = expression.args.get("seed")

        # From Snowflake UNIFORM(min, max, gen) as RANDOM(), RANDOM(seed), or constant value -> Extract seed
        if gen:
            seed = gen.this

        return self.func("UNIFORM", expression.this, expression.expression, seed)
