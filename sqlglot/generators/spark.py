from __future__ import annotations


from sqlglot import exp
from sqlglot import generator
from sqlglot.dialects.dialect import (
    array_append_sql,
    rename_func,
    unit_to_var,
    timestampdiff_sql,
    date_delta_to_binary_interval_op,
    groupconcat_sql,
)
from sqlglot.generators.spark2 import Spark2Generator, temporary_storage_provider
from sqlglot.helper import seq_get
from sqlglot.transforms import (
    ctas_with_tmp_tables_to_create_tmp_view,
    remove_unique_constraints,
    preprocess,
    move_partitioned_by_to_schema_columns,
)


def _normalize_partition(e: exp.Expr) -> exp.Expr:
    """Normalize the expressions in PARTITION BY (<expression>, <expression>, ...)"""
    if isinstance(e, str):
        return exp.to_identifier(e)
    if isinstance(e, exp.Literal):
        return exp.to_identifier(e.name)
    return e


def _dateadd_sql(self: SparkGenerator, expression: exp.TsOrDsAdd | exp.TimestampAdd) -> str:
    if not expression.unit or (
        isinstance(expression, exp.TsOrDsAdd) and expression.text("unit").upper() == "DAY"
    ):
        # Coming from Hive/Spark2 DATE_ADD or roundtripping the 2-arg version of Spark3/DB
        return self.func("DATE_ADD", expression.this, expression.expression)

    this = self.func(
        "DATE_ADD",
        unit_to_var(expression),
        expression.expression,
        expression.this,
    )

    if isinstance(expression, exp.TsOrDsAdd):
        # The 3 arg version of DATE_ADD produces a timestamp in Spark3/DB but possibly not
        # in other dialects
        return_type = expression.return_type
        if not return_type.is_type(exp.DType.TIMESTAMP, exp.DType.DATETIME):
            this = f"CAST({this} AS {return_type})"

    return this


def _groupconcat_sql(self: SparkGenerator, expression: exp.GroupConcat) -> str:
    if self.dialect.version < (4,):
        expr = exp.ArrayToString(
            this=exp.ArrayAgg(this=expression.this),
            expression=expression.args.get("separator") or exp.Literal.string(""),
        )
        return self.sql(expr)

    return groupconcat_sql(self, expression)


class SparkGenerator(Spark2Generator):
    SUPPORTS_TO_NUMBER = True
    PAD_FILL_PATTERN_IS_REQUIRED = False
    SUPPORTS_CONVERT_TIMEZONE = True
    SUPPORTS_MEDIAN = True
    SUPPORTS_UNIX_SECONDS = True
    SUPPORTS_DECODE_CASE = True
    SET_ASSIGNMENT_REQUIRES_VARIABLE_KEYWORD = True

    TYPE_MAPPING = {
        **Spark2Generator.TYPE_MAPPING,
        exp.DType.MONEY: "DECIMAL(15, 4)",
        exp.DType.SMALLMONEY: "DECIMAL(6, 4)",
        exp.DType.UUID: "STRING",
        exp.DType.TIMESTAMPLTZ: "TIMESTAMP_LTZ",
        exp.DType.TIMESTAMPNTZ: "TIMESTAMP_NTZ",
    }

    TRANSFORMS = {
        k: v
        for k, v in {
            **Spark2Generator.TRANSFORMS,
            exp.ArrayConstructCompact: lambda self, e: self.func(
                "ARRAY_COMPACT", self.func("ARRAY", *e.expressions)
            ),
            exp.ArrayInsert: lambda self, e: self.func(
                "ARRAY_INSERT", e.this, e.args.get("position"), e.expression
            ),
            exp.ArrayAppend: array_append_sql("ARRAY_APPEND"),
            exp.ArrayPrepend: array_append_sql("ARRAY_PREPEND"),
            exp.BitwiseAndAgg: rename_func("BIT_AND"),
            exp.BitwiseOrAgg: rename_func("BIT_OR"),
            exp.BitwiseXorAgg: rename_func("BIT_XOR"),
            exp.BitwiseCount: rename_func("BIT_COUNT"),
            exp.Create: preprocess(
                [
                    remove_unique_constraints,
                    lambda e: ctas_with_tmp_tables_to_create_tmp_view(
                        e, temporary_storage_provider
                    ),
                    move_partitioned_by_to_schema_columns,
                ]
            ),
            exp.CurrentVersion: rename_func("VERSION"),
            exp.DateFromUnixDate: rename_func("DATE_FROM_UNIX_DATE"),
            exp.DatetimeAdd: date_delta_to_binary_interval_op(cast=False),
            exp.DatetimeSub: date_delta_to_binary_interval_op(cast=False),
            exp.GroupConcat: _groupconcat_sql,
            exp.EndsWith: rename_func("ENDSWITH"),
            exp.JSONKeys: rename_func("JSON_OBJECT_KEYS"),
            exp.PartitionedByProperty: lambda self, e: (
                f"PARTITIONED BY {self.wrap(self.expressions(sqls=[_normalize_partition(e) for e in e.this.expressions], skip_first=True))}"
            ),
            exp.SafeAdd: rename_func("TRY_ADD"),
            exp.SafeDivide: rename_func("TRY_DIVIDE"),
            exp.SafeMultiply: rename_func("TRY_MULTIPLY"),
            exp.SafeSubtract: rename_func("TRY_SUBTRACT"),
            exp.StartsWith: rename_func("STARTSWITH"),
            exp.TimeAdd: date_delta_to_binary_interval_op(cast=False),
            exp.TimeSub: date_delta_to_binary_interval_op(cast=False),
            exp.TsOrDsAdd: _dateadd_sql,
            exp.TimestampAdd: _dateadd_sql,
            exp.TimestampFromParts: rename_func("MAKE_TIMESTAMP"),
            exp.TimestampSub: date_delta_to_binary_interval_op(cast=False),
            exp.DatetimeDiff: timestampdiff_sql,
            exp.TimestampDiff: timestampdiff_sql,
            exp.TryCast: lambda self, e: (
                self.trycast_sql(e) if e.args.get("safe") else self.cast_sql(e)
            ),
            exp.AnyValue: None,
            exp.DateDiff: None,
            exp.With: None,
        }.items()
        if v is not None
    }

    def ignorenulls_sql(self, expression: exp.IgnoreNulls) -> str:
        return generator.Generator.ignorenulls_sql(self, expression)

    def bracket_sql(self, expression: exp.Bracket) -> str:
        if expression.args.get("safe"):
            key = seq_get(self.bracket_offset_expressions(expression, index_offset=1), 0)
            return self.func("TRY_ELEMENT_AT", expression.this, key)

        return super().bracket_sql(expression)

    def computedcolumnconstraint_sql(self, expression: exp.ComputedColumnConstraint) -> str:
        return f"GENERATED ALWAYS AS ({self.sql(expression, 'this')})"

    def anyvalue_sql(self, expression: exp.AnyValue) -> str:
        return self.function_fallback_sql(expression)

    def datediff_sql(self, expression: exp.DateDiff) -> str:
        end = self.sql(expression, "this")
        start = self.sql(expression, "expression")

        if expression.unit:
            return self.func("DATEDIFF", unit_to_var(expression), start, end)

        return self.func("DATEDIFF", end, start)

    def placeholder_sql(self, expression: exp.Placeholder) -> str:
        if not expression.args.get("widget"):
            return super().placeholder_sql(expression)

        return f"{{{expression.name}}}"

    def readparquet_sql(self, expression: exp.ReadParquet) -> str:
        if len(expression.expressions) != 1:
            self.unsupported("READ_PARQUET with multiple arguments is not supported")
            return ""

        parquet_file = expression.expressions[0]
        return f"parquet.`{parquet_file.name}`"

    def ifblock_sql(self, expression: exp.IfBlock) -> str:
        condition = expression.this
        true_block = expression.args.get("true")

        condition_expr = None
        if isinstance(condition, exp.Not):
            inner = condition.this
            if isinstance(inner, exp.Is) and isinstance(inner.expression, exp.Null):
                condition_expr = inner.this

        if isinstance(condition_expr, exp.ObjectId):
            object_type = condition_expr.expression
            if (
                (object_type is None or object_type.name.upper() == "U")
                and isinstance(true_block, exp.Block)
                and isinstance(drop := true_block.expressions[0], exp.Drop)
            ):
                drop.set("exists", True)
                return self.sql(drop)

        return super().ifblock_sql(expression)
