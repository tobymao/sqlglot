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


def _listagg_sort(order: exp.Order, value: exp.Expr) -> tuple[exp.Struct, exp.Lambda]:
    fields = [exp.alias_(value, "value")]
    comparison = exp.case()
    for i, key in enumerate(order.expressions):
        name = f"k{i}"
        fields.append(exp.alias_(key.this, name))
        left, right = exp.column(name, "l"), exp.column(name, "r")
        before = left > right if key.args.get("desc") else left < right
        null_first = left if key.args.get("nulls_first") else right
        comparison.when(
            exp.NullSafeNEQ(this=left, expression=right),
            exp.If(
                this=exp.Coalesce(this=before, expressions=[null_first.is_(exp.null())]),
                true=exp.Literal.number(-1),
                false=exp.Literal.number(1),
            ),
            copy=False,
        )

    return exp.Struct(expressions=fields), exp.Lambda(
        this=comparison.else_(exp.Literal.number(0), copy=False),
        expressions=[exp.to_identifier("l"), exp.to_identifier("r")],
    )


class SparkGenerator(Spark2Generator):
    SUPPORTS_TO_NUMBER = True
    PAD_FILL_PATTERN_IS_REQUIRED = False
    SUPPORTS_CONVERT_TIMEZONE = True
    SUPPORTS_MEDIAN = True
    SUPPORTS_UNIX_SECONDS = True
    MERGE_SCHEMA_EVOLUTION_MIN_VERSION = (4, 0)
    ORDERED_PERCENTILE_MIN_VERSIONS = dict.fromkeys(exp.PERCENTILES, (3, 3))
    LISTAGG_MIN_VERSION = (4, 0)
    SUPPORTS_DECODE_CASE = True
    SET_ASSIGNMENT_REQUIRES_VARIABLE_KEYWORD = True

    TYPE_MAPPING = {
        **Spark2Generator.TYPE_MAPPING,
        exp.DType.MONEY: "DECIMAL",
        exp.DType.SMALLMONEY: "DECIMAL",
        exp.DType.UUID: "STRING",
        exp.DType.TIMESTAMPLTZ: "TIMESTAMP_LTZ",
        exp.DType.TIMESTAMPNTZ: "TIMESTAMP_NTZ",
    }

    TYPE_PARAM_SETTINGS = {
        **Spark2Generator.TYPE_PARAM_SETTINGS,
        exp.DType.MONEY: ((15, 4), ()),
        exp.DType.SMALLMONEY: ((6, 4), ()),
    }

    TRANSFORMS = {
        k: v
        for k, v in {
            **Spark2Generator.TRANSFORMS,
            exp.ArrayConstructCompact: lambda self, e: self.func(
                "ARRAY_COMPACT", self.func("ARRAY", *e.expressions)
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
            exp.StabilityProperty: lambda self, e: (
                "DETERMINISTIC" if e.name == "IMMUTABLE" else "NOT DETERMINISTIC"
            ),
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

    def dateadd_sql(self, expression: exp.DateAdd | exp.DateSub) -> str:
        if expression.text("unit").upper() in (
            "HOUR",
            "MINUTE",
            "SECOND",
            "MILLISECOND",
            "MICROSECOND",
        ):
            amount = expression.expression
            return self.func(
                "TIMESTAMPADD",
                unit_to_var(expression),
                -amount if isinstance(expression, exp.DateSub) else amount,
                expression.this,
            )

        return super().dateadd_sql(expression)

    def ignorenulls_sql(self, expression: exp.IgnoreNulls) -> str:
        return generator.Generator.ignorenulls_sql(self, expression)

    def groupconcat_sql(
        self,
        expression: exp.GroupConcat,
        condition: exp.Expr | None = None,
        window: exp.Window | None = None,
    ) -> str:
        if self.dialect.version >= self.LISTAGG_MIN_VERSION:
            return groupconcat_sql(self, expression)

        this = expression.this
        limit = this.expression if isinstance(this, exp.Limit) else None
        if limit:
            this = this.this
        order = this if isinstance(this, exp.Order) else None
        value = order.this if order else this
        distinct = isinstance(value, exp.Distinct)
        if distinct:
            value = value.expressions[0]

        comparator = None
        if order:
            # Collect each value with its sort keys. A struct containing a NULL value
            # would otherwise survive COLLECT_LIST's implicit NULL filtering.
            condition = exp.and_(condition, value.is_(exp.null()).not_())
            value, comparator = _listagg_sort(order, value)

        if condition is not None:
            value = exp.If(this=condition, true=value, false=exp.null())
        values: exp.Expr = (
            exp.ArrayUniqueAgg(this=value) if distinct and not order else exp.ArrayAgg(this=value)
        )
        if window:
            values = exp.Window(**{**window.args, "this": values})
        if comparator:
            values = exp.Dot(
                this=exp.ArraySort(this=values, expression=comparator),
                expression=exp.to_identifier("value"),
            )
            if distinct:
                values = exp.ArrayDistinct(this=values)
        if limit:
            values = exp.ArraySlice(this=values, start=exp.Literal.number(1), end=limit)

        return self.func(
            "ARRAY_JOIN",
            exp.Nullif(this=values, expression=exp.array()),
            expression.args.get("separator") or exp.Literal.string(""),
        )

    def filter_sql(self, expression: exp.Filter) -> str:
        if (
            isinstance(expression.this, exp.GroupConcat)
            and self.dialect.version < self.LISTAGG_MIN_VERSION
        ):
            return self.groupconcat_sql(expression.this, condition=expression.expression.this)
        return super().filter_sql(expression)

    def window_sql(self, expression: exp.Window) -> str:
        if self.dialect.version < self.LISTAGG_MIN_VERSION:
            agg = expression.this
            condition = None
            if isinstance(agg, exp.Filter):
                condition, agg = agg.expression.this, agg.this
            if isinstance(agg, exp.GroupConcat):
                return self.groupconcat_sql(agg, condition=condition, window=expression)
        return super().window_sql(expression)

    def arrayinsert_sql(self, expression: exp.ArrayInsert, index_offset: int = 0) -> str:
        return super().arrayinsert_sql(expression, index_offset=1)

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
