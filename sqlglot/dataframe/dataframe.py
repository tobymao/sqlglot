from copy import copy
import functools
import typing as t

from sqlglot import expressions as exp
from sqlglot.dataframe.column import Column
from sqlglot.dataframe import functions as F
from sqlglot.dataframe.util import ensure_strings, convert_join_type, ensure_columns
from sqlglot.helper import ensure_list
from sqlglot.dataframe.operations import Operation, operation
from sqlglot.dataframe.dataframe_na_functions import DataFrameNaFunctions
from sqlglot.dataframe.transforms import ORDERED_TRANSFORMS

if t.TYPE_CHECKING:
    from sqlglot.dataframe.session import SparkSession


class DataFrame:
    def __init__(self, spark: "SparkSession", expression: exp.Select, branch_id: str, sequence_id: str, last_op: t.Optional[Operation] = Operation.NO_OP, group_by_columns: t.List[Column] = None, pending_join_hints: t.List[exp.Expression] = None, pending_select_hints: t.List[exp.Expression] = None, **kwargs):
        self.spark = spark
        self.expression = expression
        self.branch_id = branch_id
        self.sequence_id = sequence_id
        self.last_op = last_op
        self.group_by_columns = group_by_columns or None
        self.pending_join_hints = pending_join_hints or []
        self.pending_select_hints = pending_select_hints or []

    def __getattr__(self, column_name: str) -> "Column":
        return self[column_name]

    def __getitem__(self, column_name: str) -> "Column":
        column_name = f"{self.branch_id}.{column_name}"
        return Column(column_name)

    @property
    def latest_cte_name(self) -> str:
        if len(self.expression.ctes) == 0:
            return self.expression.find(exp.Table).alias_or_name
        return self.expression.ctes[-1].alias

    def sql(self) -> str:
        df = self._resolve_pending_hints()
        expression = df.expression.copy()
        for transform in ORDERED_TRANSFORMS:
            expression = expression.transform(transform,
                                              name_to_sequence_id_mapping=df.spark.name_to_sequence_id_mapping,
                                              known_ids=df.spark.known_ids,
                                              known_branch_ids=df.spark.known_branch_ids,
                                              known_sequence_ids=df.spark.known_sequence_ids)
        return expression.sql(dialect="spark", pretty=True)

    def copy(self, **kwargs) -> "DataFrame":
        kwargs = {**{k: copy(v) for k, v in vars(self).copy().items()}, **kwargs}
        return DataFrame(**kwargs)

    def _create_cte_from_expression(self, expression: exp.Expression, branch_id: t.Optional[str] = None,
                                    sequence_id: t.Optional[str] = None, **kwargs) -> t.Tuple[exp.CTE, str]:
        name = self.spark.random_name
        expression_to_cte = expression.copy()
        expression_to_cte.set("with", None)
        cte = exp.Select().with_(name, as_=expression_to_cte, **kwargs).ctes[0]
        cte.set("branch_id", branch_id or self.branch_id)
        cte.set("sequence_id", sequence_id or self.sequence_id)
        return cte, name

    def _ensure_list_of_columns(self, cols: t.Union[str, t.Iterable[str], Column, t.Iterable[Column]]) -> t.List[Column]:
        columns = ensure_list(cols)
        columns = ensure_strings(columns)
        columns = ensure_columns(columns)
        return columns

    @classmethod
    def _add_ctes_to_expression(cls, expression: exp.Expression, ctes: t.List[exp.CTE]) -> exp.Expression:
        expression = expression.copy()
        with_expression = expression.args.get("with")
        if with_expression:
            existing_ctes = with_expression.args["expressions"]
            existsing_cte_names = [x.alias_or_name for x in existing_ctes]
            for cte in ctes:
                if cte.alias_or_name not in existsing_cte_names:
                    existing_ctes.append(cte)
        else:
            existing_ctes = ctes
        expression.set("with", exp.With(expressions=existing_ctes))
        return expression

    def _convert_leaf_to_cte(self, sequence_id: t.Optional[str] = None) -> "DataFrame":
        df = self._resolve_pending_hints()
        sequence_id = sequence_id or df.sequence_id
        expression = df.expression.copy()
        cte_expression, cte_name = df._create_cte_from_expression(expression=expression, sequence_id=sequence_id)
        new_expression = exp.Select()
        new_expression = df._add_ctes_to_expression(new_expression, expression.ctes + [cte_expression])
        sel_columns = df._get_outer_select_columns(cte_expression)
        new_expression = new_expression.from_(cte_name).select(*[x.alias_or_name for x in sel_columns])
        return df.copy(expression=new_expression, sequence_id=sequence_id)

    @classmethod
    def _get_outer_select_columns(cls, item: t.Union[exp.Expression, "DataFrame"]) -> t.List[Column]:
        expression = item.expression if isinstance(item, DataFrame) else item
        return [Column(x) for x in dict.fromkeys(expression.find(exp.Select).args.get("expressions", []))]

    def _resolve_pending_hints(self) -> "DataFrame":
        expression = self.expression.copy()
        hint_expression = expression.args.get("hint", exp.Hint(expressions=[]))
        for hint in self.pending_select_hints:
            hint_expression.args.get("expressions").append(hint)
        expression_without_cte = expression.copy()
        expression_without_cte.args.pop("with", None)
        join_expression = expression_without_cte.find(exp.Join)
        if join_expression:
            for hint in self.pending_join_hints:
                hint_expression.args.get("expressions").append(hint)
        if hint_expression.expressions:
            expression.set("hint", hint_expression)
        return self.copy(expression=expression,
                         pending_select_hints=None,
                         pending_join_hints=None if join_expression else self.pending_join_hints)

    @operation(Operation.SELECT)
    def select(self, *cols, **kwargs) -> "DataFrame":
        cols = ensure_columns(cols)
        kwargs["append"] = kwargs.get("append", False)
        return self.copy(expression=self.expression.select(*[x.expression for x in cols], **kwargs))

    @operation(Operation.NO_OP)
    def alias(self, name: str, **kwargs) -> "DataFrame":
        sequence_id = self.spark.random_sequence_id
        self.spark.add_alias_to_mapping(name, sequence_id)
        return self._convert_leaf_to_cte(sequence_id=sequence_id)

    @operation(Operation.WHERE)
    def where(self, column: t.Union[Column, bool], **kwargs) -> "DataFrame":
        return self.copy(expression=self.expression.where(column.expression))

    @operation(Operation.GROUP_BY)
    def groupBy(self, *cols, **kwargs) -> "DataFrame":
        cols = ensure_columns(cols)
        df_copy = self.copy(expression=self.expression.group_by(*[x.expression for x in cols]), group_by_columns=cols)
        return df_copy

    @operation(Operation.SELECT)
    def agg(self, col: Column, **kwargs) -> "DataFrame":
        cols = self.group_by_columns + [col]
        return self.select.__wrapped__(self, *cols, **kwargs)

    @operation(Operation.FROM)
    def join(self, other_df: "DataFrame", on: t.Union[str, t.List[str], Column, t.List[Column]], how: str = 'inner', **kwargs) -> "DataFrame":
        other_df = other_df._convert_leaf_to_cte()
        pre_join_self_latest_cte_name = self.latest_cte_name
        columns = self._ensure_list_of_columns(on)
        join_type = convert_join_type(how)
        if isinstance(columns[0].expression, exp.Column):
            join_columns = columns
            join_clause = functools.reduce(lambda x, y: x & y, [
                col.copy().set_table_name(pre_join_self_latest_cte_name) == col.copy().set_table_name(other_df.latest_cte_name)
                for col in columns
            ])
        else:
            if len(columns) > 1:
                columns = [functools.reduce(lambda x, y: x & y, columns)]
            join_clause = columns[0]
            join_columns = [
                Column(x).set_table_name(pre_join_self_latest_cte_name) if i % 2 == 0 else Column(x).set_table_name(other_df.latest_cte_name)
                for i, x in enumerate(join_clause.expression.find_all(exp.Column))
            ]
        self_columns = [column.set_table_name(pre_join_self_latest_cte_name, copy=True) for column in self._get_outer_select_columns(self)]
        other_columns = [column.set_table_name(other_df.latest_cte_name, copy=True) for column in self._get_outer_select_columns(other_df)]
        all_columns = list({column.alias_or_name: column for column in join_columns + self_columns + other_columns}.values())
        new_df = self.copy(expression=self.expression.join(other_df.latest_cte_name, on=join_clause.expression, join_type=join_type))
        new_df.expression = new_df._add_ctes_to_expression(new_df.expression, other_df.expression.ctes)
        new_df.pending_join_hints.extend(other_df.pending_join_hints)
        new_df.pending_select_hints.extend(other_df.pending_select_hints)
        new_df = new_df.select.__wrapped__(new_df, *all_columns)
        return new_df

    @operation(Operation.ORDER_BY)
    def orderBy(self, *cols: t.Union[str, Column], ascending: t.Optional[t.Union[t.Any, t.List[t.Any]]] = None) -> "DataFrame":
        """
        This implementation lets any ordered columns take priority over whatever is provided in `ascending`. Spark
        has irregular behavior and can result in runtime errors. Users shouldn't be mixing the two anyways so this
        is unlikely to come up.
        """
        cols = self._ensure_list_of_columns(cols)
        pre_ordered_col_indexes = [x for x in [i if isinstance(col.expression, exp.Ordered) else None for i, col in enumerate(cols)] if x is not None]
        if ascending is None:
            ascending = [True] * len(cols)
        elif not isinstance(ascending, list):
            ascending = [ascending] * len(cols)
        ascending = [bool(x) for i, x in enumerate(ascending)]
        assert len(cols) == len(ascending), "The length of items in ascending must equal the number of columns provided"
        col_and_ascending = list(zip(cols, ascending))
        order_by_columns = [
            exp.Ordered(this=col.expression, desc=not asc)
            if i not in pre_ordered_col_indexes else cols[i].expression
            for i, (col, asc) in enumerate(col_and_ascending)
        ]
        return self.copy(expression=self.expression.order_by(*order_by_columns))

    sort = orderBy

    def _set_operation(self, clazz: t.Callable, other: "DataFrame", distinct: bool):
        other_df = other._convert_leaf_to_cte()
        base_expression = self.expression.copy()
        base_expression = self._add_ctes_to_expression(base_expression, other_df.expression.ctes)
        all_ctes = base_expression.ctes
        other_df.expression.set("with", None)
        base_expression.set("with", None)
        operation = clazz(this=base_expression, distinct=distinct, expression=other_df.expression)
        operation.set("with", exp.With(expressions=all_ctes))
        return self.copy(expression=operation)._convert_leaf_to_cte()

    @operation(Operation.FROM)
    def union(self, other: "DataFrame") -> "DataFrame":
        return self._set_operation(exp.Union, other, False)

    unionAll = union

    @operation(Operation.FROM)
    def intersect(self, other: "DataFrame") -> "DataFrame":
        return self._set_operation(exp.Intersect, other, True)

    @operation(Operation.FROM)
    def intersectAll(self, other: "DataFrame") -> "DataFrame":
        return self._set_operation(exp.Intersect, other, False)

    @operation(Operation.FROM)
    def exceptAll(self, other: "DataFrame") -> "DataFrame":
        return self._set_operation(exp.Except, other, False)

    @operation(Operation.SELECT)
    def distinct(self) -> "DataFrame":
        expression = self.expression.copy()
        expression.set("distinct", exp.Distinct())
        return self.copy(expression=expression)

    @property
    def na(self) -> "DataFrameNaFunctions":
        return DataFrameNaFunctions(self)

    @operation(Operation.FROM)
    def dropna(self, how: str = "any", thresh: t.Optional[int] = None,
               subset: t.Optional[t.Union[str, t.Tuple[str, ...], t.List[str]]] = None) -> "DataFrame":
        if self.expression.ctes[-1].find(exp.Star) is not None:
            raise RuntimeError("Cannot use `dropna` when a * expression is used")
        minimum_non_null = thresh
        new_df = self.copy()
        all_columns = self._get_outer_select_columns(new_df.expression)
        if subset:
            null_check_columns = self._ensure_list_of_columns(subset)
        else:
            null_check_columns = all_columns
        if thresh is None:
            minimum_num_nulls = 1 if how == "any" else len(null_check_columns)
        else:
            minimum_num_nulls = len(null_check_columns) - minimum_non_null + 1
        if minimum_num_nulls > len(null_check_columns):
            raise RuntimeError(f"The minimum num nulls for dropna must be less than or equal to the number of columns. "
                               f"Minimum num nulls: {minimum_num_nulls}, Num Columns: {len(null_check_columns)}")
        if_null_checks = [
            F.when(column.isNull(), F.lit(1)).otherwise(F.lit(0))
            for column in null_check_columns
        ]
        nulls_added_together = functools.reduce(lambda x, y: x + y, if_null_checks)
        num_nulls = nulls_added_together.alias("num_nulls")
        new_df = new_df.select(num_nulls, append=True)
        filtered_df = new_df.where(F.col("num_nulls") < F.lit(minimum_num_nulls))
        final_df = filtered_df.select(*all_columns)
        return final_df

    @operation(Operation.FROM)
    def fillna(self,
               value: t.Union[int, bool, float, str, t.Dict[str, t.Any]],
               subset: t.Optional[t.Union[str, t.Tuple[str, ...], t.List[str]]] = None) -> "DataFrame":
        """
        Functionality Difference: If you provide a value to replace a null and that type conflicts
        with the type of the column then PySpark will just ignore your replacement.
        This will try to cast them to be the same in some cases. So they won't always match.
        Best to not mix types so make sure replacement is the same type as the column

        Possibility for improvement: Use `typeof` function to get the type of the column
        and check if it matches the type of the value provided. If not then make it null.
        """
        from sqlglot.dataframe.functions import lit
        values = None
        columns = None
        new_df = self.copy()
        all_columns = self._get_outer_select_columns(new_df.expression)
        all_column_mapping = {
            column.alias_or_name: column
            for column in all_columns
        }
        if isinstance(value, dict):
            values = value.values()
            columns = self._ensure_list_of_columns(list(value.keys()))
        if not columns:
            columns = self._ensure_list_of_columns(subset) if subset else all_columns
        if not values:
            values = [value] * len(columns)
        values = [lit(value) for value in values]

        null_replacement_mapping = {
            column.alias_or_name: (
                F.when(column.isNull(), value)
                .otherwise(column)
                .alias(column.alias_or_name)
            )
            for column, value in zip(columns, values)
        }
        null_replacement_mapping = {**all_column_mapping, **null_replacement_mapping}
        null_replacement_columns = [
            null_replacement_mapping[column.alias_or_name]
            for column in all_columns
        ]
        new_df = new_df.select(*null_replacement_columns)
        return new_df

    @operation(Operation.FROM)
    def replace(self, to_replace: t.Union[bool, int, float, str, t.List, t.Dict],
                value: t.Optional[t.Union[bool, int, float, str, t.List]] = None,
                subset: t.Optional[t.Union[str, t.List[str]]] = None) -> "DataFrame":
        from sqlglot.dataframe.functions import lit
        old_values = None
        subset = ensure_list(subset)
        new_df = self.copy()
        all_columns = self._get_outer_select_columns(new_df.expression)
        all_column_mapping = {
            column.alias_or_name: column
            for column in all_columns
        }

        columns = self._ensure_list_of_columns(subset) if subset else all_columns
        if isinstance(to_replace, dict):
            old_values = list(to_replace.keys())
            new_values = list(to_replace.values())
        elif not old_values and isinstance(to_replace, list):
            assert isinstance(value, list), "value must be a list since the replacements are a list"
            assert len(to_replace) == len(value), "the replacements and values must be the same length"
            old_values = to_replace
            new_values = value
        else:
            old_values = [to_replace] * len(columns)
            new_values = [value] * len(columns)
        old_values = [lit(value) for value in old_values]
        new_values = [lit(value) for value in new_values]

        replacement_mapping = {}
        for column in columns:
            expression = None
            for i, (old_value, new_value) in enumerate(zip(old_values, new_values)):
                if i == 0:
                    expression = F.when(column == old_value, new_value)
                else:
                    expression = expression.when(column == old_value, new_value)
            replacement_mapping[column.alias_or_name] = expression.otherwise(column).alias(column.expression.alias_or_name)

        replacement_mapping = {**all_column_mapping, **replacement_mapping}
        replacement_columns = [
            replacement_mapping[column.alias_or_name]
            for column in all_columns
        ]
        new_df = new_df.select(*replacement_columns)
        return new_df

    @operation(Operation.SELECT)
    def withColumn(self, colName: str, col: Column) -> "DataFrame":
        return self.copy().select(col.alias(colName), append=True)

    @operation(Operation.SELECT)
    def drop(self, *cols: t.Union[str, "Column"]) -> "DataFrame":
        all_columns = self._get_outer_select_columns(self.expression)
        drop_cols = self._ensure_list_of_columns(cols)
        new_columns = [col for col in all_columns if
                       col.alias_or_name not in [drop_column.alias_or_name for drop_column in drop_cols]]
        return self.copy().select(*new_columns, append=False)

    @operation(Operation.LIMIT)
    def limit(self, num: int) -> "DataFrame":
        return self.copy(expression=self.expression.limit(num))

    def _hint(self, hint_name: str, args: t.List["Column"]) -> "DataFrame":
        hint_name = hint_name.upper()
        hint_expression = exp.Anonymous(this=hint_name, expressions=[parameter.expression for parameter in args])
        new_df = self.copy()
        if hint_name in self.spark.join_hint_names:
            new_df.pending_join_hints.append(hint_expression)
        else:
            new_df.pending_select_hints.append(hint_expression)
        return new_df

    @operation(Operation.NO_OP)
    def hint(self, name: str, *parameters: t.Optional[t.Union[int, str]]) -> "DataFrame":
        parameters = ensure_list(parameters)
        parameters = ensure_columns(parameters) if parameters else ensure_columns([self.branch_id])
        return self._hint(name, parameters)

    @operation(Operation.NO_OP)
    def repartition(self, numPartitions: t.Union[int, str], *cols: t.Union[int, str]) -> "DataFrame":
        num_partitions = ensure_columns([numPartitions])
        cols = ensure_columns(ensure_list(cols))
        args = num_partitions + cols
        return self._hint("repartition", args)

    @operation(Operation.NO_OP)
    def coalesce(self, numPartitions: int) -> "DataFrame":
        num_partitions = ensure_columns([numPartitions])
        return self._hint("coalesce", num_partitions)
