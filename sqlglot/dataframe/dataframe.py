from copy import copy
import functools
import typing as t
import uuid

import sqlglot
from sqlglot import expressions as exp
from sqlglot.dataframe.column import Column
from sqlglot.dataframe import functions as F
from sqlglot.dataframe.util import ensure_strings, convert_join_type, ensure_columns
from sqlglot.helper import ensure_list, flatten
from sqlglot.dataframe.operations import Operation, operation
from sqlglot.dataframe.dataframe_na_functions import DataFrameNaFunctions
from sqlglot.dataframe.transforms import ORDERED_TRANSFORMS

from pyspark.sql import DataFrame

if t.TYPE_CHECKING:
    from sqlglot.dataframe.session import SparkSession


class JoinInfo:
    def __init__(self, base_df: "DataFrame", other_df: "DataFrame", columns: t.List[Column], how: str, base_prejoin_latest_cte_name: str):
        self.base_df = base_df
        self.other_df = other_df
        self.columns = columns
        self.how = how
        self.base_prejoin_latest_cte_name = base_prejoin_latest_cte_name


class DataFrame:
    def __init__(self, spark: "SparkSession", expression: exp.Select, branch_id: str, sequence_id: str, last_op: t.Optional[Operation] = Operation.NO_OP, group_by_columns: t.List[Column] = None, joins_infos: t.List["JoinInfo"] = None, **kwargs):
        self.spark = spark
        self.expression = expression
        self.branch_id = branch_id
        self.sequence_id = sequence_id
        self.last_op = last_op
        self.group_by_columns = group_by_columns or None
        self.joins_infos = joins_infos or []

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
        expression = self._resolve_joined_ctes(copy=True)
        for transform in ORDERED_TRANSFORMS:
            expression = expression.transform(transform,
                                              name_to_sequence_id_mapping=self.spark.name_to_sequence_id_mapping,
                                              known_ids=self.spark.known_ids)
        return expression.sql(dialect="spark", pretty=True)

    def _resolve_joined_ctes(self, copy=True) -> exp.Expression:
        if copy:
            expression = self.expression.copy()
        else:
            expression = self
        if len(self.joins_infos) > 0:
            for join_info in self.joins_infos:
                expression = self._add_ctes_to_expression(self.expression, join_info.other_df.expression.ctes)
        return expression

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
    def _add_ctes_to_expression(cls, expression: exp.Subqueryable, ctes: t.List[exp.CTE]) -> exp.Expression:
        with_expression = expression.args.get("with")
        existing_ctes = []
        if with_expression:
            existing_ctes = with_expression.args["expressions"]
        existing_ctes.extend(ctes)
        expression.set("with", exp.With(expressions=existing_ctes))
        return expression

    def _convert_leaf_to_cte(self, sequence_id: t.Optional[str] = None) -> "DataFrame":
        sequence_id = sequence_id or self.sequence_id
        expression = self._resolve_joined_ctes(copy=True)
        cte_expression, cte_name = self._create_cte_from_expression(expression=expression, sequence_id=sequence_id)
        new_expression = exp.Select()
        new_expression = self._add_ctes_to_expression(new_expression, expression.ctes + [cte_expression])
        sel_columns = self._get_outer_select_columns(cte_expression)
        new_expression = new_expression.from_(cte_name).select(*[x.alias_or_name for x in sel_columns])
        self.joins_infos = []
        return self.copy(expression=new_expression, sequence_id=sequence_id)

    @classmethod
    def _get_outer_select_columns(cls, item: t.Union[exp.Expression, "DataFrame"]) -> t.List[Column]:
        expression = item.expression if isinstance(item, DataFrame) else item
        return [Column(x) for x in dict.fromkeys(expression.find(exp.Select).args.get("expressions", []))]

    def _replace_alias_references(self, potential_references: t.List[Column]) -> t.List[Column]:
        potential_references = ensure_list(potential_references)
        sqlglot_columns = list(
            flatten([[x.expression] if isinstance(x.expression, exp.Column) else list(x.expression.find_all(exp.Column)) for x in potential_references]))
        dfs_within_scope = [self] + [x.other_df for x in self.joins_infos]
        for col in sqlglot_columns:
            table_identifier = col.args.get("table")
            if not table_identifier:
                inner_join_columns = {y.alias_or_name for y in flatten([x.columns for x in self.joins_infos])}
                if col.alias_or_name in inner_join_columns:
                    col.set("table", exp.Identifier(this=self.joins_infos[0].base_prejoin_latest_cte_name))
                continue
            table_name = table_identifier.alias_or_name
            # matching_df = [x for x in dfs_within_scope if x.branch_id == table_name or x.name == table_name]
            matching_df = [x for x in dfs_within_scope if x.branch_id == table_name]
            if len(matching_df) > 0:
                matching_df = matching_df[0]
                col.set("table", exp.Identifier(this=matching_df.latest_cte_name))

        return potential_references

    @operation(Operation.SELECT)
    def select(self, *cols, **kwargs) -> "DataFrame":
        cols = ensure_columns(cols)
        cols = self._replace_alias_references(cols)
        kwargs["append"] = kwargs.get("append", False)
        return self.copy(expression=self.expression.select(*[x.expression for x in cols], **kwargs))

    @operation(Operation.NO_OP)
    def alias(self, name: str, **kwargs) -> "DataFrame":
        sequence_id = self.spark.random_id
        self.spark.add_alias_to_mapping(name, sequence_id)
        return self._convert_leaf_to_cte(sequence_id=sequence_id)

    @operation(Operation.WHERE)
    def where(self, column: t.Union[Column, bool], **kwargs) -> "DataFrame":
        column = self._replace_alias_references([column])[0]
        return self.copy(expression=self.expression.where(column.expression))

    @operation(Operation.GROUP_BY)
    def groupBy(self, *cols, **kwargs) -> "DataFrame":
        cols = ensure_columns(cols)
        cols = self._replace_alias_references(cols)
        df_copy = self.copy(expression=self.expression.group_by(*[x.expression for x in cols]), group_by_columns=cols)
        return df_copy

    @operation(Operation.SELECT)
    def agg(self, col: Column, **kwargs) -> "DataFrame":
        cols = self.group_by_columns + [col]
        cols = self._replace_alias_references(cols)
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
        self.joins_infos.append(JoinInfo(self, other_df, join_columns, join_type, pre_join_self_latest_cte_name))
        self_columns = [column.set_table_name(pre_join_self_latest_cte_name, copy=True) for column in self._get_outer_select_columns(self)]
        other_columns = [column.set_table_name(other_df.latest_cte_name, copy=True) for column in self._get_outer_select_columns(other_df)]
        all_columns = list({column.alias_or_name: column for column in join_columns + self_columns + other_columns}.values())
        new_df = self.copy(expression=self.expression.join(other_df.latest_cte_name, on=join_clause.expression, join_type=join_type))
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

