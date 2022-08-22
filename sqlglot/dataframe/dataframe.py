from copy import copy
import functools
import typing as t
import uuid

from sqlglot import expressions as exp
from sqlglot.dataframe.column import Column
from sqlglot.dataframe.util import ensure_strings, convert_join_type, ensure_columns
from sqlglot.helper import ensure_list, flatten
from sqlglot.dataframe.operations import Operation, operation

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
    def __init__(self, spark: "SparkSession", expression: exp.Select, branch_id: str, name: t.Optional[str] = None, last_op: t.Optional[Operation] = Operation.NO_OP, group_by_columns: t.List[Column] = None, joins_infos: t.List["JoinInfo"] = None, **kwargs):
        self.id = self.random_name
        self.spark = spark
        self.expression = expression
        self.branch_id = branch_id
        self.name = name or self.random_name
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

    @classmethod
    @property
    def random_name(cls) -> str:
        return f"a{str(uuid.uuid4())[:8]}"

    def sql(self) -> str:
        expression = self._resolve_joined_ctes(copy=True)
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

    def _create_cte_from_expression(self, expression: exp.Expression, name: str = None, **kwargs) -> t.Tuple[exp.CTE, str]:
        name = name or self.random_name
        expression_to_cte = expression.copy()
        expression_to_cte.set("with", None)
        return exp.Select().with_(name, as_=expression_to_cte, **kwargs).ctes[0], name

    def _ensure_list_of_columns(self, cols: t.Union[str, t.Iterable[str], Column, t.Iterable[Column]]) -> t.List[Column]:
        columns = ensure_list(cols)
        columns = ensure_strings(columns)
        columns = ensure_columns(columns)
        return columns

    @classmethod
    def _add_ctes_to_expression(cls, expression: exp.Subqueryable, ctes: t.List[exp.CTE]) -> exp.Expression:
        for cte in ctes:
            if cte not in expression.ctes:
                expression = expression.with_(cte.alias_or_name, cte.args["this"].sql())
        return expression

    def _convert_leaf_to_cte(self, name: t.Optional[str] = None) -> "DataFrame":
        expression = self._resolve_joined_ctes(copy=True)
        cte_expression, cte_name = self._create_cte_from_expression(expression=expression, name=name)
        new_expression = exp.Select()
        new_expression = self._add_ctes_to_expression(new_expression, expression.ctes + [cte_expression])
        sel_columns = self._get_outer_select_columns(cte_expression)
        new_expression = new_expression.from_(cte_name).select(*[x.alias_or_name for x in sel_columns])
        self.joins_infos = []
        return self.copy(expression=new_expression, name=name if name is not None else self.name)

    @classmethod
    def _get_outer_select_columns(cls, expression):
        return [x for x in dict.fromkeys(expression.find(exp.Select).args.get("expressions", []))]

    def _replace_alias_references(self, potential_references: t.List[Column]) -> t.List[Column]:
        potential_references = ensure_list(potential_references)
        sqlglot_columns = list(
            flatten([[x.expression] if isinstance(x.expression, exp.Column) else list(x.expression.find_all(exp.Column)) for x in potential_references]))
        dfs_within_scope = [self] + [x.other_df for x in self.joins_infos]
        for col in sqlglot_columns:
            table_identifier = col.args.get("table")
            if not table_identifier:
                inner_join_columns = {y.expression.alias_or_name for y in flatten([x.columns for x in self.joins_infos])}
                if col.alias_or_name in inner_join_columns:
                    col.set("table", exp.Identifier(this=self.joins_infos[0].base_prejoin_latest_cte_name))
                continue
            table_name = table_identifier.alias_or_name
            matching_df = [x for x in dfs_within_scope if x.branch_id == table_name or x.name == table_name]
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
        return self._convert_leaf_to_cte(name=name)

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
            join_columns = [Column(x) for x in join_clause.expression.find_all(exp.Column)]
            for i, column in enumerate(join_columns):
                if i % 2 == 0:
                    column.set_table_name(pre_join_self_latest_cte_name)
                else:
                    column.set_table_name(other_df.latest_cte_name)
        self.joins_infos.append(JoinInfo(self, other_df, join_columns, join_type, pre_join_self_latest_cte_name))
        return self.copy(expression=self.expression.join(other_df.latest_cte_name, on=join_clause.expression, join_type=join_type))

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
