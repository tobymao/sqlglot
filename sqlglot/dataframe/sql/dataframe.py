from __future__ import annotations

import functools
import typing as t
import zlib
from copy import copy

import sqlglot
from sqlglot import expressions as exp
from sqlglot.dataframe.sql import functions as F
from sqlglot.dataframe.sql.column import Column
from sqlglot.dataframe.sql.group import GroupedData
from sqlglot.dataframe.sql.normalize import normalize
from sqlglot.dataframe.sql.operations import Operation, operation
from sqlglot.dataframe.sql.readwriter import DataFrameWriter
from sqlglot.dataframe.sql.transforms import replace_id_value
from sqlglot.dataframe.sql.util import get_tables_from_expression_with_join
from sqlglot.dataframe.sql.window import Window
from sqlglot.helper import ensure_list, object_to_dict
from sqlglot.optimizer import optimize as optimize_func
from sqlglot.optimizer.qualify_columns import qualify_columns

if t.TYPE_CHECKING:
    from sqlglot.dataframe.sql._typing import (
        ColumnLiterals,
        ColumnOrLiteral,
        ColumnOrName,
        OutputExpressionContainer,
    )
    from sqlglot.dataframe.sql.session import SparkSession


JOIN_HINTS = {
    "BROADCAST",
    "BROADCASTJOIN",
    "MAPJOIN",
    "MERGE",
    "SHUFFLEMERGE",
    "MERGEJOIN",
    "SHUFFLE_HASH",
    "SHUFFLE_REPLICATE_NL",
}


class DataFrame:
    def __init__(
        self,
        spark: SparkSession,
        expression: exp.Select,
        branch_id: t.Optional[str] = None,
        sequence_id: t.Optional[str] = None,
        last_op: Operation = Operation.INIT,
        pending_hints: t.Optional[t.List[exp.Expression]] = None,
        output_expression_container: t.Optional[OutputExpressionContainer] = None,
        **kwargs,
    ):
        self.spark = spark
        self.expression = expression
        self.branch_id = branch_id or self.spark._random_branch_id
        self.sequence_id = sequence_id or self.spark._random_sequence_id
        self.last_op = last_op
        self.pending_hints = pending_hints or []
        self.output_expression_container = output_expression_container or exp.Select()

    def __getattr__(self, column_name: str) -> Column:
        return self[column_name]

    def __getitem__(self, column_name: str) -> Column:
        column_name = f"{self.branch_id}.{column_name}"
        return Column(column_name)

    def __copy__(self):
        return self.copy()

    @property
    def sparkSession(self):
        return self.spark

    @property
    def write(self):
        return DataFrameWriter(self)

    @property
    def latest_cte_name(self) -> str:
        if not self.expression.ctes:
            from_exp = self.expression.args["from"]
            if from_exp.alias_or_name:
                return from_exp.alias_or_name
            table_alias = from_exp.find(exp.TableAlias)
            if not table_alias:
                raise RuntimeError(
                    f"Could not find an alias name for this expression: {self.expression}"
                )
            return table_alias.alias_or_name
        return self.expression.ctes[-1].alias

    @property
    def pending_join_hints(self):
        return [hint for hint in self.pending_hints if isinstance(hint, exp.JoinHint)]

    @property
    def pending_partition_hints(self):
        return [hint for hint in self.pending_hints if isinstance(hint, exp.Anonymous)]

    @property
    def columns(self) -> t.List[str]:
        return self.expression.named_selects

    @property
    def na(self) -> DataFrameNaFunctions:
        return DataFrameNaFunctions(self)

    def _replace_cte_names_with_hashes(self, expression: exp.Select):
        expression = expression.copy()
        ctes = expression.ctes
        replacement_mapping = {}
        for cte in ctes:
            old_name_id = cte.args["alias"].this
            new_hashed_id = exp.to_identifier(
                self._create_hash_from_expression(cte.this), quoted=old_name_id.args["quoted"]
            )
            replacement_mapping[old_name_id] = new_hashed_id
            cte.set("alias", exp.TableAlias(this=new_hashed_id))
            expression = expression.transform(replace_id_value, replacement_mapping)
        return expression

    def _create_cte_from_expression(
        self,
        expression: exp.Expression,
        branch_id: t.Optional[str] = None,
        sequence_id: t.Optional[str] = None,
        **kwargs,
    ) -> t.Tuple[exp.CTE, str]:
        name = self.spark._random_name
        expression_to_cte = expression.copy()
        expression_to_cte.set("with", None)
        cte = exp.Select().with_(name, as_=expression_to_cte, **kwargs).ctes[0]
        cte.set("branch_id", branch_id or self.branch_id)
        cte.set("sequence_id", sequence_id or self.sequence_id)
        return cte, name

    @t.overload
    def _ensure_list_of_columns(self, cols: t.Collection[ColumnOrLiteral]) -> t.List[Column]:
        ...

    @t.overload
    def _ensure_list_of_columns(self, cols: ColumnOrLiteral) -> t.List[Column]:
        ...

    def _ensure_list_of_columns(self, cols):
        return Column.ensure_cols(ensure_list(cols))

    def _ensure_and_normalize_cols(self, cols):
        cols = self._ensure_list_of_columns(cols)
        normalize(self.spark, self.expression, cols)
        return cols

    def _ensure_and_normalize_col(self, col):
        col = Column.ensure_col(col)
        normalize(self.spark, self.expression, col)
        return col

    def _convert_leaf_to_cte(self, sequence_id: t.Optional[str] = None) -> DataFrame:
        df = self._resolve_pending_hints()
        sequence_id = sequence_id or df.sequence_id
        expression = df.expression.copy()
        cte_expression, cte_name = df._create_cte_from_expression(
            expression=expression, sequence_id=sequence_id
        )
        new_expression = df._add_ctes_to_expression(
            exp.Select(), expression.ctes + [cte_expression]
        )
        sel_columns = df._get_outer_select_columns(cte_expression)
        new_expression = new_expression.from_(cte_name).select(
            *[x.alias_or_name for x in sel_columns]
        )
        return df.copy(expression=new_expression, sequence_id=sequence_id)

    def _resolve_pending_hints(self) -> DataFrame:
        df = self.copy()
        if not self.pending_hints:
            return df
        expression = df.expression
        hint_expression = expression.args.get("hint") or exp.Hint(expressions=[])
        for hint in df.pending_partition_hints:
            hint_expression.args.get("expressions").append(hint)
            df.pending_hints.remove(hint)

        join_aliases = {
            join_table.alias_or_name
            for join_table in get_tables_from_expression_with_join(expression)
        }
        if join_aliases:
            for hint in df.pending_join_hints:
                for sequence_id_expression in hint.expressions:
                    sequence_id_or_name = sequence_id_expression.alias_or_name
                    sequence_ids_to_match = [sequence_id_or_name]
                    if sequence_id_or_name in df.spark.name_to_sequence_id_mapping:
                        sequence_ids_to_match = df.spark.name_to_sequence_id_mapping[
                            sequence_id_or_name
                        ]
                    matching_ctes = [
                        cte
                        for cte in reversed(expression.ctes)
                        if cte.args["sequence_id"] in sequence_ids_to_match
                    ]
                    for matching_cte in matching_ctes:
                        if matching_cte.alias_or_name in join_aliases:
                            sequence_id_expression.set("this", matching_cte.args["alias"].this)
                            df.pending_hints.remove(hint)
                            break
                hint_expression.args.get("expressions").append(hint)
        if hint_expression.expressions:
            expression.set("hint", hint_expression)
        return df

    def _hint(self, hint_name: str, args: t.List[Column]) -> DataFrame:
        hint_name = hint_name.upper()
        hint_expression = (
            exp.JoinHint(
                this=hint_name,
                expressions=[exp.to_table(parameter.alias_or_name) for parameter in args],
            )
            if hint_name in JOIN_HINTS
            else exp.Anonymous(
                this=hint_name, expressions=[parameter.expression for parameter in args]
            )
        )
        new_df = self.copy()
        new_df.pending_hints.append(hint_expression)
        return new_df

    def _set_operation(self, klass: t.Callable, other: DataFrame, distinct: bool):
        other_df = other._convert_leaf_to_cte()
        base_expression = self.expression.copy()
        base_expression = self._add_ctes_to_expression(base_expression, other_df.expression.ctes)
        all_ctes = base_expression.ctes
        other_df.expression.set("with", None)
        base_expression.set("with", None)
        operation = klass(this=base_expression, distinct=distinct, expression=other_df.expression)
        operation.set("with", exp.With(expressions=all_ctes))
        return self.copy(expression=operation)._convert_leaf_to_cte()

    def _cache(self, storage_level: str):
        df = self._convert_leaf_to_cte()
        df.expression.ctes[-1].set("cache_storage_level", storage_level)
        return df

    @classmethod
    def _add_ctes_to_expression(cls, expression: exp.Select, ctes: t.List[exp.CTE]) -> exp.Select:
        expression = expression.copy()
        with_expression = expression.args.get("with")
        if with_expression:
            existing_ctes = with_expression.expressions
            existsing_cte_names = {x.alias_or_name for x in existing_ctes}
            for cte in ctes:
                if cte.alias_or_name not in existsing_cte_names:
                    existing_ctes.append(cte)
        else:
            existing_ctes = ctes
        expression.set("with", exp.With(expressions=existing_ctes))
        return expression

    @classmethod
    def _get_outer_select_columns(cls, item: t.Union[exp.Expression, DataFrame]) -> t.List[Column]:
        expression = item.expression if isinstance(item, DataFrame) else item
        return [Column(x) for x in expression.find(exp.Select).expressions]

    @classmethod
    def _create_hash_from_expression(cls, expression: exp.Select):
        value = expression.sql(dialect="spark").encode("utf-8")
        return f"t{zlib.crc32(value)}"[:6]

    def _get_select_expressions(
        self,
    ) -> t.List[t.Tuple[t.Union[t.Type[exp.Cache], OutputExpressionContainer], exp.Select]]:
        select_expressions: t.List[
            t.Tuple[t.Union[t.Type[exp.Cache], OutputExpressionContainer], exp.Select]
        ] = []
        main_select_ctes: t.List[exp.CTE] = []
        for cte in self.expression.ctes:
            cache_storage_level = cte.args.get("cache_storage_level")
            if cache_storage_level:
                select_expression = cte.this.copy()
                select_expression.set("with", exp.With(expressions=copy(main_select_ctes)))
                select_expression.set("cte_alias_name", cte.alias_or_name)
                select_expression.set("cache_storage_level", cache_storage_level)
                select_expressions.append((exp.Cache, select_expression))
            else:
                main_select_ctes.append(cte)
        main_select = self.expression.copy()
        if main_select_ctes:
            main_select.set("with", exp.With(expressions=main_select_ctes))
        expression_select_pair = (type(self.output_expression_container), main_select)
        select_expressions.append(expression_select_pair)  # type: ignore
        return select_expressions

    def sql(self, dialect="spark", optimize=True, **kwargs) -> t.List[str]:
        df = self._resolve_pending_hints()
        select_expressions = df._get_select_expressions()
        output_expressions: t.List[t.Union[exp.Select, exp.Cache, exp.Drop]] = []
        replacement_mapping: t.Dict[exp.Identifier, exp.Identifier] = {}
        for expression_type, select_expression in select_expressions:
            select_expression = select_expression.transform(replace_id_value, replacement_mapping)
            if optimize:
                select_expression = optimize_func(select_expression)
            select_expression = df._replace_cte_names_with_hashes(select_expression)
            expression: t.Union[exp.Select, exp.Cache, exp.Drop]
            if expression_type == exp.Cache:
                cache_table_name = df._create_hash_from_expression(select_expression)
                cache_table = exp.to_table(cache_table_name)
                original_alias_name = select_expression.args["cte_alias_name"]

                replacement_mapping[exp.to_identifier(original_alias_name)] = exp.to_identifier(  # type: ignore
                    cache_table_name
                )
                sqlglot.schema.add_table(
                    cache_table_name,
                    {
                        expression.alias_or_name: expression.type.sql("spark")
                        for expression in select_expression.expressions
                    },
                )
                cache_storage_level = select_expression.args["cache_storage_level"]
                options = [
                    exp.Literal.string("storageLevel"),
                    exp.Literal.string(cache_storage_level),
                ]
                expression = exp.Cache(
                    this=cache_table, expression=select_expression, lazy=True, options=options
                )
                # We will drop the "view" if it exists before running the cache table
                output_expressions.append(exp.Drop(this=cache_table, exists=True, kind="VIEW"))
            elif expression_type == exp.Create:
                expression = df.output_expression_container.copy()
                expression.set("expression", select_expression)
            elif expression_type == exp.Insert:
                expression = df.output_expression_container.copy()
                select_without_ctes = select_expression.copy()
                select_without_ctes.set("with", None)
                expression.set("expression", select_without_ctes)
                if select_expression.ctes:
                    expression.set("with", exp.With(expressions=select_expression.ctes))
            elif expression_type == exp.Select:
                expression = select_expression
            else:
                raise ValueError(f"Invalid expression type: {expression_type}")
            output_expressions.append(expression)

        return [
            expression.sql(**{"dialect": dialect, **kwargs}) for expression in output_expressions
        ]

    def copy(self, **kwargs) -> DataFrame:
        return DataFrame(**object_to_dict(self, **kwargs))

    @operation(Operation.SELECT)
    def select(self, *cols, **kwargs) -> DataFrame:
        cols = self._ensure_and_normalize_cols(cols)
        kwargs["append"] = kwargs.get("append", False)
        if self.expression.args.get("joins"):
            ambiguous_cols = [col for col in cols if not col.column_expression.table]
            if ambiguous_cols:
                join_table_identifiers = [
                    x.this for x in get_tables_from_expression_with_join(self.expression)
                ]
                cte_names_in_join = [x.this for x in join_table_identifiers]
                for ambiguous_col in ambiguous_cols:
                    ctes_with_column = [
                        cte
                        for cte in self.expression.ctes
                        if cte.alias_or_name in cte_names_in_join
                        and ambiguous_col.alias_or_name in cte.this.named_selects
                    ]
                    # If the select column does not specify a table and there is a join
                    # then we assume they are referring to the left table
                    if len(ctes_with_column) > 1:
                        table_identifier = self.expression.args["from"].args["expressions"][0].this
                    else:
                        table_identifier = ctes_with_column[0].args["alias"].this
                    ambiguous_col.expression.set("table", table_identifier)
        expression = self.expression.select(*[x.expression for x in cols], **kwargs)
        qualify_columns(expression, sqlglot.schema)
        return self.copy(expression=expression, **kwargs)

    @operation(Operation.NO_OP)
    def alias(self, name: str, **kwargs) -> DataFrame:
        new_sequence_id = self.spark._random_sequence_id
        df = self.copy()
        for join_hint in df.pending_join_hints:
            for expression in join_hint.expressions:
                if expression.alias_or_name == self.sequence_id:
                    expression.set("this", Column.ensure_col(new_sequence_id).expression)
        df.spark._add_alias_to_mapping(name, new_sequence_id)
        return df._convert_leaf_to_cte(sequence_id=new_sequence_id)

    @operation(Operation.WHERE)
    def where(self, column: t.Union[Column, bool], **kwargs) -> DataFrame:
        col = self._ensure_and_normalize_col(column)
        return self.copy(expression=self.expression.where(col.expression))

    filter = where

    @operation(Operation.GROUP_BY)
    def groupBy(self, *cols, **kwargs) -> GroupedData:
        columns = self._ensure_and_normalize_cols(cols)
        return GroupedData(self, columns, self.last_op)

    @operation(Operation.SELECT)
    def agg(self, *exprs, **kwargs) -> DataFrame:
        cols = self._ensure_and_normalize_cols(exprs)
        return self.groupBy().agg(*cols)

    @operation(Operation.FROM)
    def join(
        self,
        other_df: DataFrame,
        on: t.Union[str, t.List[str], Column, t.List[Column]],
        how: str = "inner",
        **kwargs,
    ) -> DataFrame:
        other_df = other_df._convert_leaf_to_cte()
        pre_join_self_latest_cte_name = self.latest_cte_name
        columns = self._ensure_and_normalize_cols(on)
        join_type = how.replace("_", " ")
        if isinstance(columns[0].expression, exp.Column):
            join_columns = [
                Column(x).set_table_name(pre_join_self_latest_cte_name) for x in columns
            ]
            join_clause = functools.reduce(
                lambda x, y: x & y,
                [
                    col.copy().set_table_name(pre_join_self_latest_cte_name)
                    == col.copy().set_table_name(other_df.latest_cte_name)
                    for col in columns
                ],
            )
        else:
            if len(columns) > 1:
                columns = [functools.reduce(lambda x, y: x & y, columns)]
            join_clause = columns[0]
            join_columns = [
                Column(x).set_table_name(pre_join_self_latest_cte_name)
                if i % 2 == 0
                else Column(x).set_table_name(other_df.latest_cte_name)
                for i, x in enumerate(join_clause.expression.find_all(exp.Column))
            ]
        self_columns = [
            column.set_table_name(pre_join_self_latest_cte_name, copy=True)
            for column in self._get_outer_select_columns(self)
        ]
        other_columns = [
            column.set_table_name(other_df.latest_cte_name, copy=True)
            for column in self._get_outer_select_columns(other_df)
        ]
        column_value_mapping = {
            column.alias_or_name
            if not isinstance(column.expression.this, exp.Star)
            else column.sql(): column
            for column in other_columns + self_columns + join_columns
        }
        all_columns = [
            column_value_mapping[name]
            for name in {x.alias_or_name: None for x in join_columns + self_columns + other_columns}
        ]
        new_df = self.copy(
            expression=self.expression.join(
                other_df.latest_cte_name, on=join_clause.expression, join_type=join_type
            )
        )
        new_df.expression = new_df._add_ctes_to_expression(
            new_df.expression, other_df.expression.ctes
        )
        new_df.pending_hints.extend(other_df.pending_hints)
        new_df = new_df.select.__wrapped__(new_df, *all_columns)
        return new_df

    @operation(Operation.ORDER_BY)
    def orderBy(
        self,
        *cols: t.Union[str, Column],
        ascending: t.Optional[t.Union[t.Any, t.List[t.Any]]] = None,
    ) -> DataFrame:
        """
        This implementation lets any ordered columns take priority over whatever is provided in `ascending`. Spark
        has irregular behavior and can result in runtime errors. Users shouldn't be mixing the two anyways so this
        is unlikely to come up.
        """
        columns = self._ensure_and_normalize_cols(cols)
        pre_ordered_col_indexes = [
            x
            for x in [
                i if isinstance(col.expression, exp.Ordered) else None
                for i, col in enumerate(columns)
            ]
            if x is not None
        ]
        if ascending is None:
            ascending = [True] * len(columns)
        elif not isinstance(ascending, list):
            ascending = [ascending] * len(columns)
        ascending = [bool(x) for i, x in enumerate(ascending)]
        assert len(columns) == len(
            ascending
        ), "The length of items in ascending must equal the number of columns provided"
        col_and_ascending = list(zip(columns, ascending))
        order_by_columns = [
            exp.Ordered(this=col.expression, desc=not asc)
            if i not in pre_ordered_col_indexes
            else columns[i].column_expression
            for i, (col, asc) in enumerate(col_and_ascending)
        ]
        return self.copy(expression=self.expression.order_by(*order_by_columns))

    sort = orderBy

    @operation(Operation.FROM)
    def union(self, other: DataFrame) -> DataFrame:
        return self._set_operation(exp.Union, other, False)

    unionAll = union

    @operation(Operation.FROM)
    def unionByName(self, other: DataFrame, allowMissingColumns: bool = False):
        l_columns = self.columns
        r_columns = other.columns
        if not allowMissingColumns:
            l_expressions = l_columns
            r_expressions = l_columns
        else:
            l_expressions = []
            r_expressions = []
            r_columns_unused = copy(r_columns)
            for l_column in l_columns:
                l_expressions.append(l_column)
                if l_column in r_columns:
                    r_expressions.append(l_column)
                    r_columns_unused.remove(l_column)
                else:
                    r_expressions.append(exp.alias_(exp.Null(), l_column))
            for r_column in r_columns_unused:
                l_expressions.append(exp.alias_(exp.Null(), r_column))
                r_expressions.append(r_column)
        r_df = (
            other.copy()._convert_leaf_to_cte().select(*self._ensure_list_of_columns(r_expressions))
        )
        l_df = self.copy()
        if allowMissingColumns:
            l_df = l_df._convert_leaf_to_cte().select(*self._ensure_list_of_columns(l_expressions))
        return l_df._set_operation(exp.Union, r_df, False)

    @operation(Operation.FROM)
    def intersect(self, other: DataFrame) -> DataFrame:
        return self._set_operation(exp.Intersect, other, True)

    @operation(Operation.FROM)
    def intersectAll(self, other: DataFrame) -> DataFrame:
        return self._set_operation(exp.Intersect, other, False)

    @operation(Operation.FROM)
    def exceptAll(self, other: DataFrame) -> DataFrame:
        return self._set_operation(exp.Except, other, False)

    @operation(Operation.SELECT)
    def distinct(self) -> DataFrame:
        return self.copy(expression=self.expression.distinct())

    @operation(Operation.SELECT)
    def dropDuplicates(self, subset: t.Optional[t.List[str]] = None):
        if not subset:
            return self.distinct()
        column_names = ensure_list(subset)
        window = Window.partitionBy(*column_names).orderBy(*column_names)
        return (
            self.copy()
            .withColumn("row_num", F.row_number().over(window))
            .where(F.col("row_num") == F.lit(1))
            .drop("row_num")
        )

    @operation(Operation.FROM)
    def dropna(
        self,
        how: str = "any",
        thresh: t.Optional[int] = None,
        subset: t.Optional[t.Union[str, t.Tuple[str, ...], t.List[str]]] = None,
    ) -> DataFrame:
        minimum_non_null = thresh or 0  # will be determined later if thresh is null
        new_df = self.copy()
        all_columns = self._get_outer_select_columns(new_df.expression)
        if subset:
            null_check_columns = self._ensure_and_normalize_cols(subset)
        else:
            null_check_columns = all_columns
        if thresh is None:
            minimum_num_nulls = 1 if how == "any" else len(null_check_columns)
        else:
            minimum_num_nulls = len(null_check_columns) - minimum_non_null + 1
        if minimum_num_nulls > len(null_check_columns):
            raise RuntimeError(
                f"The minimum num nulls for dropna must be less than or equal to the number of columns. "
                f"Minimum num nulls: {minimum_num_nulls}, Num Columns: {len(null_check_columns)}"
            )
        if_null_checks = [
            F.when(column.isNull(), F.lit(1)).otherwise(F.lit(0)) for column in null_check_columns
        ]
        nulls_added_together = functools.reduce(lambda x, y: x + y, if_null_checks)
        num_nulls = nulls_added_together.alias("num_nulls")
        new_df = new_df.select(num_nulls, append=True)
        filtered_df = new_df.where(F.col("num_nulls") < F.lit(minimum_num_nulls))
        final_df = filtered_df.select(*all_columns)
        return final_df

    @operation(Operation.FROM)
    def fillna(
        self,
        value: t.Union[ColumnLiterals],
        subset: t.Optional[t.Union[str, t.Tuple[str, ...], t.List[str]]] = None,
    ) -> DataFrame:
        """
        Functionality Difference: If you provide a value to replace a null and that type conflicts
        with the type of the column then PySpark will just ignore your replacement.
        This will try to cast them to be the same in some cases. So they won't always match.
        Best to not mix types so make sure replacement is the same type as the column

        Possibility for improvement: Use `typeof` function to get the type of the column
        and check if it matches the type of the value provided. If not then make it null.
        """
        from sqlglot.dataframe.sql.functions import lit

        values = None
        columns = None
        new_df = self.copy()
        all_columns = self._get_outer_select_columns(new_df.expression)
        all_column_mapping = {column.alias_or_name: column for column in all_columns}
        if isinstance(value, dict):
            values = value.values()
            columns = self._ensure_and_normalize_cols(list(value))
        if not columns:
            columns = self._ensure_and_normalize_cols(subset) if subset else all_columns
        if not values:
            values = [value] * len(columns)
        value_columns = [lit(value) for value in values]

        null_replacement_mapping = {
            column.alias_or_name: (
                F.when(column.isNull(), value).otherwise(column).alias(column.alias_or_name)
            )
            for column, value in zip(columns, value_columns)
        }
        null_replacement_mapping = {**all_column_mapping, **null_replacement_mapping}
        null_replacement_columns = [
            null_replacement_mapping[column.alias_or_name] for column in all_columns
        ]
        new_df = new_df.select(*null_replacement_columns)
        return new_df

    @operation(Operation.FROM)
    def replace(
        self,
        to_replace: t.Union[bool, int, float, str, t.List, t.Dict],
        value: t.Optional[t.Union[bool, int, float, str, t.List]] = None,
        subset: t.Optional[t.Collection[ColumnOrName] | ColumnOrName] = None,
    ) -> DataFrame:
        from sqlglot.dataframe.sql.functions import lit

        old_values = None
        new_df = self.copy()
        all_columns = self._get_outer_select_columns(new_df.expression)
        all_column_mapping = {column.alias_or_name: column for column in all_columns}

        columns = self._ensure_and_normalize_cols(subset) if subset else all_columns
        if isinstance(to_replace, dict):
            old_values = list(to_replace)
            new_values = list(to_replace.values())
        elif not old_values and isinstance(to_replace, list):
            assert isinstance(value, list), "value must be a list since the replacements are a list"
            assert len(to_replace) == len(
                value
            ), "the replacements and values must be the same length"
            old_values = to_replace
            new_values = value
        else:
            old_values = [to_replace] * len(columns)
            new_values = [value] * len(columns)
        old_values = [lit(value) for value in old_values]
        new_values = [lit(value) for value in new_values]

        replacement_mapping = {}
        for column in columns:
            expression = Column(None)
            for i, (old_value, new_value) in enumerate(zip(old_values, new_values)):
                if i == 0:
                    expression = F.when(column == old_value, new_value)
                else:
                    expression = expression.when(column == old_value, new_value)  # type: ignore
            replacement_mapping[column.alias_or_name] = expression.otherwise(column).alias(
                column.expression.alias_or_name
            )

        replacement_mapping = {**all_column_mapping, **replacement_mapping}
        replacement_columns = [replacement_mapping[column.alias_or_name] for column in all_columns]
        new_df = new_df.select(*replacement_columns)
        return new_df

    @operation(Operation.SELECT)
    def withColumn(self, colName: str, col: Column) -> DataFrame:
        col = self._ensure_and_normalize_col(col)
        existing_col_names = self.expression.named_selects
        existing_col_index = (
            existing_col_names.index(colName) if colName in existing_col_names else None
        )
        if existing_col_index:
            expression = self.expression.copy()
            expression.expressions[existing_col_index] = col.expression
            return self.copy(expression=expression)
        return self.copy().select(col.alias(colName), append=True)

    @operation(Operation.SELECT)
    def withColumnRenamed(self, existing: str, new: str):
        expression = self.expression.copy()
        existing_columns = [
            expression
            for expression in expression.expressions
            if expression.alias_or_name == existing
        ]
        if not existing_columns:
            raise ValueError("Tried to rename a column that doesn't exist")
        for existing_column in existing_columns:
            if isinstance(existing_column, exp.Column):
                existing_column.replace(exp.alias_(existing_column.copy(), new))
            else:
                existing_column.set("alias", exp.to_identifier(new))
        return self.copy(expression=expression)

    @operation(Operation.SELECT)
    def drop(self, *cols: t.Union[str, Column]) -> DataFrame:
        all_columns = self._get_outer_select_columns(self.expression)
        drop_cols = self._ensure_and_normalize_cols(cols)
        new_columns = [
            col
            for col in all_columns
            if col.alias_or_name not in [drop_column.alias_or_name for drop_column in drop_cols]
        ]
        return self.copy().select(*new_columns, append=False)

    @operation(Operation.LIMIT)
    def limit(self, num: int) -> DataFrame:
        return self.copy(expression=self.expression.limit(num))

    @operation(Operation.NO_OP)
    def hint(self, name: str, *parameters: t.Optional[t.Union[str, int]]) -> DataFrame:
        parameter_list = ensure_list(parameters)
        parameter_columns = (
            self._ensure_list_of_columns(parameter_list)
            if parameters
            else Column.ensure_cols([self.sequence_id])
        )
        return self._hint(name, parameter_columns)

    @operation(Operation.NO_OP)
    def repartition(
        self, numPartitions: t.Union[int, ColumnOrName], *cols: ColumnOrName
    ) -> DataFrame:
        num_partition_cols = self._ensure_list_of_columns(numPartitions)
        columns = self._ensure_and_normalize_cols(cols)
        args = num_partition_cols + columns
        return self._hint("repartition", args)

    @operation(Operation.NO_OP)
    def coalesce(self, numPartitions: int) -> DataFrame:
        num_partitions = Column.ensure_cols([numPartitions])
        return self._hint("coalesce", num_partitions)

    @operation(Operation.NO_OP)
    def cache(self) -> DataFrame:
        return self._cache(storage_level="MEMORY_AND_DISK")

    @operation(Operation.NO_OP)
    def persist(self, storageLevel: str = "MEMORY_AND_DISK_SER") -> DataFrame:
        """
        Storage Level Options: https://spark.apache.org/docs/3.0.0-preview/sql-ref-syntax-aux-cache-cache-table.html
        """
        return self._cache(storageLevel)


class DataFrameNaFunctions:
    def __init__(self, df: DataFrame):
        self.df = df

    def drop(
        self,
        how: str = "any",
        thresh: t.Optional[int] = None,
        subset: t.Optional[t.Union[str, t.Tuple[str, ...], t.List[str]]] = None,
    ) -> DataFrame:
        return self.df.dropna(how=how, thresh=thresh, subset=subset)

    def fill(
        self,
        value: t.Union[int, bool, float, str, t.Dict[str, t.Any]],
        subset: t.Optional[t.Union[str, t.Tuple[str, ...], t.List[str]]] = None,
    ) -> DataFrame:
        return self.df.fillna(value=value, subset=subset)

    def replace(
        self,
        to_replace: t.Union[bool, int, float, str, t.List, t.Dict],
        value: t.Optional[t.Union[bool, int, float, str, t.List]] = None,
        subset: t.Optional[t.Union[str, t.List[str]]] = None,
    ) -> DataFrame:
        return self.df.replace(to_replace=to_replace, value=value, subset=subset)
