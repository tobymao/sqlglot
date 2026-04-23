from __future__ import annotations

import functools
import logging
import typing as t

from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.helper import (
    ensure_list,
    is_date_unit,
    is_iso_date,
    is_iso_datetime,
    seq_get,
)
from sqlglot.optimizer.scope import Scope, traverse_scope
from sqlglot.schema import MappingSchema, Schema, ensure_schema

if t.TYPE_CHECKING:
    from sqlglot._typing import B, E

    # TODO (mypyc): should be -> exp.DType but some coercion lambdas return DataType
    # (e.g. l.type). The original code used t.cast(exp.DType, ...) to satisfy mypy, but
    # mypyc enforces t.cast at runtime, rejecting DataType values. Widened to accept both.
    BinaryCoercionFunc = t.Callable[
        [exp.Expr, exp.Expr], t.Optional[t.Union[exp.DataType, exp.DType]]
    ]
    BinaryCoercions = dict[tuple[exp.DType, exp.DType], BinaryCoercionFunc]

    from sqlglot.dialects.dialect import DialectType
    from sqlglot.typing import ExprMetadataType

logger = logging.getLogger("sqlglot")

# EXTRACT/DATE_PART specifiers that return BIGINT instead of INT
BIGINT_EXTRACT_DATE_PARTS = {
    "EPOCH_SECOND",
    "EPOCH_MILLISECOND",
    "EPOCH_MICROSECOND",
    "EPOCH_NANOSECOND",
    "NANOSECOND",
}


def annotate_types(
    expression: E,
    schema: dict[str, object] | Schema | None = None,
    expression_metadata: ExprMetadataType | None = None,
    coerces_to: dict[exp.DType, set[exp.DType]] | None = None,
    dialect: DialectType = None,
    overwrite_types: bool = True,
) -> E:
    """
    Infers the types of an expression, annotating its AST accordingly.

    Example:
        >>> import sqlglot
        >>> schema = {"y": {"cola": "SMALLINT"}}
        >>> sql = "SELECT x.cola + 2.5 AS cola FROM (SELECT y.cola AS cola FROM y AS y) AS x"
        >>> annotated_expr = annotate_types(sqlglot.parse_one(sql), schema=schema)
        >>> annotated_expr.expressions[0].type.this  # Get the type of "x.cola + 2.5 AS cola"
        <DType.DOUBLE: 'DOUBLE'>

    Args:
        expression: Expr to annotate.
        schema: Database schema.
        expression_metadata: Maps expression type to corresponding annotation function.
        coerces_to: Maps expression type to set of types that it can be coerced into.
        overwrite_types: Re-annotate the existing AST types.

    Returns:
        The expression annotated with types.
    """

    schema = ensure_schema(schema, dialect=dialect)

    return TypeAnnotator(
        schema=schema,
        expression_metadata=expression_metadata,
        coerces_to=coerces_to,
        overwrite_types=overwrite_types,
    ).annotate(expression)


def _coerce_date_literal(l: exp.Expr, unit: exp.Expr | None) -> exp.DType:
    date_text = l.name
    is_iso_date_ = is_iso_date(date_text)

    if is_iso_date_ and is_date_unit(unit):
        return exp.DType.DATE

    # An ISO date is also an ISO datetime, but not vice versa
    if is_iso_date_ or is_iso_datetime(date_text):
        return exp.DType.DATETIME

    return exp.DType.UNKNOWN


def _coerce_date(l: exp.Expr, unit: exp.Expr | None) -> exp.DType:
    if not is_date_unit(unit):
        return exp.DType.DATETIME
    return l.type.this if l.type else exp.DType.UNKNOWN


def swap_args(func: BinaryCoercionFunc) -> BinaryCoercionFunc:
    @functools.wraps(func)
    def _swapped(l: exp.Expr, r: exp.Expr) -> exp.DataType | exp.DType | None:
        return func(r, l)

    return _swapped


def swap_all(coercions: BinaryCoercions) -> BinaryCoercions:
    return {**coercions, **{(b, a): swap_args(func) for (a, b), func in coercions.items()}}


def _build_coerces_to() -> dict[exp.DType, set[exp.DType]]:
    # Highest-to-lowest type precedence, as specified in Spark's docs (ANSI):
    # https://spark.apache.org/docs/3.2.0/sql-ref-ansi-compliance.html
    text_precedence = (
        exp.DType.TEXT,
        exp.DType.NVARCHAR,
        exp.DType.VARCHAR,
        exp.DType.NCHAR,
        exp.DType.CHAR,
    )
    numeric_precedence = (
        exp.DType.DECFLOAT,
        exp.DType.DOUBLE,
        exp.DType.FLOAT,
        exp.DType.BIGDECIMAL,
        exp.DType.DECIMAL,
        exp.DType.BIGINT,
        exp.DType.INT,
        exp.DType.SMALLINT,
        exp.DType.TINYINT,
    )
    timelike_precedence = (
        exp.DType.TIMESTAMPLTZ,
        exp.DType.TIMESTAMPTZ,
        exp.DType.TIMESTAMP,
        exp.DType.DATETIME,
        exp.DType.DATE,
    )

    result: dict[exp.DType, set[exp.DType]] = {}
    for type_precedence in (text_precedence, numeric_precedence, timelike_precedence):
        coerces_to: set[exp.DType] = set()
        for data_type in type_precedence:
            result[data_type] = coerces_to.copy()
            coerces_to |= {data_type}
    return result


_COERCES_TO = _build_coerces_to()


class TypeAnnotator:
    NESTED_TYPES: t.ClassVar = {
        exp.DType.ARRAY,
    }

    # Specifies what types a given type can be coerced into
    COERCES_TO: t.ClassVar[dict[exp.DType, set[exp.DType]]] = _COERCES_TO

    # Coercion functions for binary operations.
    # Map of type pairs to a callable that takes both sides of the binary operation and returns the resulting type.
    BINARY_COERCIONS: t.ClassVar = {
        **swap_all(
            {
                (t, exp.DType.INTERVAL): lambda l, r: _coerce_date_literal(l, r.args.get("unit"))
                for t in exp.DataType.TEXT_TYPES
            }
        ),
        **swap_all(
            {
                # text + numeric will yield the numeric type to match most dialects' semantics
                (text, numeric): lambda l, r: (
                    l.type if l.type in exp.DataType.NUMERIC_TYPES else r.type
                )
                for text in exp.DataType.TEXT_TYPES
                for numeric in exp.DataType.NUMERIC_TYPES
            }
        ),
        **swap_all(
            {
                (exp.DType.DATE, exp.DType.INTERVAL): lambda l, r: _coerce_date(
                    l, r.args.get("unit")
                ),
            }
        ),
    }

    def __init__(
        self,
        schema: Schema,
        expression_metadata: ExprMetadataType | None = None,
        coerces_to: dict[exp.DType, set[exp.DType]] | None = None,
        binary_coercions: BinaryCoercions | None = None,
        overwrite_types: bool = True,
    ) -> None:
        self.schema = schema
        dialect = schema.dialect or Dialect()
        self.dialect = dialect
        self.expression_metadata = expression_metadata or dialect.EXPRESSION_METADATA
        self.coerces_to = coerces_to or dialect.COERCES_TO or self.COERCES_TO
        self.binary_coercions = binary_coercions or self.BINARY_COERCIONS

        # Caches the ids of annotated sub-Exprs, to ensure we only visit them once
        self._visited: set[int] = set()

        # Caches NULL-annotated expressions to set them to UNKNOWN after type inference is completed
        self._null_expressions: dict[int, exp.Expr] = {}

        # Databricks and Spark ≥v3 actually support NULL (i.e., VOID) as a type
        self._supports_null_type = dialect.SUPPORTS_NULL_TYPE

        # Maps an exp.SetOperation's id (e.g. UNION) to its projection types. This is computed if the
        # exp.SetOperation is the expression of a scope source, as selecting from it multiple times
        # would reprocess the entire subtree to coerce the types of its operands' projections
        self._setop_column_types: dict[int, dict[str, exp.DataType | exp.DType]] = {}

        # When set to False, this enables partial annotation by skipping already-annotated nodes
        self._overwrite_types = overwrite_types

        # Maps Scope to its corresponding selected sources
        self._scope_selects: dict[Scope, dict[str, dict[str, t.Any]]] = {}

    def clear(self) -> None:
        self._visited.clear()
        self._null_expressions.clear()
        self._setop_column_types.clear()
        self._scope_selects.clear()

    # TODO (mypyc): should be expression: E -> E but mypyc resolves the TypeVar
    # to the isinstance-narrowed type, causing runtime type check failures.
    def _set_type(
        self, expression: exp.Expr, target_type: exp.DataType | exp.DType | None
    ) -> exp.Expr:
        prev_type = expression.type
        expression_id = id(expression)

        # TODO (mypyc): expression.type = ... should work but mypyc compiles the property
        # setter to enforce the getter's return type (Optional[DataType]), rejecting DType.
        # Bypass by converting and assigning to _type directly.
        dtype = target_type or exp.DType.UNKNOWN
        expression._type = dtype if isinstance(dtype, exp.DataType) else dtype.into_expr()
        self._visited.add(expression_id)

        if (
            not self._supports_null_type
            and t.cast(exp.DataType, expression.type).this == exp.DType.NULL
        ):
            self._null_expressions[expression_id] = expression
        elif prev_type and t.cast(exp.DataType, prev_type).this == exp.DType.NULL:
            self._null_expressions.pop(expression_id, None)

        return expression

    def annotate(self, expression: E, annotate_scope: bool = True) -> E:
        # This flag is used to avoid costly scope traversals when we only care about annotating
        # non-column expressions (partial type inference), e.g., when simplifying in the optimizer
        if annotate_scope:
            for scope in traverse_scope(expression):
                self.annotate_scope(scope)

        # This takes care of non-traversable expressions
        self._annotate_expression(expression)

        # Replace NULL type with the default type of the targeted dialect, since the former is not an actual type;
        # it is mostly used to aid type coercion, e.g. in query set operations.
        # TODO (mypyc): uses list() + _set_type instead of direct expr.type = ... because
        # mypyc's property setter bypass rejects DType, and _set_type modifies the dict.
        for expr in list(self._null_expressions.values()):
            self._set_type(expr, self.dialect.DEFAULT_NULL_TYPE)

        return expression

    def _get_scope_selects(self, scope: Scope) -> dict[str, dict[str, t.Any]]:
        if scope not in self._scope_selects:
            selects = {}
            for name, source in scope.sources.items():
                if not isinstance(source, Scope):
                    continue

                expression = source.expression
                if isinstance(expression, exp.UDTF):
                    values = []

                    if isinstance(expression, exp.Lateral):
                        if isinstance(expression.this, exp.Explode):
                            values = [expression.this.this]
                    elif isinstance(expression, exp.Unnest):
                        values = [expression]
                    elif not isinstance(expression, exp.TableFromRows):
                        values = expression.expressions[0].expressions

                    if not values:
                        continue

                    alias_column_names = expression.alias_column_names

                    if isinstance(expression, exp.Unnest):
                        exp_type = expression.type
                    elif isinstance(expression, exp.Lateral) and isinstance(
                        expression.this, exp.Explode
                    ):
                        exp_type = expression.this.type
                    else:
                        exp_type = None

                    struct_type = (
                        exp_type if exp_type and exp_type.is_type(exp.DType.STRUCT) else None
                    )

                    if struct_type:
                        selects[name] = {
                            col_def.name: t.cast(t.Union[exp.DataType, exp.DType], col_def.kind)
                            for col_def in struct_type.expressions
                            if isinstance(col_def, exp.ColumnDef) and col_def.kind
                        }
                    else:
                        selects[name] = {
                            alias: column.type for alias, column in zip(alias_column_names, values)
                        }
                elif isinstance(expression, exp.SetOperation) and len(
                    expression.left.selects
                ) == len(expression.right.selects):
                    selects[name] = self._get_setop_column_types(expression)
                elif isinstance(expression, exp.Selectable):
                    selects[name] = {s.alias_or_name: s.type for s in expression.selects if s.type}

            for pivot in scope.pivots:
                pivot_source = scope.sources.get(pivot.alias)
                if not pivot_source:
                    continue

                inner_name = (
                    pivot_source.name if isinstance(pivot_source, exp.Table) else pivot.alias
                )
                col_types = dict(selects.get(inner_name, {}))

                if pivot.unpivot:
                    for field in pivot.fields:
                        field_col = field.this

                        first = seq_get(field.expressions, 0)
                        if not first:
                            continue

                        is_pivot_alias = isinstance(first, exp.PivotAlias)

                        # FOR column type from the alias literal, or VARCHAR if no alias
                        if is_pivot_alias:
                            alias_node = first.args.get("alias")
                            if alias_node:
                                col_types[field_col.name] = alias_node.type
                        else:
                            col_types[field_col.name] = exp.DataType.build(
                                "VARCHAR", dialect=self.dialect
                            )

                        # Value column types from the IN source columns
                        src = first.this if is_pivot_alias else first
                        src_cols = src.expressions if isinstance(src, exp.Tuple) else [src]
                        for val_expr in pivot.expressions:
                            val_cols = (
                                val_expr.expressions
                                if isinstance(val_expr, exp.Tuple)
                                else [val_expr]
                            )
                            for val_col, src_col in zip(val_cols, src_cols):
                                src_type = col_types.get(src_col.output_name) or src_col.type
                                if isinstance(src_type, exp.DataType) and not src_type.is_type(
                                    exp.DType.UNKNOWN
                                ):
                                    col_types[val_col.output_name] = src_type

                if col_types:
                    selects[pivot.alias] = col_types

            self._scope_selects[scope] = selects

        return self._scope_selects[scope]

    def annotate_scope(self, scope: Scope) -> None:
        if isinstance(self.schema, MappingSchema):
            for table_column in scope.table_columns:
                source = scope.sources.get(table_column.name)

                if isinstance(source, exp.Table):
                    schema = self.schema.find(
                        source, raise_on_missing=False, ensure_data_types=True
                    )
                    if not isinstance(schema, dict):
                        continue

                    struct_type = exp.DataType(
                        this=exp.DType.STRUCT,
                        expressions=[
                            exp.ColumnDef(this=exp.to_identifier(str(c)), kind=kind)
                            for c, kind in schema.items()
                        ],
                        nested=True,
                    )
                    self._set_type(table_column, struct_type)
                elif (
                    isinstance(source, Scope)
                    and isinstance(source.expression, exp.Query)
                    and (
                        source.expression.meta.get("query_type") or exp.DType.UNKNOWN.into_expr()
                    ).is_type(exp.DType.STRUCT)
                ):
                    self._set_type(table_column, source.expression.meta["query_type"])

        # Iterate through all the expressions of the current scope in post-order, and annotate
        self._annotate_expression(scope.expression, scope)
        self._fixup_order_by_aliases(scope)

        if self.dialect.QUERY_RESULTS_ARE_STRUCTS and isinstance(scope.expression, exp.Query):
            struct_type = exp.DataType(
                this=exp.DType.STRUCT,
                expressions=[
                    exp.ColumnDef(
                        this=exp.to_identifier(select.output_name),
                        kind=select.type.copy() if select.type else None,
                    )
                    for select in scope.expression.selects
                ],
                nested=True,
            )

            if not any(
                cd.kind.is_type(exp.DType.UNKNOWN) for cd in struct_type.expressions if cd.kind
            ):
                # We don't use `_set_type` on purpose here. If we annotated the query directly, then
                # using it in other contexts (e.g., ARRAY(<query>)) could result in incorrect type
                # annotations, i.e., it shouldn't be interpreted as a STRUCT value.
                scope.expression.meta["query_type"] = struct_type

    def _annotate_expression(
        self,
        expression: exp.Expr,
        scope: Scope | None = None,
    ) -> None:
        stack = [(expression, False)]

        while stack:
            expr, children_annotated = stack.pop()

            if id(expr) in self._visited or (
                not self._overwrite_types and expr.type and not expr.is_type(exp.DType.UNKNOWN)
            ):
                continue  # We've already inferred the expression's type

            if not children_annotated:
                stack.append((expr, True))
                for child_expr in expr.iter_expressions():
                    stack.append((child_expr, False))
                continue

            if scope and isinstance(expr, exp.Column) and expr.table:
                source = None
                source_scope: Scope | None = scope
                while source_scope and not source:
                    source = source_scope.sources.get(expr.table)
                    if not source:
                        source_scope = source_scope.parent

                if isinstance(source, exp.Table):
                    schema_type = self.schema.get_column_type(source, expr)
                    if schema_type.is_type(exp.DType.UNKNOWN) and source.args.get("pivots"):
                        pivot_type = (
                            self._get_scope_selects(scope).get(expr.table, {}).get(expr.name)
                        )
                        if pivot_type:
                            schema_type = pivot_type
                    self._set_type(expr, schema_type)
                elif source and source_scope:
                    col_type = (
                        self._get_scope_selects(source_scope).get(expr.table, {}).get(expr.name)
                    )
                    if col_type:
                        self._set_type(expr, col_type)
                    elif isinstance(source.expression, exp.Unnest):
                        self._set_type(expr, source.expression.type)
                    else:
                        self._set_type(expr, exp.DType.UNKNOWN)
                else:
                    self._set_type(expr, exp.DType.UNKNOWN)

                if expr.is_type(exp.DType.JSON) and (dot_parts := expr.meta.get("dot_parts")):
                    # JSON dot access is case sensitive across all dialects, so we need to undo the normalization.
                    i = iter(dot_parts)
                    parent = expr.parent
                    while isinstance(parent, exp.Dot):
                        parent.expression.replace(exp.to_identifier(next(i), quoted=True))
                        parent = parent.parent

                    expr.meta.pop("dot_parts", None)

                if expr.type and expr.type.args.get("nullable") is False:
                    expr.meta["nonnull"] = True
                continue

            spec = self.expression_metadata.get(expr.__class__)

            if spec and (annotator := spec.get("annotator")):
                annotator(self, expr)
            elif spec and (returns := spec.get("returns")):
                self._set_type(expr, returns)
            else:
                self._set_type(expr, exp.DType.UNKNOWN)

    def _fixup_order_by_aliases(self, scope: Scope) -> None:
        query = scope.expression
        if not isinstance(query, exp.Query):
            return

        order = query.args.get("order")
        if not order:
            return

        # Build alias -> type map from fully-annotated projections (last match wins,
        # consistent with how _expand_alias_refs handles duplicate aliases).
        alias_types: dict[str, exp.DataType | exp.DType] = {}
        for sel in query.selects:
            if (
                isinstance(sel, exp.Alias)
                and sel.this.type
                and not sel.this.is_type(exp.DType.UNKNOWN)
            ):
                alias_types[sel.alias] = sel.this.type

        if not alias_types:
            return

        for ordered in order.expressions:
            alias_cols = [
                c for c in ordered.find_all(exp.Column) if not c.table and c.name in alias_types
            ]
            for col in alias_cols:
                self._set_type(col, alias_types[col.name])

            if alias_cols:
                for node in ordered.walk(prune=lambda n: isinstance(n, exp.Subquery)):
                    if not isinstance(node, (exp.Column, exp.Literal)):
                        self._visited.discard(id(node))
                self._annotate_expression(ordered, scope)

    def _maybe_coerce(
        self,
        type1: exp.DataType | exp.DType,
        type2: exp.DataType | exp.DType,
    ) -> exp.DataType | exp.DType:
        """
        Returns type2 if type1 can be coerced into it, otherwise type1.

        If either type is parameterized (e.g. DECIMAL(18, 2) contains two parameters),
        we assume type1 does not coerce into type2, so we also return it in this case.
        """
        if isinstance(type1, exp.DataType):
            if type1.expressions:
                return type1
            type1_value = type1.this
        else:
            type1_value = type1

        if isinstance(type2, exp.DataType):
            if type2.expressions:
                return type2
            type2_value = type2.this
        else:
            type2_value = type2

        # We propagate the UNKNOWN type upwards if found
        if exp.DType.UNKNOWN in (type1_value, type2_value):
            return exp.DType.UNKNOWN

        if type1_value == exp.DType.NULL:
            return type2_value
        if type2_value == exp.DType.NULL:
            return type1_value

        return type2_value if type2_value in self.coerces_to.get(type1_value, {}) else type1_value

    def _get_setop_column_types(
        self, setop: exp.SetOperation
    ) -> dict[str, exp.DataType | exp.DType]:
        """
        Computes and returns the coerced column types for a SetOperation.

        This handles UNION, INTERSECT, EXCEPT, etc., coercing types across
        left and right operands for all projections/columns.

        Args:
            setop: The SetOperation expression to analyze

        Returns:
            Dictionary mapping column names to their coerced types
        """
        setop_id = id(setop)
        if setop_id in self._setop_column_types:
            return self._setop_column_types[setop_id]

        col_types: dict[str, exp.DataType | exp.DType] = {}

        # Validate that left and right have same number of projections
        if not (
            isinstance(setop, exp.SetOperation)
            and setop.left.selects
            and setop.right.selects
            and len(setop.left.selects) == len(setop.right.selects)
        ):
            return col_types

        # Process a chain / sub-tree of set operations
        for set_op in setop.walk(
            prune=lambda n: not isinstance(n, (exp.SetOperation, exp.Subquery))
        ):
            if not isinstance(set_op, exp.SetOperation):
                continue

            if set_op.args.get("by_name"):
                r_type_by_select = {s.alias_or_name: s.type for s in set_op.right.selects}
                setop_cols = {
                    s.alias_or_name: self._maybe_coerce(
                        t.cast(exp.DataType, s.type),
                        r_type_by_select.get(s.alias_or_name) or exp.DType.UNKNOWN,
                    )
                    for s in set_op.left.selects
                }
            else:
                setop_cols = {
                    ls.alias_or_name: self._maybe_coerce(
                        t.cast(exp.DataType, ls.type), t.cast(exp.DataType, rs.type)
                    )
                    for ls, rs in zip(set_op.left.selects, set_op.right.selects)
                }

            # Coerce intermediate results with the previously registered types, if they exist
            for col_name, col_type in setop_cols.items():
                col_types[col_name] = self._maybe_coerce(
                    col_type, col_types.get(col_name, exp.DType.NULL)
                )

        self._setop_column_types[setop_id] = col_types
        return col_types

    def _annotate_binary(self, expression: B) -> B:
        left, right = expression.left, expression.right
        if not left or not right:
            expression_sql = expression.sql(self.dialect)
            logger.warning(f"Failed to annotate badly formed binary expression: {expression_sql}")
            self._set_type(expression, None)
            return expression

        left_type, right_type = left.type.this, right.type.this  # type: ignore

        # TODO (mypyc): should be isinstance(expression, (exp.Connector, exp.Predicate)) but
        # mypyc narrows the variable to the first type in a tuple/or isinstance check when
        # the types are sibling @trait classes, rejecting instances of the second type.
        if issubclass(type(expression), (exp.Connector, exp.Predicate)):
            self._set_type(expression, exp.DType.BOOLEAN)
        elif (left_type, right_type) in self.binary_coercions:
            self._set_type(expression, self.binary_coercions[(left_type, right_type)](left, right))
        else:
            self._annotate_by_args(expression, left, right)

        if isinstance(expression, exp.Is) or (
            left.meta.get("nonnull") is True and right.meta.get("nonnull") is True
        ):
            expression.meta["nonnull"] = True

        return expression

    def _annotate_unary(self, expression: E) -> E:
        if isinstance(expression, exp.Not):
            self._set_type(expression, exp.DType.BOOLEAN)
        else:
            self._set_type(expression, expression.this.type)

        if expression.this.meta.get("nonnull") is True:
            expression.meta["nonnull"] = True

        return expression

    def _annotate_literal(self, expression: exp.Literal) -> exp.Literal:
        if expression.is_string:
            self._set_type(expression, exp.DType.VARCHAR)
        elif expression.is_int:
            self._set_type(expression, exp.DType.INT)
        else:
            self._set_type(expression, exp.DType.DOUBLE)

        expression.meta["nonnull"] = True

        return expression

    def _annotate_by_args(
        self,
        expression: E,
        *args: str | exp.Expr,
        promote: bool = False,
        array: bool = False,
    ) -> E:
        literal_type = None
        non_literal_type = None
        nested_type = None

        for arg in args:
            if isinstance(arg, str):
                expressions = expression.args.get(arg)
            else:
                expressions = arg

            for expr in ensure_list(expressions):
                expr_type = expr.type

                if expr_type.is_type(exp.DType.UNKNOWN):
                    self._set_type(expression, exp.DType.UNKNOWN)
                    return expression

                if nested_type:
                    continue

                # Stop coercing at the first nested data type found
                if expr_type.args.get("nested"):
                    nested_type = expr_type
                elif isinstance(expr, exp.Literal):
                    literal_type = self._maybe_coerce(literal_type or expr_type, expr_type)
                else:
                    non_literal_type = self._maybe_coerce(non_literal_type or expr_type, expr_type)

        result_type = None

        if nested_type:
            result_type = nested_type
        elif literal_type and non_literal_type:
            if self.dialect.PRIORITIZE_NON_LITERAL_TYPES:
                literal_this_type = (
                    literal_type.this if isinstance(literal_type, exp.DataType) else literal_type
                )
                non_literal_this_type = (
                    non_literal_type.this
                    if isinstance(non_literal_type, exp.DataType)
                    else non_literal_type
                )
                if (
                    literal_this_type in exp.DataType.INTEGER_TYPES
                    and non_literal_this_type in exp.DataType.INTEGER_TYPES
                ) or (
                    literal_this_type in exp.DataType.REAL_TYPES
                    and non_literal_this_type in exp.DataType.REAL_TYPES
                ):
                    result_type = non_literal_type
        else:
            result_type = literal_type or non_literal_type or exp.DType.UNKNOWN

        self._set_type(
            expression,
            result_type or self._maybe_coerce(non_literal_type, literal_type),  # type: ignore
        )

        if promote:
            if expression.type.this in exp.DataType.INTEGER_TYPES:  # type: ignore
                self._set_type(expression, exp.DType.BIGINT)
            elif expression.type.this in exp.DataType.FLOAT_TYPES:  # type: ignore
                self._set_type(expression, exp.DType.DOUBLE)

        if array:
            self._set_type(
                expression,
                exp.DataType(this=exp.DType.ARRAY, expressions=[expression.type], nested=True),
            )

        return expression

    def _annotate_timeunit(
        self, expression: exp.TimeUnit | exp.DateTrunc
    ) -> exp.TimeUnit | exp.DateTrunc:
        if expression.this.type.this in exp.DataType.TEXT_TYPES:
            datatype = _coerce_date_literal(expression.this, expression.unit)
        elif expression.this.type.this in exp.DataType.TEMPORAL_TYPES:
            datatype = _coerce_date(expression.this, expression.unit)
        else:
            datatype = exp.DType.UNKNOWN

        self._set_type(expression, datatype)
        return expression

    def _annotate_bracket(self, expression: exp.Bracket) -> exp.Bracket:
        bracket_arg = expression.expressions[0]
        this = expression.this

        if isinstance(bracket_arg, exp.Slice):
            self._set_type(expression, this.type)
        elif this.type.is_type(exp.DType.ARRAY):
            self._set_type(expression, seq_get(this.type.expressions, 0))
        elif isinstance(this, (exp.Map, exp.VarMap)) and bracket_arg in this.keys:
            index = this.keys.index(bracket_arg)
            value = seq_get(this.values, index)
            self._set_type(expression, value.type if value else None)
        else:
            self._set_type(expression, exp.DType.UNKNOWN)

        return expression

    def _annotate_div(self, expression: exp.Div) -> exp.Div:
        left_type, right_type = expression.left.type.this, expression.right.type.this  # type: ignore

        if (
            expression.args.get("typed")
            and left_type in exp.DataType.INTEGER_TYPES
            and right_type in exp.DataType.INTEGER_TYPES
        ):
            self._set_type(expression, exp.DType.BIGINT)
        else:
            self._set_type(expression, self._maybe_coerce(left_type, right_type))
            if expression.type and expression.type.this not in exp.DataType.REAL_TYPES:
                self._set_type(expression, self._maybe_coerce(expression.type, exp.DType.DOUBLE))

        return expression

    def _annotate_dot(self, expression: exp.Dot) -> exp.Dot:
        self._set_type(expression, None)

        # Propagate type from qualified UDF calls (e.g., db.my_udf(...))
        if isinstance(expression.expression, exp.Anonymous):
            self._set_type(expression, expression.expression.type)
            return expression

        this_type = expression.this.type

        if this_type and this_type.is_type(exp.DType.STRUCT):
            for e in this_type.expressions:
                if e.name == expression.expression.name:
                    self._set_type(expression, e.kind)
                    break

        return expression

    def _annotate_explode(self, expression: exp.Explode) -> exp.Explode:
        self._set_type(expression, seq_get(expression.this.type.expressions, 0))
        return expression

    def _annotate_unnest(self, expression: exp.Unnest) -> exp.Unnest:
        child = seq_get(expression.expressions, 0)

        if child and child.is_type(exp.DType.ARRAY):
            expr_type = seq_get(child.type.expressions, 0)
        else:
            expr_type = None

        self._set_type(expression, expr_type)
        return expression

    def _annotate_subquery(self, expression: exp.Subquery) -> exp.Subquery:
        # For scalar subqueries (subqueries with a single projection), infer the type
        # from that single projection. This allows type propagation in cases like:
        # SELECT (SELECT 1 AS c) AS c
        query = expression.unnest()

        if isinstance(query, exp.Query):
            selects = query.selects
            if len(selects) == 1:
                self._set_type(expression, selects[0].type)
                return expression

        self._set_type(expression, exp.DType.UNKNOWN)
        return expression

    def _annotate_struct_value(self, expression: exp.Expr) -> exp.DataType | None | exp.ColumnDef:
        # Case: STRUCT(key AS value)
        this: exp.Expr | None = None
        kind = expression.type

        if alias := expression.args.get("alias"):
            this = alias.copy()
        elif expression.expression:
            # Case: STRUCT(key = value) or STRUCT(key := value)
            this = expression.this.copy()
            kind = expression.expression.type
        elif isinstance(expression, exp.Column):
            # Case: STRUCT(c)
            this = expression.this.copy()

        if kind and kind.is_type(exp.DType.UNKNOWN):
            return None

        if this:
            return exp.ColumnDef(this=this, kind=kind)

        return kind

    def _annotate_struct(self, expression: exp.Struct) -> exp.Struct:
        expressions = []
        for expr in expression.expressions:
            struct_field_type = self._annotate_struct_value(expr)
            if struct_field_type is None:
                self._set_type(expression, None)
                return expression

            expressions.append(struct_field_type)

        self._set_type(
            expression,
            exp.DataType(this=exp.DType.STRUCT, expressions=expressions, nested=True),
        )
        return expression

    @t.overload
    def _annotate_map(self, expression: exp.Map) -> exp.Map: ...

    @t.overload
    def _annotate_map(self, expression: exp.VarMap) -> exp.VarMap: ...

    def _annotate_map(self, expression):
        keys = expression.args.get("keys")
        values = expression.args.get("values")

        map_type = exp.DataType(this=exp.DType.MAP)
        if isinstance(keys, exp.Array) and isinstance(values, exp.Array):
            key_type = seq_get(keys.type.expressions, 0) or exp.DType.UNKNOWN
            value_type = seq_get(values.type.expressions, 0) or exp.DType.UNKNOWN

            if key_type != exp.DType.UNKNOWN and value_type != exp.DType.UNKNOWN:
                map_type.set("expressions", [key_type, value_type])
                map_type.set("nested", True)

        self._set_type(expression, map_type)
        return expression

    def _annotate_to_map(self, expression: exp.ToMap) -> exp.ToMap:
        map_type = exp.DataType(this=exp.DType.MAP)
        arg = expression.this
        if arg.is_type(exp.DType.STRUCT):
            for coldef in arg.type.expressions:
                kind = coldef.kind
                if kind != exp.DType.UNKNOWN:
                    map_type.set("expressions", [exp.DType.VARCHAR.into_expr(), kind])
                    map_type.set("nested", True)
                    break

        self._set_type(expression, map_type)
        return expression

    def _annotate_extract(self, expression: exp.Extract) -> exp.Extract:
        part = expression.name
        if part == "TIME":
            self._set_type(expression, exp.DType.TIME)
        elif part == "DATE":
            self._set_type(expression, exp.DType.DATE)
        elif part in BIGINT_EXTRACT_DATE_PARTS:
            self._set_type(expression, exp.DType.BIGINT)
        else:
            self._set_type(expression, exp.DType.INT)
        return expression

    def _annotate_by_array_element(self, expression: exp.Expr) -> exp.Expr:
        array_arg = expression.this
        if array_arg.type.is_type(exp.DType.ARRAY):
            element_type = seq_get(array_arg.type.expressions, 0) or exp.DType.UNKNOWN
            self._set_type(expression, element_type)
        else:
            self._set_type(expression, exp.DType.UNKNOWN)

        return expression
