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

    BinaryCoercionFunc = t.Callable[[exp.Expression, exp.Expression], exp.DataType.Type]
    BinaryCoercions = t.Dict[
        t.Tuple[exp.DataType.Type, exp.DataType.Type],
        BinaryCoercionFunc,
    ]

    from sqlglot.dialects.dialect import DialectType
    from sqlglot.typing import ExpressionMetadataType

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
    schema: t.Optional[t.Dict | Schema] = None,
    expression_metadata: t.Optional[ExpressionMetadataType] = None,
    coerces_to: t.Optional[t.Dict[exp.DataType.Type, t.Set[exp.DataType.Type]]] = None,
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
        <Type.DOUBLE: 'DOUBLE'>

    Args:
        expression: Expression to annotate.
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


def _coerce_date_literal(l: exp.Expression, unit: t.Optional[exp.Expression]) -> exp.DataType.Type:
    date_text = l.name
    is_iso_date_ = is_iso_date(date_text)

    if is_iso_date_ and is_date_unit(unit):
        return exp.DataType.Type.DATE

    # An ISO date is also an ISO datetime, but not vice versa
    if is_iso_date_ or is_iso_datetime(date_text):
        return exp.DataType.Type.DATETIME

    return exp.DataType.Type.UNKNOWN


def _coerce_date(l: exp.Expression, unit: t.Optional[exp.Expression]) -> exp.DataType.Type:
    if not is_date_unit(unit):
        return exp.DataType.Type.DATETIME
    return l.type.this if l.type else exp.DataType.Type.UNKNOWN


def swap_args(func: BinaryCoercionFunc) -> BinaryCoercionFunc:
    @functools.wraps(func)
    def _swapped(l: exp.Expression, r: exp.Expression) -> exp.DataType.Type:
        return func(r, l)

    return _swapped


def swap_all(coercions: BinaryCoercions) -> BinaryCoercions:
    return {**coercions, **{(b, a): swap_args(func) for (a, b), func in coercions.items()}}


class _TypeAnnotator(type):
    def __new__(cls, clsname, bases, attrs):
        klass = super().__new__(cls, clsname, bases, attrs)

        # Highest-to-lowest type precedence, as specified in Spark's docs (ANSI):
        # https://spark.apache.org/docs/3.2.0/sql-ref-ansi-compliance.html
        text_precedence = (
            exp.DataType.Type.TEXT,
            exp.DataType.Type.NVARCHAR,
            exp.DataType.Type.VARCHAR,
            exp.DataType.Type.NCHAR,
            exp.DataType.Type.CHAR,
        )
        numeric_precedence = (
            exp.DataType.Type.DECFLOAT,
            exp.DataType.Type.DOUBLE,
            exp.DataType.Type.FLOAT,
            exp.DataType.Type.BIGDECIMAL,
            exp.DataType.Type.DECIMAL,
            exp.DataType.Type.BIGINT,
            exp.DataType.Type.INT,
            exp.DataType.Type.SMALLINT,
            exp.DataType.Type.TINYINT,
        )
        timelike_precedence = (
            exp.DataType.Type.TIMESTAMPLTZ,
            exp.DataType.Type.TIMESTAMPTZ,
            exp.DataType.Type.TIMESTAMP,
            exp.DataType.Type.DATETIME,
            exp.DataType.Type.DATE,
        )

        for type_precedence in (text_precedence, numeric_precedence, timelike_precedence):
            coerces_to = set()
            for data_type in type_precedence:
                klass.COERCES_TO[data_type] = coerces_to.copy()
                coerces_to |= {data_type}
        return klass


class TypeAnnotator(metaclass=_TypeAnnotator):
    NESTED_TYPES = {
        exp.DataType.Type.ARRAY,
    }

    # Specifies what types a given type can be coerced into (autofilled)
    COERCES_TO: t.Dict[exp.DataType.Type, t.Set[exp.DataType.Type]] = {}

    # Coercion functions for binary operations.
    # Map of type pairs to a callable that takes both sides of the binary operation and returns the resulting type.
    BINARY_COERCIONS: BinaryCoercions = {
        **swap_all(
            {
                (t, exp.DataType.Type.INTERVAL): lambda l, r: _coerce_date_literal(
                    l, r.args.get("unit")
                )
                for t in exp.DataType.TEXT_TYPES
            }
        ),
        **swap_all(
            {
                # text + numeric will yield the numeric type to match most dialects' semantics
                (text, numeric): lambda l, r: t.cast(
                    exp.DataType.Type, l.type if l.type in exp.DataType.NUMERIC_TYPES else r.type
                )
                for text in exp.DataType.TEXT_TYPES
                for numeric in exp.DataType.NUMERIC_TYPES
            }
        ),
        **swap_all(
            {
                (exp.DataType.Type.DATE, exp.DataType.Type.INTERVAL): lambda l, r: _coerce_date(
                    l, r.args.get("unit")
                ),
            }
        ),
    }

    def __init__(
        self,
        schema: Schema,
        expression_metadata: t.Optional[ExpressionMetadataType] = None,
        coerces_to: t.Optional[t.Dict[exp.DataType.Type, t.Set[exp.DataType.Type]]] = None,
        binary_coercions: t.Optional[BinaryCoercions] = None,
        overwrite_types: bool = True,
    ) -> None:
        self.schema = schema
        dialect = schema.dialect or Dialect()
        self.dialect = dialect
        self.expression_metadata = expression_metadata or dialect.EXPRESSION_METADATA
        self.coerces_to = coerces_to or dialect.COERCES_TO or self.COERCES_TO
        self.binary_coercions = binary_coercions or self.BINARY_COERCIONS

        # Caches the ids of annotated sub-Expressions, to ensure we only visit them once
        self._visited: t.Set[int] = set()

        # Caches NULL-annotated expressions to set them to UNKNOWN after type inference is completed
        self._null_expressions: t.Dict[int, exp.Expression] = {}

        # Databricks and Spark ≥v3 actually support NULL (i.e., VOID) as a type
        self._supports_null_type = dialect.SUPPORTS_NULL_TYPE

        # Maps an exp.SetOperation's id (e.g. UNION) to its projection types. This is computed if the
        # exp.SetOperation is the expression of a scope source, as selecting from it multiple times
        # would reprocess the entire subtree to coerce the types of its operands' projections
        self._setop_column_types: t.Dict[int, t.Dict[str, exp.DataType | exp.DataType.Type]] = {}

        # When set to False, this enables partial annotation by skipping already-annotated nodes
        self._overwrite_types = overwrite_types

    def clear(self) -> None:
        self._visited.clear()
        self._null_expressions.clear()
        self._setop_column_types.clear()

    def _set_type(
        self, expression: E, target_type: t.Optional[exp.DataType | exp.DataType.Type]
    ) -> E:
        prev_type = expression.type
        expression_id = id(expression)

        expression.type = target_type or exp.DataType.Type.UNKNOWN  # type: ignore
        self._visited.add(expression_id)

        if (
            not self._supports_null_type
            and t.cast(exp.DataType, expression.type).this == exp.DataType.Type.NULL
        ):
            self._null_expressions[expression_id] = expression
        elif prev_type and t.cast(exp.DataType, prev_type).this == exp.DataType.Type.NULL:
            self._null_expressions.pop(expression_id, None)

        if (
            isinstance(expression, exp.Column)
            and expression.is_type(exp.DataType.Type.JSON)
            and (dot_parts := expression.meta.get("dot_parts"))
        ):
            # JSON dot access is case sensitive across all dialects, so we need to undo the normalization.
            i = iter(dot_parts)
            parent = expression.parent
            while isinstance(parent, exp.Dot):
                parent.expression.set("this", exp.to_identifier(next(i), quoted=True))
                parent = parent.parent

            expression.meta.pop("dot_parts", None)

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
        for expr in self._null_expressions.values():
            expr.type = self.dialect.DEFAULT_NULL_TYPE

        return expression

    def annotate_scope(self, scope: Scope) -> None:
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

                if (
                    isinstance(expression, exp.Unnest)
                    and not alias_column_names
                    and expression.type
                    and expression.type.is_type(exp.DataType.Type.STRUCT)
                ):
                    selects[name] = {
                        col_def.name: t.cast(t.Union[exp.DataType, exp.DataType.Type], col_def.kind)
                        for col_def in expression.type.expressions
                        if isinstance(col_def, exp.ColumnDef) and col_def.kind
                    }
                else:
                    selects[name] = {
                        alias: column.type for alias, column in zip(alias_column_names, values)
                    }
            elif isinstance(expression, exp.SetOperation) and len(expression.left.selects) == len(
                expression.right.selects
            ):
                selects[name] = self._get_setop_column_types(expression)

            else:
                selects[name] = {s.alias_or_name: s.type for s in expression.selects}

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
                        this=exp.DataType.Type.STRUCT,
                        expressions=[
                            exp.ColumnDef(this=exp.to_identifier(c), kind=kind)
                            for c, kind in schema.items()
                        ],
                        nested=True,
                    )
                    self._set_type(table_column, struct_type)
                elif (
                    isinstance(source, Scope)
                    and isinstance(source.expression, exp.Query)
                    and (
                        source.expression.meta.get("query_type") or exp.DataType.build("UNKNOWN")
                    ).is_type(exp.DataType.Type.STRUCT)
                ):
                    self._set_type(table_column, source.expression.meta["query_type"])

        # Iterate through all the expressions of the current scope in post-order, and annotate
        self._annotate_expression(scope.expression, scope, selects)

        if self.dialect.QUERY_RESULTS_ARE_STRUCTS and isinstance(scope.expression, exp.Query):
            struct_type = exp.DataType(
                this=exp.DataType.Type.STRUCT,
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
                cd.kind.is_type(exp.DataType.Type.UNKNOWN)
                for cd in struct_type.expressions
                if cd.kind
            ):
                # We don't use `_set_type` on purpose here. If we annotated the query directly, then
                # using it in other contexts (e.g., ARRAY(<query>)) could result in incorrect type
                # annotations, i.e., it shouldn't be interpreted as a STRUCT value.
                scope.expression.meta["query_type"] = struct_type

    def _annotate_expression(
        self,
        expression: exp.Expression,
        scope: t.Optional[Scope] = None,
        selects: t.Optional[t.Dict[str, t.Dict[str, t.Any]]] = None,
    ) -> None:
        stack = [(expression, False)]
        selects = selects or {}

        while stack:
            expr, children_annotated = stack.pop()

            if id(expr) in self._visited or (
                not self._overwrite_types
                and expr.type
                and not expr.is_type(exp.DataType.Type.UNKNOWN)
            ):
                continue  # We've already inferred the expression's type

            if not children_annotated:
                stack.append((expr, True))
                for child_expr in expr.iter_expressions():
                    stack.append((child_expr, False))
                continue

            if scope and isinstance(expr, exp.Column) and expr.table:
                source = scope.sources.get(expr.table)
                if isinstance(source, exp.Table):
                    self._set_type(expr, self.schema.get_column_type(source, expr))
                elif source:
                    if expr.table in selects and expr.name in selects[expr.table]:
                        self._set_type(expr, selects[expr.table][expr.name])
                    elif isinstance(source.expression, exp.Unnest):
                        self._set_type(expr, source.expression.type)
                    else:
                        self._set_type(expr, exp.DataType.Type.UNKNOWN)
                else:
                    self._set_type(expr, exp.DataType.Type.UNKNOWN)

                if expr.type and expr.type.args.get("nullable") is False:
                    expr.meta["nonnull"] = True
                continue

            spec = self.expression_metadata.get(expr.__class__)

            if spec and (annotator := spec.get("annotator")):
                annotator(self, expr)
            elif spec and (returns := spec.get("returns")):
                self._set_type(expr, t.cast(exp.DataType.Type, returns))
            else:
                self._set_type(expr, exp.DataType.Type.UNKNOWN)

    def _maybe_coerce(
        self,
        type1: exp.DataType | exp.DataType.Type,
        type2: exp.DataType | exp.DataType.Type,
    ) -> exp.DataType | exp.DataType.Type:
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
        if exp.DataType.Type.UNKNOWN in (type1_value, type2_value):
            return exp.DataType.Type.UNKNOWN

        if type1_value == exp.DataType.Type.NULL:
            return type2_value
        if type2_value == exp.DataType.Type.NULL:
            return type1_value

        return type2_value if type2_value in self.coerces_to.get(type1_value, {}) else type1_value

    def _get_setop_column_types(
        self, setop: exp.SetOperation
    ) -> t.Dict[str, exp.DataType | exp.DataType.Type]:
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

        col_types: t.Dict[str, exp.DataType | exp.DataType.Type] = {}

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
                        r_type_by_select.get(s.alias_or_name) or exp.DataType.Type.UNKNOWN,
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
                    col_type, col_types.get(col_name, exp.DataType.Type.NULL)
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

        if isinstance(expression, (exp.Connector, exp.Predicate)):
            self._set_type(expression, exp.DataType.Type.BOOLEAN)
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
            self._set_type(expression, exp.DataType.Type.BOOLEAN)
        else:
            self._set_type(expression, expression.this.type)

        if expression.this.meta.get("nonnull") is True:
            expression.meta["nonnull"] = True

        return expression

    def _annotate_literal(self, expression: exp.Literal) -> exp.Literal:
        if expression.is_string:
            self._set_type(expression, exp.DataType.Type.VARCHAR)
        elif expression.is_int:
            self._set_type(expression, exp.DataType.Type.INT)
        else:
            self._set_type(expression, exp.DataType.Type.DOUBLE)

        expression.meta["nonnull"] = True

        return expression

    @t.no_type_check
    def _annotate_by_args(
        self,
        expression: E,
        *args: str | exp.Expression,
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

                # Stop at the first nested data type found - we don't want to _maybe_coerce nested types
                if expr_type.args.get("nested"):
                    nested_type = expr_type
                    break

                if isinstance(expr, exp.Literal):
                    literal_type = self._maybe_coerce(literal_type or expr_type, expr_type)
                else:
                    non_literal_type = self._maybe_coerce(non_literal_type or expr_type, expr_type)

            if nested_type:
                break

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
            result_type = literal_type or non_literal_type or exp.DataType.Type.UNKNOWN

        self._set_type(
            expression, result_type or self._maybe_coerce(non_literal_type, literal_type)
        )

        if promote:
            if expression.type.this in exp.DataType.INTEGER_TYPES:
                self._set_type(expression, exp.DataType.Type.BIGINT)
            elif expression.type.this in exp.DataType.FLOAT_TYPES:
                self._set_type(expression, exp.DataType.Type.DOUBLE)

        if array:
            self._set_type(
                expression,
                exp.DataType(
                    this=exp.DataType.Type.ARRAY, expressions=[expression.type], nested=True
                ),
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
            datatype = exp.DataType.Type.UNKNOWN

        self._set_type(expression, datatype)
        return expression

    def _annotate_bracket(self, expression: exp.Bracket) -> exp.Bracket:
        bracket_arg = expression.expressions[0]
        this = expression.this

        if isinstance(bracket_arg, exp.Slice):
            self._set_type(expression, this.type)
        elif this.type.is_type(exp.DataType.Type.ARRAY):
            self._set_type(expression, seq_get(this.type.expressions, 0))
        elif isinstance(this, (exp.Map, exp.VarMap)) and bracket_arg in this.keys:
            index = this.keys.index(bracket_arg)
            value = seq_get(this.values, index)
            self._set_type(expression, value.type if value else None)
        else:
            self._set_type(expression, exp.DataType.Type.UNKNOWN)

        return expression

    def _annotate_div(self, expression: exp.Div) -> exp.Div:
        left_type, right_type = expression.left.type.this, expression.right.type.this  # type: ignore

        if (
            expression.args.get("typed")
            and left_type in exp.DataType.INTEGER_TYPES
            and right_type in exp.DataType.INTEGER_TYPES
        ):
            self._set_type(expression, exp.DataType.Type.BIGINT)
        else:
            self._set_type(expression, self._maybe_coerce(left_type, right_type))
            if expression.type and expression.type.this not in exp.DataType.REAL_TYPES:
                self._set_type(
                    expression, self._maybe_coerce(expression.type, exp.DataType.Type.DOUBLE)
                )

        return expression

    def _annotate_dot(self, expression: exp.Dot) -> exp.Dot:
        self._set_type(expression, None)
        this_type = expression.this.type

        if this_type and this_type.is_type(exp.DataType.Type.STRUCT):
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

        if child and child.is_type(exp.DataType.Type.ARRAY):
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

        self._set_type(expression, exp.DataType.Type.UNKNOWN)
        return expression

    def _annotate_struct_value(
        self, expression: exp.Expression
    ) -> t.Optional[exp.DataType] | exp.ColumnDef:
        # Case: STRUCT(key AS value)
        this: t.Optional[exp.Expression] = None
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

        if kind and kind.is_type(exp.DataType.Type.UNKNOWN):
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
            exp.DataType(this=exp.DataType.Type.STRUCT, expressions=expressions, nested=True),
        )
        return expression

    @t.overload
    def _annotate_map(self, expression: exp.Map) -> exp.Map: ...

    @t.overload
    def _annotate_map(self, expression: exp.VarMap) -> exp.VarMap: ...

    def _annotate_map(self, expression):
        keys = expression.args.get("keys")
        values = expression.args.get("values")

        map_type = exp.DataType(this=exp.DataType.Type.MAP)
        if isinstance(keys, exp.Array) and isinstance(values, exp.Array):
            key_type = seq_get(keys.type.expressions, 0) or exp.DataType.Type.UNKNOWN
            value_type = seq_get(values.type.expressions, 0) or exp.DataType.Type.UNKNOWN

            if key_type != exp.DataType.Type.UNKNOWN and value_type != exp.DataType.Type.UNKNOWN:
                map_type.set("expressions", [key_type, value_type])
                map_type.set("nested", True)

        self._set_type(expression, map_type)
        return expression

    def _annotate_to_map(self, expression: exp.ToMap) -> exp.ToMap:
        map_type = exp.DataType(this=exp.DataType.Type.MAP)
        arg = expression.this
        if arg.is_type(exp.DataType.Type.STRUCT):
            for coldef in arg.type.expressions:
                kind = coldef.kind
                if kind != exp.DataType.Type.UNKNOWN:
                    map_type.set("expressions", [exp.DataType.build("varchar"), kind])
                    map_type.set("nested", True)
                    break

        self._set_type(expression, map_type)
        return expression

    def _annotate_extract(self, expression: exp.Extract) -> exp.Extract:
        part = expression.name
        if part == "TIME":
            self._set_type(expression, exp.DataType.Type.TIME)
        elif part == "DATE":
            self._set_type(expression, exp.DataType.Type.DATE)
        elif part in BIGINT_EXTRACT_DATE_PARTS:
            self._set_type(expression, exp.DataType.Type.BIGINT)
        else:
            self._set_type(expression, exp.DataType.Type.INT)
        return expression

    def _annotate_by_array_element(self, expression: exp.Expression) -> exp.Expression:
        array_arg = expression.this
        if array_arg.type.is_type(exp.DataType.Type.ARRAY):
            element_type = seq_get(array_arg.type.expressions, 0) or exp.DataType.Type.UNKNOWN
            self._set_type(expression, element_type)
        else:
            self._set_type(expression, exp.DataType.Type.UNKNOWN)

        return expression
