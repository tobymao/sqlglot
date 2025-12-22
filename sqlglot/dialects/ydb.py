import typing as t

from sqlglot import exp, tokens, generator, transforms, TokenType, parser, Generator, Expression
from sqlglot.dialects.dialect import Dialect, unit_to_var
from sqlglot.dialects.dialect import NormalizationStrategy, concat_to_dpipe_sql
from sqlglot.helper import name_sequence, seq_get, flatten
from sqlglot.optimizer.simplify import simplify
from sqlglot.transforms import move_ctes_to_top_level
from sqlglot.optimizer.scope import find_in_scope, ScopeType, traverse_scope

JOIN_ATTRS = ("on", "side", "kind", "using", "method")


def rename_func_not_normalize(name: str) -> t.Callable[[Generator, exp.Expression], str]:
    return lambda self, expression: self.func(
        name, *flatten(expression.args.values()), normalize=False
    )


def table_names_to_lower_case(expression: exp.Expression) -> exp.Expression:
    for table in expression.find_all(exp.Table):
        if isinstance(table.this, exp.Identifier):
            ident = table.this
            table.set("this", ident.this.lower())
    return expression


def eliminate_join_marks(expression: exp.Expression) -> exp.Expression:
    """
    Remove join marks from an AST. This rule assumes that all marked columns are qualified.
    If this does not hold for a query, consider running `sqlglot.optimizer.qualify` first.

    For example,
        SELECT * FROM a, b WHERE a.id = b.id(+)    -- ... is converted to
        SELECT * FROM a LEFT JOIN b ON a.id = b.id -- this

    Args:
        expression: The AST to remove join marks from.

    Returns:
       The AST with join marks removed.
    """
    from sqlglot.optimizer.scope import traverse_scope

    for scope in traverse_scope(expression):
        query = scope.expression

        where = query.args.get("where")
        joins = query.args.get("joins")

        if not where or not joins:
            continue

        query_from = query.args["from"]

        # These keep track of the joins to be replaced
        new_joins: t.Dict[str, exp.Join] = {}
        old_joins = {join.alias_or_name: join for join in joins}

        for column in scope.columns:
            if not column.args.get("join_mark"):
                continue

            predicate = column.find_ancestor(exp.Predicate, exp.Select)
            if not isinstance(predicate, exp.Binary):
                continue

            predicate_parent = predicate.parent
            join_predicate = predicate.pop()

            left_columns = [
                c for c in join_predicate.left.find_all(exp.Column) if c.args.get("join_mark")
            ]
            right_columns = [
                c for c in join_predicate.right.find_all(exp.Column) if c.args.get("join_mark")
            ]
            if left_columns and right_columns:
                raise ValueError("The (+) marker cannot appear in both sides of a binary predicate")

            marked_column_tables = set()
            for col in left_columns or right_columns:
                if not isinstance(col, exp.Column) or not hasattr(col, "table"):
                    continue
                table = col.table
                if not table:
                    raise ValueError(f"Column {col} needs to be qualified with a table")

                col.set("join_mark", False)
                marked_column_tables.add(table)

            if not len(marked_column_tables) == 1:
                raise ValueError(f"Columns of only a single table can be marked with (+) in a given binary predicate")

            # Add predicate if join already copied, or add join if it is new
            # Use the last col that was processed (should have table attribute)
            last_col = None
            for col in left_columns or right_columns:
                if isinstance(col, exp.Column) and hasattr(col, "table") and col.table:
                    last_col = col
                    break
            if last_col:
                join_this = old_joins.get(last_col.table, query_from).this
            else:
                join_this = query_from.this
            existing_join = new_joins.get(join_this.alias_or_name)
            if existing_join:
                existing_join.set("on", exp.and_(existing_join.args["on"], join_predicate))
            else:
                new_joins[join_this.alias_or_name] = exp.Join(
                    this=join_this.copy(), on=join_predicate.copy(), kind="LEFT"
                )

            # If the parent of the target predicate is a binary node, then it now has only one child
            if isinstance(predicate_parent, exp.Binary):
                if predicate_parent.left is None:
                    predicate_parent.replace(predicate_parent.right)
                else:
                    predicate_parent.replace(predicate_parent.left)

        only_old_join_sources = old_joins.keys() - new_joins.keys()

        if query_from.alias_or_name in new_joins:
            if len(only_old_join_sources) < 1:
                raise ValueError("Cannot determine which table to use in the new FROM clause")
            new_from_name = list(only_old_join_sources)[0]
            query.set("from", exp.From(this=old_joins.pop(new_from_name).this))
            only_old_join_sources.remove(new_from_name)

        if new_joins:
            only_old_join_expressions = []
            for old_join_source in only_old_join_sources:
                old_join_expression = old_joins[old_join_source]
                if not old_join_expression.kind:
                    old_join_expression.set("kind", "CROSS")

                only_old_join_expressions.append(old_join_expression)

            query.set("joins", list(new_joins.values()) + only_old_join_expressions)

        if not where.this:
            where.pop()

    return expression


def make_db_name_lower(expression: exp.Expression) -> exp.Expression:
    """
    Converts all database names to uppercase

    Args:
        expression: The SQL expression to modify

    Returns:
        Modified expression with uppercase database names
    """
    for table in expression.find_all(exp.Table):
        if table.db:
            table.set("db", table.db.lower())

    return expression


def apply_alias_to_select_from_table(expression: exp.Expression) -> Expression:
    """
    Applies aliases to columns in SELECT statements that reference tables

    Args:
        expression: The SQL expression to modify

    Returns:
        Modified expression with aliases applied to columns
    """
    for column in expression.find_all(exp.Column):
        if not isinstance(column.this, exp.Star):
            if hasattr(column, "table") and column.table and len(column.table) > 0:
                if isinstance(column.parent, exp.Select):
                    column.replace(exp.alias_(column, column.alias_or_name))
    return expression


def _replace(expression, condition):
    """
    Helper function to replace an expression with a condition

    Args:
        expression: The expression to replace
        condition: The condition to replace with

    Returns:
        The replaced expression
    """
    return expression.replace(exp.condition(condition))


def _other_operand(expression):
    """
    Returns the other operand of a binary operation involving a subquery

    Args:
        expression: The expression containing a binary operation

    Returns:
        The operand that is not a subquery, or None
    """
    if isinstance(expression, exp.In):
        return expression.this

    if isinstance(expression, (exp.Any, exp.All)):
        return _other_operand(expression.parent)

    if isinstance(expression, exp.Binary):
        return (
            expression.right
            if isinstance(expression.left, (exp.Subquery, exp.Any, exp.Exists, exp.All))
            else expression.left
        )

    return None


class YDB(Dialect):
    """
    YDB SQL dialect implementation for sqlglot.
    Implements the specific syntax and features of YDB database.
    """

    DATE_FORMAT = "'%Y-%m-%d'"
    TIME_FORMAT = "'%Y-%m-%d %H:%M:%S'"

    TIME_MAPPING = {
        "%Y": "%Y",
        "%m": "%m",
        "%d": "%d",
        "%H": "%H",
        "%M": "%M",
        "%S": "%S",
    }
    NORMALIZE_FUNCTIONS = False

    class Tokenizer(tokens.Tokenizer):
        """
        Tokenizer implementation for YDB SQL dialect.
        Defines how the SQL text is broken into tokens.
        """

        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
        }

        SUPPORTS_VALUES_DEFAULT = False
        QUOTES = ["'", '"']
        COMMENTS = ["--", ("/*", "*/")]
        IDENTIFIERS = ["`"]

    class Parser(parser.Parser):
        def _parse_struct_types(self, type_required=True) -> t.Optional[exp.Expression]:
            if not self._curr:
                return None

            key = self._parse_id_var()
            if not key:
                return None

            if not self._match(TokenType.COLON):
                self.raise_error("Expected colon after struct key")

            value = self._parse_conjunction()
            if not value:
                self.raise_error("Expected value after colon")

            return exp.EQ(this=key, expression=value)

        def _parse_primary(self) -> t.Optional[exp.Expression]:
            if self._match(TokenType.L_PAREN):
                comments = self._prev_comments
                query = self._parse_select()

                if query:
                    expressions = [query]
                else:
                    expressions = self._parse_expressions()

                lambda_expr = self._parse_lambda_body(expressions)
                if lambda_expr:
                    return lambda_expr

                this = self._parse_query_modifiers(seq_get(expressions, 0))

                if not this and self._match(TokenType.R_PAREN, advance=False):
                    this = self.expression(exp.Tuple)
                elif isinstance(this, exp.UNWRAPPED_QUERIES):
                    this = self._parse_subquery(this=this, parse_alias=False)
                elif isinstance(this, exp.Subquery):
                    this = self._parse_subquery(
                        this=self._parse_set_operations(this), parse_alias=False
                    )
                elif len(expressions) > 1 or self._prev.token_type == TokenType.COMMA:
                    this = self.expression(exp.Tuple, expressions=expressions)
                else:
                    this = self.expression(exp.Paren, this=this)

                if this:
                    this.add_comments(comments)

                self._match_r_paren(expression=this)
                return this
            return super()._parse_primary()

        def _parse_lambda_body(self, params):
            if (
                self._curr.token_type != TokenType.R_PAREN
                or self._next.token_type != TokenType.ARROW
            ):
                return None
            self._advance()
            self._advance()
            self._match(TokenType.L_PAREN)

            if not (self._curr.text == "RETURN"):
                self.raise_error("Expected lambda body expression after '->'")
            self._advance()
            body = self._parse_conjunction()
            if not body:
                self.raise_error("Expected lambda body expression after '->'")

            self._match(TokenType.R_BRACE)
            return exp.Lambda(this=body, expressions=params)

    class Generator(generator.Generator):
        """
        SQL Generator for YDB dialect.
        Responsible for translating SQL AST back to SQL text with YDB-specific syntax.
        """

        SUPPORTS_VALUES_DEFAULT = False
        NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_SENSITIVE
        JOIN_HINTS = False
        TABLE_HINTS = False
        QUERY_HINTS = False
        NVL2_SUPPORTED = False
        JSON_PATH_BRACKETED_KEY_SUPPORTED = False
        SUPPORTS_CREATE_TABLE_LIKE = False
        SUPPORTS_TABLE_ALIAS_COLUMNS = False
        SUPPORTS_TO_NUMBER = False
        EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE = False
        SUPPORTS_MEDIAN = False
        JSON_KEY_VALUE_PAIR_SEP = ","
        VARCHAR_REQUIRES_SIZE = False
        CAN_IMPLEMENT_ARRAY_ANY = True
        STRUCT_DELIMITER = ("<|", "|>")
        NULL_ORDERING_SUPPORTED: t.Optional[bool] = True
        NULL_ORDERING = None
        MATCHED_BY_SOURCE = False

        def __init__(self, **kwargs):
            """
            Initialize the YDB SQL Generator with optional configuration.

            Args:
                **kwargs: Additional keyword arguments to pass to the parent Generator.
            """
            super().__init__(**kwargs)
            self.expression_to_alias = {}
            self.ydb_variables = {}

        def create_sql(self, expression: exp.Create, pretty=True) -> str:
            """
            Generate SQL for CREATE expressions with special handling for CREATE VIEW.

            Args:
                expression: The CREATE expression to generate SQL for
                pretty: Whether to format the SQL with indentation

            Returns:
                Generated SQL string
            """
            if expression.kind == "VIEW" and expression.this and expression.this.this:
                ident = expression.this.this
                ident_sql = self.sql(ident)
                sql = self.sql(expression.expression)

                return f"CREATE VIEW {ident_sql} WITH (security_invoker = TRUE) AS {sql}"
            elif expression.kind == "FUNCTION":
                # CREATE -> FUNCTION -> TABLE
                func_name = self.sql(expression.this.this.alias_or_name)

                params = []
                for param in expression.this.expressions:
                    if isinstance(param, exp.ColumnDef):
                        param_name = self.sql(param.this)
                        params.append(f"${param_name}")
                    else:
                        params.append(self.sql(param))

                params_str = ", ".join(params)

                body = f" RETURN {self.sql(expression.expression)}"
                return f"${func_name} = ({params_str}) -> {{ {body} }};"
            else:
                return super().create_sql(expression)

        def table_sql(self, expression: exp.Table, copy=True) -> str:
            """
            Generate SQL for TABLE expressions with proper quoting and database prefix.

            Args:
                expression: The TABLE expression
                copy: Whether to copy the expression before processing

            Returns:
                Generated SQL string for the table reference
            """
            prefix = f"{expression.db}/" if expression.db else ""
            sql = f"`{prefix}{expression.name}`"

            if expression.alias:
                sql += f" AS {expression.alias}"

            return sql

        def is_sql(self, expression: exp.Is) -> str:
            """
            Generate SQL for IS expressions with special handling for IS NOT NULL.

            Args:
                expression: The IS expression

            Returns:
                Generated SQL string
            """
            is_sql = super().is_sql(expression)

            if isinstance(expression.parent, exp.Not):
                # value IS NOT NULL -> NOT (value IS NULL)
                is_sql = self.wrap(is_sql)

            return is_sql

        def anonymous_sql(self, expression: exp.Anonymous) -> str:
            """
            Generate SQL for Anonymous functions, with special handling for YQL lambda variables.
            Variables starting with $ should not be normalized.

            Args:
                expression: The Anonymous expression

            Returns:
                Generated SQL string
            """
            # We don't normalize qualified functions such as a.b.foo(), because they can be case-sensitive
            parent = expression.parent
            is_qualified = isinstance(parent, exp.Dot) and expression is parent.expression

            func_name = self.sql(expression, "this")
            # Don't normalize YQL lambda variables (starting with $) or qualified functions
            normalize = not (is_qualified or func_name.startswith("$"))
            return self.func(func_name, *expression.expressions, normalize=normalize)

        # YDB doesn't allow comparison of nullable and non-nullable types.
        # Wrapping it in a lambda can help circumvent this limitation.
        # def _wrap_non_optional(self, expr: exp.Expression) -> exp.Expression:
        #     """
        #     Helper to wrap non-Optional types using the YQL lambda function.
        #     Uses the $wrap_non_optional_in_comparisons lambda function.
        #
        #     Args:
        #         expr: The expression to potentially wrap
        #
        #     Returns:
        #         Expression wrapped using the lambda function
        #     """
        #     # Use the lambda function: $wrap_non_optional_in_comparisons(expr)
        #     return exp.Anonymous(this="$wrap_non_optional_in_comparisons", expressions=[expr])
        #
        # def eq_sql(self, expression: exp.EQ) -> str:
        #     """
        #     Generate SQL for EQ (equals) with Just() for non-Optional types.
        #     Wraps non-Optional values with Just() to make them Optional.
        #
        #     Args:
        #         expression: The EQ expression
        #
        #     Returns:
        #         Generated SQL string with Just() wrapping for non-Optional types
        #     """
        #     left = self._wrap_non_optional(expression.this)
        #     right = self._wrap_non_optional(expression.expression)
        #     return self.binary(exp.EQ(this=left, expression=right), "=")
        #
        # def neq_sql(self, expression: exp.NEQ) -> str:
        #     """
        #     Generate SQL for NEQ (not equals) with Just() for non-Optional types.
        #     Wraps non-Optional values with Just() to make them Optional.
        #
        #     Args:
        #         expression: The NEQ expression
        #
        #     Returns:
        #         Generated SQL string with Just() wrapping for non-Optional types
        #     """
        #     left = self._wrap_non_optional(expression.this)
        #     right = self._wrap_non_optional(expression.expression)
        #     return self.binary(exp.NEQ(this=left, expression=right), "<>")
        #
        # def gt_sql(self, expression: exp.GT) -> str:
        #     """
        #     Generate SQL for GT (greater than) with Just() for non-Optional types.
        #     Wraps non-Optional values with Just() to make them Optional.
        #
        #     Args:
        #         expression: The GT expression
        #
        #     Returns:
        #         Generated SQL string with Just() wrapping for non-Optional types
        #     """
        #     left = self._wrap_non_optional(expression.this)
        #     right = self._wrap_non_optional(expression.expression)
        #     return self.binary(exp.GT(this=left, expression=right), ">")
        #
        # def gte_sql(self, expression: exp.GTE) -> str:
        #     """
        #     Generate SQL for GTE (greater than or equal) with Just() for non-Optional types.
        #     Wraps non-Optional values with Just() to make them Optional.
        #
        #     Args:
        #         expression: The GTE expression
        #
        #     Returns:
        #         Generated SQL string with Just() wrapping for non-Optional types
        #     """
        #     left = self._wrap_non_optional(expression.this)
        #     right = self._wrap_non_optional(expression.expression)
        #     return self.binary(exp.GTE(this=left, expression=right), ">=")
        #
        # def lt_sql(self, expression: exp.LT) -> str:
        #     """
        #     Generate SQL for LT (less than) with Just() for non-Optional types.
        #     Wraps non-Optional values with Just() to make them Optional.
        #
        #     Args:
        #         expression: The LT expression
        #
        #     Returns:
        #         Generated SQL string with Just() wrapping for non-Optional types
        #     """
        #     left = self._wrap_non_optional(expression.this)
        #     right = self._wrap_non_optional(expression.expression)
        #     return self.binary(exp.LT(this=left, expression=right), "<")
        #
        # def lte_sql(self, expression: exp.LTE) -> str:
        #     """
        #     Generate SQL for LTE (less than or equal) with Just() for non-Optional types.
        #     Wraps non-Optional values with Just() to make them Optional.
        #
        #     Args:
        #         expression: The LTE expression
        #
        #     Returns:
        #         Generated SQL string with Just() wrapping for non-Optional types
        #     """
        #     left = self._wrap_non_optional(expression.this)
        #     right = self._wrap_non_optional(expression.expression)
        #     return self.binary(exp.LTE(this=left, expression=right), "<=")

        def datatype_sql(self, expression: exp.DataType) -> str:
            """
            Generate SQL for data type expressions with YDB-specific type mapping.

            Args:
                expression: The data type expression

            Returns:
                Generated SQL string for the data type
            """
            if (
                expression.is_type(exp.DataType.Type.NVARCHAR)
                or expression.is_type(exp.DataType.Type.VARCHAR)
                or expression.is_type(exp.DataType.Type.CHAR)
            ):
                expression = exp.DataType.build("text")
            elif expression.is_type(exp.DataType.Type.DECIMAL):
                size_expressions = list(expression.find_all(exp.DataTypeParam))

                column_def = expression.parent
                is_pk = False
                if isinstance(column_def, exp.ColumnDef):
                    for constraint in column_def.constraints:
                        if isinstance(constraint.kind, exp.PrimaryKeyColumnConstraint):
                            expression = exp.DataType.build("int64")
                            is_pk = True

                if is_pk:
                    pass
                elif not size_expressions:
                    expression = exp.DataType.build("int64")
                else:
                    if len(size_expressions) == 1 or (
                            len(size_expressions) == 2 and int(size_expressions[1].name) == 0
                    ):
                        if isinstance(size_expressions[0].this, exp.Star):
                            expression = exp.DataType.build("decimal(38, 0)")
                        else:
                            mantis = int(size_expressions[0].name)
                            expression = exp.DataType.build(f"decimal({mantis}, 0)")
                    else:
                        precision = int(size_expressions[0].name)
                        scale = int(size_expressions[1].name)
                        expression = exp.DataType.build(f"decimal({precision}, {scale})")
            elif expression.is_type(exp.DataType.Type.TIMESTAMP):
                expression = exp.DataType.build("Timestamp")
            elif expression.this in exp.DataType.TEMPORAL_TYPES:
                expression = exp.DataType.build(expression.this)
            elif expression.is_type("float"):
                size_expression = expression.find(exp.DataTypeParam)
                if size_expression:
                    size = int(size_expression.name)
                    expression = (
                        exp.DataType.build("float") if size <= 32 else exp.DataType.build("double")
                    )

            return super().datatype_sql(expression)


        def primarykeycolumnconstraint_sql(self, expression: exp.PrimaryKeyColumnConstraint) -> str:
            """
            Generate SQL for PRIMARY KEY column constraints.
            In YDB, these are handled differently at the table level.

            Args:
                expression: The PRIMARY KEY column constraint

            Returns:
                Empty string as YDB handles primary keys differently
            """
            return ""

        def _cte_to_lambda(self, expression: exp.Expression) -> str:
            """
            Convert Common Table Expressions (CTEs) to YDB-style lambdas.

            Args:
                expression: The SQL expression containing CTEs

            Returns:
                YDB-specific SQL with lambdas instead of CTEs
            """

            all_ctes = list(expression.find_all(exp.CTE))

            if not all_ctes:
                output = self.sql(expression)
            else:
                aliases = []

                def _table_to_var(node):
                    if (isinstance(node, exp.Table)) and node.name in aliases:
                        return exp.Var(this=f"${node.name} AS {node.alias_or_name}")
                    return node

                for cte in all_ctes:
                    alias = cte.alias
                    aliases.append(alias)

                expression.transform(_table_to_var, copy=False)

                for cte in all_ctes:
                    cte.pop()

                all_with = list(expression.find_all(exp.With))
                for w in all_with:
                    w.pop()

                output = ""

                for cte in all_ctes:
                    cte_sql = self.sql(cte.this)
                    output += f"${cte.alias_or_name} = ({cte_sql});\n\n"

                body_sql = self.sql(expression)

                output += body_sql

            ydb_vars_sql = ""
            for var_name, subquery in self.ydb_variables.items():
                subquery_sql = self.sql(subquery)
                ydb_vars_sql += f"${var_name} = ({subquery_sql});\n"
            self.ydb_variables = {}
            output = ydb_vars_sql + output
            return output

        def _generate_create_table(self, expression: exp.Expression) -> str:
            """
            Generate CREATE TABLE SQL with YDB-specific syntax.
            Handles primary keys, constraints, and partitioning.

            Args:
                expression: The CREATE TABLE expression

            Returns:
                SQL string for creating a table in YDB
            """
            # Clean up index parts from table
            for ex in list(expression.this.expressions):
                if isinstance(ex, exp.Identifier):
                    ex.pop()

            def enforce_not_null(col):
                """Add NOT NULL constraint if not present"""
                for constraint in col.constraints:
                    if isinstance(constraint.kind, exp.NotNullColumnConstraint):
                        break
                else:
                    col.append(
                        "constraints", exp.ColumnConstraint(kind=exp.NotNullColumnConstraint())
                    )

            def enforce_pk(col):
                """Add PRIMARY KEY constraint if not present"""
                for constraint in col.constraints:
                    if isinstance(constraint.kind, exp.PrimaryKeyColumnConstraint):
                        break
                else:
                    col.append(
                        "constraints", exp.ColumnConstraint(kind=exp.PrimaryKeyColumnConstraint())
                    )

            pks = list(expression.find_all(exp.PrimaryKey))
            if len(pks) > 0:
                for pk in pks:
                    for pk_ex in pk.expressions:
                        pk_cols = [
                            col
                            for col in expression.this.find_all(exp.ColumnDef)
                            if col.alias_or_name.lower() == pk_ex.alias_or_name.lower()
                        ]
                        if len(pk_cols) > 0:
                            col = pk_cols[0]
                            enforce_not_null(col)
                            enforce_pk(col)
                    pk.pop()

            def is_pk(col):
                """Check if a column has a PRIMARY KEY constraint"""
                for constraint in col.constraints:
                    if isinstance(constraint, exp.ColumnConstraint):
                        if isinstance(constraint.kind, exp.PrimaryKeyColumnConstraint):
                            return True
                return False

            for col in expression.find_all(exp.ColumnDef):
                if is_pk(col):
                    break
            else:
                col = list(expression.find_all(exp.ColumnDef))[0]
                enforce_pk(col)

            for col in expression.this.find_all(exp.ColumnDef):
                if is_pk(col):
                    enforce_not_null(col)

            for constraint in list(expression.this.find_all(exp.Constraint)):
                constraint.pop()

            sql = super().generate(expression)

            pk_s = []
            for col in expression.find_all(exp.ColumnDef):
                if is_pk(col):
                    pk_s.append(col.alias_or_name)

            if not pk_s:
                raise ValueError("No primary key columns found")
            ind = sql.rfind(")")
            col_names = ",".join([f"`{pk}`" for pk in pk_s])
            sql = sql[:ind] + f", PRIMARY KEY({col_names}))\nPARTITION BY HASH ({col_names});"
            return sql

        def generate(self, expression: exp.Expression, copy: bool = True) -> str:
            """
            Generate SQL for any expression with YDB-specific handling.

            Args:
                expression: The SQL expression to generate
                copy: Whether to copy the expression before processing

            Returns:
                Generated SQL string
            """

            self.unnest_subqueries(expression)

            expression = expression.copy() if copy else expression

            # Without pragmas, some queries may not work - for example, implicit cross joins are disabled by default.
            # pragma_statements = []
            #
            # if isinstance(expression, (exp.Select, exp.Insert, exp.Update, exp.Delete, exp.Create)):
            #     pragma_statements = ['PRAGMA AnsiImplicitCrossJoin;',
            #                          'PRAGMA AnsiInForEmptyOrNullableItemsCollections;']

            if not isinstance(expression, exp.Create) or (
                isinstance(expression, exp.Create)
                and expression.kind
                and expression.kind.lower() != "table"
            ):
                sql = self._cte_to_lambda(expression)
            else:
                sql = self._generate_create_table(expression)

            # can be uncommented to support comparisons of optional types with non-optional
            # wrap_lambda = '$wrap_non_optional_in_comparisons = ($column) -> {RETURN IF(FormatType(TypeOf($column)) LIKE "Optional<%", $column, Just($column))};\n\n'
            # return "\n".join(pragma_statements) + "\n" + wrap_lambda + sql
            return sql

        def unnest_subqueries(self, expression):
            """
            Rewrite sqlglot AST to convert some predicates with subqueries into joins.

            Convert scalar subqueries into cross joins.
            Convert correlated or vectorized subqueries into a group by so it is not a many to many left join.

            Example:
                >>> import sqlglot
                >>> expression = sqlglot.parse_one("SELECT * FROM x AS x WHERE (SELECT y.a AS a FROM y AS y WHERE x.a = y.a) = 1 ")
                >>> unnest_subqueries(expression).sql()
                'SELECT * FROM x AS x LEFT JOIN (SELECT y.a AS a FROM y AS y WHERE TRUE GROUP BY y.a) AS _u_0 ON x.a = _u_0.a WHERE _u_0.a = 1'

            Args:
                expression (sqlglot.Expression): expression to unnest
            Returns:
                sqlglot.Expression: unnested expression
            """
            next_alias_name = name_sequence("_u_")

            for scope in traverse_scope(expression):
                select = scope.expression
                parent = select.parent_select
                if not parent:
                    continue
                if scope.external_columns:
                    self.decorrelate(select, parent, scope.external_columns, next_alias_name)
                if scope.scope_type == ScopeType.SUBQUERY:
                    self.unnest(select, parent, next_alias_name)

            return expression

        @staticmethod
        def remove_star_when_other_columns(expression: exp.Expression) -> exp.Expression:
            """
            Remove * from SELECT list when there are other columns present.

            Args:
                expression: The SQL expression to modify

            Returns:
                Modified expression without redundant *
            """
            for select_expr in expression.find_all(exp.Select):
                expressions = select_expr.expressions

                # Check if there's a * and at least one other column
                has_star = any(
                    isinstance(expr, exp.Star)
                    or (isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star))
                    for expr in expressions
                )

                has_other_columns = any(
                    not (
                        isinstance(expr, exp.Star)
                        or (isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star))
                    )
                    for expr in expressions
                )

                if has_star and has_other_columns:
                    # Remove all * expressions
                    new_expressions = [
                        expr
                        for expr in expressions
                        if not (
                            isinstance(expr, exp.Star)
                            or (isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star))
                        )
                    ]
                    select_expr.set("expressions", new_expressions)

            return expression

        def unnest(self, select, parent_select, next_alias_name):
            """
            Unnests a subquery by transforming it into a join
            """
            if len(select.selects) > 1:
                return
            self.ensure_select_aliases(select)

            predicate = select.find_ancestor(exp.Condition)
            if (
                not predicate
                or parent_select is not predicate.parent_select
                or not parent_select.args.get("from")
            ):
                return

            if any(
                isinstance(expr, exp.Star)
                or (isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star))
                for expr in select.selects
            ):
                return

            if isinstance(select, exp.SetOperation):
                select = exp.select(*select.selects).from_(select.subquery(next_alias_name()))

            alias = next_alias_name()
            clause = predicate.find_ancestor(exp.Having, exp.Where, exp.Join)

            # This subquery returns a scalar and can just be converted to a cross join
            if not isinstance(predicate, (exp.In, exp.Any)):
                first_select = select.selects[0]
                column_alias = first_select.alias_or_name

                if (
                    not column_alias
                    or column_alias == ""
                    or (column_alias == "*" and isinstance(first_select, exp.AggFunc))
                ):
                    if isinstance(first_select, exp.Alias):
                        expr = first_select.this
                    else:
                        expr = first_select

                    # Generate a meaningful alias based on the expression type
                    if isinstance(expr, exp.AggFunc):
                        func_name = expr.sql_name().lower() if hasattr(expr, "sql_name") else "agg"
                        column_alias = f"_{func_name}"
                    else:
                        column_alias = "_col"

                    # Add alias to the select if it doesn't have one
                    if not isinstance(first_select, exp.Alias):
                        new_selects = [exp.alias_(first_select.copy(), column_alias)]
                        if len(select.selects) > 1:
                            new_selects.extend(select.selects[1:])
                        select.set("expressions", new_selects)
                        # Update first_select to point to the newly aliased expression
                        first_select = select.selects[0]
                    elif not first_select.alias or first_select.alias_or_name == "*":
                        first_select.set("alias", exp.to_identifier(column_alias))

                    # Re-read the alias after setting it to ensure we have the correct value
                    column_alias = first_select.alias_or_name

                column = exp.column(column_alias, alias)

                clause_parent_select = clause.parent_select if clause else None

                if (isinstance(clause, exp.Having) and clause_parent_select is parent_select) or (
                    (not clause or clause_parent_select is not parent_select)
                    and (
                        parent_select.args.get("group")
                        or any(
                            find_in_scope(select, exp.AggFunc) for select in parent_select.selects
                        )
                    )
                ):
                    column = exp.Max(this=column)
                elif not isinstance(select.parent, exp.Subquery) and not isinstance(
                    select.parent, exp.Exists
                ):
                    return

                _replace(select.parent, column)
                parent_select.join(select, join_type="CROSS", join_alias=alias, copy=False)
                return

            if select.find(exp.Limit, exp.Offset):
                return

            if isinstance(predicate, exp.Any):
                predicate = predicate.find_ancestor(exp.EQ)

                if not predicate or parent_select is not predicate.parent_select:
                    return

            column = _other_operand(predicate)
            self.ensure_select_aliases(select)
            value = select.selects[0]
            join_key = exp.column(value.alias, alias)
            join_key_not_null = join_key.is_(exp.null()).not_()

            if isinstance(clause, exp.Join):
                _replace(predicate, exp.true())
                parent_select.where(join_key_not_null, copy=False)
            else:
                _replace(predicate, join_key_not_null)

            group = select.args.get("group")

            if group:
                # Remove table qualifiers from GROUP BY expressions
                group_expressions = []
                for expr in group.expressions:
                    if isinstance(expr, exp.Column) and expr.table:
                        # Remove table qualifier
                        unqualified_expr = exp.Column(this=expr.this)
                        group_expressions.append(unqualified_expr)
                    else:
                        group_expressions.append(expr)

                # Check if value.this (without qualifier) matches any group expression
                value_this_unqualified = value.this
                if isinstance(value_this_unqualified, exp.Column) and value_this_unqualified.table:
                    value_this_unqualified = exp.Column(this=value_this_unqualified.this)

                if {value_this_unqualified} != set(group_expressions):
                    select = (
                        exp.select(exp.alias_(exp.column(value.alias, "_q"), value.alias))
                        .from_(select.subquery("_q", copy=False), copy=False)
                        .group_by(exp.column(value.alias, "_q"), copy=False)
                    )
                else:
                    # Update group with unqualified expressions
                    new_group = exp.Group(expressions=group_expressions)
                    select.set("group", new_group)
            elif not find_in_scope(value.this, exp.AggFunc):
                # Remove table qualifier from value.this if it's a column for GROUP BY
                group_by_expr = value.this
                if isinstance(group_by_expr, exp.Column) and group_by_expr.table:
                    group_by_expr = exp.Column(this=group_by_expr.this)
                select = select.group_by(group_by_expr, copy=False)

            parent_select.join(
                select,
                on=column.eq(join_key),
                join_type="LEFT",
                join_alias=alias,
                copy=False,
            )

        @staticmethod
        def ensure_select_aliases(select, default_prefix="_col"):
            """
            Ensure all select expressions have a non-empty, unique alias.
            Use the original column name as alias if possible.
            """
            for i, expr in enumerate(select.selects):
                if isinstance(expr, exp.Alias):
                    alias_name = expr.alias_or_name
                    if not alias_name or alias_name == "*":
                        base_name = (
                            expr.this.alias_or_name
                            if hasattr(expr.this, "alias_or_name")
                            else f"{default_prefix}{i}"
                        )
                        expr.set("alias", exp.to_identifier(base_name))
                elif isinstance(expr, exp.Column):
                    base_name = expr.alias_or_name or f"{default_prefix}{i}"
                    select.selects[i] = exp.alias_(expr, base_name)
                else:
                    select.selects[i] = exp.alias_(expr, f"{default_prefix}{i}")

        def decorrelate(self, select, parent_select, external_columns, next_alias_name):
            """
            Decorrelates a subquery by transforming it into a join
            """
            where = select.args.get("where")
            if not where or where.find(exp.Or) or select.find(exp.Limit, exp.Offset):
                return

            table_alias = next_alias_name()
            keys = []

            # for all external columns in the where statement, find the relevant predicate
            # keys to convert it into a join
            for column in external_columns:
                predicate = column.find_ancestor(exp.Predicate)

                if isinstance(predicate, exp.Binary):
                    key = (
                        predicate.right
                        if any(node is column for node in predicate.left.walk())
                        else predicate.left
                    )
                elif isinstance(predicate, exp.Between):
                    key = predicate.this
                else:
                    return

                keys.append((key, column, predicate))

            is_subquery_projection = any(
                node is select.parent
                for node in map(lambda s: s.unalias(), parent_select.selects)
                if isinstance(node, exp.Subquery)
            )

            value = select.selects[0]
            key_aliases = {}
            group_by = []

            external_tables = [
                col.table
                for col in external_columns
                if isinstance(col, exp.Column) and hasattr(col, "table") and col.table
            ]

            external_column_set = set()
            for col in external_columns:
                if isinstance(col, exp.Column):
                    if col.table:
                        external_column_set.add(
                            (
                                col.table,
                                col.this.name if hasattr(col.this, "name") else col.alias_or_name,
                            )
                        )

            def is_external_column(col):
                if not isinstance(col, exp.Column):
                    return False
                col_table = col.table if col.table else None
                col_name = col.this.name if hasattr(col.this, "name") else col.alias_or_name
                return (col_table, col_name) in external_column_set or (
                    None,
                    col_name,
                ) in external_column_set

            keys = [
                (key, column, predicate)
                for key, column, predicate in keys
                if isinstance(key, exp.Column)
                and (
                    not key.table  # No table qualifier = from subquery
                    or (
                        key.table and key.table not in external_tables
                    )  # Has qualifier but not external
                )
                and is_external_column(column)
            ]  # Verify column is actually external

            parent_predicate = select.find_ancestor(exp.Predicate)
            is_exists = isinstance(parent_predicate, exp.Exists)

            if is_exists and not keys:
                return

            if is_exists:
                select.set("expressions", [])

            for key, _, predicate in keys:
                if is_exists:
                    if key not in key_aliases:
                        alias_name = next_alias_name()
                        key_aliases[key] = alias_name

                        key_copy = key.copy()
                        if isinstance(key_copy, exp.Column) and key_copy.table:
                            key_copy.set("table", None)

                        select.select(exp.alias_(key_copy, alias_name, quoted=False), copy=False)

                    if isinstance(predicate, exp.EQ) and key not in group_by:
                        group_by.append(key)
                else:
                    if value and key == value.this:
                        alias = value.alias if value.alias != "" else next_alias_name()
                        key_aliases[key] = alias
                        group_by.append(key)
                    else:
                        key_aliases[key] = next_alias_name()
                        if isinstance(predicate, exp.EQ) and key not in group_by:
                            group_by.append(key)

            if is_exists:
                value_alias = "_exists_flag"
                select.select(
                    exp.alias_(exp.Literal.number(1), value_alias, quoted=False), copy=False
                )
                alias = exp.column(value_alias, table_alias)
            elif value:
                agg_func = exp.Max if is_subquery_projection else exp.ArrayAgg

                # exists queries should not have any selects as it only checks if there are any rows
                # all selects will be added by the optimizer and only used for join keys
                for key, alias_val in key_aliases.items():
                    if key in group_by:
                        # add all keys to the projections of the subquery
                        # so that we can use it as a join keyjoin_sql
                        select.select(exp.alias_(key.copy(), alias_val, quoted=False), copy=False)
                    else:
                        select.select(
                            exp.alias_(agg_func(this=key.copy()), alias_val, quoted=False),
                            copy=False,
                        )

                if not value.alias_or_name or value.alias_or_name == "*":
                    # Generate a meaningful alias based on the expression type
                    if isinstance(value.this, exp.Count):
                        value_alias = "_count"
                    elif isinstance(value.this, exp.AggFunc):
                        func_name = (
                            value.this.sql_name().lower()
                            if hasattr(value.this, "sql_name")
                            else "agg"
                        )
                        value_alias = f"_{func_name}"
                    else:
                        value_alias = next_alias_name()

                    if isinstance(value, exp.Alias):
                        value.set("alias", value_alias)
                    else:
                        value = exp.alias_(value, value_alias)
                        select.selects[0] = value
                else:
                    value_alias = value.alias_or_name
                alias = exp.column(value_alias, table_alias)
            else:
                return

            self.remove_star_when_other_columns(select)
            other = _other_operand(parent_predicate)
            op_type = type(parent_predicate.parent) if parent_predicate else None

            if is_exists:
                if key_aliases:
                    first_key_alias = list(key_aliases.values())[0]
                    alias = exp.column(first_key_alias, table_alias)
                    parent_predicate.replace(exp.condition(f"NOT {self.sql(alias)} IS NULL"))
                else:
                    if select.selects:
                        first_select = select.selects[0]
                        alias_name = first_select.alias_or_name or "_exists"
                        alias = exp.column(alias_name, table_alias)
                        parent_predicate.replace(exp.condition(f"NOT {self.sql(alias)} IS NULL"))
            elif isinstance(parent_predicate, exp.All):
                if not issubclass(op_type, exp.Binary):
                    raise ValueError("op_type must be a subclass of Binary")
                assert issubclass(op_type, exp.Binary)
                predicate = op_type(this=other, expression=exp.column("_x"))
                _replace(parent_predicate.parent, f"ARRAY_ALL({alias}, _x -> {predicate})")
            elif isinstance(parent_predicate, exp.Any):
                if not issubclass(op_type, exp.Binary):
                    raise ValueError("op_type must be a subclass of Binary")
                if value.this in group_by:
                    predicate = op_type(this=other, expression=alias)
                    _replace(parent_predicate.parent, predicate)
                else:
                    predicate = op_type(this=other, expression=exp.column("_x"))
                    _replace(parent_predicate, f"ARRAY_ANY({alias}, _x -> {predicate})")
            elif isinstance(parent_predicate, exp.In):
                if value.this in group_by:
                    _replace(parent_predicate, f"{other} = {alias}")
                else:
                    _replace(
                        parent_predicate,
                        f"ARRAY_ANY({alias}, _x -> _x = {parent_predicate.this})",
                    )
            else:
                if is_subquery_projection and select.parent.alias:
                    alias = exp.alias_(alias, select.parent.alias)

                # COUNT always returns 0 on empty datasets, so we need take that into consideration here
                # by transforming all counts into 0 and using that as the coalesced value
                # However, don't add COALESCE if value.this is a Star (from COUNT(*)) -
                # scalar subqueries are handled by unnest which creates proper aliases
                if value.find(exp.Count) and not isinstance(value.this, exp.Star):

                    def remove_aggs(node):
                        if isinstance(node, exp.Count):
                            return exp.Literal.number(0)
                        elif isinstance(node, exp.AggFunc):
                            return exp.null()
                        return node

                    transformed = value.this.transform(remove_aggs)
                    # Only add COALESCE if the transformed expression is not a Star
                    if not isinstance(transformed, exp.Star):
                        alias = exp.Coalesce(this=alias, expressions=[transformed])

                select.parent.replace(alias)

            on_predicates = []

            for key, column, predicate in keys:
                if isinstance(predicate, exp.EQ):
                    predicate.replace(exp.true())

                    # Create the ON condition: external_column = subquery_alias.column_alias
                    if key in key_aliases:
                        # Use the alias we created for the key in the SELECT list
                        nested_col = exp.column(key_aliases[key], table_alias)

                        external_col_copy = column.copy()

                        on_predicates.append(exp.EQ(this=external_col_copy, expression=nested_col))
                else:
                    if key in key_aliases:
                        nested_col = exp.column(key_aliases[key], table_alias)

                        key.replace(nested_col)

            if group_by:
                new_group_by = []
                for gb_expr in group_by:
                    if isinstance(gb_expr, exp.Column) and gb_expr.table:
                        unqualified_expr = exp.Column(this=gb_expr.this)
                        new_group_by.append(unqualified_expr)
                    else:
                        new_group_by.append(gb_expr)
                group_by = new_group_by

            if on_predicates:
                if len(on_predicates) == 1:
                    on_clause = on_predicates[0]
                else:
                    on_clause = on_predicates[0]
                    for pred in on_predicates[1:]:
                        on_clause = exp.and_(on_clause, pred)

                parent_select.join(
                    select.group_by(*group_by, copy=False) if group_by else select,
                    on=on_clause,
                    join_type="LEFT",
                    join_alias=table_alias,
                    copy=False,
                )
            else:
                parent_select.join(
                    select.group_by(*group_by, copy=False) if group_by else select,
                    join_type="CROSS",
                    join_alias=table_alias,
                    copy=False,
                )

        STRING_TYPE_MAPPING = {
            exp.DataType.Type.BLOB: "String",
            exp.DataType.Type.CHAR: "String",
            exp.DataType.Type.LONGBLOB: "String",
            exp.DataType.Type.LONGTEXT: "String",
            exp.DataType.Type.MEDIUMBLOB: "String",
            exp.DataType.Type.MEDIUMTEXT: "String",
            exp.DataType.Type.TINYBLOB: "String",
            exp.DataType.Type.TINYTEXT: "String",
            exp.DataType.Type.TEXT: "Utf8",
            exp.DataType.Type.VARBINARY: "String",
            exp.DataType.Type.VARCHAR: "Utf8",
        }

        def _date_trunc_sql(self, expression: exp.DateTrunc) -> str:
            """
            Generate SQL for DATE_TRUNC function with YDB-specific implementation.

            Args:
                expression: The DATE_TRUNC expression

            Returns:
                YDB-specific SQL for truncating dates
            """
            expr = self.sql(expression, "this")
            unit = expression.text("unit").upper()

            if unit == "WEEK":
                return f"DateTime::MakeDate(DateTime::StartOfWeek({expr}))"
            elif unit == "MONTH":
                return f"DateTime::MakeDate(DateTime::StartOfMonth({expr}))"
            elif unit == "QUARTER":
                return f"DateTime::MakeDate(DateTime::StartOfQuarter({expr}))"
            elif unit == "YEAR":
                return f"DateTime::MakeDate(DateTime::StartOfYear({expr}))"
            else:
                if unit != "DAY":
                    self.unsupported(f"Unexpected interval unit: {unit}")
                return self.func("DATE", expr)

        def _current_timestamp_sql(self, expression: exp.CurrentTimestamp) -> str:
            """
            Generate SQL for CURRENT_TIMESTAMP function with YDB-specific implementation.

            Args:
                expression: The CURRENT_TIMESTAMP expression

            Returns:
                YDB-specific SQL for current timestamp
            """
            return 'AddTimezone(CurrentUtcTimestamp(), "Europe/Moscow")'

        def _str_to_date(self, expression: exp.StrToDate) -> str:
            """
            Generate SQL for STR_TO_DATE function with YDB-specific implementation.

            Args:
                expression: The STR_TO_DATE expression

            Returns:
                YDB-specific SQL for converting strings to dates
            """
            str_value = expression.this.name
            # formatted_time = self.format_time(expression, self.dialect.INVERSE_FORMAT_MAPPING,
            #                                   self.dialect.INVERSE_FORMAT_TRIE)
            formatted_time = self.format_time(expression)
            return f'DateTime::MakeTimestamp(DateTime::Parse({formatted_time})("{str_value}"))'

        def _extract(self, expression: exp.Extract) -> str:
            """
            Generate SQL for EXTRACT function with YDB-specific implementation.

            Args:
                expression: The EXTRACT expression

            Returns:
                YDB-specific SQL for extracting date parts
            """
            unit = expression.name.upper()
            expr = self.sql(expression.expression)

            if unit == "WEEK":
                return f"DateTime::GetWeekOfYear({expr})"
            elif unit == "MONTH":
                return f"DateTime::GetMonth({expr})"
            elif unit == "YEAR":
                return f"DateTime::GetYear({expr})"
            else:
                if unit != "DAY":
                    self.unsupported(f"Unexpected interval unit: {unit}")
                return self.func("DATE", expr)

        def _lambda(self, expression: exp.Lambda, arrow_sep: str = "->") -> str:
            """
            Generate SQL for Lambda expressions with YDB-specific syntax.

            Args:
                expression: The Lambda expression
                arrow_sep: The separator to use between parameters and body

            Returns:
                YDB-specific SQL for lambda functions
            """
            for ident in expression.find_all(exp.Identifier):
                new_ident = exp.to_identifier("$" + ident.alias_or_name)
                new_ident.set("quoted", False)
                ident.replace(new_ident)

            args = self.expressions(expression, flat=True)
            args = f"({args})" if len(args.split(",")) > 1 else args
            return f"({args}) {arrow_sep} {{RETURN {self.sql(expression, 'this')}}}"

        def _is_simple_expression(self, expr: exp.Expression) -> bool:
            """
            Check if an expression is simple enough to be used directly in CASE/IF.
            Simple expressions are literals, columns, identifiers, and basic operations.

            Args:
                expr: The expression to check

            Returns:
                True if the expression is simple, False otherwise
            """
            if isinstance(expr, (exp.Literal, exp.Null)):
                return True

            if isinstance(expr, exp.Column):
                col_name = (
                    expr.this.name
                    if hasattr(expr.this, "name")
                    else (expr.alias_or_name if hasattr(expr, "alias_or_name") else None)
                )
                if not col_name or col_name == "*" or col_name == "":
                    return False
                return True

            if isinstance(expr, (exp.Star, exp.Identifier)):
                return True

            if isinstance(expr, exp.Binary):
                return self._is_simple_expression(expr.this) and self._is_simple_expression(
                    expr.expression
                )
            if isinstance(expr, exp.Paren):
                return self._is_simple_expression(expr.this)
            if isinstance(expr, (exp.Subquery, exp.Case, exp.If, exp.Func, exp.AggFunc)):
                return False
            return not any(
                isinstance(node, (exp.Subquery, exp.Case, exp.If, exp.Func, exp.AggFunc))
                for node in expr.walk()
                if node is not expr
            )

        def _references_unnest_alias(self, expr: exp.Expression) -> bool:
            """
            Check if an expression references table aliases from unnesting (like _u_0, _u_1).
            These aliases are only available in the main query, not in standalone SELECT statements.

            Args:
                expr: The expression to check

            Returns:
                True if the expression references an unnest alias, False otherwise
            """
            for node in expr.walk():
                if isinstance(node, exp.Column) and hasattr(node, "table") and node.table:
                    table_name = (
                        node.table
                        if isinstance(node.table, str)
                        else (node.table.name if hasattr(node.table, "name") else str(node.table))
                    )
                    if table_name and table_name.startswith("_u_"):
                        return True
            return False

        def _extract_to_variable(
            self, expr: exp.Expression, var_prefix: str = "_case_var"
        ) -> exp.Expression:
            """
            Extract an expression to a variable and return a Var reference.
            Wraps non-subquery expressions in a SELECT statement for YQL.

            If the expression references unnest aliases (like _u_0), it cannot be extracted
            to a variable since those aliases are not in scope. In that case, return the
            expression as-is.

            Args:
                expr: The expression to extract
                var_prefix: Prefix for variable name

            Returns:
                A Var expression referencing the extracted variable, or the original expression
                if it references unnest aliases
            """
            # Check if expression references unnest aliases - these cannot be extracted
            if self._references_unnest_alias(expr):
                # Return the expression as-is - it must stay inline in the main query
                return expr.copy()

            var_counter = len(self.ydb_variables)
            var_name = f"{var_prefix}_{var_counter}"

            # If it's already a subquery, use it directly
            if isinstance(expr, exp.Subquery):
                self.ydb_variables[var_name] = expr.copy()
            else:
                # Wrap the expression in a SELECT statement
                # For scalar expressions, SELECT returns a single row with the value
                select_expr = exp.select(expr.copy())
                self.ydb_variables[var_name] = select_expr

            # Reference the variable directly (YQL variables are scalars when from SELECT)
            return exp.Var(this=f"${var_name}")

        def _if(self, expression: exp.If) -> str:
            # Extract complex expressions to variables
            condition = expression.this
            true_expr = expression.args.get("true")
            false_expr = expression.args.get("false")

            # Extract condition if it's not simple
            if not self._is_simple_expression(condition):
                condition = self._extract_to_variable(condition, "_if_cond")
            else:
                condition = condition.copy()

            # Extract true expression if it's not simple
            if true_expr and not self._is_simple_expression(true_expr):
                true_expr = self._extract_to_variable(true_expr, "_if_true")
            elif true_expr:
                true_expr = true_expr.copy()

            # Extract false expression if it's not simple
            if false_expr and not self._is_simple_expression(false_expr):
                false_expr = self._extract_to_variable(false_expr, "_if_false")
            elif false_expr:
                false_expr = false_expr.copy()

            this = self.sql(condition)
            true = self.sql(true_expr) if true_expr else ""
            false = self.sql(false_expr) if false_expr else ""
            return f"IF({this}, {true}, {false})"

        def _null_if(self, expression: exp.Nullif) -> str:
            lhs = expression.this
            rhs = expression.expression

            cond = exp.EQ(this=lhs, expression=rhs)
            return self.sql(exp.If(this=cond, true=exp.Null(), false=lhs))

        E = t.TypeVar("E", bound=Expression)

        def _simplify_unless_literal(self, expression: E) -> E:
            if not isinstance(expression, exp.Literal):
                expression = simplify(expression, dialect=self.dialect)
            return expression

        # we move the WHERE expression from ON, using literals
        def join_sql(self, expression: exp.Join) -> str:
            on_condition = expression.args.get("on")
            join_kind = expression.kind or ""

            # If LEFT/RIGHT/FULL JOIN has no ON clause, convert to CROSS JOIN
            # YDB requires LEFT JOINs to have an ON clause
            if not on_condition and any(
                kind in join_kind.upper() for kind in ["LEFT", "RIGHT", "FULL", "OUTER", ""]
            ):
                expression.set("kind", None)
                expression.set("on", None)
                return super().join_sql(expression)

            if on_condition:
                # Extract all non-equality conditions (including those with literals)
                # YDB only allows equality predicates in JOIN ON
                literal_conditions: list[Expression] = []
                non_equality_conditions: list[Expression] = []
                equality_conditions: list[Expression] = []

                if isinstance(on_condition, exp.And):
                    conditions = list(on_condition.flatten())
                else:
                    conditions = [on_condition]

                for cond in conditions:
                    # Check if it's an equality predicate
                    if isinstance(cond, exp.EQ):
                        # Check if it's a true equi-join (columns from different tables)
                        left = cond.this
                        right = cond.expression
                        if (
                            isinstance(left, exp.Column)
                            and isinstance(right, exp.Column)
                            and hasattr(left, "table")
                            and hasattr(right, "table")
                            and left.table
                            and right.table
                            and left.table != right.table
                        ):
                            equality_conditions.append(cond)
                        else:
                            if self._contains_literals(cond):
                                literal_conditions.append(cond)
                            else:
                                non_equality_conditions.append(cond)
                    else:
                        if self._contains_literals(cond):
                            literal_conditions.append(cond)
                        else:
                            non_equality_conditions.append(cond)

                conditions_to_move = literal_conditions + non_equality_conditions

                if equality_conditions:
                    if len(equality_conditions) == 1:
                        on_condition = equality_conditions[0]
                    else:
                        on_condition = equality_conditions[0]
                        for cond in equality_conditions[1:]:
                            on_condition = exp.and_(on_condition, cond)
                    expression.set("on", on_condition)
                else:
                    # No valid equality conditions
                    # For LEFT/RIGHT/FULL JOINs, YDB requires ON clause, so convert to CROSS JOIN
                    join_kind = expression.side or ""
                    if any(
                        kind in join_kind.upper() for kind in ["LEFT", "RIGHT", "FULL", "OUTER"]
                    ):
                        # Convert to CROSS JOIN by removing kind and ON
                        expression.set("kind", None)
                        expression.set("on", None)
                        expression.set("side", "CROSS")
                    else:
                        expression.set("on", None)

                if conditions_to_move:
                    select_stmt = expression.find_ancestor(exp.Select)
                    if select_stmt:
                        combined_condition = conditions_to_move[0]
                        for cond in conditions_to_move[1:]:
                            combined_condition = exp.and_(combined_condition, cond)

                        existing_where = select_stmt.args.get("where")
                        if existing_where:
                            new_where = exp.and_(existing_where.this, combined_condition)
                            select_stmt.set("where", exp.Where(this=new_where))
                        else:
                            select_stmt.set("where", exp.Where(this=combined_condition))

                join_sql = super().join_sql(expression)
                return join_sql

            return super().join_sql(expression)

        def select_sql(self, expression: exp.Select) -> str:
            """
            Generate SELECT SQL without modifying the original expressions.
            The GROUP BY and ORDER BY will handle alias references separately.
            """
            # Store the original-to-alias mapping for GROUP BY/ORDER BY reference
            self.expression_to_alias = {}

            # Build mapping of original expressions to their aliases
            for select_expr in expression.expressions:
                if isinstance(select_expr, exp.Alias):
                    expr_sql = self.sql(select_expr.this).strip()
                    self.expression_to_alias[expr_sql] = select_expr.alias_or_name
                else:
                    expr_sql = self.sql(select_expr).strip()
                    if isinstance(select_expr, (exp.Column, exp.Identifier)):
                        self.expression_to_alias[expr_sql] = select_expr.alias_or_name

            # Generate SQL without modifying expressions
            return super().select_sql(expression)

        def _contains_literals(self, condition: exp.Expression) -> bool:
            return condition.find(exp.Literal) is not None

        def where_sql(self, expression: exp.Where) -> str:
            original_where = super().where_sql(expression) if expression else ""
            return original_where

        def _date_add(self, expression: exp.Expression) -> str:
            this = expression.this
            unit = unit_to_var(expression.expression)
            op = (
                "+"
                if isinstance(
                    expression, (exp.DateAdd, exp.TimeAdd, exp.DatetimeAdd, exp.TsOrDsAdd)
                )
                else "-"
            )

            expr = expression.expression

            source = None
            if isinstance(this, exp.Literal):
                if " " in this.name:
                    source = f"DateTime::MakeDateTime(DateTime::ParseIso8601({self.sql(this).replace(' ', 'T')}))"
                else:
                    source = f"CAST({self.sql(this)} AS DATE)"
            else:
                source = self.sql(this)
            if not unit:
                return ""
            if unit.name in ["MONTH", "YEARS"]:
                to_type = (
                    "DateTime"
                    if isinstance(expression, (exp.DatetimeAdd, exp.DatetimeSub))
                    else "Date"
                )
                if unit.name == "YEARS":
                    return f"DateTime::Make{to_type}(DateTime::ShiftYears({source}, {op if op == '-' else ''}{expr.name}))"
                if unit.name == "MONTH":
                    return f"DateTime::Make{to_type}(DateTime::ShiftMonths({source}, {op if op == '-' else ''}{expr.name}))"
                return ""
            else:
                if unit.name == "DAY":
                    interval_expr = f"DateTime::IntervalFromDays({expr.name})"
                elif unit.name == "HOUR":
                    interval_expr = f"DateTime::IntervalFromHours({expr.name})"
                elif unit.name == "MINUTE":
                    interval_expr = f"DateTime::IntervalFromMinutes({expr.name})"
                elif unit.name == "SECOND":
                    interval_expr = f"DateTime::IntervalFromSeconds({expr.name})"
                else:
                    raise ValueError(f"Unsupported interval type: {unit.name}")

                return f"{source} {op} {interval_expr}"

        def _arrayany(self, expression: exp.ArrayAny) -> str:
            """
            Generate SQL for ARRAY_ANY function with YDB-specific implementation.

            Args:
                expression: The ARRAY_ANY expression

            Returns:
                YDB-specific SQL for array existence checks
            """
            param = expression.expression.expressions[0]
            column_references = {}

            for ident in expression.expression.this.find_all(exp.Column):
                if len(ident.parts) < 2:
                    continue

                table_reference = ident.parts[0]
                column_reference = ident.parts[1]
                column_references[
                    f"{table_reference.alias_or_name}.{column_reference.alias_or_name}"
                ] = (table_reference, column_reference)

            if len(column_references) > 0:
                table_aliases = {}
                next_alias = name_sequence("p_")
                for column_reference in column_references:
                    table_aliases[column_reference] = next_alias()

                params_l = [
                    f"${param}" for param in [param.alias_or_name] + list(table_aliases.values())
                ]
                params = f"({', '.join(params_l)})"

                for ident in list(expression.expression.this.find_all(exp.Column)):
                    if len(ident.parts) < 2:
                        continue

                    table_reference = ident.parts[0]
                    column_reference = ident.parts[1]
                    full_column_reference = (
                        f"{table_reference.alias_or_name}.{column_reference.alias_or_name}"
                    )
                    table_alias = table_aliases[full_column_reference]
                    table_reference.pop()
                    column_reference.replace(exp.to_identifier(table_alias))

                lambda_sql = self.sql(expression.expression)
                table_aliases_sql = (
                    f"({', '.join([expression.this.alias_or_name] + list(table_aliases.keys()))})"
                )

                return f"ListHasItems({params}->(ListFilter(${param.alias_or_name}, {lambda_sql})){table_aliases_sql})"
            else:
                return f"ListHasItems(ListFilter({self.sql(expression.expression)}))"

        def _set_sql(self, expression: exp.Set) -> str:
            eq = expression.find(exp.EQ)
            if not eq:
                return ""
            var_name = exp.Identifier(this="$" + eq.this.name)

            new_eq = exp.EQ(this=var_name, expression=eq.expression)

            return self.binary(new_eq, "=")

        def _group_by(self, expression: exp.Group) -> str:
            """Generate GROUP BY using alias references."""
            select_stmt = expression.find_ancestor(exp.Select)

            if not select_stmt:
                group_by_items = ", ".join(self.sql(e) for e in expression.expressions)
                return f" GROUP BY {group_by_items}" if group_by_items else " GROUP BY"

            transformed = []
            for gb_expr in expression.expressions:
                gb_sql = self.sql(gb_expr).strip()

                # Check if we have a stored mapping for this expression
                if hasattr(self, "expression_to_alias") and gb_sql in self.expression_to_alias:
                    alias_name = self.expression_to_alias[gb_sql]
                    alias_expr = exp.alias_(gb_expr, alias_name)
                    transformed.append(alias_expr)
                else:
                    if isinstance(gb_expr, (exp.Column, exp.Identifier)):
                        # Use the column name as the alias
                        column_name = gb_expr.alias_or_name
                        alias_expr = exp.alias_(gb_expr, column_name)
                        transformed.append(alias_expr)
                    else:
                        transformed.append(gb_expr)

            group_by_items = ", ".join(f"{self.sql(e)}" for e in transformed) if transformed else ""

            # Handle ROLLUP, CUBE, and GROUPING SETS
            rollup = self.expressions(expression, key="rollup")
            cube = self.expressions(expression, key="cube")
            grouping_sets = self.expressions(expression, key="grouping_sets")

            # Build the GROUP BY clause
            if group_by_items:
                result = f" GROUP BY ({group_by_items})"
            else:
                result = " GROUP BY"

            # Add ROLLUP, CUBE, or GROUPING SETS
            if rollup:
                result += f" {rollup}"
            elif cube:
                result += f" {cube}"
            elif grouping_sets:
                result += f" {grouping_sets}"

            return result

        def _order_sql(self, expression: exp.Order) -> str:
            """Generate ORDER BY using alias references."""
            select_stmt = expression.find_ancestor(exp.Select)

            if not select_stmt:
                return super().order_sql(expression)

            orders = []
            for order_expr in expression.expressions:
                if isinstance(order_expr, exp.Ordered):
                    expr = order_expr.this
                    expr_sql = self.sql(expr).strip()

                    if (
                        hasattr(self, "expression_to_alias")
                        and expr_sql in self.expression_to_alias
                    ):
                        alias_name = self.expression_to_alias[expr_sql]
                        alias_expr = exp.to_identifier(alias_name)
                        ordered = exp.Ordered(this=alias_expr, desc=order_expr.args.get("desc"))
                        orders.append(ordered)
                    else:
                        orders.append(order_expr)
                else:
                    expr_sql = self.sql(order_expr).strip()
                    if (
                        hasattr(self, "expression_to_alias")
                        and expr_sql in self.expression_to_alias
                    ):
                        alias_name = self.expression_to_alias[expr_sql]
                        alias_expr = exp.to_identifier(alias_name)
                        orders.append(alias_expr)
                    else:
                        orders.append(order_expr)
            if not orders:
                return ""

            order_sql = ", ".join(self.sql(e) for e in orders)
            return f" ORDER BY {order_sql}"

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            **STRING_TYPE_MAPPING,
            exp.DataType.Type.TINYINT: "INT8",
            exp.DataType.Type.SMALLINT: "INT16",
            exp.DataType.Type.INT: "INT32",
            exp.DataType.Type.BIGINT: "INT64",
            exp.DataType.Type.DECIMAL: "Decimal",
            exp.DataType.Type.FLOAT: "Float",
            exp.DataType.Type.DOUBLE: "Double",
            exp.DataType.Type.BOOLEAN: "Uint8",
            exp.DataType.Type.TIMESTAMP: "Timestamp",
            exp.DataType.Type.BIT: "Uint8",
            exp.DataType.Type.VARCHAR: "String",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.Create: create_sql,
            exp.DefaultColumnConstraint: lambda self, e: "",
            exp.DateTrunc: _date_trunc_sql,
            exp.Select: transforms.preprocess(
                [apply_alias_to_select_from_table, move_ctes_to_top_level]
            ),
            exp.CurrentTimestamp: _current_timestamp_sql,
            exp.StrToDate: _str_to_date,
            exp.Extract: _extract,
            exp.ArraySize: rename_func_not_normalize("ListLength"),
            exp.ArrayFilter: rename_func_not_normalize("ListFilter"),
            exp.Lambda: _lambda,
            exp.ArrayAny: _arrayany,
            exp.ArrayAgg: rename_func_not_normalize("AGGREGATE_LIST"),
            exp.Concat: concat_to_dpipe_sql,
            exp.If: _if,
            exp.Nullif: _null_if,
            exp.DateAdd: _date_add,
            exp.DateSub: _date_add,
            exp.JSONBContains: rename_func_not_normalize("Yson::Contains"),
            exp.ForeignKey: lambda self, e: self.unsupported("constraint not supported"),
            exp.StringToArray: rename_func_not_normalize("String::SplitToList"),
            exp.Array: rename_func_not_normalize("AsList"),
            exp.ArrayToString: rename_func_not_normalize("String::JoinFromList"),
            exp.Upper: rename_func_not_normalize("String::Upper"),
            exp.Lower: rename_func_not_normalize("String::Lower"),
            exp.StrPosition: rename_func_not_normalize("Find"),
            exp.Length: rename_func_not_normalize("String::Length"),
            exp.Unnest: rename_func_not_normalize("FLATTEN BY"),
            exp.Round: rename_func_not_normalize("Math::Round"),
            exp.Set: _set_sql,
            exp.Group: _group_by,
            exp.Order: _order_sql,
        }
