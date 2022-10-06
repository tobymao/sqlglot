from sqlglot import alias, exp
from sqlglot.errors import OptimizeError
from sqlglot.schema import ensure_schema
from sqlglot.optimizer.scope import traverse_scope
from sqlglot.optimizer.qualify_columns import _pop_table_column_aliases, _Resolver, _expand_using, _expand_group_by, _expand_order_by, _qualify_columns, _qualify_outputs, _add_except_columns, _add_replace_columns


def qualify_columns_and_dedup(expression, schema):
    """
    Rewrite sqlglot AST to have fully qualified columns.

    Example:
        >>> import sqlglot
        >>> schema = {"tbl": {"col": "INT"}}
        >>> expression = sqlglot.parse_one("SELECT col FROM tbl")
        >>> qualify_columns(expression, schema).sql()
        'SELECT tbl.col AS col FROM tbl'

    Args:
        expression (sqlglot.Expression): expression to qualify
        schema (dict|sqlglot.optimizer.Schema): Database schema
    Returns:
        sqlglot.Expression: qualified expression
    """
    schema = ensure_schema(schema)

    for scope in traverse_scope(expression):
        resolver = _Resolver(scope, schema)
        _pop_table_column_aliases(scope.ctes)
        _pop_table_column_aliases(scope.derived_tables)
        _expand_using(scope, resolver)
        _expand_group_by(scope, resolver)
        _expand_order_by(scope)
        _qualify_columns(scope, resolver)
        if not isinstance(scope.expression, exp.UDTF):
            _expand_stars_and_dedup(scope, resolver)
            _qualify_outputs(scope)
        # _check_unknown_tables(scope)

    return expression


def _expand_stars_and_dedup(scope, resolver):
    """Expand stars to lists of column selections"""

    new_selections = []
    except_columns = {}
    replace_columns = {}

    for expression in scope.selects:
        if isinstance(expression, exp.Star):
            tables = list(scope.selected_sources)
            _add_except_columns(expression, tables, except_columns)
            _add_replace_columns(expression, tables, replace_columns)
        elif isinstance(expression, exp.Column) and isinstance(expression.this, exp.Star):
            tables = [expression.table] if expression.table else list(scope.selected_sources)
            _add_except_columns(expression.this, tables, except_columns)
            _add_replace_columns(expression.this, tables, replace_columns)
        else:
            new_selections.append(expression)
            continue

        for table in tables:
            if table not in scope.sources:
                raise OptimizeError(f"Unknown table: {table}")
            columns = resolver.get_source_columns(table)
            table_id = id(table)
            for name in columns:
                if name not in except_columns.get(table_id, set()):
                    alias_ = replace_columns.get(table_id, {}).get(name, name)
                    column = exp.column(name, table)
                    new_selections.append(alias(column, alias_) if alias_ != name else column)

    scope.expression.set("expressions", list({col.alias_or_name: col for col in new_selections}.values()))
