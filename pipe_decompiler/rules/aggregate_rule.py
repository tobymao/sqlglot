"""AGGREGATE rule: emit AGGREGATE operator for GROUP BY + aggregate functions."""

from __future__ import annotations

from sqlglot import exp

from ..result import PipeOperator, PipeOpType


def _has_agg_func(expr: exp.Expression) -> bool:
    """Check if an expression contains an aggregate function (not inside a Window)."""
    for node in expr.walk():
        if isinstance(node, exp.Window):
            return False
        if isinstance(node, exp.AggFunc):
            return True
    return False


def _strip_table_qualifier(sql: str) -> str:
    """Remove table qualifier from a simple column reference like T2.name -> name."""
    if "." in sql and sql.count(".") == 1:
        parts = sql.split(".")
        if " " not in parts[0] and "(" not in parts[0]:
            return parts[1]
    return sql


def _collect_having_agg_exprs(
    ast: exp.Select, existing_agg_sqls: set[str], dialect: str = "sqlite"
) -> list[tuple[str, str]]:
    """Collect aggregate expressions from HAVING that aren't already in SELECT.

    Returns list of (agg_sql, alias) tuples.
    """
    having = ast.args.get("having")
    if not having:
        return []

    result = []
    counter = 0
    seen = set()

    for node in having.walk():
        if isinstance(node, exp.AggFunc):
            # Skip aggregates inside subqueries — they belong to the inner query
            select_ancestor = node.find_ancestor(exp.Select)
            if select_ancestor is not None and select_ancestor is not ast:
                continue

            agg_sql = node.sql(dialect=dialect)
            upper_sql = agg_sql.upper()

            if upper_sql not in existing_agg_sqls and upper_sql not in seen:
                counter += 1
                alias = f"_having{counter}"
                result.append((agg_sql, alias))
                seen.add(upper_sql)

    return result


def _substitute_having_aliases(having_sql: str, having_aliases: list[tuple[str, str]]) -> str:
    """Replace aggregate expressions in HAVING SQL with their aliases."""
    result = having_sql
    for agg_sql, alias in having_aliases:
        # Case-insensitive replacement
        import re

        pattern = re.escape(agg_sql)
        result = re.sub(pattern, alias, result, flags=re.IGNORECASE)
    return result


def emit(
    ast: exp.Select,
    dialect: str = "sqlite",
    extra_agg_exprs: list[tuple[str, str]] | None = None,
) -> tuple[list[PipeOperator], dict[str, str]]:
    """Emit AGGREGATE and optional HAVING-WHERE operators.

    Returns (operators, group_expr_aliases) where group_expr_aliases maps
    GROUP BY expression SQL (uppercased) to alias names for non-simple expressions.
    """
    group = ast.args.get("group")
    select_exprs = ast.expressions

    if not group:
        # Aggregates without GROUP BY
        parts = [expr.sql(dialect=dialect) for expr in select_exprs]
        agg_fragment = "AGGREGATE " + ", ".join(parts)
        return [PipeOperator(op_type=PipeOpType.AGGREGATE, sql_fragment=agg_fragment)], {}

    group_exprs = group.expressions
    # Include both qualified and unqualified forms for matching
    group_strs_set = set()
    for e in group_exprs:
        sql = e.sql(dialect=dialect).upper()
        group_strs_set.add(sql)
        group_strs_set.add(_strip_table_qualifier(sql))

    # Collect existing aggregate SQL from SELECT (for dedup with HAVING)
    existing_agg_sqls = set()
    for expr in select_exprs:
        if _has_agg_func(expr):
            if isinstance(expr, exp.Alias):
                existing_agg_sqls.add(expr.this.sql(dialect=dialect).upper())
            else:
                existing_agg_sqls.add(expr.sql(dialect=dialect).upper())

    # Collect HAVING aggregate expressions not in SELECT
    having_aliases = _collect_having_agg_exprs(ast, existing_agg_sqls, dialect=dialect)

    # Build AGGREGATE expression list
    agg_parts = []
    agg_counter = 0
    for expr in select_exprs:
        expr_sql = expr.sql(dialect=dialect)

        if _has_agg_func(expr):
            if isinstance(expr, exp.Alias):
                agg_parts.append(expr_sql)
            else:
                agg_counter += 1
                alias = f"_agg{agg_counter}"
                agg_parts.append(f"{expr_sql} AS {alias}")
        else:
            upper_sql = expr_sql.upper()
            stripped = _strip_table_qualifier(upper_sql)
            if upper_sql not in group_strs_set and stripped not in group_strs_set:
                agg_parts.append(expr_sql)

    # Add extra aggregate expressions from ORDER BY
    if extra_agg_exprs:
        for agg_sql, alias in extra_agg_exprs:
            agg_parts.append(f"{agg_sql} AS {alias}")

    # Add HAVING aggregate expressions
    for agg_sql, alias in having_aliases:
        agg_parts.append(f"{agg_sql} AS {alias}")

    # Build GROUP BY and alias non-simple expressions for CTE reference
    group_strs = []
    group_expr_aliases: dict[str, str] = {}
    grp_alias_counter = 0
    for e in group_exprs:
        expr_sql = e.sql(dialect=dialect)
        group_strs.append(expr_sql)
        # For function-call GROUP BY expressions, add an alias so they can be
        # referenced by name after CTE wrapping (avoids qualified column mismatch)
        if not isinstance(e, exp.Column):
            grp_alias_counter += 1
            alias = f"_grp{grp_alias_counter}"
            agg_parts.append(f"{expr_sql} AS {alias}")
            group_expr_aliases[expr_sql.upper()] = alias
            # Also store stripped version for matching in projection_rule
            stripped = e.copy()
            for col in stripped.find_all(exp.Column):
                col.set("table", None)
            group_expr_aliases[stripped.sql(dialect=dialect).upper()] = alias

    agg_fragment = "AGGREGATE"
    if agg_parts:
        agg_fragment += " " + ", ".join(agg_parts)
    agg_fragment += " GROUP BY " + ", ".join(group_strs)

    operators = [PipeOperator(op_type=PipeOpType.AGGREGATE, sql_fragment=agg_fragment)]

    # HAVING → WHERE after AGGREGATE, with aggregate aliases substituted
    # and table qualifiers stripped (CTE context after AGGREGATE)
    having = ast.args.get("having")
    if having:
        having_sql = having.this.sql(dialect=dialect)

        # Build full alias map for substitution (SELECT + HAVING aliases)
        all_aliases = list(having_aliases)
        # Also map SELECT aggregates to their aliases
        agg_counter = 0
        for expr in select_exprs:
            if _has_agg_func(expr):
                if isinstance(expr, exp.Alias):
                    agg_sql = expr.this.sql(dialect=dialect)
                    all_aliases.append((agg_sql, expr.alias))
                else:
                    agg_counter += 1
                    agg_sql = expr.sql(dialect=dialect)
                    all_aliases.append((agg_sql, f"_agg{agg_counter}"))

        # Substitute aggregate aliases first (on qualified SQL)
        having_sql = _substitute_having_aliases(having_sql, all_aliases)

        # Then strip table qualifiers from remaining column references
        # (after AGGREGATE CTE, table aliases like T1/T2 don't exist)
        having_ast = exp.maybe_parse(having_sql, dialect=dialect)
        for col in having_ast.find_all(exp.Column):
            col.set("table", None)
        having_sql = having_ast.sql(dialect=dialect)

        operators.append(PipeOperator(op_type=PipeOpType.WHERE, sql_fragment=f"WHERE {having_sql}"))

    return operators, group_expr_aliases


def has_aggregates(ast: exp.Select) -> bool:
    """Check if the SELECT has any aggregate functions."""
    for expr in ast.expressions:
        if _has_agg_func(expr):
            return True
    return False
