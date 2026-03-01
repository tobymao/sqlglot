"""CTE rule: extract WITH clause and recursively decompile each CTE body."""

from __future__ import annotations

from sqlglot import exp

from ..result import PipeOperator, PipeOpType, PipeQuery


def extract_ctes(
    ast: exp.Select, decompile_fn, dialect: str = "sqlite"
) -> tuple[list[tuple[str, PipeQuery]], list[str]]:
    """Extract CTEs from the AST and recursively decompile each body.

    Returns (ctes, cte_names) where ctes is a list of (name, PipeQuery) tuples.
    If a CTE body's pipe form would create nested WITH (via |> SELECT or inner CTEs),
    falls back to standard SQL to avoid SQLite errors.
    """
    with_ = ast.args.get("with_")
    if not with_:
        return [], []

    ctes = []
    cte_names = []

    for cte_expr in with_.expressions:
        name = cte_expr.alias
        cte_names.append(name)

        body = cte_expr.this
        from ..emitter import emit_pipe_query

        pipe_query = emit_pipe_query(body, dialect=dialect)

        # Check if pipe form would create nested CTEs
        # Both SELECT and AGGREGATE operators create implicit CTEs during transpilation
        has_cte_creating_op = any(
            op.op_type in (PipeOpType.SELECT, PipeOpType.AGGREGATE) for op in pipe_query.operators
        )
        has_inner_ctes = bool(pipe_query.ctes)

        if has_cte_creating_op or has_inner_ctes:
            # Fall back to standard SQL to avoid nested WITH
            fallback_sql = body.sql(dialect=dialect)
            pipe_query = PipeQuery(
                operators=[PipeOperator(op_type=PipeOpType.FROM, sql_fragment=fallback_sql)]
            )

        ctes.append((name, pipe_query))

    return ctes, cte_names
