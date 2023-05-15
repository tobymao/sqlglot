import itertools

from sqlglot import alias, exp
from sqlglot.helper import csv_reader
from sqlglot.optimizer.scope import Scope, traverse_scope


def qualify_tables(expression, db=None, catalog=None, schema=None):
    """
    Rewrite sqlglot AST to have fully qualified tables. Additionally, this
    replaces "join constructs" (*) by equivalent SELECT * subqueries.

    Examples:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("SELECT 1 FROM tbl")
        >>> qualify_tables(expression, db="db").sql()
        'SELECT 1 FROM db.tbl AS tbl'
        >>>
        >>> expression = sqlglot.parse_one("SELECT * FROM (tbl1 JOIN tbl2 ON id1 = id2)")
        >>> qualify_tables(expression).sql()
        'SELECT * FROM (SELECT * FROM tbl1 AS tbl1 JOIN tbl2 AS tbl2 ON id1 = id2) AS _q_0'

    Args:
        expression (sqlglot.Expression): expression to qualify
        db (str): Database name
        catalog (str): Catalog name
        schema: A schema to populate

    Returns:
        sqlglot.Expression: qualified expression

    (*) See section 7.2.1.2 in https://www.postgresql.org/docs/current/queries-table-expressions.html
    """
    sequence = itertools.count()

    next_name = lambda: f"_q_{next(sequence)}"

    for scope in traverse_scope(expression):
        for derived_table in itertools.chain(scope.ctes, scope.derived_tables):
            # Expand join construct
            if isinstance(derived_table, exp.Subquery):
                unnested = derived_table.unnest()
                if isinstance(unnested, exp.Table):
                    derived_table.this.replace(exp.select("*").from_(unnested.copy(), copy=False))

            if not derived_table.args.get("alias"):
                alias_ = next_name()
                derived_table.set("alias", exp.TableAlias(this=exp.to_identifier(alias_)))
                scope.rename_source(None, alias_)

            pivots = derived_table.args.get("pivots")
            if pivots and not pivots[0].alias:
                pivots[0].set("alias", exp.TableAlias(this=exp.to_identifier(next_name())))

        for name, source in scope.sources.items():
            if isinstance(source, exp.Table):
                if isinstance(source.this, exp.Identifier):
                    if not source.args.get("db"):
                        source.set("db", exp.to_identifier(db))
                    if not source.args.get("catalog"):
                        source.set("catalog", exp.to_identifier(catalog))

                if not source.alias:
                    source = source.replace(
                        alias(
                            source,
                            name or source.name or next_name(),
                            copy=True,
                            table=True,
                        )
                    )

                pivots = source.args.get("pivots")
                if pivots and not pivots[0].alias:
                    pivots[0].set("alias", exp.TableAlias(this=exp.to_identifier(next_name())))

                if schema and isinstance(source.this, exp.ReadCSV):
                    with csv_reader(source.this) as reader:
                        header = next(reader)
                        columns = next(reader)
                        schema.add_table(
                            source, {k: type(v).__name__ for k, v in zip(header, columns)}
                        )
            elif isinstance(source, Scope) and source.is_udtf:
                udtf = source.expression
                table_alias = udtf.args.get("alias") or exp.TableAlias(this=next_name())
                udtf.set("alias", table_alias)

                if not table_alias.name:
                    table_alias.set("this", next_name())
                if isinstance(udtf, exp.Values) and not table_alias.columns:
                    for i, e in enumerate(udtf.expressions[0].expressions):
                        table_alias.append("columns", exp.to_identifier(f"_col_{i}"))

    return expression
