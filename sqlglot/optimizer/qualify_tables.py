import itertools

from sqlglot import alias, exp
from sqlglot.helper import csv_reader
from sqlglot.optimizer.scope import Scope, traverse_scope


def qualify_tables(expression, db=None, catalog=None, schema=None):
    """
    Rewrite sqlglot AST to have fully qualified tables.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("SELECT 1 FROM tbl")
        >>> qualify_tables(expression, db="db").sql()
        'SELECT 1 FROM db.tbl AS tbl'

    Args:
        expression (sqlglot.Expression): expression to qualify
        db (str): Database name
        catalog (str): Catalog name
        schema: A schema to populate
    Returns:
        sqlglot.Expression: qualified expression
    """
    sequence = itertools.count()

    next_name = lambda: f"_q_{next(sequence)}"

    for scope in traverse_scope(expression):
        for derived_table in itertools.chain(scope.ctes, scope.derived_tables):
            if not derived_table.args.get("alias"):
                alias_ = f"_q_{next(sequence)}"
                derived_table.set("alias", exp.TableAlias(this=exp.to_identifier(alias_)))
                scope.rename_source(None, alias_)

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
                            source.copy(),
                            name if name else next_name(),
                            table=True,
                        )
                    )

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

    return expression
