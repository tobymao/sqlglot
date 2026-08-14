from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, DialectType

if t.TYPE_CHECKING:
    from sqlglot._typing import E


@t.overload
def normalize_identifiers(
    expression: E, dialect: DialectType = None, store_original_column_identifiers: bool = False
) -> E: ...


@t.overload
def normalize_identifiers(
    expression: str, dialect: DialectType = None, store_original_column_identifiers: bool = False
) -> exp.Identifier: ...


def normalize_identifiers(expression, dialect=None, store_original_column_identifiers=False):
    """
    Normalize identifiers by converting them to either lower or upper case,
    ensuring the semantics are preserved in each case (e.g. by respecting
    case-sensitivity).

    This transformation reflects how identifiers would be resolved by the engine corresponding
    to each SQL dialect, and plays a very important role in the standardization of the AST.

    It's possible to make this a no-op by adding a special comment next to the
    identifier of interest:

        SELECT a /* sqlglot.meta case_sensitive */ FROM table

    In this example, the identifier `a` will not be normalized.

    Note:
        Some dialects (e.g. DuckDB) treat all identifiers as case-insensitive even
        when they're quoted, so in these cases all identifiers are normalized.

    Known limitation:
        Engines that preserve case expose it as data, i.e., as output column names, so normalizing
        can change what a statement produces. E.g., `CREATE TABLE t AS SELECT 1 AS Foo` materializes
        a `foo` column in DuckDB after this transformation, instead of `Foo`.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one('SELECT Bar.A AS A FROM "Foo".Bar')
        >>> normalize_identifiers(expression).sql()
        'SELECT bar.a AS a FROM "Foo".bar'
        >>> normalize_identifiers("foo", dialect="snowflake").sql(dialect="snowflake")
        'FOO'

    Args:
        expression: The expression to transform.
        dialect: The dialect to use in order to decide how to normalize identifiers.
        store_original_column_identifiers: Whether to store the original column identifiers in
            the meta data of the expression in case we want to undo the normalization at a later point.

    Returns:
        The transformed expression.
    """
    dialect = Dialect.get_or_raise(dialect)

    if isinstance(expression, str):
        expression = exp.parse_identifier(expression, dialect=dialect)

    for node in expression.walk(prune=lambda n: bool(n.meta_get("case_sensitive"))):
        if node.meta_get("case_sensitive"):
            continue

        if store_original_column_identifiers:
            if isinstance(node, exp.Column):
                parent = node
                while parent and isinstance(parent.parent, exp.Dot):
                    parent = parent.parent

                node.meta["dot_parts"] = [p.name for p in parent.parts]
            elif isinstance(node.parent, exp.Dot) and not isinstance(
                node, (exp.Dot, exp.Identifier)
            ):
                # Dot access rooted at an arbitrary expression, e.g. PARSE_JSON(...).key
                dot_parts = []
                child, parent = node, node.parent
                while isinstance(parent, exp.Dot) and parent.this is child:
                    dot_parts.append(parent.expression.name)
                    child, parent = parent, parent.parent

                if dot_parts:
                    node.meta["dot_parts"] = dot_parts

        if isinstance(node, exp.Identifier):
            dialect.normalize_identifier(node)

    return expression
