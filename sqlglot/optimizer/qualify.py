from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, DialectType
from sqlglot.optimizer.isolate_table_selects import isolate_table_selects
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.qualify_columns import (
    qualify_columns as qualify_columns_func,
    quote_identifiers as quote_identifiers_func,
    validate_qualify_columns as validate_qualify_columns_func,
)
from sqlglot.optimizer.qualify_tables import qualify_tables
from sqlglot.schema import Schema, ensure_schema
from sqlglot._typing import E


def qualify(
    expression: E,
    dialect: DialectType = None,
    db: str | None = None,
    catalog: str | None = None,
    schema: dict[str, object] | Schema | None = None,
    expand_alias_refs: bool = True,
    expand_stars: bool = True,
    isolate_tables: bool = False,
    qualify_columns: bool = True,
    allow_partial_qualification: bool = False,
    validate_qualify_columns: bool = True,
    quote_identifiers: bool = True,
    identify: bool = True,
    canonicalize_table_aliases: bool = False,
    on_qualify: t.Callable[[exp.Table], None] | None = None,
    sql: str | None = None,
) -> E:
    """
    Rewrite sqlglot AST to have normalized and qualified tables and columns.

    This step is necessary for all further SQLGlot optimizations.

    Example:
        >>> import sqlglot
        >>> schema = {"tbl": {"col": "INT"}}
        >>> expression = sqlglot.parse_one("SELECT col FROM tbl")
        >>> qualify(expression, schema=schema).sql()
        'SELECT "tbl"."col" AS "col" FROM "tbl" AS "tbl"'

    Args:
        expression: Expr to qualify.
        db: Default database name for tables.
        catalog: Default catalog name for tables.
        schema: Schema with the column names and types of every physical table the query reads.
            Qualification raises if a table it needs to resolve columns for is missing.
            Columns of unknown function outputs require unambiguous source attribution.
        expand_alias_refs: Whether to expand references to aliases.
        expand_stars: Whether to expand star queries. This is a necessary step
            for most of the optimizer's rules to work; do not set to False unless you
            know what you're doing!
        isolate_tables: Whether to isolate table selects.
        qualify_columns: Whether to qualify columns.
        allow_partial_qualification: Whether to allow partial qualification.
        validate_qualify_columns: Whether to validate remaining unresolved columns. Disabling
            this does not permit guessing ownership when a source's columns are unknown.
        quote_identifiers: Whether to run the quote_identifiers step.
            This step is necessary to ensure correctness for case sensitive queries.
            But this flag is provided in case this step is performed at a later time.
        identify: If True, quote all identifiers, else only necessary ones.
        canonicalize_table_aliases: Whether to use canonical aliases (_0, _1, ...) for all sources
            instead of preserving table names.
        on_qualify: Callback after a table has been qualified.
        sql: Original SQL string for error highlighting. If not provided, errors will not include
            highlighting. Requires that the expression has position metadata from parsing.

    Returns:
        The qualified expression.
    """
    schema = ensure_schema(schema, dialect=dialect)
    dialect = Dialect.get_or_raise(dialect)

    expression = normalize_identifiers(
        expression,
        dialect=dialect,
        store_original_column_identifiers=True,
    )
    expression = qualify_tables(
        expression,
        db=db,
        catalog=catalog,
        dialect=dialect,
        on_qualify=on_qualify,
        canonicalize_table_aliases=canonicalize_table_aliases,
    )

    if isolate_tables:
        expression = isolate_table_selects(expression, schema=schema)

    if qualify_columns:
        expression = qualify_columns_func(
            expression,
            schema,
            expand_alias_refs=expand_alias_refs,
            expand_stars=expand_stars,
            allow_partial_qualification=allow_partial_qualification,
        )

    if quote_identifiers:
        expression = quote_identifiers_func(expression, dialect=dialect, identify=identify)

    if validate_qualify_columns:
        validate_qualify_columns_func(expression, sql=sql)

    return expression
