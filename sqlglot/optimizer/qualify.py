from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.dialects.dialect import DialectType
from sqlglot.optimizer.isolate_table_selects import isolate_table_selects
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.qualify_columns import (
    qualify_columns as qualify_columns_func,
    quote_identifiers as quote_identifiers_func,
    validate_qualify_columns as validate_qualify_columns_func,
)
from sqlglot.optimizer.qualify_tables import qualify_tables
from sqlglot.schema import Schema, ensure_schema


def qualify(
    expression: exp.Expression,
    dialect: DialectType = None,
    db: t.Optional[str] = None,
    catalog: t.Optional[str] = None,
    schema: t.Optional[dict | Schema] = None,
    expand_alias_refs: bool = True,
    infer_schema: t.Optional[bool] = None,
    isolate_tables: bool = False,
    qualify_columns: bool = True,
    validate_qualify_columns: bool = True,
    quote_identifiers: bool = True,
    identify: bool = True,
) -> exp.Expression:
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
        expression: Expression to qualify.
        db: Default database name for tables.
        catalog: Default catalog name for tables.
        schema: Schema to infer column names and types.
        expand_alias_refs: Whether or not to expand references to aliases.
        infer_schema: Whether or not to infer the schema if missing.
        isolate_tables: Whether or not to isolate table selects.
        qualify_columns: Whether or not to qualify columns.
        validate_qualify_columns: Whether or not to validate columns.
        quote_identifiers: Whether or not to run the quote_identifiers step.
            This step is necessary to ensure correctness for case sensitive queries.
            But this flag is provided in case this step is performed at a later time.
        identify: If True, quote all identifiers, else only necessary ones.

    Returns:
        The qualified expression.
    """
    schema = ensure_schema(schema, dialect=dialect)
    expression = qualify_tables(expression, db=db, catalog=catalog, schema=schema)
    expression = normalize_identifiers(expression, dialect=dialect)

    if isolate_tables:
        expression = isolate_table_selects(expression, schema=schema)

    if qualify_columns:
        expression = qualify_columns_func(
            expression, schema, expand_alias_refs=expand_alias_refs, infer_schema=infer_schema
        )

    if quote_identifiers:
        expression = quote_identifiers_func(expression, dialect=dialect, identify=identify)

    if validate_qualify_columns:
        validate_qualify_columns_func(expression)

    return expression
