from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, DialectType, NormalizationStrategy
from sqlglot.tokenizer_core import TokenType

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

    pattern_variables = set()

    for node in expression.walk(prune=lambda n: bool(n.meta_get("case_sensitive"))):
        if node.meta_get("case_sensitive"):
            continue

        if isinstance(node, exp.MatchRecognize):
            pattern_variables.update(_normalize_pattern_variables(node, dialect))

        # A dot chain is recorded once, at its outermost Dot. Ancestors are visited before
        # their descendants, so none of the names it wraps have been normalized yet
        if (
            store_original_column_identifiers
            and isinstance(node, (exp.Column, exp.Dot))
            and not isinstance(node.parent, exp.Dot)
        ):
            if isinstance(node, exp.Column):
                node.meta["dot_parts"] = [p.name for p in node.parts]
            elif not node.is_star:
                root, dot_parts = node, []
                while isinstance(root, exp.Dot):
                    dot_parts.append(root.expression.name)
                    root = root.this

                dot_parts.reverse()

                # The chain may be rooted at a column (j.k), or at an arbitrary expression (f().k)
                if isinstance(root, exp.Column):
                    dot_parts = [p.name for p in root.parts] + dot_parts

                root.meta["dot_parts"] = dot_parts

        if isinstance(node, exp.Identifier) and id(node) not in pattern_variables:
            dialect.normalize_identifier(node)

    return expression


def _normalize_pattern_variables(match: exp.MatchRecognize, dialect: Dialect) -> list[int]:
    """
    Uppercases the unquoted names of `match`'s pattern variables, e.g. `a` in `DEFINE a AS ...` and
    `a.price` in its MEASURES and DEFINE, and returns the ids of their identifiers.

    Engines resolve unquoted pattern variables as uppercase, as the SQL standard does with
    identifiers, even when they lowercase other identifiers, e.g. Trino. The PATTERN is kept as
    text, so the names that reference it have to match it this way once they're quoted.
    """
    if dialect.normalization_strategy is NormalizationStrategy.CASE_SENSITIVE:
        return []

    def canonical(name: str, quoted: bool) -> str:
        return name if quoted else name.upper()

    defines = [
        alias
        for definition in match.args.get("define") or []
        if isinstance(alias := definition.args.get("alias"), exp.Identifier)
    ]
    names = {canonical(i.name, i.quoted) for i in defines}
    if pattern := match.args.get("pattern"):
        names.update(
            canonical(token.text, token.token_type == TokenType.IDENTIFIER)
            for token in dialect.tokenize(pattern.name)
            if token.token_type == TokenType.IDENTIFIER
            or (
                (token.text[:1].isalpha() or token.text[:1] == "_")
                and token.text.upper() != "PERMUTE"
            )
        )

    identifiers = list(defines)
    for key in ("measures", "define"):
        for arg in match.args.get(key) or []:
            for column in arg.find_all(exp.Column):
                qualifier = column.args.get("table")
                if (
                    isinstance(qualifier, exp.Identifier)
                    and not column.args.get("db")
                    and canonical(qualifier.name, qualifier.quoted) in names
                ):
                    identifiers.append(qualifier)

    for identifier in identifiers:
        if not identifier.quoted:
            identifier.set("this", identifier.name.upper())

    return [id(identifier) for identifier in identifiers]
