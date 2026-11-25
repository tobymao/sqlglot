from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, DialectType, NormalizationStrategy
from sqlglot.errors import TokenError
from sqlglot.tokens import TokenType

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
            pattern_variables.update(id(i) for i in _pattern_variables(node, dialect))

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

        if isinstance(node, exp.Identifier):
            if id(node) not in pattern_variables:
                dialect.normalize_identifier(node)
            elif not node.quoted:
                # Engines resolve unquoted pattern variables as uppercase, even when they lowercase
                # other identifiers, e.g. Trino, and the PATTERN they must match is kept as text
                node.set("this", node.name.upper())

    return expression


def _pattern_variables(match: exp.MatchRecognize, dialect: Dialect) -> list[exp.Identifier]:
    """The identifiers naming `match`'s pattern variables, e.g. `a` in `DEFINE a AS a.x > 0`."""
    if dialect.normalization_strategy is NormalizationStrategy.CASE_SENSITIVE:
        return []

    def canonical(name: str, quoted: bool) -> str:
        return name if quoted else name.upper()

    defines = [
        alias
        for definition in match.args.get("define") or []
        if isinstance(alias := definition.args.get("alias"), exp.Identifier)
    ]
    subsets = [
        identifier
        for subset in match.args.get("subset") or []
        for identifier in subset.find_all(exp.Identifier)
    ]
    names = {canonical(i.name, i.quoted) for i in (*defines, *subsets)}

    if pattern := match.args.get("pattern"):
        try:
            tokens = dialect.tokenize(pattern.name)
        except TokenError:
            tokens = []
        names.update(
            canonical(token.text, token.token_type == TokenType.IDENTIFIER)
            for token in tokens
            if token.token_type == TokenType.IDENTIFIER
            or (
                (token.text[:1].isalpha() or token.text[:1] == "_")
                and token.text.upper() != "PERMUTE"
            )
        )

    identifiers = [*defines, *subsets]
    for arg in (*(match.args.get("measures") or []), *(match.args.get("define") or [])):
        for node in arg.walk(prune=lambda n: isinstance(n, exp.Query)):
            if not isinstance(node, exp.Column):
                continue
            parent = node.parent
            if len(node.parts) > 1 or (
                isinstance(parent, exp.Anonymous) and parent.name.upper() == "CLASSIFIER"
            ):
                qualifier = node.parts[0]
                if (
                    isinstance(qualifier, exp.Identifier)
                    and canonical(qualifier.name, qualifier.quoted) in names
                ):
                    identifiers.append(qualifier)

    return identifiers
