from __future__ import annotations

import typing as t

from sqlglot import exp, generator


def _generate_as_hive(expression: exp.Expr) -> bool:
    if isinstance(expression, exp.Create):
        if expression.kind == "TABLE":
            properties = expression.args.get("properties")

            # CREATE EXTERNAL TABLE is Hive
            if properties and properties.find(exp.ExternalProperty):
                return True

            # Any CREATE TABLE other than CREATE TABLE ... As <query> is Hive
            if not isinstance(expression.expression, exp.Query):
                return True
        else:
            # CREATE VIEW is Trino, but CREATE SCHEMA, CREATE DATABASE, etc, is Hive
            return expression.kind != "VIEW"
    elif isinstance(expression, (exp.Alter, exp.Drop, exp.Describe, exp.Show)):
        if isinstance(expression, exp.Drop) and expression.kind == "VIEW":
            # DROP VIEW is Trino, because CREATE VIEW is as well
            return False

        # Everything else, e.g., ALTER statements, is Hive
        return True

    return False


def _generator_kwargs(
    pretty: t.Optional[bool | int],
    identify: str | bool,
    normalize: bool,
    pad: int,
    indent: int,
    normalize_functions: t.Optional[str | bool],
    unsupported_level: t.Any,
    max_unsupported: int,
    leading_comma: bool,
    max_text_width: int,
    comments: bool,
) -> t.Dict[str, t.Any]:
    kwargs: t.Dict[str, t.Any] = {
        "pretty": pretty,
        "identify": identify,
        "normalize": normalize,
        "pad": pad,
        "indent": indent,
        "normalize_functions": normalize_functions,
        "max_unsupported": max_unsupported,
        "leading_comma": leading_comma,
        "max_text_width": max_text_width,
        "comments": comments,
    }
    if unsupported_level is not None:
        kwargs["unsupported_level"] = unsupported_level
    return kwargs


class AthenaGenerator(generator.Generator):
    __slots__ = ("_hive_generator", "_trino_generator")

    def __init__(
        self,
        pretty: t.Optional[bool | int] = None,
        identify: str | bool = False,
        normalize: bool = False,
        pad: int = 2,
        indent: int = 2,
        normalize_functions: t.Optional[str | bool] = None,
        unsupported_level: t.Any = None,
        max_unsupported: int = 3,
        leading_comma: bool = False,
        max_text_width: int = 80,
        comments: bool = True,
        dialect: t.Any = None,
        hive: t.Any = None,
        trino: t.Any = None,
    ) -> None:
        import sqlglot.dialects.athena as athena_mod
        import sqlglot.dialects.hive as hive_mod
        import sqlglot.dialects.trino as trino_mod

        hive_dialect = hive or hive_mod.Hive()
        trino_dialect = trino or trino_mod.Trino()

        kwargs = _generator_kwargs(
            pretty,
            identify,
            normalize,
            pad,
            indent,
            normalize_functions,
            unsupported_level,
            max_unsupported,
            leading_comma,
            max_text_width,
            comments,
        )

        generator.Generator.__init__(self, dialect=dialect, **kwargs)
        self._hive_generator: generator.Generator = athena_mod._HiveGenerator(
            dialect=hive_dialect, **kwargs
        )
        self._trino_generator: generator.Generator = athena_mod._TrinoGenerator(
            dialect=trino_dialect, **kwargs
        )

    def generate(self, expression: exp.Expr, copy: bool = True) -> str:
        if _generate_as_hive(expression):
            return self._hive_generator.generate(expression, copy=copy)

        return self._trino_generator.generate(expression, copy=copy)
