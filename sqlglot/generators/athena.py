from __future__ import annotations

import typing as t

from sqlglot import exp, generator
from sqlglot.generators.hive import HiveGenerator
from sqlglot.generators.trino import TrinoGenerator


def _is_iceberg_table(properties: exp.Properties) -> bool:
    for p in properties.expressions:
        if isinstance(p, exp.Property) and p.name == "table_type":
            return p.text("value").lower() == "iceberg"

    return False


def _location_property_sql(self: AthenaTrinoGenerator, e: exp.LocationProperty) -> str:
    # If table_type='iceberg', the LocationProperty is called 'location'
    # Otherwise, it's called 'external_location'
    # ref: https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html

    prop_name = "external_location"

    if isinstance(e.parent, exp.Properties):
        if _is_iceberg_table(e.parent):
            prop_name = "location"

    return f"{prop_name}={self.sql(e, 'this')}"


def _partitioned_by_property_sql(self: AthenaTrinoGenerator, e: exp.PartitionedByProperty) -> str:
    # If table_type='iceberg' then the table property for partitioning is called 'partitioning'
    # If table_type='hive' it's called 'partitioned_by'
    # ref: https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html#ctas-table-properties

    prop_name = "partitioned_by"

    if isinstance(e.parent, exp.Properties):
        if _is_iceberg_table(e.parent):
            prop_name = "partitioning"

    return f"{prop_name}={self.sql(e, 'this')}"


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
    pretty: bool | int | None,
    identify: str | bool,
    normalize: bool,
    pad: int,
    indent: int,
    normalize_functions: str | bool | None,
    unsupported_level: t.Any,
    max_unsupported: int,
    leading_comma: bool,
    max_text_width: int,
    comments: bool,
) -> dict[str, t.Any]:
    kwargs: dict[str, t.Any] = {
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


class _HiveGenerator(HiveGenerator):
    def alter_sql(self, expression: exp.Alter) -> str:
        if isinstance(expression, exp.Alter) and expression.kind == "TABLE":
            if expression.actions and isinstance(expression.actions[0], exp.ColumnDef):
                new_actions = exp.Schema(expressions=expression.actions)
                expression.set("actions", [new_actions])

        return super().alter_sql(expression)


class AthenaTrinoGenerator(TrinoGenerator):
    PROPERTIES_LOCATION = {
        **TrinoGenerator.PROPERTIES_LOCATION,
        exp.LocationProperty: exp.Properties.Location.POST_WITH,
    }

    TRANSFORMS = {
        **TrinoGenerator.TRANSFORMS,
        exp.PartitionedByProperty: _partitioned_by_property_sql,
        exp.LocationProperty: _location_property_sql,
    }


class AthenaGenerator(generator.Generator):
    SELECT_KINDS: tuple[str, ...] = ()
    SUPPORTS_DECODE_CASE = False

    AFTER_HAVING_MODIFIER_TRANSFORMS = generator.AFTER_HAVING_MODIFIER_TRANSFORMS

    __slots__ = ("_hive_generator", "_trino_generator")

    def __init__(
        self,
        pretty: bool | int | None = None,
        identify: str | bool = False,
        normalize: bool = False,
        pad: int = 2,
        indent: int = 2,
        normalize_functions: str | bool | None = None,
        unsupported_level: t.Any = None,
        max_unsupported: int = 3,
        leading_comma: bool = False,
        max_text_width: int = 80,
        comments: bool = True,
        dialect: t.Any = None,
        hive: t.Any = None,
        trino: t.Any = None,
    ) -> None:
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
        self._hive_generator: generator.Generator = _HiveGenerator(dialect=hive_dialect, **kwargs)
        self._trino_generator: generator.Generator = AthenaTrinoGenerator(
            dialect=trino_dialect, **kwargs
        )

    def generate(self, expression: exp.Expr, copy: bool = True) -> str:
        if _generate_as_hive(expression):
            return self._hive_generator.generate(expression, copy=copy)

        return self._trino_generator.generate(expression, copy=copy)
