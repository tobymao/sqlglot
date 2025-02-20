from __future__ import annotations

from typing import Any, Callable, ClassVar

from sqlglot import exp
from sqlglot.dialects.hive import Hive
from sqlglot.dialects.trino import Trino
from sqlglot.tokens import TokenType


def _generate_as_hive(expression: exp.Expression) -> bool:
    if isinstance(expression, exp.Create):
        if expression.kind == "TABLE":
            properties: exp.Properties | None = expression.args.get("properties")
            if properties and properties.find(exp.ExternalProperty):
                return True  # CREATE EXTERNAL TABLE is Hive
            if not isinstance(expression.expression, exp.Select):
                return True  # any CREATE TABLE other than CREATE TABLE AS SELECT is Hive
        else:
            return expression.kind != "VIEW"  # CREATE VIEW is never Hive but CREATE SCHEMA etc is
    # https://docs.aws.amazon.com/athena/latest/ug/ddl-reference.html
    elif isinstance(expression, (exp.Alter, exp.Drop, exp.Describe)):
        # DROP VIEW is Trino (I guess because CREATE VIEW is), everything else is Hive
        return not (isinstance(expression, exp.Drop) and expression.kind == "VIEW")

    return False


def _is_iceberg_table(properties: exp.Properties) -> bool:
    table_type_property = next(
        (
            p
            for p in properties.expressions
            if isinstance(p, exp.Property) and p.name == "table_type"
        ),
        None,
    )

    return bool(table_type_property and table_type_property.text("value").lower() == "iceberg")


def _location_property_sql(self: Athena.Generator, e: exp.LocationProperty):
    # If table_type='iceberg', the LocationProperty is called 'location'
    # Otherwise, it's called 'external_location'
    # ref: https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html

    prop_name = "external_location"

    if isinstance(e.parent, exp.Properties) and _is_iceberg_table(e.parent):
        prop_name = "location"

    return f"{prop_name}={self.sql(e, 'this')}"


def _partitioned_by_property_sql(self: Athena.Generator, e: exp.PartitionedByProperty) -> str:
    # If table_type='iceberg' then the table property for partitioning is called 'partitioning'
    # If table_type='hive' it's called 'partitioned_by'
    # ref: https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html#ctas-table-properties

    prop_name = "partitioned_by"

    if isinstance(e.parent, exp.Properties) and _is_iceberg_table(e.parent):
        prop_name = "partitioning"

    return f"{prop_name}={self.sql(e, 'this')}"


def _show_parser(*args: Any, **kwargs: Any) -> Callable[[Athena.Parser], exp.Show]:
    def _parse(self: Athena.Parser) -> exp.Show:
        return self._parse_show_athena(*args, **kwargs)

    return _parse


class Athena(Trino):
    """
    Over the years, it looks like AWS has taken various execution engines, bolted on AWS-specific
    modifications and then built the Athena service around them.

    Thus, Athena is not simply hosted Trino, it's more like a router that routes SQL queries to an
    execution engine depending on the query type.

    As at 2024-09-10, assuming your Athena workgroup is configured to use "Athena engine version 3",
    the following engines exist:

    Hive:
     - Accepts mostly the same syntax as Hadoop / Hive
     - Uses backticks to quote identifiers
     - Has a distinctive DDL syntax (around things like setting table properties, storage locations
       etc) that is different from Trino
     - Used for *most* DDL, with some exceptions that get routed to the Trino engine instead:
        - CREATE [EXTERNAL] TABLE (without AS SELECT)
        - ALTER
        - DROP

    Trino:
      - Uses double quotes to quote identifiers
      - Used for DDL operations that involve SELECT queries, eg:
        - CREATE VIEW / DROP VIEW
        - CREATE TABLE... AS SELECT
      - Used for DML operations
        - SELECT, INSERT, UPDATE, DELETE, MERGE

    The SQLGlot Athena dialect tries to identify which engine a query would be routed to and then
    uses the parser / generator for that engine rather than trying to create a universal syntax that
    can handle both types.
    """

    class Tokenizer(Trino.Tokenizer):
        """
        The Tokenizer is flexible enough to tokenize queries across both the Hive and Trino engines
        """

        IDENTIFIERS: ClassVar[list] = ['"', "`"]
        KEYWORDS: ClassVar[dict] = {
            **Hive.Tokenizer.KEYWORDS,
            **Trino.Tokenizer.KEYWORDS,
            "UNLOAD": TokenType.COMMAND,
        }

        COMMANDS: ClassVar[set] = Trino.Tokenizer.COMMANDS - {TokenType.SHOW}

    class Parser(Trino.Parser):
        """
        Parse queries for the Athena Trino execution engine
        """

        STATEMENT_PARSERS: ClassVar[dict] = {
            **Trino.Parser.STATEMENT_PARSERS,
            TokenType.USING: lambda self: self._parse_as_command(self._prev),
            TokenType.SHOW: lambda self: self._parse_show(),
        }

        SHOW_PARSERS: ClassVar[dict] = {
            "COLUMNS FROM": _show_parser("COLUMNS", target="FROM"),
            "COLUMNS IN": _show_parser("COLUMNS", target="IN"),
            "CREATE TABLE": _show_parser("CREATE TABLE", target=True),
            "CREATE VIEW": _show_parser("CREATE VIEW", target=True),
            "DATABASES": _show_parser("DATABASES"),
            "PARTITIONS": _show_parser("PARTITIONS", target=True),
            "SCHEMAS": _show_parser("SCHEMAS"),
            "TABLES": _show_parser("TABLES", target="IN"),
            "TBLPROPERTIES": _show_parser("TBLPROPERTIES", target=True),
            "VIEWS": _show_parser("VIEWS", target="IN"),
        }

        def _parse_show_athena(
            self,
            this: str,
            target: bool | str = False,
        ) -> exp.Show:
            if target:
                if isinstance(target, str):
                    self._match_text_seq(target)
                target_id = self._parse_id_var()
            else:
                target_id = None

            db = None

            if self._match(TokenType.FROM) or self._match(TokenType.IN):
                db = self._parse_id_var()
            elif self._match(TokenType.DOT):
                db = target_id
                target_id = self._parse_id_var()

            _like = {"like": self._match_text_seq("LIKE") and self._parse_string()}
            if this in ["DATABASES", "SCHEMAS", "TABLES", "VIEWS"]:
                _like = {"rlike": _like["like"]}

            # 1. if using a regular expression in `SHOW TABLES` prefix it with `LIKE` this is valid athena syntax,
            #    despite missing from the current AWS docs
            # 2. `SHOW TBLPROPERTIES` does not currently parse optional `('property_name')`.
            return self.expression(
                exp.Show,
                this=this,
                target=target_id,
                db=db,
                **_like,
            )

    class _HiveGenerator(Hive.Generator):
        def alter_sql(self, expression: exp.Alter) -> str:
            # package any ALTER TABLE ADD actions into a Schema object
            # so it gets generated as `ALTER TABLE .. ADD COLUMNS(...)`
            # instead of `ALTER TABLE ... ADD COLUMN` which is invalid syntax on Athena
            if (
                isinstance(expression, exp.Alter)
                and expression.kind == "TABLE"
                and expression.actions
                and isinstance(expression.actions[0], exp.ColumnDef)
            ):
                new_actions = exp.Schema(expressions=expression.actions)
                expression.set("actions", [new_actions])

            return super().alter_sql(expression)

    class Generator(Trino.Generator):
        """
        Generate queries for the Athena Trino execution engine
        """

        PROPERTIES_LOCATION: ClassVar[dict] = {
            **Trino.Generator.PROPERTIES_LOCATION,
            exp.LocationProperty: exp.Properties.Location.POST_WITH,
        }

        TRANSFORMS: ClassVar[dict] = {
            **Trino.Generator.TRANSFORMS,
            exp.FileFormatProperty: lambda self, e: f"format={self.sql(e, 'this')}",
            exp.PartitionedByProperty: _partitioned_by_property_sql,
            exp.LocationProperty: _location_property_sql,
        }

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

            hive_kwargs = {**kwargs, "dialect": "hive"}

            self._hive_generator = Athena._HiveGenerator(*args, **hive_kwargs)

        def generate(self, expression: exp.Expression, copy: bool = True) -> str:
            if _generate_as_hive(expression):
                return self._hive_generator.generate(expression, copy)

            return super().generate(expression, copy)

        def _prefixed_sql(self, prefix: str, expression: exp.Expression, arg: str) -> str:
            sql = self.sql(expression, arg)
            return f"{prefix} {sql}" if sql else ""

        def show_sql(self, expression: exp.Show) -> str:
            this = f"{expression.name}"
            db = self.sql(expression, "db") or ""
            target = self.sql(expression, "target") or ""
            if db and target:
                target = f"{db}.{target}"
                db = ""
            if this in ["COLUMNS", "TABLES", "VIEWS"]:
                target = f"IN {target}"
            like = self._prefixed_sql("LIKE", expression, "like")
            rlike = self._prefixed_sql("LIKE", expression, "rlike")
            print(this, target, db, like, rlike)

            return " ".join(f"SHOW {this} {target} {db} {like or rlike}".split())
