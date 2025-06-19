from __future__ import annotations


from sqlglot import exp
from sqlglot.dialects.tsql import TSQL


def _to_timestamp_sql(self: "Fabric.Generator", expression: exp.UnixToTime) -> str:
    """
    Convert UnixToTime to DATEADD expression for Fabric using microseconds.

    Uses the logic: DATEADD(microsecond, CAST(@ts*1e6 AS BIGINT), CAST('1970-01-01' AS DATETIME2(6)))
    """
    timestamp = expression.this

    # Convert to microseconds by multiplying by 1e6
    microseconds = exp.Cast(
        this=exp.Mul(this=timestamp, expression=exp.Literal.number("1000000")),
        to=exp.DataType(this=exp.DataType.Type.BIGINT),
    )

    # Use DATETIME2(6) for the epoch to support microsecond precision
    datetime2_type = exp.DataType(
        this=exp.DataType.Type.DATETIME2,
        expressions=[exp.DataTypeParam(this=exp.Literal.number("6"))],
    )
    unix_epoch = exp.Cast(this=exp.Literal.string("1970-01-01"), to=datetime2_type)

    return self.func("DATEADD", exp.var("MICROSECOND"), microseconds, unix_epoch)


class Fabric(TSQL):
    """
    Microsoft Fabric dialect that inherits from T-SQL.

    Key differences from T-SQL:
    - Uses uppercase INFORMATION_SCHEMA references in conditional DDL
    - Only supports a subset of T-SQL data types (maps unsupported types to alternatives)
    - DATETIME2 and TIME precision limited to 6 digits maximum
    - VARCHAR(MAX) and VARBINARY(MAX) are supported (in preview)

    Data type mappings for unsupported types:
    - MONEY/SMALLMONEY -> DECIMAL
    - DATETIME/SMALLDATETIME -> DATETIME2
    - NCHAR/NVARCHAR -> CHAR/VARCHAR (no Unicode support in Parquet)
    - TEXT -> VARCHAR
    - IMAGE -> VARBINARY
    - TINYINT -> SMALLINT
    - JSON/XML -> VARCHAR
    """

    class Parser(TSQL.Parser):
        FUNCTIONS = {
            **TSQL.Parser.FUNCTIONS,
            "TO_TIMESTAMP": exp.UnixToTime.from_arg_list,
        }

    class Generator(TSQL.Generator):
        # Fabric-specific type mappings - override T-SQL types that aren't supported
        # Reference: https://learn.microsoft.com/en-us/fabric/data-warehouse/data-types
        TYPE_MAPPING = {
            **TSQL.Generator.TYPE_MAPPING,
            # Fabric doesn't support these types, map to alternatives
            exp.DataType.Type.MONEY: "DECIMAL",
            exp.DataType.Type.SMALLMONEY: "DECIMAL",
            exp.DataType.Type.DATETIME: "DATETIME2",
            exp.DataType.Type.SMALLDATETIME: "DATETIME2",
            exp.DataType.Type.TIMESTAMPTZ: "DATETIME2",
            exp.DataType.Type.NCHAR: "CHAR",
            exp.DataType.Type.NVARCHAR: "VARCHAR",
            exp.DataType.Type.TEXT: "VARCHAR",
            exp.DataType.Type.IMAGE: "VARBINARY",
            exp.DataType.Type.TINYINT: "SMALLINT",
            exp.DataType.Type.UTINYINT: "SMALLINT",  # T-SQL parses TINYINT as UTINYINT
            exp.DataType.Type.JSON: "VARCHAR",
            exp.DataType.Type.XML: "VARCHAR",
            # Override T-SQL mappings that use different names in Fabric
            exp.DataType.Type.DECIMAL: "DECIMAL",  # T-SQL uses NUMERIC
            exp.DataType.Type.DOUBLE: "FLOAT",
        }

        # Fabric-specific function transformations
        TRANSFORMS = {
            **TSQL.Generator.TRANSFORMS,
            exp.UnixToTime: _to_timestamp_sql,
        }

        def datatype_sql(self, expression: exp.DataType) -> str:
            # Handle precision limits for DATETIME2 and TIME in Fabric (max 6 digits)
            if expression.is_type(exp.DataType.Type.DATETIME2, exp.DataType.Type.TIME):
                expressions = expression.expressions
                if expressions:
                    # Handle both Literal and DataTypeParam
                    param = expressions[0]
                    if isinstance(param, exp.DataTypeParam):
                        param = param.this

                    if isinstance(param, exp.Literal):
                        prec_value = int(param.this)
                        if prec_value > 6:
                            # Create new expression with capped precision
                            new_param = exp.DataTypeParam(this=exp.Literal.number("6"))
                            expression = exp.DataType.build(
                                expression.this, expressions=[new_param]
                            )

            return super().datatype_sql(expression)

        def create_sql(self, expression: exp.Create) -> str:
            kind = expression.kind
            exists = expression.args.pop("exists", None)

            like_property = expression.find(exp.LikeProperty)
            if like_property:
                ctas_expression = like_property.this
            else:
                ctas_expression = expression.expression

            if kind == "VIEW":
                expression.this.set("catalog", None)
                with_ = expression.args.get("with")
                if ctas_expression and with_:
                    # We've already preprocessed the Create expression to bubble up any nested CTEs,
                    # but CREATE VIEW actually requires the WITH clause to come after it so we need
                    # to amend the AST by moving the CTEs to the CREATE VIEW statement's query.
                    ctas_expression.set("with", with_.pop())

            table = expression.find(exp.Table)

            # Convert CTAS statement to SELECT .. INTO ..
            if kind == "TABLE" and ctas_expression:
                if isinstance(ctas_expression, exp.UNWRAPPED_QUERIES):
                    ctas_expression = ctas_expression.subquery()

                properties = expression.args.get("properties") or exp.Properties()
                is_temp = any(isinstance(p, exp.TemporaryProperty) for p in properties.expressions)

                select_into = exp.select("*").from_(exp.alias_(ctas_expression, "temp", table=True))
                select_into.set("into", exp.Into(this=table, temporary=is_temp))

                if like_property:
                    select_into.limit(0, copy=False)

                sql = self.sql(select_into)
            else:
                sql = super(TSQL.Generator, self).create_sql(expression)

            if exists:
                identifier = self.sql(exp.Literal.string(exp.table_name(table) if table else ""))
                sql_with_ctes = self.prepend_ctes(expression, sql)
                sql_literal = self.sql(exp.Literal.string(sql_with_ctes))
                if kind == "SCHEMA":
                    return f"""IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = {identifier}) EXEC({sql_literal})"""
                elif kind == "TABLE":
                    assert table
                    where = exp.and_(
                        exp.column("TABLE_NAME").eq(table.name),
                        exp.column("TABLE_SCHEMA").eq(table.db) if table.db else None,
                        exp.column("TABLE_CATALOG").eq(table.catalog) if table.catalog else None,
                    )
                    return f"""IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE {where}) EXEC({sql_literal})"""
                elif kind == "INDEX":
                    index = self.sql(exp.Literal.string(expression.this.text("this")))
                    return f"""IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = object_id({identifier}) AND name = {index}) EXEC({sql_literal})"""
            elif expression.args.get("replace"):
                sql = sql.replace("CREATE OR REPLACE ", "CREATE OR ALTER ", 1)

            return self.prepend_ctes(expression, sql)
