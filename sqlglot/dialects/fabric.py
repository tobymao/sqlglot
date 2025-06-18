from __future__ import annotations


from sqlglot import exp
from sqlglot.dialects.tsql import TSQL


class Fabric(TSQL):
    """
    Microsoft Fabric dialect that inherits from T-SQL but uses uppercase
    INFORMATION_SCHEMA references as required by Fabric.
    """

    class Generator(TSQL.Generator):
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
