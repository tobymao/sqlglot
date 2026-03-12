from __future__ import annotations

from sqlglot import exp
from sqlglot.helper import seq_get
from sqlglot.dialects.postgres import Postgres
from sqlglot.parsers.materialize import MaterializeParser

from sqlglot.transforms import (
    remove_unique_constraints,
    ctas_with_tmp_tables_to_create_tmp_view,
    preprocess,
)


class Materialize(Postgres):
    Parser = MaterializeParser

    class Generator(Postgres.Generator):
        SUPPORTS_CREATE_TABLE_LIKE = False
        SUPPORTS_BETWEEN_FLAGS = False

        TRANSFORMS = {
            **Postgres.Generator.TRANSFORMS,
            exp.AutoIncrementColumnConstraint: lambda self, e: "",
            exp.Create: preprocess(
                [
                    remove_unique_constraints,
                    ctas_with_tmp_tables_to_create_tmp_view,
                ]
            ),
            exp.GeneratedAsIdentityColumnConstraint: lambda self, e: "",
            exp.OnConflict: lambda self, e: "",
            exp.PrimaryKeyColumnConstraint: lambda self, e: "",
        }
        TRANSFORMS.pop(exp.ToMap)

        def propertyeq_sql(self, expression: exp.PropertyEQ) -> str:
            return self.binary(expression, "=>")

        def datatype_sql(self, expression: exp.DataType) -> str:
            if expression.is_type(exp.DType.LIST):
                if expression.expressions:
                    return f"{self.expressions(expression, flat=True)} LIST"
                return "LIST"

            if expression.is_type(exp.DType.MAP) and len(expression.expressions) == 2:
                key, value = expression.expressions
                return f"MAP[{self.sql(key)} => {self.sql(value)}]"

            return super().datatype_sql(expression)

        def list_sql(self, expression: exp.List) -> str:
            if isinstance(seq_get(expression.expressions, 0), exp.Select):
                return self.func("LIST", seq_get(expression.expressions, 0))

            return f"{self.normalize_func('LIST')}[{self.expressions(expression, flat=True)}]"

        def tomap_sql(self, expression: exp.ToMap) -> str:
            if isinstance(expression.this, exp.Select):
                return self.func("MAP", expression.this)
            return f"{self.normalize_func('MAP')}[{self.expressions(expression.this)}]"
