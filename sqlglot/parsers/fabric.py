from __future__ import annotations

from sqlglot import exp
from sqlglot.helper import mypyc_attr
from sqlglot.parsers.tsql import Parser as TSQLParser


@mypyc_attr(allow_interpreted_subclasses=True)
class FabricParser(TSQLParser):
    def _parse_create(self) -> exp.Create | exp.Command:
        create = super()._parse_create()

        if isinstance(create, exp.Create):
            # Transform VARCHAR/CHAR without precision to VARCHAR(1)/CHAR(1)
            if create.kind == "TABLE" and isinstance(create.this, exp.Schema):
                for column in create.this.expressions:
                    if isinstance(column, exp.ColumnDef):
                        column_type = column.kind
                        if (
                            isinstance(column_type, exp.DataType)
                            and column_type.this in (exp.DType.VARCHAR, exp.DType.CHAR)
                            and not column_type.expressions
                        ):
                            # Add default precision of 1 to VARCHAR/CHAR without precision
                            # When n isn't specified in a data definition or variable declaration statement, the default length is 1.
                            # https://learn.microsoft.com/en-us/sql/t-sql/data-types/char-and-varchar-transact-sql?view=sql-server-ver17#remarks
                            column_type.set("expressions", [exp.Literal.number("1")])

        return create
