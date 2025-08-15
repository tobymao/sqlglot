from __future__ import annotations


from sqlglot import exp, transforms
from sqlglot.dialects.dialect import NormalizationStrategy
from sqlglot.dialects.tsql import TSQL
from sqlglot.tokens import TokenType


def _cap_data_type_precision(expression: exp.DataType, max_precision: int = 6) -> exp.DataType:
    """
    Cap the precision of to a maximum of `max_precision` digits.
    If no precision is specified, default to `max_precision`.
    """

    precision_param = expression.find(exp.DataTypeParam)

    if precision_param and precision_param.this.is_int:
        current_precision = precision_param.this.to_py()
        target_precision = min(current_precision, max_precision)
    else:
        target_precision = max_precision

    return exp.DataType(
        this=expression.this,
        expressions=[exp.DataTypeParam(this=exp.Literal.number(target_precision))],
    )


def _add_default_precision_to_varchar(expression: exp.Expression) -> exp.Expression:
    """Transform function to add VARCHAR(MAX) or CHAR(MAX) for cross-dialect conversion."""
    if (
        isinstance(expression, exp.Create)
        and expression.kind == "TABLE"
        and isinstance(expression.this, exp.Schema)
    ):
        for column in expression.this.expressions:
            if isinstance(column, exp.ColumnDef):
                column_type = column.kind
                if (
                    isinstance(column_type, exp.DataType)
                    and column_type.this in (exp.DataType.Type.VARCHAR, exp.DataType.Type.CHAR)
                    and not column_type.expressions
                ):
                    # For transpilation, VARCHAR/CHAR without precision becomes VARCHAR(MAX)/CHAR(MAX)
                    column_type.set("expressions", [exp.var("MAX")])

    return expression


class Fabric(TSQL):
    """
    Microsoft Fabric Data Warehouse dialect that inherits from T-SQL.

    Microsoft Fabric is a cloud-based analytics platform that provides a unified
    data warehouse experience. While it shares much of T-SQL's syntax, it has
    specific differences and limitations that this dialect addresses.

    Key differences from T-SQL:
    - Case-sensitive identifiers (unlike T-SQL which is case-insensitive)
    - Limited data type support with mappings to supported alternatives
    - Temporal types (DATETIME2, DATETIMEOFFSET, TIME) limited to 6 digits precision
    - Certain legacy types (MONEY, SMALLMONEY, etc.) are not supported
    - Unicode types (NCHAR, NVARCHAR) are mapped to non-unicode equivalents

    References:
    - Data Types: https://learn.microsoft.com/en-us/fabric/data-warehouse/data-types
    - T-SQL Surface Area: https://learn.microsoft.com/en-us/fabric/data-warehouse/tsql-surface-area
    """

    # Fabric is case-sensitive unlike T-SQL which is case-insensitive
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_SENSITIVE

    class Tokenizer(TSQL.Tokenizer):
        # Override T-SQL tokenizer to handle TIMESTAMP differently
        # In T-SQL, TIMESTAMP is a synonym for ROWVERSION, but in Fabric we want it to be a datetime type
        # Also add UTINYINT keyword mapping since T-SQL doesn't have it
        KEYWORDS = {
            **TSQL.Tokenizer.KEYWORDS,
            "TIMESTAMP": TokenType.TIMESTAMP,
            "UTINYINT": TokenType.UTINYINT,
        }

    class Parser(TSQL.Parser):
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
                                and column_type.this
                                in (exp.DataType.Type.VARCHAR, exp.DataType.Type.CHAR)
                                and not column_type.expressions
                            ):
                                # Add default precision of 1 to VARCHAR/CHAR without precision
                                # When n isn't specified in a data definition or variable declaration statement, the default length is 1.
                                # https://learn.microsoft.com/en-us/sql/t-sql/data-types/char-and-varchar-transact-sql?view=sql-server-ver17#remarks
                                column_type.set("expressions", [exp.Literal.number("1")])

            return create

    class Generator(TSQL.Generator):
        # Fabric-specific type mappings - override T-SQL types that aren't supported
        # Reference: https://learn.microsoft.com/en-us/fabric/data-warehouse/data-types
        TYPE_MAPPING = {
            **TSQL.Generator.TYPE_MAPPING,
            exp.DataType.Type.DATETIME: "DATETIME2",
            exp.DataType.Type.DECIMAL: "DECIMAL",
            exp.DataType.Type.IMAGE: "VARBINARY",
            exp.DataType.Type.INT: "INT",
            exp.DataType.Type.JSON: "VARCHAR",
            exp.DataType.Type.MONEY: "DECIMAL",
            exp.DataType.Type.NCHAR: "CHAR",
            exp.DataType.Type.NVARCHAR: "VARCHAR",
            exp.DataType.Type.ROWVERSION: "ROWVERSION",
            exp.DataType.Type.SMALLDATETIME: "DATETIME2",
            exp.DataType.Type.SMALLMONEY: "DECIMAL",
            exp.DataType.Type.TIMESTAMP: "DATETIME2",
            exp.DataType.Type.TIMESTAMPNTZ: "DATETIME2",
            exp.DataType.Type.TIMESTAMPTZ: "DATETIME2",
            exp.DataType.Type.TINYINT: "SMALLINT",
            exp.DataType.Type.UTINYINT: "SMALLINT",
            exp.DataType.Type.UUID: "VARBINARY(MAX)",
            exp.DataType.Type.XML: "VARCHAR",
        }

        TRANSFORMS = {
            **TSQL.Generator.TRANSFORMS,
            exp.Create: transforms.preprocess([_add_default_precision_to_varchar]),
        }

        def datatype_sql(self, expression: exp.DataType) -> str:
            # Check if this is a temporal type that needs precision handling. Fabric limits temporal
            # types to max 6 digits precision. When no precision is specified, we default to 6 digits.
            if (
                expression.is_type(*exp.DataType.TEMPORAL_TYPES)
                and expression.this != exp.DataType.Type.DATE
            ):
                # Create a new expression with the capped precision
                expression = _cap_data_type_precision(expression)

            return super().datatype_sql(expression)

        def cast_sql(self, expression: exp.Cast, safe_prefix: str | None = None) -> str:
            # Cast to DATETIMEOFFSET if inside an AT TIME ZONE expression
            # https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetimeoffset-transact-sql#microsoft-fabric-support
            if expression.is_type(exp.DataType.Type.TIMESTAMPTZ):
                at_time_zone = expression.find_ancestor(exp.AtTimeZone, exp.Select)

                # Return normal cast, if the expression is not in an AT TIME ZONE context
                if not isinstance(at_time_zone, exp.AtTimeZone):
                    return super().cast_sql(expression, safe_prefix)

                # Get the precision from the original TIMESTAMPTZ cast and cap it to 6
                capped_data_type = _cap_data_type_precision(expression.to, max_precision=6)
                precision = capped_data_type.find(exp.DataTypeParam)
                precision_value = (
                    precision.this.to_py() if precision and precision.this.is_int else 6
                )

                # Do the cast explicitly to bypass sqlglot's default handling
                datetimeoffset = f"CAST({expression.this} AS DATETIMEOFFSET({precision_value}))"

                return self.sql(datetimeoffset)

            return super().cast_sql(expression, safe_prefix)

        def attimezone_sql(self, expression: exp.AtTimeZone) -> str:
            # Wrap the AT TIME ZONE expression in a cast to DATETIME2 if it contains a TIMESTAMPTZ
            ## https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetimeoffset-transact-sql#microsoft-fabric-support
            timestamptz_cast = expression.find(exp.Cast)
            if timestamptz_cast and timestamptz_cast.to.is_type(exp.DataType.Type.TIMESTAMPTZ):
                # Get the precision from the original TIMESTAMPTZ cast and cap it to 6
                data_type = timestamptz_cast.to
                capped_data_type = _cap_data_type_precision(data_type, max_precision=6)
                precision_param = capped_data_type.find(exp.DataTypeParam)
                precision = precision_param.this.to_py() if precision_param else 6

                # Generate the AT TIME ZONE expression (which will handle the inner cast conversion)
                at_time_zone_sql = super().attimezone_sql(expression)

                # Wrap it in an outer cast to DATETIME2
                return f"CAST({at_time_zone_sql} AS DATETIME2({precision}))"

            return super().attimezone_sql(expression)

        def unixtotime_sql(self, expression: exp.UnixToTime) -> str:
            scale = expression.args.get("scale")
            timestamp = expression.this

            if scale not in (None, exp.UnixToTime.SECONDS):
                self.unsupported(f"UnixToTime scale {scale} is not supported by Fabric")
                return ""

            # Convert unix timestamp (seconds) to microseconds and round to avoid decimals
            microseconds = timestamp * exp.Literal.number("1e6")
            rounded = exp.func("round", microseconds, 0)
            rounded_ms_as_bigint = exp.cast(rounded, exp.DataType.Type.BIGINT)

            # Create the base datetime as '1970-01-01' cast to DATETIME2(6)
            epoch_start = exp.cast("'1970-01-01'", "datetime2(6)", dialect="fabric")

            dateadd = exp.DateAdd(
                this=epoch_start,
                expression=rounded_ms_as_bigint,
                unit=exp.Literal.string("MICROSECONDS"),
            )
            return self.sql(dateadd)
