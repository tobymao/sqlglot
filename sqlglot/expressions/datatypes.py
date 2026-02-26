"""sqlglot expressions datatypes."""

from __future__ import annotations

import typing as t
from enum import auto

from sqlglot.helper import AutoName
from sqlglot.errors import ErrorLevel, ParseError
from sqlglot.expressions.core import (
    Expression,
    _TimeUnit,
    Identifier,
    Dot,
    maybe_copy,
)

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType


class DataTypeParam(Expression):
    arg_types = {"this": True, "expression": False}

    @property
    def name(self) -> str:
        return self.this.name


class DType(AutoName):
    ARRAY = auto()
    AGGREGATEFUNCTION = auto()
    SIMPLEAGGREGATEFUNCTION = auto()
    BIGDECIMAL = auto()
    BIGINT = auto()
    BIGNUM = auto()
    BIGSERIAL = auto()
    BINARY = auto()
    BIT = auto()
    BLOB = auto()
    BOOLEAN = auto()
    BPCHAR = auto()
    CHAR = auto()
    DATE = auto()
    DATE32 = auto()
    DATEMULTIRANGE = auto()
    DATERANGE = auto()
    DATETIME = auto()
    DATETIME2 = auto()
    DATETIME64 = auto()
    DECIMAL = auto()
    DECIMAL32 = auto()
    DECIMAL64 = auto()
    DECIMAL128 = auto()
    DECIMAL256 = auto()
    DECFLOAT = auto()
    DOUBLE = auto()
    DYNAMIC = auto()
    ENUM = auto()
    ENUM8 = auto()
    ENUM16 = auto()
    FILE = auto()
    FIXEDSTRING = auto()
    FLOAT = auto()
    GEOGRAPHY = auto()
    GEOGRAPHYPOINT = auto()
    GEOMETRY = auto()
    POINT = auto()
    RING = auto()
    LINESTRING = auto()
    MULTILINESTRING = auto()
    POLYGON = auto()
    MULTIPOLYGON = auto()
    HLLSKETCH = auto()
    HSTORE = auto()
    IMAGE = auto()
    INET = auto()
    INT = auto()
    INT128 = auto()
    INT256 = auto()
    INT4MULTIRANGE = auto()
    INT4RANGE = auto()
    INT8MULTIRANGE = auto()
    INT8RANGE = auto()
    INTERVAL = auto()
    IPADDRESS = auto()
    IPPREFIX = auto()
    IPV4 = auto()
    IPV6 = auto()
    JSON = auto()
    JSONB = auto()
    LIST = auto()
    LONGBLOB = auto()
    LONGTEXT = auto()
    LOWCARDINALITY = auto()
    MAP = auto()
    MEDIUMBLOB = auto()
    MEDIUMINT = auto()
    MEDIUMTEXT = auto()
    MONEY = auto()
    NAME = auto()
    NCHAR = auto()
    NESTED = auto()
    NOTHING = auto()
    NULL = auto()
    NUMMULTIRANGE = auto()
    NUMRANGE = auto()
    NVARCHAR = auto()
    OBJECT = auto()
    RANGE = auto()
    ROWVERSION = auto()
    SERIAL = auto()
    SET = auto()
    SMALLDATETIME = auto()
    SMALLINT = auto()
    SMALLMONEY = auto()
    SMALLSERIAL = auto()
    STRUCT = auto()
    SUPER = auto()
    TEXT = auto()
    TINYBLOB = auto()
    TINYTEXT = auto()
    TIME = auto()
    TIMETZ = auto()
    TIME_NS = auto()
    TIMESTAMP = auto()
    TIMESTAMPNTZ = auto()
    TIMESTAMPLTZ = auto()
    TIMESTAMPTZ = auto()
    TIMESTAMP_S = auto()
    TIMESTAMP_MS = auto()
    TIMESTAMP_NS = auto()
    TINYINT = auto()
    TSMULTIRANGE = auto()
    TSRANGE = auto()
    TSTZMULTIRANGE = auto()
    TSTZRANGE = auto()
    UBIGINT = auto()
    UINT = auto()
    UINT128 = auto()
    UINT256 = auto()
    UMEDIUMINT = auto()
    UDECIMAL = auto()
    UDOUBLE = auto()
    UNION = auto()
    UNKNOWN = auto()  # Sentinel value, useful for type annotation
    USERDEFINED = "USER-DEFINED"
    USMALLINT = auto()
    UTINYINT = auto()
    UUID = auto()
    VARBINARY = auto()
    VARCHAR = auto()
    VARIANT = auto()
    VECTOR = auto()
    XML = auto()
    YEAR = auto()
    TDIGEST = auto()


class DataType(Expression):
    arg_types = {
        "this": True,
        "expressions": False,
        "nested": False,
        "values": False,
        "kind": False,
        "nullable": False,
    }

    Type: t.ClassVar[t.Type[DType]] = DType

    STRUCT_TYPES: t.ClassVar[t.Set[DType]] = {
        DType.FILE,
        DType.NESTED,
        DType.OBJECT,
        DType.STRUCT,
        DType.UNION,
    }

    ARRAY_TYPES: t.ClassVar[t.Set[DType]] = {
        DType.ARRAY,
        DType.LIST,
    }

    NESTED_TYPES: t.ClassVar[t.Set[DType]] = {
        DType.FILE,
        DType.NESTED,
        DType.OBJECT,
        DType.STRUCT,
        DType.UNION,
        DType.ARRAY,
        DType.LIST,
        DType.MAP,
    }

    TEXT_TYPES: t.ClassVar[t.Set[DType]] = {
        DType.CHAR,
        DType.NCHAR,
        DType.NVARCHAR,
        DType.TEXT,
        DType.VARCHAR,
        DType.NAME,
    }

    SIGNED_INTEGER_TYPES: t.ClassVar[t.Set[DType]] = {
        DType.BIGINT,
        DType.INT,
        DType.INT128,
        DType.INT256,
        DType.MEDIUMINT,
        DType.SMALLINT,
        DType.TINYINT,
    }

    UNSIGNED_INTEGER_TYPES: t.ClassVar[t.Set[DType]] = {
        DType.UBIGINT,
        DType.UINT,
        DType.UINT128,
        DType.UINT256,
        DType.UMEDIUMINT,
        DType.USMALLINT,
        DType.UTINYINT,
    }

    INTEGER_TYPES: t.ClassVar[t.Set[DType]] = {
        DType.BIGINT,
        DType.INT,
        DType.INT128,
        DType.INT256,
        DType.MEDIUMINT,
        DType.SMALLINT,
        DType.TINYINT,
        DType.UBIGINT,
        DType.UINT,
        DType.UINT128,
        DType.UINT256,
        DType.UMEDIUMINT,
        DType.USMALLINT,
        DType.UTINYINT,
        DType.BIT,
    }

    FLOAT_TYPES: t.ClassVar[t.Set[DType]] = {
        DType.DOUBLE,
        DType.FLOAT,
    }

    REAL_TYPES: t.ClassVar[t.Set[DType]] = {
        DType.DOUBLE,
        DType.FLOAT,
        DType.BIGDECIMAL,
        DType.DECIMAL,
        DType.DECIMAL32,
        DType.DECIMAL64,
        DType.DECIMAL128,
        DType.DECIMAL256,
        DType.DECFLOAT,
        DType.MONEY,
        DType.SMALLMONEY,
        DType.UDECIMAL,
        DType.UDOUBLE,
    }

    NUMERIC_TYPES: t.ClassVar[t.Set[DType]] = {
        DType.BIGINT,
        DType.INT,
        DType.INT128,
        DType.INT256,
        DType.MEDIUMINT,
        DType.SMALLINT,
        DType.TINYINT,
        DType.UBIGINT,
        DType.UINT,
        DType.UINT128,
        DType.UINT256,
        DType.UMEDIUMINT,
        DType.USMALLINT,
        DType.UTINYINT,
        DType.BIT,
        DType.DOUBLE,
        DType.FLOAT,
        DType.BIGDECIMAL,
        DType.DECIMAL,
        DType.DECIMAL32,
        DType.DECIMAL64,
        DType.DECIMAL128,
        DType.DECIMAL256,
        DType.DECFLOAT,
        DType.MONEY,
        DType.SMALLMONEY,
        DType.UDECIMAL,
        DType.UDOUBLE,
    }

    TEMPORAL_TYPES: t.ClassVar[t.Set[DType]] = {
        DType.DATE,
        DType.DATE32,
        DType.DATETIME,
        DType.DATETIME2,
        DType.DATETIME64,
        DType.SMALLDATETIME,
        DType.TIME,
        DType.TIMESTAMP,
        DType.TIMESTAMPNTZ,
        DType.TIMESTAMPLTZ,
        DType.TIMESTAMPTZ,
        DType.TIMESTAMP_MS,
        DType.TIMESTAMP_NS,
        DType.TIMESTAMP_S,
        DType.TIMETZ,
    }

    @classmethod
    def build(
        cls,
        dtype: DATA_TYPE,
        dialect: DialectType = None,
        udt: bool = False,
        copy: bool = True,
        **kwargs,
    ) -> DataType:
        """
        Constructs a DataType object.

        Args:
            dtype: the data type of interest.
            dialect: the dialect to use for parsing `dtype`, in case it's a string.
            udt: when set to True, `dtype` will be used as-is if it can't be parsed into a
                DataType, thus creating a user-defined type.
            copy: whether to copy the data type.
            kwargs: additional arguments to pass in the constructor of DataType.

        Returns:
            The constructed DataType object.
        """
        from sqlglot import parse_one

        if isinstance(dtype, str):
            if dtype.upper() == "UNKNOWN":
                return DataType(this=DType.UNKNOWN, **kwargs)

            try:
                data_type_exp = parse_one(
                    dtype, read=dialect, into=DataType, error_level=ErrorLevel.IGNORE
                )
            except ParseError:
                if udt:
                    return DataType(this=DType.USERDEFINED, kind=dtype, **kwargs)
                raise
        elif isinstance(dtype, (Identifier, Dot)) and udt:
            return DataType(this=DType.USERDEFINED, kind=dtype, **kwargs)
        elif isinstance(dtype, DType):
            data_type_exp = DataType(this=dtype)
        elif isinstance(dtype, DataType):
            return maybe_copy(dtype, copy)
        else:
            raise ValueError(f"Invalid data type: {type(dtype)}. Expected str or DType")

        if kwargs:
            for k, v in kwargs.items():
                data_type_exp.set(k, v)
        return data_type_exp

    def is_type(self, *dtypes: DATA_TYPE, check_nullable: bool = False) -> bool:
        """
        Checks whether this DataType matches one of the provided data types. Nested types or precision
        will be compared using "structural equivalence" semantics, so e.g. array<int> != array<float>.

        Args:
            dtypes: the data types to compare this DataType to.
            check_nullable: whether to take the NULLABLE type constructor into account for the comparison.
                If false, it means that NULLABLE<INT> is equivalent to INT.

        Returns:
            True, if and only if there is a type in `dtypes` which is equal to this DataType.
        """
        self_is_nullable = self.args.get("nullable")
        for dtype in dtypes:
            other_type = DataType.build(dtype, copy=False, udt=True)
            other_is_nullable = other_type.args.get("nullable")
            if (
                other_type.expressions
                or (check_nullable and (self_is_nullable or other_is_nullable))
                or self.this == DType.USERDEFINED
                or other_type.this == DType.USERDEFINED
            ):
                matches = self == other_type
            else:
                matches = self.this == other_type.this

            if matches:
                return True
        return False


class PseudoType(DataType):
    arg_types = {"this": True}


class ObjectIdentifier(DataType):
    arg_types = {"this": True}


class IntervalSpan(DataType):
    arg_types = {"this": True, "expression": True}


class Interval(_TimeUnit):
    arg_types = {"this": False, "unit": False}


DATA_TYPE = t.Union[str, Identifier, Dot, DataType, DType]
