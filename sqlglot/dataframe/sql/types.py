import typing as t


class DataType:
    def __repr__(self) -> str:
        return self.__class__.__name__ + "()"

    def __hash__(self) -> int:
        return hash(str(self))

    def __eq__(self, other: t.Any) -> bool:
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other: t.Any) -> bool:
        return not self.__eq__(other)

    def __str__(self) -> str:
        return self.typeName()

    @classmethod
    def typeName(cls) -> str:
        return cls.__name__[:-4].lower()

    def simpleString(self) -> str:
        return str(self)

    def jsonValue(self) -> t.Union[str, t.Dict[str, t.Any]]:
        return str(self)


class DataTypeWithLength(DataType):
    def __init__(self, length: int):
        self.length = length

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.length})"

    def __str__(self) -> str:
        return f"{self.typeName()}({self.length})"


class StringType(DataType):
    pass


class CharType(DataTypeWithLength):
    pass


class VarcharType(DataTypeWithLength):
    pass


class BinaryType(DataType):
    pass


class BooleanType(DataType):
    pass


class DateType(DataType):
    pass


class TimestampType(DataType):
    pass


class TimestampNTZType(DataType):
    @classmethod
    def typeName(cls) -> str:
        return "timestamp_ntz"


class DecimalType(DataType):
    def __init__(self, precision: int = 10, scale: int = 0):
        self.precision = precision
        self.scale = scale

    def simpleString(self) -> str:
        return f"decimal({self.precision}, {self.scale})"

    def jsonValue(self) -> str:
        return f"decimal({self.precision}, {self.scale})"

    def __repr__(self) -> str:
        return f"DecimalType({self.precision}, {self.scale})"


class DoubleType(DataType):
    pass


class FloatType(DataType):
    pass


class ByteType(DataType):
    def __str__(self) -> str:
        return "tinyint"


class IntegerType(DataType):
    def __str__(self) -> str:
        return "int"


class LongType(DataType):
    def __str__(self) -> str:
        return "bigint"


class ShortType(DataType):
    def __str__(self) -> str:
        return "smallint"


class ArrayType(DataType):
    def __init__(self, elementType: DataType, containsNull: bool = True):
        self.elementType = elementType
        self.containsNull = containsNull

    def __repr__(self) -> str:
        return f"ArrayType({self.elementType, str(self.containsNull)}"

    def simpleString(self) -> str:
        return f"array<{self.elementType.simpleString()}>"

    def jsonValue(self) -> t.Dict[str, t.Any]:
        return {
            "type": self.typeName(),
            "elementType": self.elementType.jsonValue(),
            "containsNull": self.containsNull,
        }


class MapType(DataType):
    def __init__(self, keyType: DataType, valueType: DataType, valueContainsNull: bool = True):
        self.keyType = keyType
        self.valueType = valueType
        self.valueContainsNull = valueContainsNull

    def __repr__(self) -> str:
        return f"MapType({self.keyType}, {self.valueType}, {str(self.valueContainsNull)})"

    def simpleString(self) -> str:
        return f"map<{self.keyType.simpleString()}, {self.valueType.simpleString()}>"

    def jsonValue(self) -> t.Dict[str, t.Any]:
        return {
            "type": self.typeName(),
            "keyType": self.keyType.jsonValue(),
            "valueType": self.valueType.jsonValue(),
            "valueContainsNull": self.valueContainsNull,
        }


class StructField(DataType):
    def __init__(
        self,
        name: str,
        dataType: DataType,
        nullable: bool = True,
        metadata: t.Optional[t.Dict[str, t.Any]] = None,
    ):
        self.name = name
        self.dataType = dataType
        self.nullable = nullable
        self.metadata = metadata or {}

    def __repr__(self) -> str:
        return f"StructField('{self.name}', {self.dataType}, {str(self.nullable)})"

    def simpleString(self) -> str:
        return f"{self.name}:{self.dataType.simpleString()}"

    def jsonValue(self) -> t.Dict[str, t.Any]:
        return {
            "name": self.name,
            "type": self.dataType.jsonValue(),
            "nullable": self.nullable,
            "metadata": self.metadata,
        }


class StructType(DataType):
    def __init__(self, fields: t.Optional[t.List[StructField]] = None):
        if not fields:
            self.fields = []
            self.names = []
        else:
            self.fields = fields
            self.names = [f.name for f in fields]

    def __iter__(self) -> t.Iterator[StructField]:
        return iter(self.fields)

    def __len__(self) -> int:
        return len(self.fields)

    def __repr__(self) -> str:
        return f"StructType({', '.join(str(field) for field in self)})"

    def simpleString(self) -> str:
        return f"struct<{', '.join(x.simpleString() for x in self)}>"

    def jsonValue(self) -> t.Dict[str, t.Any]:
        return {"type": self.typeName(), "fields": [x.jsonValue() for x in self]}

    def fieldNames(self) -> t.List[str]:
        return list(self.names)
