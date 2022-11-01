import unittest

from sqlglot.dataframe.sql import types


class TestDataframeTypes(unittest.TestCase):
    def test_string(self):
        self.assertEqual("string", types.StringType().simpleString())

    def test_char(self):
        self.assertEqual("char(100)", types.CharType(100).simpleString())

    def test_varchar(self):
        self.assertEqual("varchar(65)", types.VarcharType(65).simpleString())

    def test_binary(self):
        self.assertEqual("binary", types.BinaryType().simpleString())

    def test_boolean(self):
        self.assertEqual("boolean", types.BooleanType().simpleString())

    def test_date(self):
        self.assertEqual("date", types.DateType().simpleString())

    def test_timestamp(self):
        self.assertEqual("timestamp", types.TimestampType().simpleString())

    def test_timestamp_ntz(self):
        self.assertEqual("timestamp_ntz", types.TimestampNTZType().simpleString())

    def test_decimal(self):
        self.assertEqual("decimal(10, 3)", types.DecimalType(10, 3).simpleString())

    def test_double(self):
        self.assertEqual("double", types.DoubleType().simpleString())

    def test_float(self):
        self.assertEqual("float", types.FloatType().simpleString())

    def test_byte(self):
        self.assertEqual("tinyint", types.ByteType().simpleString())

    def test_integer(self):
        self.assertEqual("int", types.IntegerType().simpleString())

    def test_long(self):
        self.assertEqual("bigint", types.LongType().simpleString())

    def test_short(self):
        self.assertEqual("smallint", types.ShortType().simpleString())

    def test_array(self):
        self.assertEqual("array<int>", types.ArrayType(types.IntegerType()).simpleString())

    def test_map(self):
        self.assertEqual(
            "map<int, string>",
            types.MapType(types.IntegerType(), types.StringType()).simpleString(),
        )

    def test_struct_field(self):
        self.assertEqual("cola:int", types.StructField("cola", types.IntegerType()).simpleString())

    def test_struct_type(self):
        self.assertEqual(
            "struct<cola:int, colb:string>",
            types.StructType(
                [
                    types.StructField("cola", types.IntegerType()),
                    types.StructField("colb", types.StringType()),
                ]
            ).simpleString(),
        )
