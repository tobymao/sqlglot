from tests.dialects.test_dialect import Validator


class TestRisingWave(Validator):
    dialect = "risingwave"
    maxDiff = None

    def test_risingwave(self):
        self.validate_all(
            "SELECT a FROM tbl",
            read={
                "": "SELECT a FROM tbl FOR UPDATE",
            },
        )
        # Test risingwave CREATE SOURCE.
        self.validate_identity(
            "CREATE SOURCE from_kafka (*, gen_i32_field INT AS int32_field + 2, gen_i64_field INT AS int64_field + 2, WATERMARK FOR time_col AS time_col - INTERVAL '5 SECOND') INCLUDE header foo varchar AS myheader INCLUDE key AS mykey WITH (connector='kafka', topic='my_topic') FORMAT PLAIN ENCODE PROTOBUF (A=1, B=2) KEY ENCODE PROTOBUF (A=3, B=4)"
        )
        # Test risingwave CREATE SINK.
        self.validate_identity(
            "CREATE SINK my_sink AS SELECT * FROM A WITH (connector='kafka', topic='my_topic') FORMAT PLAIN ENCODE PROTOBUF (A=1, B=2) KEY ENCODE PROTOBUF (A=3, B=4)"
        )
        self.validate_identity(
            "WITH t1 AS MATERIALIZED (SELECT 1), t2 AS NOT MATERIALIZED (SELECT 2) SELECT * FROM t1, t2"
        )
        self.validate_identity(
            """LAST_VALUE("col1") OVER (ORDER BY "col2" RANGE BETWEEN INTERVAL '1 DAY' PRECEDING AND '1 month' FOLLOWING)"""
        )
