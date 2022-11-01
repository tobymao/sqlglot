import unittest

from sqlglot import table
from sqlglot.dataframe.sql import types as df_types
from sqlglot.schema import MappingSchema, ensure_schema


class TestSchema(unittest.TestCase):
    def test_schema(self):
        schema = ensure_schema(
            {
                "x": {
                    "a": "uint64",
                }
            }
        )
        self.assertEqual(
            schema.column_names(
                table(
                    "x",
                )
            ),
            ["a"],
        )
        with self.assertRaises(ValueError):
            schema.column_names(table("x", db="db", catalog="c"))
        with self.assertRaises(ValueError):
            schema.column_names(table("x", db="db"))
        with self.assertRaises(ValueError):
            schema.column_names(table("x2"))

        with self.assertRaises(ValueError):
            schema.add_table(table("y", db="db"), {"b": "string"})
        with self.assertRaises(ValueError):
            schema.add_table(table("y", db="db", catalog="c"), {"b": "string"})

        schema.add_table(table("y"), {"b": "string"})
        schema_with_y = {
            "x": {
                "a": "uint64",
            },
            "y": {
                "b": "string",
            },
        }
        self.assertEqual(schema.schema, schema_with_y)

        new_schema = schema.copy()
        new_schema.add_table(table("z"), {"c": "string"})
        self.assertEqual(schema.schema, schema_with_y)
        self.assertEqual(
            new_schema.schema,
            {
                "x": {
                    "a": "uint64",
                },
                "y": {
                    "b": "string",
                },
                "z": {
                    "c": "string",
                },
            },
        )
        schema.add_table(table("m"), {"d": "string"})
        schema.add_table(table("n"), {"e": "string"})
        schema_with_m_n = {
            "x": {
                "a": "uint64",
            },
            "y": {
                "b": "string",
            },
            "m": {
                "d": "string",
            },
            "n": {
                "e": "string",
            },
        }
        self.assertEqual(schema.schema, schema_with_m_n)
        new_schema = schema.copy()
        new_schema.add_table(table("o"), {"f": "string"})
        new_schema.add_table(table("p"), {"g": "string"})
        self.assertEqual(schema.schema, schema_with_m_n)
        self.assertEqual(
            new_schema.schema,
            {
                "x": {
                    "a": "uint64",
                },
                "y": {
                    "b": "string",
                },
                "m": {
                    "d": "string",
                },
                "n": {
                    "e": "string",
                },
                "o": {
                    "f": "string",
                },
                "p": {
                    "g": "string",
                },
            },
        )

        schema = ensure_schema(
            {
                "db": {
                    "x": {
                        "a": "uint64",
                    }
                }
            }
        )
        self.assertEqual(schema.column_names(table("x", db="db")), ["a"])
        with self.assertRaises(ValueError):
            schema.column_names(table("x", db="db", catalog="c"))
        with self.assertRaises(ValueError):
            schema.column_names(table("x"))
        with self.assertRaises(ValueError):
            schema.column_names(table("x", db="db2"))
        with self.assertRaises(ValueError):
            schema.column_names(table("x2", db="db"))

        with self.assertRaises(ValueError):
            schema.add_table(table("y"), {"b": "string"})
        with self.assertRaises(ValueError):
            schema.add_table(table("y", db="db", catalog="c"), {"b": "string"})

        schema.add_table(table("y", db="db"), {"b": "string"})
        self.assertEqual(
            schema.schema,
            {
                "db": {
                    "x": {
                        "a": "uint64",
                    },
                    "y": {
                        "b": "string",
                    },
                }
            },
        )

        schema = ensure_schema(
            {
                "c": {
                    "db": {
                        "x": {
                            "a": "uint64",
                        }
                    }
                }
            }
        )
        self.assertEqual(schema.column_names(table("x", db="db", catalog="c")), ["a"])
        with self.assertRaises(ValueError):
            schema.column_names(table("x", db="db"))
        with self.assertRaises(ValueError):
            schema.column_names(table("x"))
        with self.assertRaises(ValueError):
            schema.column_names(table("x", db="db", catalog="c2"))
        with self.assertRaises(ValueError):
            schema.column_names(table("x", db="db2"))
        with self.assertRaises(ValueError):
            schema.column_names(table("x2", db="db"))

        with self.assertRaises(ValueError):
            schema.add_table(table("x"), {"b": "string"})
        with self.assertRaises(ValueError):
            schema.add_table(table("x", db="db"), {"b": "string"})

        schema.add_table(table("y", db="db", catalog="c"), {"a": "string", "b": "int"})
        self.assertEqual(
            schema.schema,
            {
                "c": {
                    "db": {
                        "x": {
                            "a": "uint64",
                        },
                        "y": {
                            "a": "string",
                            "b": "int",
                        },
                    }
                }
            },
        )
        schema.add_table(table("z", db="db2", catalog="c"), {"c": "string", "d": "int"})
        self.assertEqual(
            schema.schema,
            {
                "c": {
                    "db": {
                        "x": {
                            "a": "uint64",
                        },
                        "y": {
                            "a": "string",
                            "b": "int",
                        },
                    },
                    "db2": {
                        "z": {
                            "c": "string",
                            "d": "int",
                        }
                    },
                }
            },
        )
        schema.add_table(table("m", db="db2", catalog="c2"), {"e": "string", "f": "int"})
        self.assertEqual(
            schema.schema,
            {
                "c": {
                    "db": {
                        "x": {
                            "a": "uint64",
                        },
                        "y": {
                            "a": "string",
                            "b": "int",
                        },
                    },
                    "db2": {
                        "z": {
                            "c": "string",
                            "d": "int",
                        }
                    },
                },
                "c2": {
                    "db2": {
                        "m": {
                            "e": "string",
                            "f": "int",
                        }
                    }
                },
            },
        )

        schema = ensure_schema(
            {
                "x": {
                    "a": "uint64",
                }
            }
        )
        self.assertEqual(schema.column_names(table("x")), ["a"])

        schema = MappingSchema()
        schema.add_table(table("x"), {"a": "string"})
        self.assertEqual(
            schema.schema,
            {
                "x": {
                    "a": "string",
                }
            },
        )
        schema.add_table(
            table("y"), df_types.StructType([df_types.StructField("b", df_types.StringType())])
        )
        self.assertEqual(
            schema.schema,
            {
                "x": {
                    "a": "string",
                },
                "y": {
                    "b": "string",
                },
            },
        )

    def test_schema_add_table_with_and_without_mapping(self):
        schema = MappingSchema()
        schema.add_table("test")
        self.assertEqual(schema.column_names("test"), [])
        schema.add_table("test", {"x": "string"})
        self.assertEqual(schema.column_names("test"), ["x"])
        schema.add_table("test", {"x": "string", "y": "int"})
        self.assertEqual(schema.column_names("test"), ["x", "y"])
        schema.add_table("test")
        self.assertEqual(schema.column_names("test"), ["x", "y"])
