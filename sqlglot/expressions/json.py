"""sqlglot expressions - JSON functions."""

from __future__ import annotations

from sqlglot.expressions.core import Func, AggFunc, Binary, Predicate


class CheckJson(Func):
    arg_types = {"this": True}


class JSONArray(Func):
    arg_types = {
        "expressions": False,
        "null_handling": False,
        "return_type": False,
        "strict": False,
    }


class JSONArrayAgg(AggFunc):
    arg_types = {
        "this": True,
        "order": False,
        "null_handling": False,
        "return_type": False,
        "strict": False,
    }


class JSONArrayAppend(Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True
    _sql_names = ["JSON_ARRAY_APPEND"]


class JSONArrayContains(Binary, Predicate, Func):
    arg_types = {"this": True, "expression": True, "json_type": False}
    _sql_names = ["JSON_ARRAY_CONTAINS"]


class JSONArrayInsert(Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True
    _sql_names = ["JSON_ARRAY_INSERT"]


class JSONBContains(Binary, Func):
    _sql_names = ["JSONB_CONTAINS"]


class JSONBContainsAllTopKeys(Binary, Func):
    pass


class JSONBContainsAnyTopKeys(Binary, Func):
    pass


class JSONBDeleteAtPath(Binary, Func):
    pass


class JSONBExists(Func):
    arg_types = {"this": True, "path": True}
    _sql_names = ["JSONB_EXISTS"]


class JSONBExtract(Binary, Func):
    _sql_names = ["JSONB_EXTRACT"]


class JSONBExtractScalar(Binary, Func):
    arg_types = {"this": True, "expression": True, "json_type": False}
    _sql_names = ["JSONB_EXTRACT_SCALAR"]


class JSONBObjectAgg(AggFunc):
    arg_types = {"this": True, "expression": True}


class JSONBool(Func):
    pass


class JSONExists(Func):
    arg_types = {
        "this": True,
        "path": True,
        "passing": False,
        "on_condition": False,
        "from_dcolonqmark": False,
    }


class JSONExtract(Binary, Func):
    arg_types = {
        "this": True,
        "expression": True,
        "only_json_types": False,
        "expressions": False,
        "variant_extract": False,
        "json_query": False,
        "option": False,
        "quote": False,
        "on_condition": False,
        "requires_json": False,
        "emits": False,
    }
    _sql_names = ["JSON_EXTRACT"]
    is_var_len_args = True

    @property
    def output_name(self) -> str:
        return self.expression.output_name if not self.expressions else ""


class JSONExtractArray(Func):
    arg_types = {"this": True, "expression": False}
    _sql_names = ["JSON_EXTRACT_ARRAY"]


class JSONExtractScalar(Binary, Func):
    arg_types = {
        "this": True,
        "expression": True,
        "only_json_types": False,
        "expressions": False,
        "json_type": False,
        "scalar_only": False,
    }
    _sql_names = ["JSON_EXTRACT_SCALAR"]
    is_var_len_args = True

    @property
    def output_name(self) -> str:
        return self.expression.output_name


class JSONFormat(Func):
    arg_types = {"this": False, "options": False, "is_json": False, "to_json": False}
    _sql_names = ["JSON_FORMAT"]


class JSONKeys(Func):
    arg_types = {"this": True, "expression": False, "expressions": False}
    is_var_len_args = True
    _sql_names = ["JSON_KEYS"]


class JSONKeysAtDepth(Func):
    arg_types = {"this": True, "expression": False, "mode": False}


class JSONObject(Func):
    arg_types = {
        "expressions": False,
        "null_handling": False,
        "unique_keys": False,
        "return_type": False,
        "encoding": False,
    }


class JSONObjectAgg(AggFunc):
    arg_types = {
        "expressions": False,
        "null_handling": False,
        "unique_keys": False,
        "return_type": False,
        "encoding": False,
    }


class JSONRemove(Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True
    _sql_names = ["JSON_REMOVE"]


class JSONSet(Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True
    _sql_names = ["JSON_SET"]


class JSONStripNulls(Func):
    arg_types = {
        "this": True,
        "expression": False,
        "include_arrays": False,
        "remove_empty": False,
    }
    _sql_names = ["JSON_STRIP_NULLS"]


class JSONTable(Func):
    arg_types = {
        "this": True,
        "schema": True,
        "path": False,
        "error_handling": False,
        "empty_handling": False,
    }


class JSONType(Func):
    arg_types = {"this": True, "expression": False}
    _sql_names = ["JSON_TYPE"]


class ObjectId(Func):
    arg_types = {"this": True, "expression": False}


class ObjectInsert(Func):
    arg_types = {
        "this": True,
        "key": True,
        "value": True,
        "update_flag": False,
    }


class OpenJSON(Func):
    arg_types = {"this": True, "path": False, "expressions": False}


class ParseJSON(Func):
    # BigQuery, Snowflake have PARSE_JSON, Presto has JSON_PARSE
    # Snowflake also has TRY_PARSE_JSON, which is represented using `safe`
    _sql_names = ["PARSE_JSON", "JSON_PARSE"]
    arg_types = {"this": True, "expression": False, "safe": False}
