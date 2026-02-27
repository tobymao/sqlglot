"""sqlglot expressions functions."""

from __future__ import annotations

import typing as t

from sqlglot.expressions.core import (
    Func,
    Binary,
    SubqueryPredicate,
    ExpOrStr,
    maybe_parse,
    maybe_copy,
)
from sqlglot.expressions.datatypes import DataType, DATA_TYPE

# Re-export from focused submodules (backward compatibility)
from sqlglot.expressions.math import *  # noqa: F401,F403
from sqlglot.expressions.string import *  # noqa: F401,F403
from sqlglot.expressions.temporal import *  # noqa: F401,F403
from sqlglot.expressions.aggregate import *  # noqa: F401,F403
from sqlglot.expressions.array import *  # noqa: F401,F403
from sqlglot.expressions.json import *  # noqa: F401,F403


# Cast / type conversion


class Cast(Func):
    is_cast: t.ClassVar[bool] = True
    arg_types = {
        "this": True,
        "to": True,
        "format": False,
        "safe": False,
        "action": False,
        "default": False,
    }

    @property
    def name(self) -> str:
        return self.this.name

    @property
    def to(self) -> DataType:
        return self.args["to"]

    @property
    def output_name(self) -> str:
        return self.name

    def is_type(self, *dtypes: DATA_TYPE) -> bool:
        """
        Checks whether this Cast's DataType matches one of the provided data types. Nested types
        like arrays or structs will be compared using "structural equivalence" semantics, so e.g.
        array<int> != array<float>.

        Args:
            dtypes: the data types to compare this Cast's DataType to.

        Returns:
            True, if and only if there is a type in `dtypes` which is equal to this Cast's DataType.
        """
        return self.to.is_type(*dtypes)


class TryCast(Cast):
    arg_types = {**Cast.arg_types, "requires_string": False}


class JSONCast(Cast):
    pass


class CastToStrType(Func):
    arg_types = {"this": True, "to": True}


class Convert(Func):
    arg_types = {"this": True, "expression": True, "style": False, "safe": False}


# Conditional


class If(Func):
    arg_types = {"this": True, "true": True, "false": False}
    _sql_names = ["IF", "IIF"]


class Case(Func):
    arg_types = {"this": False, "ifs": True, "default": False}

    def when(self, condition: ExpOrStr, then: ExpOrStr, copy: bool = True, **opts) -> Case:
        instance = maybe_copy(self, copy)
        instance.append(
            "ifs",
            If(
                this=maybe_parse(condition, copy=copy, **opts),
                true=maybe_parse(then, copy=copy, **opts),
            ),
        )
        return instance

    def else_(self, condition: ExpOrStr, copy: bool = True, **opts) -> Case:
        instance = maybe_copy(self, copy)
        instance.set("default", maybe_parse(condition, copy=copy, **opts))
        return instance


class Coalesce(Func):
    arg_types = {"this": True, "expressions": False, "is_nvl": False, "is_null": False}
    is_var_len_args = True
    _sql_names = ["COALESCE", "IFNULL", "NVL"]


class DecodeCase(Func):
    arg_types = {"expressions": True}
    is_var_len_args = True


class EqualNull(Func):
    arg_types = {"this": True, "expression": True}


class Greatest(Func):
    arg_types = {"this": True, "expressions": False, "ignore_nulls": True}
    is_var_len_args = True


class Least(Func):
    arg_types = {"this": True, "expressions": False, "ignore_nulls": True}
    is_var_len_args = True


class Nullif(Func):
    arg_types = {"this": True, "expression": True}


class Nvl2(Func):
    arg_types = {"this": True, "true": True, "false": False}


class Try(Func):
    pass


# Predicates / misc functions


class Collate(Binary, Func):
    pass


class Collation(Func):
    pass


class ConnectByRoot(Func):
    pass


class CheckXml(Func):
    arg_types = {"this": True, "disable_auto_convert": False}


class Exists(Func, SubqueryPredicate):
    arg_types = {"this": True, "expression": False}


# Type coercions / lax types


class Float64(Func):
    arg_types = {"this": True, "expression": False}


class Int64(Func):
    pass


class IsArray(Func):
    pass


class IsNullValue(Func):
    pass


class LaxBool(Func):
    pass


class LaxFloat64(Func):
    pass


class LaxInt64(Func):
    pass


class LaxString(Func):
    pass


class ToBoolean(Func):
    arg_types = {"this": True, "safe": False}


# Session / context functions


class CurrentAccount(Func):
    arg_types = {}


class CurrentAccountName(Func):
    arg_types = {}


class CurrentAvailableRoles(Func):
    arg_types = {}


class CurrentCatalog(Func):
    arg_types = {}


class CurrentClient(Func):
    arg_types = {}


class CurrentDatabase(Func):
    arg_types = {}


class CurrentIpAddress(Func):
    arg_types = {}


class CurrentOrganizationName(Func):
    arg_types = {}


class CurrentOrganizationUser(Func):
    arg_types = {}


class CurrentRegion(Func):
    arg_types = {}


class CurrentRole(Func):
    arg_types = {}


class CurrentRoleType(Func):
    arg_types = {}


class CurrentSchema(Func):
    arg_types = {"this": False}


class CurrentSchemas(Func):
    arg_types = {"this": False}


class CurrentSecondaryRoles(Func):
    arg_types = {}


class CurrentSession(Func):
    arg_types = {}


class CurrentStatement(Func):
    arg_types = {}


class CurrentTransaction(Func):
    arg_types = {}


class CurrentUser(Func):
    arg_types = {"this": False}


class CurrentVersion(Func):
    arg_types = {}


class CurrentWarehouse(Func):
    arg_types = {}


class SessionUser(Func):
    arg_types = {}


# ML / AI


class AIClassify(Func):
    arg_types = {"this": True, "categories": True, "config": False}
    _sql_names = ["AI_CLASSIFY"]


class FeaturesAtTime(Func):
    arg_types = {"this": True, "time": False, "num_rows": False, "ignore_feature_nulls": False}


class GenerateEmbedding(Func):
    arg_types = {"this": True, "expression": True, "params_struct": False, "is_text": False}


class MLForecast(Func):
    arg_types = {"this": True, "expression": False, "params_struct": False}


class MLTranslate(Func):
    arg_types = {"this": True, "expression": True, "params_struct": True}


class Predict(Func):
    arg_types = {"this": True, "expression": True, "params_struct": False}


class VectorSearch(Func):
    arg_types = {
        "this": True,
        "column_to_search": True,
        "query_table": True,
        "query_column_to_search": False,
        "top_k": False,
        "distance_type": False,
        "options": False,
    }


# Data reading


class ReadCSV(Func):
    _sql_names = ["READ_CSV"]
    is_var_len_args = True
    arg_types = {"this": True, "expressions": False}


class ReadParquet(Func):
    is_var_len_args = True
    arg_types = {"expressions": True}


# XML


class XMLElement(Func):
    _sql_names = ["XMLELEMENT"]
    arg_types = {"this": True, "expressions": False, "evalname": False}


class XMLGet(Func):
    _sql_names = ["XMLGET"]
    arg_types = {"this": True, "expression": True, "instance": False}


class XMLTable(Func):
    arg_types = {
        "this": True,
        "namespaces": False,
        "passing": False,
        "columns": False,
        "by_ref": False,
    }


# Network / domain


class Host(Func):
    pass


class NetFunc(Func):
    pass


class ParseIp(Func):
    arg_types = {"this": True, "type": True, "permissive": False}


class RegDomain(Func):
    pass


# Misc utility


class Columns(Func):
    arg_types = {"this": True, "unpack": False}


class Normal(Func):
    arg_types = {"this": True, "stddev": True, "gen": True}


class Rand(Func):
    _sql_names = ["RAND", "RANDOM"]
    arg_types = {"this": False, "lower": False, "upper": False}


class Randn(Func):
    arg_types = {"this": False}


class Randstr(Func):
    arg_types = {"this": True, "generator": False}


class RangeBucket(Func):
    arg_types = {"this": True, "expression": True}


class RangeN(Func):
    arg_types = {"this": True, "expressions": True, "each": False}


class Seq1(Func):
    arg_types = {"this": False}


class Seq2(Func):
    arg_types = {"this": False}


class Seq4(Func):
    arg_types = {"this": False}


class Seq8(Func):
    arg_types = {"this": False}


class Uniform(Func):
    arg_types = {"this": True, "expression": True, "gen": False, "seed": False}


class Uuid(Func):
    _sql_names = ["UUID", "GEN_RANDOM_UUID", "GENERATE_UUID", "UUID_STRING"]

    arg_types = {"this": False, "name": False, "is_string": False}


class WidthBucket(Func):
    arg_types = {
        "this": True,
        "min_value": False,
        "max_value": False,
        "num_buckets": False,
        "threshold": False,
    }


class Zipf(Func):
    arg_types = {"this": True, "elementcount": True, "gen": True}
