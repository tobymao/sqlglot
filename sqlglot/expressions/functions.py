"""sqlglot expressions functions."""

from __future__ import annotations

import typing as t

from sqlglot.expressions.core import (
    ExpressionBase,
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


class Cast(ExpressionBase, Func):
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


class CastToStrType(ExpressionBase, Func):
    arg_types = {"this": True, "to": True}


class Convert(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True, "style": False, "safe": False}


# Conditional


class If(ExpressionBase, Func):
    arg_types = {"this": True, "true": True, "false": False}
    _sql_names = ["IF", "IIF"]


class Case(ExpressionBase, Func):
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


class Coalesce(ExpressionBase, Func):
    arg_types = {"this": True, "expressions": False, "is_nvl": False, "is_null": False}
    is_var_len_args = True
    _sql_names = ["COALESCE", "IFNULL", "NVL"]


class DecodeCase(ExpressionBase, Func):
    arg_types = {"expressions": True}
    is_var_len_args = True


class EqualNull(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


class Greatest(ExpressionBase, Func):
    arg_types = {"this": True, "expressions": False, "ignore_nulls": True}
    is_var_len_args = True


class Least(ExpressionBase, Func):
    arg_types = {"this": True, "expressions": False, "ignore_nulls": True}
    is_var_len_args = True


class Nullif(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


class Nvl2(ExpressionBase, Func):
    arg_types = {"this": True, "true": True, "false": False}


class Try(ExpressionBase, Func):
    pass


# Predicates / misc functions


class Collate(ExpressionBase, Binary, Func):
    pass


class Collation(ExpressionBase, Func):
    pass


class ConnectByRoot(ExpressionBase, Func):
    pass


class CheckXml(ExpressionBase, Func):
    arg_types = {"this": True, "disable_auto_convert": False}


class Exists(ExpressionBase, Func, SubqueryPredicate):
    arg_types = {"this": True, "expression": False}


# Type coercions / lax types


class Float64(ExpressionBase, Func):
    arg_types = {"this": True, "expression": False}


class Int64(ExpressionBase, Func):
    pass


class IsArray(ExpressionBase, Func):
    pass


class IsNullValue(ExpressionBase, Func):
    pass


class LaxBool(ExpressionBase, Func):
    pass


class LaxFloat64(ExpressionBase, Func):
    pass


class LaxInt64(ExpressionBase, Func):
    pass


class LaxString(ExpressionBase, Func):
    pass


class ToBoolean(ExpressionBase, Func):
    arg_types = {"this": True, "safe": False}


# Session / context functions


class CurrentAccount(ExpressionBase, Func):
    arg_types = {}


class CurrentAccountName(ExpressionBase, Func):
    arg_types = {}


class CurrentAvailableRoles(ExpressionBase, Func):
    arg_types = {}


class CurrentCatalog(ExpressionBase, Func):
    arg_types = {}


class CurrentClient(ExpressionBase, Func):
    arg_types = {}


class CurrentDatabase(ExpressionBase, Func):
    arg_types = {}


class CurrentIpAddress(ExpressionBase, Func):
    arg_types = {}


class CurrentOrganizationName(ExpressionBase, Func):
    arg_types = {}


class CurrentOrganizationUser(ExpressionBase, Func):
    arg_types = {}


class CurrentRegion(ExpressionBase, Func):
    arg_types = {}


class CurrentRole(ExpressionBase, Func):
    arg_types = {}


class CurrentRoleType(ExpressionBase, Func):
    arg_types = {}


class CurrentSchema(ExpressionBase, Func):
    arg_types = {"this": False}


class CurrentSchemas(ExpressionBase, Func):
    arg_types = {"this": False}


class CurrentSecondaryRoles(ExpressionBase, Func):
    arg_types = {}


class CurrentSession(ExpressionBase, Func):
    arg_types = {}


class CurrentStatement(ExpressionBase, Func):
    arg_types = {}


class CurrentTransaction(ExpressionBase, Func):
    arg_types = {}


class CurrentUser(ExpressionBase, Func):
    arg_types = {"this": False}


class CurrentVersion(ExpressionBase, Func):
    arg_types = {}


class CurrentWarehouse(ExpressionBase, Func):
    arg_types = {}


class SessionUser(ExpressionBase, Func):
    arg_types = {}


# ML / AI


class AIClassify(ExpressionBase, Func):
    arg_types = {"this": True, "categories": True, "config": False}
    _sql_names = ["AI_CLASSIFY"]


class FeaturesAtTime(ExpressionBase, Func):
    arg_types = {"this": True, "time": False, "num_rows": False, "ignore_feature_nulls": False}


class GenerateEmbedding(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True, "params_struct": False, "is_text": False}


class MLForecast(ExpressionBase, Func):
    arg_types = {"this": True, "expression": False, "params_struct": False}


class MLTranslate(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True, "params_struct": True}


class Predict(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True, "params_struct": False}


class VectorSearch(ExpressionBase, Func):
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


class ReadCSV(ExpressionBase, Func):
    _sql_names = ["READ_CSV"]
    is_var_len_args = True
    arg_types = {"this": True, "expressions": False}


class ReadParquet(ExpressionBase, Func):
    is_var_len_args = True
    arg_types = {"expressions": True}


# XML


class XMLElement(ExpressionBase, Func):
    _sql_names = ["XMLELEMENT"]
    arg_types = {"this": True, "expressions": False, "evalname": False}


class XMLGet(ExpressionBase, Func):
    _sql_names = ["XMLGET"]
    arg_types = {"this": True, "expression": True, "instance": False}


class XMLTable(ExpressionBase, Func):
    arg_types = {
        "this": True,
        "namespaces": False,
        "passing": False,
        "columns": False,
        "by_ref": False,
    }


# Network / domain


class Host(ExpressionBase, Func):
    pass


class NetFunc(ExpressionBase, Func):
    pass


class ParseIp(ExpressionBase, Func):
    arg_types = {"this": True, "type": True, "permissive": False}


class RegDomain(ExpressionBase, Func):
    pass


# Misc utility


class Columns(ExpressionBase, Func):
    arg_types = {"this": True, "unpack": False}


class Normal(ExpressionBase, Func):
    arg_types = {"this": True, "stddev": True, "gen": True}


class Rand(ExpressionBase, Func):
    _sql_names = ["RAND", "RANDOM"]
    arg_types = {"this": False, "lower": False, "upper": False}


class Randn(ExpressionBase, Func):
    arg_types = {"this": False}


class Randstr(ExpressionBase, Func):
    arg_types = {"this": True, "generator": False}


class RangeBucket(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


class RangeN(ExpressionBase, Func):
    arg_types = {"this": True, "expressions": True, "each": False}


class Seq1(ExpressionBase, Func):
    arg_types = {"this": False}


class Seq2(ExpressionBase, Func):
    arg_types = {"this": False}


class Seq4(ExpressionBase, Func):
    arg_types = {"this": False}


class Seq8(ExpressionBase, Func):
    arg_types = {"this": False}


class Uniform(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True, "gen": False, "seed": False}


class Uuid(ExpressionBase, Func):
    _sql_names = ["UUID", "GEN_RANDOM_UUID", "GENERATE_UUID", "UUID_STRING"]

    arg_types = {"this": False, "name": False, "is_string": False}


class WidthBucket(ExpressionBase, Func):
    arg_types = {
        "this": True,
        "min_value": False,
        "max_value": False,
        "num_buckets": False,
        "threshold": False,
    }


class Zipf(ExpressionBase, Func):
    arg_types = {"this": True, "elementcount": True, "gen": True}
