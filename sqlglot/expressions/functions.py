"""sqlglot expressions functions."""

from __future__ import annotations

import typing as t

from sqlglot.expressions.core import (
    Expression,
    Func,
    Binary,
    SubqueryPredicate,
    ExpOrStr,
    maybe_parse,
    maybe_copy,
)

# Re-export from focused submodules (backward compatibility)
from sqlglot.expressions.math import *  # noqa: F401,F403
from sqlglot.expressions.string import *  # noqa: F401,F403
from sqlglot.expressions.temporal import *  # noqa: F401,F403
from sqlglot.expressions.aggregate import *  # noqa: F401,F403
from sqlglot.expressions.array import *  # noqa: F401,F403
from sqlglot.expressions.json import *  # noqa: F401,F403

if t.TYPE_CHECKING:
    from sqlglot.expressions.datatypes import DataType, DATA_TYPE
    from typing_extensions import Unpack
    from sqlglot._typing import ParserArgs


# Cast / type conversion


class Cast(Expression, Func):
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


class CastToStrType(Expression, Func):
    arg_types = {"this": True, "to": True}


class Convert(Expression, Func):
    arg_types = {"this": True, "expression": True, "style": False, "safe": False}


# Conditional


class If(Expression, Func):
    arg_types = {"this": True, "true": True, "false": False}
    _sql_names = ["IF", "IIF"]


class Case(Expression, Func):
    arg_types = {"this": False, "ifs": True, "default": False}

    def when(
        self,
        condition: ExpOrStr,
        then: ExpOrStr,
        copy: bool = True,
        **opts: Unpack[ParserArgs],
    ) -> Case:
        instance = maybe_copy(self, copy)
        instance.append(
            "ifs",
            If(
                this=maybe_parse(condition, copy=copy, **opts),
                true=maybe_parse(then, copy=copy, **opts),
            ),
        )
        return instance

    def else_(self, condition: ExpOrStr, copy: bool = True, **opts: Unpack[ParserArgs]) -> Case:
        instance = maybe_copy(self, copy)
        instance.set("default", maybe_parse(condition, copy=copy, **opts))
        return instance


class Coalesce(Expression, Func):
    arg_types = {"this": True, "expressions": False, "is_nvl": False, "is_null": False}
    is_var_len_args = True
    _sql_names = ["COALESCE", "IFNULL", "NVL"]


class DecodeCase(Expression, Func):
    arg_types = {"expressions": True}
    is_var_len_args = True


class EqualNull(Expression, Func):
    arg_types = {"this": True, "expression": True}


class Greatest(Expression, Func):
    arg_types = {"this": True, "expressions": False, "ignore_nulls": True}
    is_var_len_args = True


class Least(Expression, Func):
    arg_types = {"this": True, "expressions": False, "ignore_nulls": True}
    is_var_len_args = True


class Nullif(Expression, Func):
    arg_types = {"this": True, "expression": True}


class Nvl2(Expression, Func):
    arg_types = {"this": True, "true": True, "false": False}


class Try(Expression, Func):
    pass


# Predicates / misc functions


class Collate(Expression, Binary, Func):
    pass


class Collation(Expression, Func):
    pass


class ConnectByRoot(Expression, Func):
    pass


class CheckXml(Expression, Func):
    arg_types = {"this": True, "disable_auto_convert": False}


class Exists(Expression, Func, SubqueryPredicate):
    arg_types = {"this": True, "expression": False}


# Type coercions / lax types


class Float64(Expression, Func):
    arg_types = {"this": True, "expression": False}


class Int64(Expression, Func):
    pass


class IsArray(Expression, Func):
    pass


class IsNullValue(Expression, Func):
    pass


class LaxBool(Expression, Func):
    pass


class LaxFloat64(Expression, Func):
    pass


class LaxInt64(Expression, Func):
    pass


class LaxString(Expression, Func):
    pass


class ToBoolean(Expression, Func):
    arg_types = {"this": True, "safe": False}


class ToVariant(Expression, Func):
    pass


# Session / context functions


class CurrentAccount(Expression, Func):
    arg_types = {}


class CurrentAccountName(Expression, Func):
    arg_types = {}


class CurrentAvailableRoles(Expression, Func):
    arg_types = {}


class CurrentCatalog(Expression, Func):
    arg_types = {}


class CurrentClient(Expression, Func):
    arg_types = {}


class CurrentDatabase(Expression, Func):
    arg_types = {}


class CurrentIpAddress(Expression, Func):
    arg_types = {}


class CurrentOrganizationName(Expression, Func):
    arg_types = {}


class CurrentOrganizationUser(Expression, Func):
    arg_types = {}


class CurrentRegion(Expression, Func):
    arg_types = {}


class CurrentRole(Expression, Func):
    arg_types = {}


class CurrentRoleType(Expression, Func):
    arg_types = {}


class CurrentSchema(Expression, Func):
    arg_types = {"this": False}


class CurrentSchemas(Expression, Func):
    arg_types = {"this": False}


class CurrentSecondaryRoles(Expression, Func):
    arg_types = {}


class CurrentSession(Expression, Func):
    arg_types = {}


class CurrentStatement(Expression, Func):
    arg_types = {}


class CurrentTransaction(Expression, Func):
    arg_types = {}


class CurrentUser(Expression, Func):
    arg_types = {"this": False}


class CurrentVersion(Expression, Func):
    arg_types = {}


class CurrentWarehouse(Expression, Func):
    arg_types = {}


class SessionUser(Expression, Func):
    arg_types = {}


# ML / AI


class AIClassify(Expression, Func):
    arg_types = {"this": True, "categories": True, "config": False}
    _sql_names = ["AI_CLASSIFY"]


class AIEmbed(Expression, Func):
    arg_types = {"expressions": False}
    is_var_len_args = True
    _sql_names = ["EMBED"]


class AISimilarity(Expression, Func):
    arg_types = {"expressions": False}
    is_var_len_args = True
    _sql_names = ["SIMILARITY"]


class AIGenerate(Expression, Func):
    arg_types = {"expressions": False}
    is_var_len_args = True
    _sql_names = ["GENERATE"]


class FeaturesAtTime(Expression, Func):
    arg_types = {"this": True, "time": False, "num_rows": False, "ignore_feature_nulls": False}


class GenerateEmbedding(Expression, Func):
    arg_types = {"this": True, "expression": True, "params_struct": False, "is_text": False}


class GenerateText(Expression, Func):
    arg_types = {"this": True, "expression": False, "params_struct": False}


class GenerateTable(Expression, Func):
    arg_types = {"this": True, "expression": False, "params_struct": False}


class GenerateBool(Expression, Func):
    arg_types = {"this": True, "expression": False, "params_struct": False}


class GenerateInt(Expression, Func):
    arg_types = {"this": True, "expression": False, "params_struct": False}


class GenerateDouble(Expression, Func):
    arg_types = {"this": True, "expression": False, "params_struct": False}


class MLForecast(Expression, Func):
    arg_types = {"this": True, "expression": False, "params_struct": False}


class AIForecast(Expression, Func):
    arg_types = {
        "this": True,
        "data_col": False,
        "timestamp_col": False,
        "model": False,
        "id_cols": False,
        "horizon": False,
        "forecast_end_timestamp": False,
        "confidence_level": False,
        "output_historical_time_series": False,
        "context_window": False,
    }


class MLTranslate(Expression, Func):
    arg_types = {"this": True, "expression": True, "params_struct": True}


class Predict(Expression, Func):
    arg_types = {"this": True, "expression": True, "params_struct": False}


class VectorSearch(Expression, Func):
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


class ReadCSV(Expression, Func):
    _sql_names = ["READ_CSV"]
    is_var_len_args = True
    arg_types = {"this": True, "expressions": False}


class ReadParquet(Expression, Func):
    is_var_len_args = True
    arg_types = {"expressions": True}


# XML


class XMLElement(Expression, Func):
    _sql_names = ["XMLELEMENT"]
    arg_types = {"this": True, "expressions": False, "evalname": False}


class XMLGet(Expression, Func):
    _sql_names = ["XMLGET"]
    arg_types = {"this": True, "expression": True, "instance": False}


class XMLTable(Expression, Func):
    arg_types = {
        "this": True,
        "namespaces": False,
        "passing": False,
        "columns": False,
        "by_ref": False,
    }


# Network / domain


class Host(Expression, Func):
    pass


class NetFunc(Expression, Func):
    pass


class ParseIp(Expression, Func):
    arg_types = {"this": True, "type": True, "permissive": False}


class RegDomain(Expression, Func):
    pass


# Misc utility


class Columns(Expression, Func):
    arg_types = {"this": True, "unpack": False}


class Normal(Expression, Func):
    arg_types = {"this": True, "stddev": True, "gen": True}


class Rand(Expression, Func):
    _sql_names = ["RAND", "RANDOM"]
    arg_types = {"this": False, "lower": False, "upper": False}


class Randn(Expression, Func):
    arg_types = {"this": False}


class Randstr(Expression, Func):
    arg_types = {"this": True, "generator": False}


class RangeBucket(Expression, Func):
    arg_types = {"this": True, "expression": True}


class RangeN(Expression, Func):
    arg_types = {"this": True, "expressions": True, "each": False}


class Seq1(Expression, Func):
    arg_types = {"this": False}


class Seq2(Expression, Func):
    arg_types = {"this": False}


class Seq4(Expression, Func):
    arg_types = {"this": False}


class Seq8(Expression, Func):
    arg_types = {"this": False}


class Uniform(Expression, Func):
    arg_types = {"this": True, "expression": True, "gen": False, "seed": False}


class Uuid(Expression, Func):
    _sql_names = ["UUID", "GEN_RANDOM_UUID", "GENERATE_UUID", "UUID_STRING"]

    arg_types = {"this": False, "name": False, "is_string": False}


class WeekStart(Expression, Func):
    pass


class WidthBucket(Expression, Func):
    arg_types = {
        "this": True,
        "min_value": False,
        "max_value": False,
        "num_buckets": False,
        "threshold": False,
    }


class Zipf(Expression, Func):
    arg_types = {"this": True, "elementcount": True, "gen": True}
