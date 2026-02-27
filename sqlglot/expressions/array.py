"""sqlglot expressions - array, map, struct, and table-valued functions."""

from __future__ import annotations

import typing as t

from sqlglot.expressions.core import (
    ExpressionBase,
    Expression,
    Func,
    Binary,
    to_identifier,
)
from sqlglot.helper import trait
from sqlglot.expressions.query import UDTF


# Array creation / construction


class Array(ExpressionBase, Func):
    arg_types = {
        "expressions": False,
        "bracket_notation": False,
        "struct_name_inheritance": False,
    }
    is_var_len_args = True


class ArrayConstructCompact(ExpressionBase, Func):
    arg_types = {"expressions": False}
    is_var_len_args = True


class List(ExpressionBase, Func):
    arg_types = {"expressions": False}
    is_var_len_args = True


class ToArray(ExpressionBase, Func):
    pass


# Array manipulation


class ArrayAppend(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True, "null_propagation": False}


class ArrayCompact(ExpressionBase, Func):
    pass


class ArrayConcat(ExpressionBase, Func):
    _sql_names = ["ARRAY_CONCAT", "ARRAY_CAT"]
    arg_types = {"this": True, "expressions": False, "null_propagation": False}
    is_var_len_args = True


class ArrayFilter(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}
    _sql_names = ["FILTER", "ARRAY_FILTER"]


class ArrayInsert(ExpressionBase, Func):
    arg_types = {"this": True, "position": True, "expression": True, "offset": False}


class ArrayPrepend(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True, "null_propagation": False}


class ArrayRemove(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True, "null_propagation": False}


class ArrayRemoveAt(ExpressionBase, Func):
    arg_types = {"this": True, "position": True}


class ArrayReverse(ExpressionBase, Func):
    pass


class ArraySlice(ExpressionBase, Func):
    arg_types = {"this": True, "start": True, "end": False, "step": False}


class ArraySort(ExpressionBase, Func):
    arg_types = {"this": True, "expression": False}


class SortArray(ExpressionBase, Func):
    arg_types = {"this": True, "asc": False, "nulls_first": False}


# Array predicates / search


class ArrayAll(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


class ArrayAny(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


class ArrayContains(ExpressionBase, Binary, Func):
    arg_types = {"this": True, "expression": True, "ensure_variant": False, "check_null": False}
    _sql_names = ["ARRAY_CONTAINS", "ARRAY_HAS"]


class ArrayContainsAll(ExpressionBase, Binary, Func):
    _sql_names = ["ARRAY_CONTAINS_ALL", "ARRAY_HAS_ALL"]


class ArrayExcept(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True, "is_multiset": False}


class ArrayIntersect(ExpressionBase, Func):
    arg_types = {"expressions": True, "is_multiset": False}
    is_var_len_args = True
    _sql_names = ["ARRAY_INTERSECT", "ARRAY_INTERSECTION"]


class ArrayOverlaps(ExpressionBase, Binary, Func):
    pass


class ArrayPosition(ExpressionBase, Binary, Func):
    arg_types = {"this": True, "expression": True, "zero_based": False}


# Array properties


class ArrayDistinct(ExpressionBase, Func):
    arg_types = {"this": True, "check_null": False}


class ArrayFirst(ExpressionBase, Func):
    arg_types = {"this": True, "expression": False}


class ArrayLast(ExpressionBase, Func):
    pass


class ArrayMax(ExpressionBase, Func):
    pass


class ArrayMin(ExpressionBase, Func):
    pass


class ArraySize(ExpressionBase, Func):
    arg_types = {"this": True, "expression": False}
    _sql_names = ["ARRAY_SIZE", "ARRAY_LENGTH"]


class ArraySum(ExpressionBase, Func):
    arg_types = {"this": True, "expression": False}


# Array conversion / utility


class ArraysZip(ExpressionBase, Func):
    arg_types = {"expressions": False}
    is_var_len_args = True


class ArrayToString(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True, "null": False}
    _sql_names = ["ARRAY_TO_STRING", "ARRAY_JOIN"]


class Flatten(ExpressionBase, Func):
    pass


class StringToArray(ExpressionBase, Func):
    arg_types = {"this": True, "expression": False, "null": False}
    _sql_names = ["STRING_TO_ARRAY", "SPLIT_BY_STRING", "STRTOK_TO_ARRAY"]


# Higher-order / lambda


class Apply(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


class Reduce(ExpressionBase, Func):
    arg_types = {"this": True, "initial": True, "merge": True, "finish": False}


class Transform(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


# Table-valued / UDTF


class GenerateSeries(ExpressionBase, Func):
    arg_types = {"start": True, "end": True, "step": False, "is_end_exclusive": False}


class ExplodingGenerateSeries(GenerateSeries):
    pass


class Generator(ExpressionBase, Func, UDTF):
    arg_types = {"rowcount": False, "timelimit": False}


class Explode(ExpressionBase, Func, UDTF):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class Inline(ExpressionBase, Func):
    pass


@trait
class ExplodeOuter(Expression):
    pass


class _ExplodeOuter(Explode, ExplodeOuter):
    _sql_names = ["EXPLODE_OUTER"]


class Posexplode(Explode):
    pass


class PosexplodeOuter(Posexplode, ExplodeOuter):
    pass


class PositionalColumn(Expression):
    pass


class Unnest(ExpressionBase, Func, UDTF):
    arg_types = {
        "expressions": True,
        "alias": False,
        "offset": False,
        "explode_array": False,
    }

    @property
    def selects(self) -> t.List[Expression]:
        columns = super().selects
        offset = self.args.get("offset")
        if offset:
            columns = columns + [to_identifier("offset") if offset is True else offset]
        return columns


# Map


class Map(ExpressionBase, Func):
    arg_types = {"keys": False, "values": False}

    @property
    def keys(self) -> t.List[Expression]:
        keys = self.args.get("keys")
        return keys.expressions if keys else []

    @property
    def values(self) -> t.List[Expression]:
        values = self.args.get("values")
        return values.expressions if values else []


class MapCat(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


class MapContainsKey(ExpressionBase, Func):
    arg_types = {"this": True, "key": True}


class MapDelete(ExpressionBase, Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True


class MapFromEntries(ExpressionBase, Func):
    pass


class MapInsert(ExpressionBase, Func):
    arg_types = {"this": True, "key": False, "value": True, "update_flag": False}


class MapKeys(ExpressionBase, Func):
    pass


class MapPick(ExpressionBase, Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True


class MapSize(ExpressionBase, Func):
    pass


class StarMap(ExpressionBase, Func):
    pass


class ToMap(ExpressionBase, Func):
    pass


class VarMap(ExpressionBase, Func):
    arg_types = {"keys": True, "values": True}
    is_var_len_args = True

    @property
    def keys(self) -> t.List[Expression]:
        return self.args["keys"].expressions

    @property
    def values(self) -> t.List[Expression]:
        return self.args["values"].expressions


# Struct


class Struct(ExpressionBase, Func):
    arg_types = {"expressions": False}
    is_var_len_args = True


class StructExtract(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True}


# Geospatial


class StDistance(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True, "use_spheroid": False}


class StPoint(ExpressionBase, Func):
    arg_types = {"this": True, "expression": True, "null": False}
    _sql_names = ["ST_POINT", "ST_MAKEPOINT"]
