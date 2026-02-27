"""sqlglot expressions - array, map, struct, and table-valued functions."""

from __future__ import annotations

import typing as t

from sqlglot.expressions.core import (
    Expression,
    Expr,
    Func,
    Binary,
    to_identifier,
)
from sqlglot.helper import trait
from sqlglot.expressions.query import UDTF


# Array creation / construction


class Array(Expression, Func):
    arg_types = {
        "expressions": False,
        "bracket_notation": False,
        "struct_name_inheritance": False,
    }
    is_var_len_args = True


class ArrayConstructCompact(Expression, Func):
    arg_types = {"expressions": False}
    is_var_len_args = True


class List(Expression, Func):
    arg_types = {"expressions": False}
    is_var_len_args = True


class ToArray(Expression, Func):
    pass


# Array manipulation


class ArrayAppend(Expression, Func):
    arg_types = {"this": True, "expression": True, "null_propagation": False}


class ArrayCompact(Expression, Func):
    pass


class ArrayConcat(Expression, Func):
    _sql_names = ["ARRAY_CONCAT", "ARRAY_CAT"]
    arg_types = {"this": True, "expressions": False, "null_propagation": False}
    is_var_len_args = True


class ArrayFilter(Expression, Func):
    arg_types = {"this": True, "expression": True}
    _sql_names = ["FILTER", "ARRAY_FILTER"]


class ArrayInsert(Expression, Func):
    arg_types = {"this": True, "position": True, "expression": True, "offset": False}


class ArrayPrepend(Expression, Func):
    arg_types = {"this": True, "expression": True, "null_propagation": False}


class ArrayRemove(Expression, Func):
    arg_types = {"this": True, "expression": True, "null_propagation": False}


class ArrayRemoveAt(Expression, Func):
    arg_types = {"this": True, "position": True}


class ArrayReverse(Expression, Func):
    pass


class ArraySlice(Expression, Func):
    arg_types = {"this": True, "start": True, "end": False, "step": False}


class ArraySort(Expression, Func):
    arg_types = {"this": True, "expression": False}


class SortArray(Expression, Func):
    arg_types = {"this": True, "asc": False, "nulls_first": False}


# Array predicates / search


class ArrayAll(Expression, Func):
    arg_types = {"this": True, "expression": True}


class ArrayAny(Expression, Func):
    arg_types = {"this": True, "expression": True}


class ArrayContains(Expression, Binary, Func):
    arg_types = {"this": True, "expression": True, "ensure_variant": False, "check_null": False}
    _sql_names = ["ARRAY_CONTAINS", "ARRAY_HAS"]


class ArrayContainsAll(Expression, Binary, Func):
    _sql_names = ["ARRAY_CONTAINS_ALL", "ARRAY_HAS_ALL"]


class ArrayExcept(Expression, Func):
    arg_types = {"this": True, "expression": True, "is_multiset": False}


class ArrayIntersect(Expression, Func):
    arg_types = {"expressions": True, "is_multiset": False}
    is_var_len_args = True
    _sql_names = ["ARRAY_INTERSECT", "ARRAY_INTERSECTION"]


class ArrayOverlaps(Expression, Binary, Func):
    pass


class ArrayPosition(Expression, Binary, Func):
    arg_types = {"this": True, "expression": True, "zero_based": False}


# Array properties


class ArrayDistinct(Expression, Func):
    arg_types = {"this": True, "check_null": False}


class ArrayFirst(Expression, Func):
    arg_types = {"this": True, "expression": False}


class ArrayLast(Expression, Func):
    pass


class ArrayMax(Expression, Func):
    pass


class ArrayMin(Expression, Func):
    pass


class ArraySize(Expression, Func):
    arg_types = {"this": True, "expression": False}
    _sql_names = ["ARRAY_SIZE", "ARRAY_LENGTH"]


class ArraySum(Expression, Func):
    arg_types = {"this": True, "expression": False}


# Array conversion / utility


class ArraysZip(Expression, Func):
    arg_types = {"expressions": False}
    is_var_len_args = True


class ArrayToString(Expression, Func):
    arg_types = {"this": True, "expression": True, "null": False}
    _sql_names = ["ARRAY_TO_STRING", "ARRAY_JOIN"]


class Flatten(Expression, Func):
    pass


class StringToArray(Expression, Func):
    arg_types = {"this": True, "expression": False, "null": False}
    _sql_names = ["STRING_TO_ARRAY", "SPLIT_BY_STRING", "STRTOK_TO_ARRAY"]


# Higher-order / lambda


class Apply(Expression, Func):
    arg_types = {"this": True, "expression": True}


class Reduce(Expression, Func):
    arg_types = {"this": True, "initial": True, "merge": True, "finish": False}


class Transform(Expression, Func):
    arg_types = {"this": True, "expression": True}


# Table-valued / UDTF


class GenerateSeries(Expression, Func):
    arg_types = {"start": True, "end": True, "step": False, "is_end_exclusive": False}


class ExplodingGenerateSeries(GenerateSeries):
    pass


class Generator(Expression, Func, UDTF):
    arg_types = {"rowcount": False, "timelimit": False}


class Explode(Expression, Func, UDTF):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class Inline(Expression, Func):
    pass


@trait
class ExplodeOuter(Expr):
    pass


class _ExplodeOuter(Explode, ExplodeOuter):
    _sql_names = ["EXPLODE_OUTER"]


class Posexplode(Explode):
    pass


class PosexplodeOuter(Posexplode, ExplodeOuter):
    pass


class PositionalColumn(Expression):
    pass


class Unnest(Expression, Func, UDTF):
    arg_types = {
        "expressions": True,
        "alias": False,
        "offset": False,
        "explode_array": False,
    }

    @property
    def selects(self) -> t.List[Expr]:
        columns = super().selects
        offset = self.args.get("offset")
        if offset:
            columns = columns + [to_identifier("offset") if offset is True else offset]
        return columns


# Map


class Map(Expression, Func):
    arg_types = {"keys": False, "values": False}

    @property
    def keys(self) -> t.List[Expr]:
        keys = self.args.get("keys")
        return keys.expressions if keys else []

    @property
    def values(self) -> t.List[Expr]:
        values = self.args.get("values")
        return values.expressions if values else []


class MapCat(Expression, Func):
    arg_types = {"this": True, "expression": True}


class MapContainsKey(Expression, Func):
    arg_types = {"this": True, "key": True}


class MapDelete(Expression, Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True


class MapFromEntries(Expression, Func):
    pass


class MapInsert(Expression, Func):
    arg_types = {"this": True, "key": False, "value": True, "update_flag": False}


class MapKeys(Expression, Func):
    pass


class MapPick(Expression, Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True


class MapSize(Expression, Func):
    pass


class StarMap(Expression, Func):
    pass


class ToMap(Expression, Func):
    pass


class VarMap(Expression, Func):
    arg_types = {"keys": True, "values": True}
    is_var_len_args = True

    @property
    def keys(self) -> t.List[Expr]:
        return self.args["keys"].expressions

    @property
    def values(self) -> t.List[Expr]:
        return self.args["values"].expressions


# Struct


class Struct(Expression, Func):
    arg_types = {"expressions": False}
    is_var_len_args = True


class StructExtract(Expression, Func):
    arg_types = {"this": True, "expression": True}


# Geospatial


class StDistance(Expression, Func):
    arg_types = {"this": True, "expression": True, "use_spheroid": False}


class StPoint(Expression, Func):
    arg_types = {"this": True, "expression": True, "null": False}
    _sql_names = ["ST_POINT", "ST_MAKEPOINT"]
