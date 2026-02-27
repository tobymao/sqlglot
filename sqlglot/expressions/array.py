"""sqlglot expressions - array, map, struct, and table-valued functions."""

from __future__ import annotations

import typing as t

from sqlglot.helper import mypyc_attr
from sqlglot.expressions.core import (
    Expression,
    Func,
    ExplodeOuter,
    Binary,
    to_identifier,
)
from sqlglot.expressions.query import UDTF


# Array creation / construction


class Array(Func):
    arg_types = {
        "expressions": False,
        "bracket_notation": False,
        "struct_name_inheritance": False,
    }
    is_var_len_args = True


class ArrayConstructCompact(Func):
    arg_types = {"expressions": False}
    is_var_len_args = True


class List(Func):
    arg_types = {"expressions": False}
    is_var_len_args = True


class ToArray(Func):
    pass


# Array manipulation


class ArrayAppend(Func):
    arg_types = {"this": True, "expression": True, "null_propagation": False}


class ArrayCompact(Func):
    pass


class ArrayConcat(Func):
    _sql_names = ["ARRAY_CONCAT", "ARRAY_CAT"]
    arg_types = {"this": True, "expressions": False, "null_propagation": False}
    is_var_len_args = True


class ArrayFilter(Func):
    arg_types = {"this": True, "expression": True}
    _sql_names = ["FILTER", "ARRAY_FILTER"]


class ArrayInsert(Func):
    arg_types = {"this": True, "position": True, "expression": True, "offset": False}


class ArrayPrepend(Func):
    arg_types = {"this": True, "expression": True, "null_propagation": False}


class ArrayRemove(Func):
    arg_types = {"this": True, "expression": True, "null_propagation": False}


class ArrayRemoveAt(Func):
    arg_types = {"this": True, "position": True}


class ArrayReverse(Func):
    pass


class ArraySlice(Func):
    arg_types = {"this": True, "start": True, "end": False, "step": False}


class ArraySort(Func):
    arg_types = {"this": True, "expression": False}


class SortArray(Func):
    arg_types = {"this": True, "asc": False, "nulls_first": False}


# Array predicates / search


class ArrayAll(Func):
    arg_types = {"this": True, "expression": True}


class ArrayAny(Func):
    arg_types = {"this": True, "expression": True}


class ArrayContains(Binary, Func):
    arg_types = {"this": True, "expression": True, "ensure_variant": False, "check_null": False}
    _sql_names = ["ARRAY_CONTAINS", "ARRAY_HAS"]


class ArrayContainsAll(Binary, Func):
    _sql_names = ["ARRAY_CONTAINS_ALL", "ARRAY_HAS_ALL"]


class ArrayExcept(Func):
    arg_types = {"this": True, "expression": True}


class ArrayIntersect(Func):
    arg_types = {"expressions": True}
    is_var_len_args = True
    _sql_names = ["ARRAY_INTERSECT", "ARRAY_INTERSECTION"]


class ArrayOverlaps(Binary, Func):
    pass


class ArrayPosition(Binary, Func):
    arg_types = {"this": True, "expression": True, "zero_based": False}


# Array properties


class ArrayDistinct(Func):
    arg_types = {"this": True, "check_null": False}


class ArrayFirst(Func):
    arg_types = {"this": True, "expression": False}


class ArrayLast(Func):
    pass


class ArrayMax(Func):
    pass


class ArrayMin(Func):
    pass


class ArraySize(Func):
    arg_types = {"this": True, "expression": False}
    _sql_names = ["ARRAY_SIZE", "ARRAY_LENGTH"]


class ArraySum(Func):
    arg_types = {"this": True, "expression": False}


# Array conversion / utility


class ArraysZip(Func):
    arg_types = {"expressions": False}
    is_var_len_args = True


class ArrayToString(Func):
    arg_types = {"this": True, "expression": True, "null": False}
    _sql_names = ["ARRAY_TO_STRING", "ARRAY_JOIN"]


class Flatten(Func):
    pass


class StringToArray(Func):
    arg_types = {"this": True, "expression": False, "null": False}
    _sql_names = ["STRING_TO_ARRAY", "SPLIT_BY_STRING", "STRTOK_TO_ARRAY"]


# Higher-order / lambda


class Apply(Func):
    arg_types = {"this": True, "expression": True}


class Reduce(Func):
    arg_types = {"this": True, "initial": True, "merge": True, "finish": False}


class Transform(Func):
    arg_types = {"this": True, "expression": True}


# Table-valued / UDTF


class GenerateSeries(Func):
    arg_types = {"start": True, "end": True, "step": False, "is_end_exclusive": False}


class ExplodingGenerateSeries(GenerateSeries):
    pass


class Generator(Func, UDTF):
    arg_types = {"rowcount": False, "timelimit": False}


@mypyc_attr(allow_interpreted_subclasses=True)
class Explode(Func, UDTF):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class Inline(Func):
    pass


class _ExplodeOuter(Explode, ExplodeOuter):
    _sql_names = ["EXPLODE_OUTER"]


class Posexplode(Explode):
    pass


class PosexplodeOuter(Posexplode, ExplodeOuter):
    pass


class PositionalColumn(Expression):
    pass


class Unnest(Func, UDTF):
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


class Map(Func):
    arg_types = {"keys": False, "values": False}

    @property
    def keys(self) -> t.List[Expression]:
        keys = self.args.get("keys")
        return keys.expressions if keys else []

    @property
    def values(self) -> t.List[Expression]:
        values = self.args.get("values")
        return values.expressions if values else []


class MapCat(Func):
    arg_types = {"this": True, "expression": True}


class MapContainsKey(Func):
    arg_types = {"this": True, "key": True}


class MapDelete(Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True


class MapFromEntries(Func):
    pass


class MapInsert(Func):
    arg_types = {"this": True, "key": False, "value": True, "update_flag": False}


class MapKeys(Func):
    pass


class MapPick(Func):
    arg_types = {"this": True, "expressions": True}
    is_var_len_args = True


class MapSize(Func):
    pass


class StarMap(Func):
    pass


class ToMap(Func):
    pass


class VarMap(Func):
    arg_types = {"keys": True, "values": True}
    is_var_len_args = True

    @property
    def keys(self) -> t.List[Expression]:
        return self.args["keys"].expressions

    @property
    def values(self) -> t.List[Expression]:
        return self.args["values"].expressions


# Struct


class Struct(Func):
    arg_types = {"expressions": False}
    is_var_len_args = True


class StructExtract(Func):
    arg_types = {"this": True, "expression": True}


# Geospatial


class StDistance(Func):
    arg_types = {"this": True, "expression": True, "use_spheroid": False}


class StPoint(Func):
    arg_types = {"this": True, "expression": True, "null": False}
    _sql_names = ["ST_POINT", "ST_MAKEPOINT"]
