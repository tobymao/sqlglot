"""sqlglot expressions constraints."""

from __future__ import annotations

from sqlglot.expressions.core import Expression, ColumnConstraintKind


class IndexConstraintOption(Expression):
    arg_types = {
        "key_block_size": False,
        "using": False,
        "parser": False,
        "comment": False,
        "visible": False,
        "engine_attr": False,
        "secondary_engine_attr": False,
    }


class ColumnConstraint(Expression):
    arg_types = {"this": False, "kind": True}

    @property
    def kind(self) -> ColumnConstraintKind:
        return self.args["kind"]


class AutoIncrementColumnConstraint(Expression, ColumnConstraintKind):
    pass


class ZeroFillColumnConstraint(ColumnConstraint):
    arg_types = {}


class PeriodForSystemTimeConstraint(Expression, ColumnConstraintKind):
    arg_types = {"this": True, "expression": True}


class CaseSpecificColumnConstraint(Expression, ColumnConstraintKind):
    arg_types = {"not_": True}


class CharacterSetColumnConstraint(Expression, ColumnConstraintKind):
    arg_types = {"this": True}


class CheckColumnConstraint(Expression, ColumnConstraintKind):
    arg_types = {"this": True, "enforced": False}


class ClusteredColumnConstraint(Expression, ColumnConstraintKind):
    pass


class CollateColumnConstraint(Expression, ColumnConstraintKind):
    pass


class CommentColumnConstraint(Expression, ColumnConstraintKind):
    pass


class CompressColumnConstraint(Expression, ColumnConstraintKind):
    arg_types = {"this": False}


class DateFormatColumnConstraint(Expression, ColumnConstraintKind):
    arg_types = {"this": True}


class DefaultColumnConstraint(Expression, ColumnConstraintKind):
    pass


class EncodeColumnConstraint(Expression, ColumnConstraintKind):
    pass


class ExcludeColumnConstraint(Expression, ColumnConstraintKind):
    pass


class EphemeralColumnConstraint(Expression, ColumnConstraintKind):
    arg_types = {"this": False}


class WithOperator(Expression):
    arg_types = {"this": True, "op": True}


class GeneratedAsIdentityColumnConstraint(Expression, ColumnConstraintKind):
    # this: True -> ALWAYS, this: False -> BY DEFAULT
    arg_types = {
        "this": False,
        "expression": False,
        "on_null": False,
        "start": False,
        "increment": False,
        "minvalue": False,
        "maxvalue": False,
        "cycle": False,
        "order": False,
    }


class GeneratedAsRowColumnConstraint(Expression, ColumnConstraintKind):
    arg_types = {"start": False, "hidden": False}


class IndexColumnConstraint(Expression, ColumnConstraintKind):
    arg_types = {
        "this": False,
        "expressions": False,
        "kind": False,
        "index_type": False,
        "options": False,
        "expression": False,  # Clickhouse
        "granularity": False,
    }


class InlineLengthColumnConstraint(Expression, ColumnConstraintKind):
    pass


class NonClusteredColumnConstraint(Expression, ColumnConstraintKind):
    pass


class NotForReplicationColumnConstraint(Expression, ColumnConstraintKind):
    arg_types = {}


class MaskingPolicyColumnConstraint(Expression, ColumnConstraintKind):
    arg_types = {"this": True, "expressions": False}


class NotNullColumnConstraint(Expression, ColumnConstraintKind):
    arg_types = {"allow_null": False}


class OnUpdateColumnConstraint(Expression, ColumnConstraintKind):
    pass


class PrimaryKeyColumnConstraint(Expression, ColumnConstraintKind):
    arg_types = {"desc": False, "options": False}


class TitleColumnConstraint(Expression, ColumnConstraintKind):
    pass


class UniqueColumnConstraint(Expression, ColumnConstraintKind):
    arg_types = {
        "this": False,
        "index_type": False,
        "on_conflict": False,
        "nulls": False,
        "options": False,
    }


class UppercaseColumnConstraint(Expression, ColumnConstraintKind):
    arg_types = {}


class WatermarkColumnConstraint(Expression):
    arg_types = {"this": True, "expression": True}


class PathColumnConstraint(Expression, ColumnConstraintKind):
    pass


class ProjectionPolicyColumnConstraint(Expression, ColumnConstraintKind):
    pass


class ComputedColumnConstraint(Expression, ColumnConstraintKind):
    arg_types = {"this": True, "persisted": False, "not_null": False, "data_type": False}


class InOutColumnConstraint(Expression, ColumnConstraintKind):
    arg_types = {"input_": False, "output": False, "variadic": False}


class Constraint(Expression):
    arg_types = {"this": True, "expressions": True}


class ForeignKey(Expression):
    arg_types = {
        "expressions": False,
        "reference": False,
        "delete": False,
        "update": False,
        "options": False,
    }


class ColumnPrefix(Expression):
    arg_types = {"this": True, "expression": True}


class PrimaryKey(Expression):
    arg_types = {"this": False, "expressions": True, "options": False, "include": False}


class IndexParameters(Expression):
    arg_types = {
        "using": False,
        "include": False,
        "columns": False,
        "with_storage": False,
        "partition_by": False,
        "tablespace": False,
        "where": False,
        "on": False,
    }


class AddConstraint(Expression):
    arg_types = {"expressions": True}
