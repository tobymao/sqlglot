"""sqlglot expressions DDL."""

from __future__ import annotations

import typing as t

from sqlglot.helper import mypyc_attr, trait
from sqlglot.expressions.core import Expression, Func
from sqlglot.expressions.query import Query

if t.TYPE_CHECKING:
    from sqlglot.expressions.query import CTE


class DDL(Expression):
    @property
    def ctes(self) -> t.List[CTE]:
        """Returns a list of all the CTEs attached to this statement."""
        with_ = self.args.get("with_")
        return with_.expressions if with_ else []

    @property
    def selects(self) -> t.List[Expression]:
        """If this statement contains a query (e.g. a CTAS), this returns the query's projections."""
        return self.expression.selects if isinstance(self.expression, Query) else []

    @property
    def named_selects(self) -> t.List[str]:
        """
        If this statement contains a query (e.g. a CTAS), this returns the output
        names of the query's projections.
        """
        return self.expression.named_selects if isinstance(self.expression, Query) else []


@trait
@mypyc_attr(allow_interpreted_subclasses=True)
class DML:
    """Trait for data manipulation language statements."""


class Create(DDL):
    arg_types = {
        "with_": False,
        "this": True,
        "kind": True,
        "expression": False,
        "exists": False,
        "properties": False,
        "replace": False,
        "refresh": False,
        "unique": False,
        "indexes": False,
        "no_schema_binding": False,
        "begin": False,
        "clone": False,
        "concurrently": False,
        "clustered": False,
    }

    @property
    def kind(self) -> t.Optional[str]:
        kind = self.args.get("kind")
        return kind and kind.upper()


class SequenceProperties(Expression):
    arg_types = {
        "increment": False,
        "minvalue": False,
        "maxvalue": False,
        "cache": False,
        "start": False,
        "owned": False,
        "options": False,
    }


class TriggerProperties(Expression):
    arg_types = {
        "table": True,
        "timing": True,
        "events": True,
        "execute": True,
        "constraint": False,
        "referenced_table": False,
        "deferrable": False,
        "initially": False,
        "referencing": False,
        "for_each": False,
        "when": False,
    }


class TriggerExecute(Expression):
    pass


class TriggerEvent(Expression):
    arg_types = {"this": True, "columns": False}


class TriggerReferencing(Expression):
    arg_types = {"old": False, "new": False}


class TruncateTable(Expression):
    arg_types = {
        "expressions": True,
        "is_database": False,
        "exists": False,
        "only": False,
        "cluster": False,
        "identity": False,
        "option": False,
        "partition": False,
    }


class Clone(Expression):
    arg_types = {"this": True, "shallow": False, "copy": False}


class Describe(Expression):
    arg_types = {
        "this": True,
        "style": False,
        "kind": False,
        "expressions": False,
        "partition": False,
        "format": False,
        "as_json": False,
    }


class Attach(Expression):
    arg_types = {"this": True, "exists": False, "expressions": False}


class Detach(Expression):
    arg_types = {"this": True, "exists": False}


class Install(Expression):
    arg_types = {"this": True, "from_": False, "force": False}


class Summarize(Expression):
    arg_types = {"this": True, "table": False}


class Kill(Expression):
    arg_types = {"this": True, "kind": False}


class Pragma(Expression):
    pass


class Declare(Expression):
    arg_types = {"expressions": True}


class DeclareItem(Expression):
    arg_types = {"this": True, "kind": False, "default": False}


class Set(Expression):
    arg_types = {"expressions": False, "unset": False, "tag": False}


class Heredoc(Expression):
    arg_types = {"this": True, "tag": False}


class SetItem(Expression):
    arg_types = {
        "this": False,
        "expressions": False,
        "kind": False,
        "collate": False,  # MySQL SET NAMES statement
        "global_": False,
    }


class Show(Expression):
    arg_types = {
        "this": True,
        "history": False,
        "terse": False,
        "target": False,
        "offset": False,
        "starts_with": False,
        "limit": False,
        "from_": False,
        "like": False,
        "where": False,
        "db": False,
        "scope": False,
        "scope_kind": False,
        "full": False,
        "mutex": False,
        "query": False,
        "channel": False,
        "global_": False,
        "log": False,
        "position": False,
        "types": False,
        "privileges": False,
        "for_table": False,
        "for_group": False,
        "for_user": False,
        "for_role": False,
        "into_outfile": False,
        "json": False,
    }


class UserDefinedFunction(Expression):
    arg_types = {"this": True, "expressions": False, "wrapped": False}


class CharacterSet(Expression):
    arg_types = {"this": True, "default": False}


class AlterColumn(Expression):
    arg_types = {
        "this": True,
        "dtype": False,
        "collate": False,
        "using": False,
        "default": False,
        "drop": False,
        "comment": False,
        "allow_null": False,
        "visible": False,
        "rename_to": False,
    }


class AlterIndex(Expression):
    arg_types = {"this": True, "visible": True}


class AlterDistStyle(Expression):
    pass


class AlterSortKey(Expression):
    arg_types = {"this": False, "expressions": False, "compound": False}


class AlterSet(Expression):
    arg_types = {
        "expressions": False,
        "option": False,
        "tablespace": False,
        "access_method": False,
        "file_format": False,
        "copy_options": False,
        "tag": False,
        "location": False,
        "serde": False,
    }


class RenameColumn(Expression):
    arg_types = {"this": True, "to": True, "exists": False}


class AlterRename(Expression):
    pass


class AlterModifySqlSecurity(Expression):
    arg_types = {"expressions": True}


class SwapTable(Expression):
    pass


class Comment(Expression):
    arg_types = {
        "this": True,
        "kind": True,
        "expression": True,
        "exists": False,
        "materialized": False,
    }


class Comprehension(Expression):
    arg_types = {
        "this": True,
        "expression": True,
        "position": False,
        "iterator": True,
        "condition": False,
    }


class MergeTreeTTLAction(Expression):
    arg_types = {
        "this": True,
        "delete": False,
        "recompress": False,
        "to_disk": False,
        "to_volume": False,
    }


class MergeTreeTTL(Expression):
    arg_types = {
        "expressions": True,
        "where": False,
        "group": False,
        "aggregates": False,
    }


class Drop(Expression):
    arg_types = {
        "this": False,
        "kind": False,
        "expressions": False,
        "exists": False,
        "temporary": False,
        "materialized": False,
        "cascade": False,
        "constraints": False,
        "purge": False,
        "cluster": False,
        "concurrently": False,
    }

    @property
    def kind(self) -> t.Optional[str]:
        kind = self.args.get("kind")
        return kind and kind.upper()


class Command(Expression):
    arg_types = {"this": True, "expression": False}


class Transaction(Expression):
    arg_types = {"this": False, "modes": False, "mark": False}


class Commit(Expression):
    arg_types = {"chain": False, "this": False, "durability": False}


class Rollback(Expression):
    arg_types = {"savepoint": False, "this": False}


class Alter(Expression):
    arg_types = {
        "this": False,
        "kind": True,
        "actions": True,
        "exists": False,
        "only": False,
        "options": False,
        "cluster": False,
        "not_valid": False,
        "check": False,
        "cascade": False,
    }

    @property
    def kind(self) -> t.Optional[str]:
        kind = self.args.get("kind")
        return kind and kind.upper()

    @property
    def actions(self) -> t.List[Expression]:
        return self.args.get("actions") or []


class AlterSession(Expression):
    arg_types = {"expressions": True, "unset": False}


class Use(Expression):
    arg_types = {"this": False, "expressions": False, "kind": False}


class NextValueFor(Func):
    arg_types = {"this": True, "order": False}


@mypyc_attr(allow_interpreted_subclasses=True)
class Execute(Expression):
    arg_types = {"this": True, "expressions": False}

    @property
    def name(self) -> str:
        return self.this.name


class ExecuteSql(Execute):
    pass
