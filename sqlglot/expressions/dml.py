"""sqlglot expressions DML."""

from __future__ import annotations

import typing as t

from sqlglot.helper import mypyc_attr
from sqlglot.expressions.core import (
    Expression,
    DML,
    DDL,
    ExpOrStr,
    _apply_builder,
    _apply_list_builder,
    maybe_copy,
    _apply_conjunction_builder,
)
from sqlglot.expressions.query import (
    Table,
    Where,
    From,
    _apply_cte_builder,
)

if t.TYPE_CHECKING:
    from typing_extensions import Self
    from sqlglot.dialects.dialect import DialectType


@mypyc_attr(allow_interpreted_subclasses=True)
class _DML(Expression, DML):
    def returning(
        self,
        expression: ExpOrStr,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> "Self":
        """
        Set the RETURNING expression. Not supported by all dialects.

        Example:
            >>> Delete().delete("tbl").returning("*", dialect="postgres").sql()
            'DELETE FROM tbl RETURNING *'

        Args:
            expression: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            Delete: the modified expression.
        """
        return _apply_builder(
            expression=expression,
            instance=self,
            arg="returning",
            prefix="RETURNING",
            dialect=dialect,
            copy=copy,
            into=Returning,
            **opts,
        )


class Delete(_DML):
    arg_types = {
        "with_": False,
        "this": False,
        "using": False,
        "where": False,
        "returning": False,
        "order": False,
        "limit": False,
        "tables": False,  # Multiple-Table Syntax (MySQL)
        "cluster": False,  # Clickhouse
    }

    def delete(
        self,
        table: ExpOrStr,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Delete:
        """
        Create a DELETE expression or replace the table on an existing DELETE expression.

        Example:
            >>> Delete().delete("tbl").sql()
            'DELETE FROM tbl'

        Args:
            table: the table from which to delete.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            Delete: the modified expression.
        """
        return _apply_builder(
            expression=table,
            instance=self,
            arg="this",
            dialect=dialect,
            into=Table,
            copy=copy,
            **opts,
        )

    def where(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Delete:
        """
        Append to or set the WHERE expressions.

        Example:
            >>> Delete().delete("tbl").where("x = 'a' OR x < 'b'").sql()
            "DELETE FROM tbl WHERE x = 'a' OR x < 'b'"

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
                Multiple expressions are combined with an AND operator.
            append: if `True`, AND the new expressions to any existing expression.
                Otherwise, this resets the expression.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            Delete: the modified expression.
        """
        return _apply_conjunction_builder(
            *expressions,
            instance=self,
            arg="where",
            append=append,
            into=Where,
            dialect=dialect,
            copy=copy,
            **opts,
        )


class Export(Expression):
    arg_types = {"this": True, "connection": False, "options": True}


class CopyParameter(Expression):
    arg_types = {"this": True, "expression": False, "expressions": False}


class Copy(_DML):
    arg_types = {
        "this": True,
        "kind": True,
        "files": False,
        "credentials": False,
        "format": False,
        "params": False,
    }


class Credentials(Expression):
    arg_types = {
        "credentials": False,
        "encryption": False,
        "storage": False,
        "iam_role": False,
        "region": False,
    }


class Directory(Expression):
    arg_types = {"this": True, "local": False, "row_format": False}


class DirectoryStage(Expression):
    pass


class Insert(DDL, _DML):
    arg_types = {
        "hint": False,
        "with_": False,
        "is_function": False,
        "this": False,
        "expression": False,
        "conflict": False,
        "returning": False,
        "overwrite": False,
        "exists": False,
        "alternative": False,
        "where": False,
        "ignore": False,
        "by_name": False,
        "stored": False,
        "partition": False,
        "settings": False,
        "source": False,
        "default": False,
    }

    def with_(
        self,
        alias: ExpOrStr,
        as_: ExpOrStr,
        recursive: t.Optional[bool] = None,
        materialized: t.Optional[bool] = None,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Insert:
        """
        Append to or set the common table expressions.

        Example:
            >>> import sqlglot
            >>> sqlglot.parse_one("INSERT INTO t SELECT x FROM cte").with_("cte", as_="SELECT * FROM tbl").sql()
            'WITH cte AS (SELECT * FROM tbl) INSERT INTO t SELECT x FROM cte'

        Args:
            alias: the SQL code string to parse as the table name.
                If an `Expression` instance is passed, this is used as-is.
            as_: the SQL code string to parse as the table expression.
                If an `Expression` instance is passed, it will be used as-is.
            recursive: set the RECURSIVE part of the expression. Defaults to `False`.
            materialized: set the MATERIALIZED part of the expression.
            append: if `True`, add to any existing expressions.
                Otherwise, this resets the expressions.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified expression.
        """
        return _apply_cte_builder(
            self,
            alias,
            as_,
            recursive=recursive,
            materialized=materialized,
            append=append,
            dialect=dialect,
            copy=copy,
            **opts,
        )


class OnConflict(Expression):
    arg_types = {
        "duplicate": False,
        "expressions": False,
        "action": False,
        "conflict_keys": False,
        "index_predicate": False,
        "constraint": False,
        "where": False,
    }


class Returning(Expression):
    arg_types = {"expressions": True, "into": False}


class LoadData(Expression):
    arg_types = {
        "this": True,
        "local": False,
        "overwrite": False,
        "inpath": True,
        "partition": False,
        "input_format": False,
        "serde": False,
    }


class Update(_DML):
    arg_types = {
        "with_": False,
        "this": False,
        "expressions": False,
        "from_": False,
        "where": False,
        "returning": False,
        "order": False,
        "limit": False,
        "options": False,
    }

    def table(
        self, expression: ExpOrStr, dialect: DialectType = None, copy: bool = True, **opts
    ) -> Update:
        """
        Set the table to update.

        Example:
            >>> Update().table("my_table").set_("x = 1").sql()
            'UPDATE my_table SET x = 1'

        Args:
            expression : the SQL code strings to parse.
                If a `Table` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `Table`.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Update expression.
        """
        return _apply_builder(
            expression=expression,
            instance=self,
            arg="this",
            into=Table,
            prefix=None,
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def set_(
        self,
        *expressions: ExpOrStr,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Update:
        """
        Append to or set the SET expressions.

        Example:
            >>> Update().table("my_table").set_("x = 1").sql()
            'UPDATE my_table SET x = 1'

        Args:
            *expressions: the SQL code strings to parse.
                If `Expression` instance(s) are passed, they will be used as-is.
                Multiple expressions are combined with a comma.
            append: if `True`, add the new expressions to any existing SET expressions.
                Otherwise, this resets the expressions.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.
        """
        return _apply_list_builder(
            *expressions,
            instance=self,
            arg="expressions",
            append=append,
            into=Expression,
            prefix=None,
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def where(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Update:
        """
        Append to or set the WHERE expressions.

        Example:
            >>> Update().table("tbl").set_("x = 1").where("x = 'a' OR x < 'b'").sql()
            "UPDATE tbl SET x = 1 WHERE x = 'a' OR x < 'b'"

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
                Multiple expressions are combined with an AND operator.
            append: if `True`, AND the new expressions to any existing expression.
                Otherwise, this resets the expression.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            Update: the modified expression.
        """
        return _apply_conjunction_builder(
            *expressions,
            instance=self,
            arg="where",
            append=append,
            into=Where,
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def from_(
        self,
        expression: t.Optional[ExpOrStr] = None,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Update:
        """
        Set the FROM expression.

        Example:
            >>> Update().table("my_table").set_("x = 1").from_("baz").sql()
            'UPDATE my_table SET x = 1 FROM baz'

        Args:
            expression : the SQL code strings to parse.
                If a `From` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `From`.
                If nothing is passed in then a from is not applied to the expression
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Update expression.
        """
        if not expression:
            return maybe_copy(self, copy)

        return _apply_builder(
            expression=expression,
            instance=self,
            arg="from_",
            into=From,
            prefix="FROM",
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def with_(
        self,
        alias: ExpOrStr,
        as_: ExpOrStr,
        recursive: t.Optional[bool] = None,
        materialized: t.Optional[bool] = None,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Update:
        """
        Append to or set the common table expressions.

        Example:
            >>> Update().table("my_table").set_("x = 1").from_("baz").with_("baz", "SELECT id FROM foo").sql()
            'WITH baz AS (SELECT id FROM foo) UPDATE my_table SET x = 1 FROM baz'

        Args:
            alias: the SQL code string to parse as the table name.
                If an `Expression` instance is passed, this is used as-is.
            as_: the SQL code string to parse as the table expression.
                If an `Expression` instance is passed, it will be used as-is.
            recursive: set the RECURSIVE part of the expression. Defaults to `False`.
            materialized: set the MATERIALIZED part of the expression.
            append: if `True`, add to any existing expressions.
                Otherwise, this resets the expressions.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified expression.
        """
        return _apply_cte_builder(
            self,
            alias,
            as_,
            recursive=recursive,
            materialized=materialized,
            append=append,
            dialect=dialect,
            copy=copy,
            **opts,
        )


class Merge(_DML):
    arg_types = {
        "this": True,
        "using": True,
        "on": False,
        "using_cond": False,
        "whens": True,
        "with_": False,
        "returning": False,
    }


class When(Expression):
    arg_types = {"matched": True, "source": False, "condition": False, "then": True}


class Whens(Expression):
    """Wraps around one or more WHEN [NOT] MATCHED [...] clauses."""

    arg_types = {"expressions": True}
