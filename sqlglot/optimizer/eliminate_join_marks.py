import typing as t
import copy

from sqlglot import expressions as exp


def _update_from(
    select: exp.Select,
    new_join_dict: t.Dict[str, exp.Join],
    old_join_dict: t.Dict[str, exp.Join],
):
    """If the from clause needs to become a new join, find an appropriate table to use as the new from.
    updates select in place

    Args:
        select (exp.Select): The select statement to update
        new_join_dict (Dict[str, exp.Join]): The dictionary of new joins
        old_join_dict (Dict[str, exp.Join]): The dictionary of old joins
    """
    old_from = select.args["from"]
    if not old_from.alias_or_name in new_join_dict.keys():
        return
    in_old_not_new = old_join_dict.keys() - new_join_dict.keys()
    if len(in_old_not_new) >= 1:
        new_from_name = list(old_join_dict.keys() - new_join_dict.keys())[0]
        new_from_this = old_join_dict[new_from_name].this
        new_from = exp.From(this=new_from_this)
        del old_join_dict[new_from_name]
        select.set("from", new_from)
    else:
        raise ValueError("Cannot determine which table to use as the new from")


def _update_join_dict(
    join: exp.Join, join_dict: t.Dict[str, exp.Join]
) -> t.Dict[str, exp.Join]:
    """Update the join dictionary with the new join.
    If the join already exists, update the on clause.

    Args:
        join (exp.Join): The join to add to the dictionary
        join_dict (Dict[str, exp.Join]): dictionary of joins where str is join.alias_or_name

    Returns:
        Dict[str, exp.Join]: The updated dictionary of joins
    """
    if join.alias_or_name in join_dict.keys():
        join_dict[join.alias_or_name].set(
            "on",
            exp.And(
                this=join_dict[join.alias_or_name].args["on"],
                expression=join.args["on"],
            ),
        )
    else:
        join_dict[join.alias_or_name] = join
    return join_dict


def _clean_binary_node(node: exp.Expression):
    """if the node is left with only one child, promote the child to the parent node.
    transformation is done in place.

    Args:
        node (exp.Expression): The node to clean"""
    if isinstance(node, exp.Binary):
        if node.left is None:
            node.replace(node.right)
        elif node.right is None:
            node.replace(node.left)


def _has_join_mark(col: exp.Expression) -> bool:
    """Check if the column has a join mark

    Args:
        col (exp.Column): The column to check
    """
    if not isinstance(col, exp.Column):
        return False
    result = col.args.get("join_mark", False)
    if isinstance(result, bool):
        return bool(result)
    return False


def _equality_to_join(
    eq: exp.Binary, old_joins: t.Dict[str, exp.Join], old_from: exp.From
) -> t.Optional[exp.Join]:
    """Convert an equality predicate to a join if it contains a join mark

    Args:
        eq (exp.Binary): The equality expression to convert to a join

    Returns:
        Optional[exp.Join]: The join expression if the equality contains a join mark (otherwise None)
    """
    if not (isinstance(eq.left, exp.Column) or isinstance(eq.right, exp.Column)):
        return None
    new_eq = copy.deepcopy(eq)
    left_has_join_mark = _has_join_mark(eq.left)
    right_has_join_mark = _has_join_mark(eq.right)

    if left_has_join_mark:
        new_eq.left.set("join_mark", False)
        assert isinstance(new_eq.left, exp.Column)
        join_on = new_eq.left.table
    elif right_has_join_mark:
        new_eq.right.set("join_mark", False)
        assert isinstance(new_eq.right, exp.Column)
        join_on = new_eq.right.table
    else:
        return None

    join_this = old_joins.get(join_on, old_from).this
    return exp.Join(this=join_this, on=new_eq, kind="LEFT")


def _eliminate_join_marks_from_select(select: exp.Select) -> exp.Select:
    """Remove join marks from the where columns in this select statement
    Converts them to joins and replaces any existing joins

    Args:
        node (exp.Select): The AST to remove join marks from

    Returns:
        exp.Select: The AST with join marks removed
    """
    if not select.args.get("where"):
        return select
    if not select.args.get("joins"):
        return select
    old_joins: t.Dict[str, exp.Join] = {
        join.alias_or_name: join for join in list(select.args.get("joins", []))
    }
    new_joins: t.Dict[str, exp.Join] = {}
    for node in select.find_all(exp.Column):
        if _has_join_mark(node):
            predicate = node.find_ancestor(exp.Predicate)
            if not isinstance(predicate, exp.Binary):
                continue
            predicate_parent = predicate.parent
            join_on = predicate.pop()
            new_join = _equality_to_join(
                join_on, old_joins=old_joins, old_from=select.args["from"]
            )
            new_joins = _update_join_dict(new_join, new_joins)
            _clean_binary_node(predicate_parent)
    _update_from(select, new_joins, old_joins)
    replacement_joins = [
        new_joins.get(join.alias_or_name, join) for join in old_joins.values()
    ]
    select.set("joins", replacement_joins)
    where = select.args["where"]
    if where:
        if not where.this:
            select.set("where", None)
    return select


def eliminate_join_marks(ast: exp.Expression) -> exp.Expression:
    """Remove join marks from an expression

    Args:
        ast (exp.Expression): The AST to remove join marks from

    Returns:
        exp.Expression: The AST with join marks removed"""
    select_nodes = list(ast.find_all(exp.Select))
    select_nodes.reverse()
    # convert inner nodes first
    for node in select_nodes:
        if not node.parent:
            continue
        replacement = _eliminate_join_marks_from_select(node)
        node.replace(replacement)
    # transform outer node
    if isinstance(ast, exp.Select):
        ast = _eliminate_join_marks_from_select(ast)
    return ast
