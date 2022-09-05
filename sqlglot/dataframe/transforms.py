import typing as t

from sqlglot import expressions as exp

EXPRESSION_TYPE = t.TypeVar("EXPRESSION_TYPE", bound="exp.Expression")


def find_all_ignoring_ctes(node: exp.Expression, target: t.Type[EXPRESSION_TYPE]) -> t.Generator[EXPRESSION_TYPE, None, None]:
    root_select = node.parent_select
    expression = root_select.copy()
    expression.args.pop("with", None)
    return expression.find_all(target)


def is_in_cte(node: exp.Expression) -> bool:
    return node.find_ancestor(exp.CTE) is not None


def has_join(node: exp.Expression) -> bool:
    root_select = node.parent_select
    return root_select.find(exp.Join) is not None


def get_matching_cte_by_name(node: exp.Expression, name: str) -> t.Optional[exp.CTE]:
    root_select = node.parent_select
    for cte in root_select.ctes:
        if cte.alias_or_name == name:
            return cte
    return None


def replace_alias_names_with_sequence_ids(node: exp.Expression, name_to_sequence_id_mapping: t.Dict[str, t.List[str]], **kwargs):
    """
    PySpark DataFrame Specific

    Replaces any alias name references with the sequence ID that is used based on the name.
    """
    if isinstance(node, exp.Column) and node.args.get("table") is not None and node.args["table"].alias_or_name in name_to_sequence_id_mapping:
        new_table_name = exp.Identifier(this=name_to_sequence_id_mapping[node.args["table"].alias_or_name][-1])
        node.set("table", new_table_name)
    return node


def replace_branch_and_sequence_ids_with_cte_name(node: exp.Expression, known_ids: t.Set[str], known_sequence_ids: t.Set[str], **kwargs):
    """
    PySpark DataFrame Specific

    Replaces branch and sequence id references with cte names
    """
    if isinstance(node, exp.Identifier) and node.alias_or_name in known_ids:
        this_id = node.alias_or_name
        root_select: exp.Select = node.parent_select
        this_cte_name = node.find_ancestor(exp.CTE).alias_or_name if is_in_cte(node) else None
        latest_cte_name = None
        # Check if we have a join and if both the tables in that join share a common branch id
        # If so we need to have this reference the left table by default unless the id is a sequence
        # id then it keeps that reference
        if has_join(node) and node.alias_or_name not in known_sequence_ids:
            table_expressions = find_all_ignoring_ctes(node, exp.Table)
            ctes = [get_matching_cte_by_name(node, table.alias_or_name) for table in table_expressions]
            if ctes and ctes[0].args["branch_id"] == ctes[1].args["branch_id"]:
                assert len(ctes) == 2
                node.set("this", exp.Identifier(this=ctes[0].alias_or_name))
                return node
        for cte in root_select.ctes:
            if this_cte_name is not None and this_cte_name == cte.alias_or_name:
                break
            branch_id = cte.args["branch_id"]
            sequence_id = cte.args["sequence_id"]
            if this_id == branch_id:
                latest_cte_name = cte.alias_or_name
            elif this_id == sequence_id:
                latest_cte_name = cte.alias_or_name
        node.set("this", exp.Identifier(this=latest_cte_name))
    return node


def add_left_hand_table_in_join_to_ambiguous_column(node: exp.Expression, **kwargs):
    """
    If a column is missing a table identifier and that column was used in a join then
    we add the left hand table of that join as an identifier for the column
    """
    if isinstance(node, exp.Column) and node.args.get("table") is None and has_join(node):
        joins = find_all_ignoring_ctes(node, exp.Join)
        best_match = None
        for join in joins:
            columns: t.Generator[exp.Column] = join.find_all(exp.Column)
            for i, column in enumerate(columns):
                if column.alias_or_name == node.alias_or_name:
                    if i % 2 == 0:
                        node.set("table", exp.Identifier(this=column.args["table"].alias_or_name))
                        return node
                    best_match = column.args["table"].alias_or_name if best_match is None else best_match
        if best_match is not None:
            node.set("table", exp.Identifier(this=best_match))
    return node


ORDERED_TRANSFORMS = [
    replace_alias_names_with_sequence_ids,
    replace_branch_and_sequence_ids_with_cte_name,
    add_left_hand_table_in_join_to_ambiguous_column
]


if __name__ == '__main__':
    import sqlglot
    expression = sqlglot.parse_one("WITH a1 as (SELECT col_a from tablea) SELECT col_a from a1 inner join employee on a1.col_a = employee.col_a")
    cte = expression.ctes[0]
    cte.set("branch_id", "a2")
    cte.set("sequence_id", "a3")
    name_to_sequence_id_mapping = {
        "blah": ["a3"],
    }
    known_ids = {"a2", "a3"}
    known_branch_ids = {"a2"}
    known_sequence_ids = {"a3"}
    result = expression.transform(replace_alias_names_with_sequence_ids, name_to_sequence_id_mapping)
    for transform in ORDERED_TRANSFORMS:
        expression = expression.transform(transform,
                                          name_to_sequence_id_mapping=name_to_sequence_id_mapping,
                                          known_ids=known_ids,
                                          known_branch_ids=known_branch_ids,
                                          known_sequence_ids=known_sequence_ids)
    print("here")
