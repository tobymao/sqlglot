"""
.. include:: ../posts/sql_diff.md

----
"""

from __future__ import annotations

import typing as t
from collections import defaultdict
from dataclasses import dataclass
from heapq import heappop, heappush

from sqlglot import Dialect, expressions as exp
from sqlglot.helper import ensure_list


@dataclass(frozen=True)
class Insert:
    """Indicates that a new node has been inserted"""

    expression: exp.Expression


@dataclass(frozen=True)
class Remove:
    """Indicates that an existing node has been removed"""

    expression: exp.Expression


@dataclass(frozen=True)
class Move:
    """Indicates that an existing node's position within the tree has changed"""

    expression: exp.Expression


@dataclass(frozen=True)
class Update:
    """Indicates that an existing node has been updated"""

    source: exp.Expression
    target: exp.Expression


@dataclass(frozen=True)
class Keep:
    """Indicates that an existing node hasn't been changed"""

    source: exp.Expression
    target: exp.Expression


if t.TYPE_CHECKING:
    from sqlglot._typing import T

    Edit = t.Union[Insert, Remove, Move, Update, Keep]


def diff(
    source: exp.Expression,
    target: exp.Expression,
    matchings: t.List[t.Tuple[exp.Expression, exp.Expression]] | None = None,
    **kwargs: t.Any,
) -> t.List[Edit]:
    """
    Returns the list of changes between the source and the target expressions.

    Examples:
        >>> diff(parse_one("a + b"), parse_one("a + c"))
        [
            Remove(expression=(COLUMN this: (IDENTIFIER this: b, quoted: False))),
            Insert(expression=(COLUMN this: (IDENTIFIER this: c, quoted: False))),
            Keep(
                source=(ADD this: ...),
                target=(ADD this: ...)
            ),
            Keep(
                source=(COLUMN this: (IDENTIFIER this: a, quoted: False)),
                target=(COLUMN this: (IDENTIFIER this: a, quoted: False))
            ),
        ]

    Args:
        source: the source expression.
        target: the target expression against which the diff should be calculated.
        matchings: the list of pre-matched node pairs which is used to help the algorithm's
            heuristics produce better results for subtrees that are known by a caller to be matching.
            Note: expression references in this list must refer to the same node objects that are
            referenced in source / target trees.

    Returns:
        the list of Insert, Remove, Move, Update and Keep objects for each node in the source and the
        target expression trees. This list represents a sequence of steps needed to transform the source
        expression tree into the target one.
    """
    matchings = matchings or []
    matching_ids = {id(n) for pair in matchings for n in pair}

    def compute_node_mappings(
        original: exp.Expression, copy: exp.Expression
    ) -> t.Dict[int, exp.Expression]:
        return {
            id(old_node): new_node
            for (old_node, _, _), (new_node, _, _) in zip(original.walk(), copy.walk())
            if id(old_node) in matching_ids
        }

    source_copy = source.copy()
    target_copy = target.copy()

    node_mappings = {
        **compute_node_mappings(source, source_copy),
        **compute_node_mappings(target, target_copy),
    }
    matchings_copy = [(node_mappings[id(s)], node_mappings[id(t)]) for s, t in matchings]

    return ChangeDistiller(**kwargs).diff(source_copy, target_copy, matchings=matchings_copy)


LEAF_EXPRESSION_TYPES = (
    exp.Boolean,
    exp.DataType,
    exp.Identifier,
    exp.Literal,
)


class ChangeDistiller:
    """
    The implementation of the Change Distiller algorithm described by Beat Fluri and Martin Pinzger in
    their paper https://ieeexplore.ieee.org/document/4339230, which in turn is based on the algorithm by
    Chawathe et al. described in http://ilpubs.stanford.edu:8090/115/1/1995-46.pdf.
    """

    def __init__(self, f: float = 0.6, t: float = 0.6) -> None:
        self.f = f
        self.t = t
        self._sql_generator = Dialect().generator()

    def diff(
        self,
        source: exp.Expression,
        target: exp.Expression,
        matchings: t.List[t.Tuple[exp.Expression, exp.Expression]] | None = None,
    ) -> t.List[Edit]:
        matchings = matchings or []
        pre_matched_nodes = {id(s): id(t) for s, t in matchings}
        if len({n for pair in pre_matched_nodes.items() for n in pair}) != 2 * len(matchings):
            raise ValueError("Each node can be referenced at most once in the list of matchings")

        self._source = source
        self._target = target
        self._source_index = {id(n): n for n, *_ in self._source.bfs()}
        self._target_index = {id(n): n for n, *_ in self._target.bfs()}
        self._unmatched_source_nodes = set(self._source_index) - set(pre_matched_nodes)
        self._unmatched_target_nodes = set(self._target_index) - set(pre_matched_nodes.values())
        self._bigram_histo_cache: t.Dict[int, t.DefaultDict[str, int]] = {}

        matching_set = self._compute_matching_set() | {(s, t) for s, t in pre_matched_nodes.items()}
        return self._generate_edit_script(matching_set)

    def _generate_edit_script(self, matching_set: t.Set[t.Tuple[int, int]]) -> t.List[Edit]:
        edit_script: t.List[Edit] = []
        for removed_node_id in self._unmatched_source_nodes:
            edit_script.append(Remove(self._source_index[removed_node_id]))
        for inserted_node_id in self._unmatched_target_nodes:
            edit_script.append(Insert(self._target_index[inserted_node_id]))
        for kept_source_node_id, kept_target_node_id in matching_set:
            source_node = self._source_index[kept_source_node_id]
            target_node = self._target_index[kept_target_node_id]
            if not isinstance(source_node, LEAF_EXPRESSION_TYPES) or source_node == target_node:
                edit_script.extend(
                    self._generate_move_edits(source_node, target_node, matching_set)
                )
                edit_script.append(Keep(source_node, target_node))
            else:
                edit_script.append(Update(source_node, target_node))

        return edit_script

    def _generate_move_edits(
        self, source: exp.Expression, target: exp.Expression, matching_set: t.Set[t.Tuple[int, int]]
    ) -> t.List[Move]:
        source_args = [id(e) for e in _expression_only_args(source)]
        target_args = [id(e) for e in _expression_only_args(target)]

        args_lcs = set(_lcs(source_args, target_args, lambda l, r: (l, r) in matching_set))

        move_edits = []
        for a in source_args:
            if a not in args_lcs and a not in self._unmatched_source_nodes:
                move_edits.append(Move(self._source_index[a]))

        return move_edits

    def _compute_matching_set(self) -> t.Set[t.Tuple[int, int]]:
        leaves_matching_set = self._compute_leaf_matching_set()
        matching_set = leaves_matching_set.copy()

        ordered_unmatched_source_nodes = {
            id(n): None for n, *_ in self._source.bfs() if id(n) in self._unmatched_source_nodes
        }
        ordered_unmatched_target_nodes = {
            id(n): None for n, *_ in self._target.bfs() if id(n) in self._unmatched_target_nodes
        }

        for source_node_id in ordered_unmatched_source_nodes:
            for target_node_id in ordered_unmatched_target_nodes:
                source_node = self._source_index[source_node_id]
                target_node = self._target_index[target_node_id]
                if _is_same_type(source_node, target_node):
                    source_leaf_ids = {id(l) for l in _get_leaves(source_node)}
                    target_leaf_ids = {id(l) for l in _get_leaves(target_node)}

                    max_leaves_num = max(len(source_leaf_ids), len(target_leaf_ids))
                    if max_leaves_num:
                        common_leaves_num = sum(
                            1 if s in source_leaf_ids and t in target_leaf_ids else 0
                            for s, t in leaves_matching_set
                        )
                        leaf_similarity_score = common_leaves_num / max_leaves_num
                    else:
                        leaf_similarity_score = 0.0

                    adjusted_t = (
                        self.t if min(len(source_leaf_ids), len(target_leaf_ids)) > 4 else 0.4
                    )

                    if leaf_similarity_score >= 0.8 or (
                        leaf_similarity_score >= adjusted_t
                        and self._dice_coefficient(source_node, target_node) >= self.f
                    ):
                        matching_set.add((source_node_id, target_node_id))
                        self._unmatched_source_nodes.remove(source_node_id)
                        self._unmatched_target_nodes.remove(target_node_id)
                        ordered_unmatched_target_nodes.pop(target_node_id, None)
                        break

        return matching_set

    def _compute_leaf_matching_set(self) -> t.Set[t.Tuple[int, int]]:
        candidate_matchings: t.List[t.Tuple[float, int, int, exp.Expression, exp.Expression]] = []
        source_leaves = list(_get_leaves(self._source))
        target_leaves = list(_get_leaves(self._target))
        for source_leaf in source_leaves:
            for target_leaf in target_leaves:
                if _is_same_type(source_leaf, target_leaf):
                    similarity_score = self._dice_coefficient(source_leaf, target_leaf)
                    if similarity_score >= self.f:
                        heappush(
                            candidate_matchings,
                            (
                                -similarity_score,
                                -_parent_similarity_score(source_leaf, target_leaf),
                                len(candidate_matchings),
                                source_leaf,
                                target_leaf,
                            ),
                        )

        # Pick best matchings based on the highest score
        matching_set = set()
        while candidate_matchings:
            _, _, _, source_leaf, target_leaf = heappop(candidate_matchings)
            if (
                id(source_leaf) in self._unmatched_source_nodes
                and id(target_leaf) in self._unmatched_target_nodes
            ):
                matching_set.add((id(source_leaf), id(target_leaf)))
                self._unmatched_source_nodes.remove(id(source_leaf))
                self._unmatched_target_nodes.remove(id(target_leaf))

        return matching_set

    def _dice_coefficient(self, source: exp.Expression, target: exp.Expression) -> float:
        source_histo = self._bigram_histo(source)
        target_histo = self._bigram_histo(target)

        total_grams = sum(source_histo.values()) + sum(target_histo.values())
        if not total_grams:
            return 1.0 if source == target else 0.0

        overlap_len = 0
        overlapping_grams = set(source_histo) & set(target_histo)
        for g in overlapping_grams:
            overlap_len += min(source_histo[g], target_histo[g])

        return 2 * overlap_len / total_grams

    def _bigram_histo(self, expression: exp.Expression) -> t.DefaultDict[str, int]:
        if id(expression) in self._bigram_histo_cache:
            return self._bigram_histo_cache[id(expression)]

        expression_str = self._sql_generator.generate(expression)
        count = max(0, len(expression_str) - 1)
        bigram_histo: t.DefaultDict[str, int] = defaultdict(int)
        for i in range(count):
            bigram_histo[expression_str[i : i + 2]] += 1

        self._bigram_histo_cache[id(expression)] = bigram_histo
        return bigram_histo


def _get_leaves(expression: exp.Expression) -> t.Iterator[exp.Expression]:
    has_child_exprs = False

    for _, node in expression.iter_expressions():
        has_child_exprs = True
        yield from _get_leaves(node)

    if not has_child_exprs:
        yield expression


def _is_same_type(source: exp.Expression, target: exp.Expression) -> bool:
    if type(source) is type(target) and (
        not isinstance(source, exp.Identifier) or type(source.parent) is type(target.parent)
    ):
        if isinstance(source, exp.Join):
            return source.args.get("side") == target.args.get("side")

        if isinstance(source, exp.Anonymous):
            return source.this == target.this

        return True

    return False


def _parent_similarity_score(
    source: t.Optional[exp.Expression], target: t.Optional[exp.Expression]
) -> int:
    if source is None or target is None or type(source) is not type(target):
        return 0

    return 1 + _parent_similarity_score(source.parent, target.parent)


def _expression_only_args(expression: exp.Expression) -> t.List[exp.Expression]:
    args: t.List[t.Union[exp.Expression, t.List]] = []
    if expression:
        for a in expression.args.values():
            args.extend(ensure_list(a))
    return [a for a in args if isinstance(a, exp.Expression)]


def _lcs(
    seq_a: t.Sequence[T], seq_b: t.Sequence[T], equal: t.Callable[[T, T], bool]
) -> t.Sequence[t.Optional[T]]:
    """Calculates the longest common subsequence"""

    len_a = len(seq_a)
    len_b = len(seq_b)
    lcs_result = [[None] * (len_b + 1) for i in range(len_a + 1)]

    for i in range(len_a + 1):
        for j in range(len_b + 1):
            if i == 0 or j == 0:
                lcs_result[i][j] = []  # type: ignore
            elif equal(seq_a[i - 1], seq_b[j - 1]):
                lcs_result[i][j] = lcs_result[i - 1][j - 1] + [seq_a[i - 1]]  # type: ignore
            else:
                lcs_result[i][j] = (
                    lcs_result[i - 1][j]
                    if len(lcs_result[i - 1][j]) > len(lcs_result[i][j - 1])  # type: ignore
                    else lcs_result[i][j - 1]
                )

    return lcs_result[len_a][len_b]  # type: ignore
