from __future__ import annotations

import typing as t

from sqlglot import exp

# One recorded argument mutation: (node, arg_key, value before the mutation).
JournalEntry = tuple[exp.Expr, str, t.Any]
Journal = list[JournalEntry]


def record(journal: Journal, node: exp.Expr, arg_key: str) -> None:
    """Records the current value of `node.args[arg_key]` so `revert` can restore it.

    Must be called before the argument is mutated. List values are copied shallowly,
    so the caller may freely mutate or replace the list afterwards.
    """
    value = node.args.get(arg_key)
    journal.append((node, arg_key, list(value) if type(value) is list else value))


def revert(journal: Journal) -> None:
    """Restores every recorded argument, newest first, and empties the journal.

    `set` reattaches the restored expressions (their `parent`, `arg_key` and `index`),
    so rules that only detach or reorder nodes leave the tree exactly as it was before
    they ran. Rules that replace nodes with new ones can't be reverted this way.
    """
    for node, arg_key, value in reversed(journal):
        node.set(arg_key, value)

    journal.clear()
