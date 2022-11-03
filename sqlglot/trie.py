import typing as t

key = t.Sequence[t.Hashable]


def new_trie(keywords: t.Iterable[key]) -> t.Dict:
    """
    Creates a new trie out of a collection of keywords.

    The trie is represented as a sequence of nested dictionaries keyed by either single character
    strings, or by 0, which is used to designate that a keyword is in the trie.

    Example:
        >>> new_trie(["bla", "foo", "blab"])
        {'b': {'l': {'a': {0: True, 'b': {0: True}}}}, 'f': {'o': {'o': {0: True}}}}

    Args:
        keywords: the keywords to create the trie from.

    Returns:
        The trie corresponding to `keywords`.
    """
    trie: t.Dict = {}

    for key in keywords:
        current = trie

        for char in key:
            current = current.setdefault(char, {})
        current[0] = True

    return trie


def in_trie(trie: t.Dict, key: key) -> t.Tuple[int, t.Dict]:
    """
    Checks whether a key is in a trie.

    Examples:
        >>> in_trie(new_trie(["cat"]), "bob")
        (0, {'c': {'a': {'t': {0: True}}}})

        >>> in_trie(new_trie(["cat"]), "ca")
        (1, {'t': {0: True}})

        >>> in_trie(new_trie(["cat"]), "cat")
        (2, {0: True})

    Args:
        trie: the trie to be searched.
        key: the target key.

    Returns:
        A pair `(value, subtrie)`, where `subtrie` is the sub-trie we get at the point where the search stops, and `value`
        is either 0 (search was unsuccessfull), 1 (`value` is a prefix of a keyword in `trie`) or 2 (`key is in `trie`).
    """
    if not key:
        return (0, trie)

    current = trie

    for char in key:
        if char not in current:
            return (0, current)
        current = current[char]

    if 0 in current:
        return (2, current)
    return (1, current)
