def new_trie(keywords):
    trie = {}

    for key in keywords:
        current = trie

        for char in key:
            current = current.setdefault(char, {})
        current[0] = True

    return trie


def in_trie(trie, key):
    if not key:
        return 0

    current = trie

    for char in key:
        if char not in current:
            return 0
        current = current[char]

    if 0 in current:
        return 2
    return 1
