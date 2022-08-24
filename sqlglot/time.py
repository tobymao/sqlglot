# the generic time format is based on python time.strftime
# https://docs.python.org/3/library/time.html#time.strftime
from sqlglot.trie import in_trie, new_trie


def format_time(string, mapping, trie=None):
    """
    Converts a time string given a mapping.

    Examples:
        >>> format_time("%Y", {"%Y": "YYYY"})
        'YYYY'

    mapping: Dictionary of time format to target time format
    trie: Optional trie, can be passed in for performance
    """
    start = 0
    end = 1
    size = len(string)
    trie = trie or new_trie(mapping)
    current = trie
    chunks = []
    sym = None

    while end <= size:
        chars = string[start:end]
        result, current = in_trie(current, chars[-1])

        if result == 0:
            if sym:
                end -= 1
                chars = sym
                sym = None
            start += len(chars)
            chunks.append(chars)
            current = trie
        elif result == 2:
            sym = chars

        end += 1

        if result and end > size:
            chunks.append(chars)

    return "".join(mapping.get(chars, chars) for chars in chunks)
