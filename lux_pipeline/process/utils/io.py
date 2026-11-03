"""Small file helpers shared by components that read line-oriented files."""

import os

COUNT_LINES_BELOW = 100_000_000


def count_lines(fh, limit=COUNT_LINES_BELOW):
    """Count the lines in an open file and seek back to the start.

    Returns -1 when the file cannot be seeked (a stream or a compressed
    wrapper) or is at least ``limit`` bytes, in which case counting would
    cost as much as reading it. A caller then reports progress without a
    total.
    """
    try:
        length = fh.seek(0, os.SEEK_END)
        fh.seek(0)
    except (OSError, AttributeError, ValueError):
        return -1
    if length >= limit:
        return -1
    lines = sum(1 for _ in fh)
    fh.seek(0)
    return lines
