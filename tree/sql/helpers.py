"""Pure-Python ports of the ``bytea`` path byte helpers.

These mirror the PL/pgSQL functions in :mod:`tree.sql.postgresql` (``tree_mid``,
``tree_int_to_seg``, ``tree_level``, ``tree_upper``, ``tree_parent_prefix``) so
that backends without a database-side trigger can compute paths in Python, and
so the ORM lookups can precompute these helpers on constant operands instead of
calling a database function. A path is ``<segment> 0x00`` per depth level, where
``0x00`` is the reserved delimiter (never inside a segment) and segment bytes run
``0x01..0xFF``. See :mod:`tree.sql.postgresql` for the full rationale.
"""

# The level delimiter separating path segments.
DELIMITER = b'\x00'


def tree_mid(a: bytes | None, b: bytes | None) -> bytes:
    """Order-preserving "between" segment (fractional indexing).

    Returns a segment (no trailing ``0x00``) strictly between the lower
    neighbour ``a`` (``None`` => -infinity) and the upper ``b`` (``None`` =>
    +infinity). Segment bytes are base-256 fraction digits valued ``byte - 1``;
    past the end of ``a`` we read the low filler ``0x01``, past the end of ``b``
    the virtual value ``256``. Never emits ``0x00`` and always ends on a byte
    ``>= 0x02``, so every gap stays splittable forever.
    """
    result = bytearray()
    i = 0
    while True:
        x = a[i] if a is not None and i < len(a) else 1
        y = b[i] if b is not None and i < len(b) else 256
        if y - x >= 2:
            result.append(x + (y - x) // 2)
            return bytes(result)
        result.append(x)
        i += 1


def tree_int_to_seg(rank: int, width: int) -> bytes:
    """Fixed-width big-endian base-254 encoding of a rebuild ``rank``.

    Digits are mapped to bytes ``0x02..0xFF`` (left-padded with ``0x02``), so a
    rebuilt segment never emits ``0x00`` or ``0x01`` -- leaving ``0x01``-prefixed
    room below every rebuilt sibling for ``tree_mid`` to still insert before it.
    """
    result = bytearray()
    r = rank
    for _ in range(width):
        result.insert(0, r % 254 + 2)
        r //= 254
    return bytes(result)


def tree_level(p: bytes | None) -> int | None:
    """Depth = number of ``0x00`` level delimiters (``None`` for ``None``)."""
    if p is None:
        return None
    return p.count(0)


def tree_upper(p: bytes | None) -> bytes | None:
    """Exclusive upper bound of ``p``'s descendant range.

    Drops ``p``'s trailing ``0x00`` and appends ``0x01`` (sorts above the
    delimiter but below every segment byte). ``None`` for the empty prefix (the
    virtual root), meaning the range is unbounded above.
    """
    if not p:
        return None
    return p[:-1] + b'\x01'


def tree_parent_prefix(p: bytes | None) -> bytes:
    """Parent path of ``p`` (everything up to and including the previous
    ``0x00``), or ``b''`` for a root."""
    if not p:
        return b''
    segments = p.split(DELIMITER)[:-1]
    return b''.join(segment + DELIMITER for segment in segments[:-1])


def seg_width(child_count: int) -> int:
    """Minimal base-254 segment width holding ``child_count`` ranks.

    Matches the ``CASE`` in the PL/pgSQL rebuild so Python and PostgreSQL rebuilds
    produce identical paths.
    """
    if child_count <= 254:
        return 1
    if child_count <= 64516:
        return 2
    if child_count <= 16387064:
        return 3
    return 4
