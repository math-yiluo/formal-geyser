"""Shorten text for a bounded field without cutting a word in half.

A plain slice cut Task 0's title in the Linux pilot at "following Henniar", for "Henniart's":
project setup kept the first 120 characters of the goal's first line, mid-word and with no sign
that anything was left out. Text that has to fit a limit is shortened here instead.
"""
from __future__ import annotations

ELLIPSIS = "…"
# A word boundary is used only if it keeps at least this share of the room; a single word
# longer than that is cut, still marked.
_KEEP = 0.6


def shorten(text: str, limit: int) -> str:
    """`text` if it has at most `limit` characters; else its longest beginning that ends at a
    word boundary and fits with an ellipsis after it."""
    if limit < 1:
        raise ValueError("A text limit is at least one character")
    if len(text) <= limit:
        return text
    room = limit - len(ELLIPSIS)
    head = text[:room]
    if not text[room].isspace() and not head[-1:].isspace():
        # The cut falls inside a word: end at the last word boundary before it.
        boundary = max(head.rfind(" "), head.rfind("\n"), head.rfind("\t"))
        if boundary >= room * _KEEP:
            head = head[:boundary]
    return head.rstrip(" \t\n,;:") + ELLIPSIS
