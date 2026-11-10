"""TaskHub's use of glibc's allocator: fewer arenas, freed memory given back."""
from __future__ import annotations

import sys

import pytest

from formal_geyser import allocator


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="glibc's allocator")
def test_the_allocator_takes_an_arena_limit_and_gives_freed_memory_back():
    assert allocator.limit_arenas(8) is True  # A generous limit, so other tests' threads are unaffected.
    held = [bytearray(64 * 2**10) for _ in range(2000)]  # 125 MiB in many arena chunks.
    before = allocator.private_memory()
    del held
    allocator.trim()
    after = allocator.private_memory()
    assert before is not None and after is not None and after < before


def test_elsewhere_it_does_nothing(monkeypatch):
    monkeypatch.setattr(allocator.sys, "platform", "win32")
    assert allocator.limit_arenas() is False and allocator.trim() is False
