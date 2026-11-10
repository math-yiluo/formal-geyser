"""Text shortened for a bounded field keeps whole words and says it was shortened."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from formal_geyser.coordination.project_setup import SetupStart, _contract
from formal_geyser.coordination.text_bounds import ELLIPSIS, shorten

GOAL = ("Formalize the local Langlands correspondence for GL_n(F) in Lean 4, for all n ≥ 1 and p-adic fields F, "
        "following Henniart’s proof strategy. \n\nAlongside this, build a reusable library.")


def test_text_that_fits_is_unchanged():
    assert shorten("Prove the lemma.", 16) == "Prove the lemma."


def test_a_cut_ends_at_a_word_and_is_marked():
    first_line = GOAL.splitlines()[0]
    # The pilot's Task 0 title kept 120 characters: "... following Henniar".
    short = shorten(first_line, 120)
    assert len(short) <= 120 and short.endswith("following" + ELLIPSIS)
    for limit in range(10, len(first_line)):
        text = shorten(first_line, limit)
        assert len(text) <= limit and text.endswith(ELLIPSIS)
        kept = text[:-1]
        assert first_line.startswith(kept)
        # What is kept ends at the end of a word of the original.
        assert first_line[len(kept)] in " ,;:" or kept.endswith((" ", ",", ";", ":")) or len(kept) < limit * 0.6


def test_a_single_long_word_is_cut_but_still_marked():
    assert shorten("Supercalifragilistic", 8) == "Superca" + ELLIPSIS
    assert shorten("ab", 1) == ELLIPSIS
    with pytest.raises(ValueError):
        shorten("text", 0)


def test_task_0_is_titled_by_the_whole_first_line_of_the_goal():
    request = SetupStart(key="setup", goal=GOAL, model="model", effort="high")
    contract = _contract(SimpleNamespace(config=SimpleNamespace(domain="general")), request)
    assert contract.title == GOAL.splitlines()[0].strip()
    assert contract.title.endswith("following Henniart’s proof strategy.")
    # A longer first line is shortened at a word, within the title's room.
    long_goal = "Formalize " + "very " * 80 + "much."
    title = _contract(SimpleNamespace(config=SimpleNamespace(domain="general")),
                      SetupStart(key="setup", goal=long_goal, model="model", effort="high")).title
    assert len(title) <= 200 and title.endswith("very" + ELLIPSIS)
