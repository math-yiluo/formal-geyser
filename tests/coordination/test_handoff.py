"""A handoff file can start a new conversation only when it is complete and current."""
from __future__ import annotations

import pytest

from formal_geyser.coordination.runtime.handoff import (
    HANDOFF_MAX_CHARACTERS,
    HANDOFF_SECTIONS,
    check_handoff,
    handoff_path,
    handoff_sha256,
    handoff_template,
    referenced_paths,
)

TASK = "0f3c2a5e-1b7d-4c1e-9a55-6d1f0b2c3d4e"


def write(workspace, text):
    target = workspace / handoff_path(TASK)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def complete(**overrides):
    body = {"Goal": "Prove the valuation bound. Contract: `.private/material/task.md`.",
            "Current state": "Lemma `Valuation/Bound.lean` compiles; candidate not staged.",
            "Next step": "Add the last case, then run `lean_check` on the module again.",
            "Remaining plan": "Stage the pair, then answer review.", "Key decisions": "Use `Nat.find`.",
            "Dead ends": "Induction on the degree loops.", "Key facts": "Mathlib lemma `padicValNat.mul`.",
            "Open questions": "None."}
    body.update(overrides)
    return "# Handoff\n\n" + "".join(f"## {name}\n\n{text}\n\n" for name, text in body.items())


@pytest.fixture
def workspace(tmp_path):
    (tmp_path / ".private/material").mkdir(parents=True)
    (tmp_path / ".private/material/task.md").write_text("contract", encoding="utf-8")
    (tmp_path / "Valuation").mkdir()
    (tmp_path / "Valuation/Bound.lean").write_text("theorem bound : True := trivial", encoding="utf-8")
    return tmp_path


def test_a_complete_current_handoff_is_valid(workspace):
    write(workspace, complete())
    check = check_handoff(workspace, TASK)
    assert check.valid and check.problems == () and check.sha256 == handoff_sha256(workspace, TASK)
    assert check.record()["path"] == f".private/tasks/{TASK}/handoff.md"


def test_missing_unreadable_or_empty_required_sections_are_reported(workspace):
    assert check_handoff(workspace, TASK).problems == ("The handoff file does not exist",)
    write(workspace, complete(**{"Next step": "TODO", "Current state": "-"}).replace("## Goal", "## Aim"))
    problems = check_handoff(workspace, TASK).problems
    assert problems == ("Section 'Goal' is missing", "Section 'Current state' is empty",
                        "Section 'Next step' is empty")


def test_unchanged_since_the_reminder_is_not_an_update(workspace):
    write(workspace, complete())
    before = handoff_sha256(workspace, TASK)
    assert check_handoff(workspace, TASK, previous_sha256=before).problems == (
        "The handoff file was not updated after the context passed the limit",)
    write(workspace, complete(**{"Next step": "Stage the candidate."}))
    assert check_handoff(workspace, TASK, previous_sha256=before).valid


def test_a_wait_reads_as_words_in_the_state_file():
    from formal_geyser.coordination.runtime.state_packet import wait_text
    assert wait_text({"mode": "any", "requirements": [{"key": "a", "artifact_id": "a1", "policy": "general.peer@1"}],
                      "validations": ["c2"]}) == (
        "artifact `a1` accepted or the machine validation of `c2`")
    assert wait_text({"subscriptions": [{"task_id": "t1", "need": "a proof of S"}]}) == (
        "a change to task `t1` that meets: a proof of S")


def test_a_review_has_its_own_sections(workspace):
    template = handoff_template("Review of the bound", review=True)
    assert [line[3:] for line in template.splitlines() if line.startswith("## ")] == [
        "Goal", "Checked so far", "Findings", "Next step", "Open questions"]
    write(workspace, template.replace("## Checked so far\n\n", "## Checked so far\n\nThe statement.\n")
          .replace("## Next step\n\n", "## Next step\n\nCheck the proof.\n"))
    assert check_handoff(workspace, TASK, review=True).valid
    assert not check_handoff(workspace, TASK).valid  # A task's handoff needs Current state.


def test_files_not_in_the_workspace_are_noted_and_the_size_is_bounded(workspace):
    # Planned files and Mathlib or literature paths are fine to name: only noted.
    write(workspace, complete(**{"Key facts": "See `notes/gone.md`, [draft](drafts/old.lean) and `a/b/`."}))
    check = check_handoff(workspace, TASK)
    assert check.valid and check.notes == (
        "it names a file not in the workspace: notes/gone.md",
        "it names a file not in the workspace: a/b/",
        "it names a file not in the workspace: drafts/old.lean")
    write(workspace, complete(**{"Remaining plan": "x" * HANDOFF_MAX_CHARACTERS}))
    [problem] = check_handoff(workspace, TASK).problems
    assert problem.endswith(f"the limit is {HANDOFF_MAX_CHARACTERS}")


def test_only_path_like_spans_are_checked():
    text = ("Recheck with `lean_check Valuation.Bound`, use `Nat.succ_le_of_lt` and `Mathlib.Order.Basic`; "
            "edit `Valuation/Bound.lean` and see [the wiki](wiki/README.md#usage) or https://example.org/a/b.")
    assert referenced_paths(text) == ["Valuation/Bound.lean", "wiki/README.md"]


def test_the_template_has_every_section_and_rejects_bad_task_ids():
    template = handoff_template("Valuation bound", "tasks/x.md")
    assert [line[3:] for line in template.splitlines() if line.startswith("## ")] == list(HANDOFF_SECTIONS)
    assert "Contract: `tasks/x.md`." in template
    with pytest.raises(ValueError):
        handoff_path("../escape")
