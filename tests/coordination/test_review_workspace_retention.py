"""Retention never deletes unrecorded work or its immutable evidence."""
import json

from tests.coordination.test_repository import contents, freeze, manifest, workspace
from tests.coordination.test_repository import store as store


def receipt(repository, location):
    return json.loads(repository._session_path(location)[1].read_text(encoding="utf-8"))


def test_old_finalized_checkout_can_be_reclaimed_without_losing_snapshot(store, tmp_path):
    repository, blobs = store
    base = repository.initialize(blobs)["head"]
    final = freeze(store, tmp_path, "finalized", {"Example.lean": b"def x := 1", "Example.md": b"One."}, base)
    location = workspace(tmp_path, "finalized")
    result = repository.prune_integration_workspace(location, blobs)
    assert result["state"] == "reclaimed" and result["bytes"] > 0
    assert not location.exists()
    assert blobs.get_json(final["snapshot"])["files"]
    assert repository.snapshot(final["commit"], blobs) == final["snapshot"]
    assert receipt(repository, location)["retention"]["state"] == "reclaimed"
    assert repository.prune_integration_workspace(location, blobs)["state"] == "already_absent"


def test_an_unfrozen_checkout_is_recorded_exactly_then_reclaimed(store, tmp_path):
    repository, blobs = store
    base = repository.initialize(blobs)["head"]
    location = workspace(tmp_path, "unfinished")
    repository.prepare(blobs, manifest(blobs, {"Work.md": b"first"}), base, location, ["Work.md"])
    (location / "Work.md").write_text("the Integrator's edit", encoding="utf-8")
    result = repository.prune_integration_workspace(location, blobs)
    assert result["state"] == "reclaimed_unfrozen" and result["phase"] == "prepared"
    assert not location.exists()
    # Its working files exist only in the recorded snapshot now, byte for byte.
    assert receipt(repository, location)["retention"]["snapshot"] == result["snapshot"]
    assert contents(blobs, result["snapshot"])["Work.md"] == b"the Integrator's edit"


def test_a_checkout_changed_after_freezing_keeps_its_changes_in_the_record(store, tmp_path):
    repository, blobs = store
    base = repository.initialize(blobs)["head"]
    final = freeze(store, tmp_path, "changed", {"Work.md": b"first"}, base)
    location = workspace(tmp_path, "changed")
    (location / "Work.md").write_text("unsaved follow-up", encoding="utf-8")
    result = repository.prune_integration_workspace(location, blobs)
    assert result["state"] == "reclaimed_modified" and result["commit"] == final["commit"]
    assert result["snapshot"] != final["snapshot"]
    assert contents(blobs, result["snapshot"])["Work.md"] == b"unsaved follow-up"
    assert contents(blobs, final["snapshot"])["Work.md"] == b"first"


def test_a_checkout_left_part_way_through_preparation_is_reclaimed_too(store, tmp_path):
    repository, blobs = store
    base = repository.initialize(blobs)["head"]
    location = workspace(tmp_path, "interrupted")
    repository.prepare(blobs, manifest(blobs, {"Work.md": b"first"}), base, location, ["Work.md"])
    metadata = repository._session_path(location)[1]
    session = json.loads(metadata.read_text(encoding="utf-8"))
    metadata.write_text(json.dumps({**session, "phase": "merging"}), encoding="utf-8")
    result = repository.prune_integration_workspace(location, blobs)
    assert result["state"] == "reclaimed_unfrozen" and result["phase"] == "merging"
    assert not location.exists() and contents(blobs, result["snapshot"])["Work.md"] == b"first"


def test_private_extra_files_are_never_discarded(store, tmp_path):
    repository, blobs = store
    base = repository.initialize(blobs)["head"]
    freeze(store, tmp_path, "private", {"Work.md": b"first"}, base)
    location = workspace(tmp_path, "private")
    (location / ".private").mkdir()
    (location / ".private" / "scratch.md").write_text("Keep this.", encoding="utf-8")
    assert repository.prune_integration_workspace(location, blobs)["state"] == "retained_extra_files"
    assert location.exists()
    unfrozen = workspace(tmp_path, "private-unfrozen")
    repository.prepare(blobs, manifest(blobs, {"Work.md": b"first"}), base, unfrozen, ["Work.md"])
    (unfrozen / "drafts").mkdir()
    (unfrozen / "drafts" / "plan.md").write_text("Keep this too.", encoding="utf-8")
    assert repository.prune_integration_workspace(unfrozen, blobs)["state"] == "retained_extra_files"
    assert (unfrozen / "drafts" / "plan.md").is_file()
