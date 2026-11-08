"""Superseded published Library views and old shared DAG revisions are removed after their grace period; the
current view, the newest few and revision 0 stay."""
from __future__ import annotations

import json
import os
import time

from formal_geyser.coordination.content import VIEW_GRACE_SECONDS, VIEWS_KEPT, NativeContentStore
from formal_geyser.coordination.shared_dags import REVISION_DAYS, REVISIONS_KEPT, DagFiles

DAY = 86400


def view(store: NativeContentStore, number: int, installed: float) -> str:
    commit = f"{number:040x}"
    files = store.library_root / commit / "files"
    files.mkdir(parents=True)
    (files / "Basic.lean").write_text("-- view", encoding="utf-8")
    (files / "Basic.lean").chmod(0o444)  # Installed views are read-only.
    (store.library_root / commit / "manifest.json").write_text("{}", encoding="utf-8")
    os.utime(store.library_root / commit, (installed, installed))
    return commit


def test_superseded_views_go_after_their_grace_period(tmp_path):
    store = NativeContentStore(tmp_path / ".task-based")
    now = time.time()
    commits = [view(store, index, now - (10 - index) * DAY) for index in range(8)]
    # Views 8 to 10 are the newest three; view 8 superseded view 7 only two hours ago.
    commits += [view(store, 8, now - 7200), view(store, 9, now - 5400), view(store, 10, now - 3600)]
    (store.library_root / "current.json").write_text(json.dumps({"commit": commits[2]}), encoding="utf-8")
    assert VIEWS_KEPT == 3 and VIEW_GRACE_SECONDS == DAY
    removed = store.prune_public_views(now=now)
    left = {path.name for path in store.library_root.iterdir() if path.is_dir()}
    assert left == {commits[2], commits[7], commits[8], commits[9], commits[10]}  # Current, grace, newest three.
    assert sorted(removed) == sorted(set(commits) - left)
    assert not any(path.name.startswith(".native-content-") for path in store.library_root.iterdir())
    assert store.prune_public_views(now=now) == []


def test_a_project_without_a_current_view_keeps_everything(tmp_path):
    store = NativeContentStore(tmp_path / ".task-based")
    view(store, 1, time.time() - 30 * DAY)
    assert store.prune_public_views() == []


def test_old_dag_revisions_go_but_revision_zero_and_the_newest_stay(tmp_path):
    store = NativeContentStore(tmp_path / ".task-based")
    files = DagFiles(store)
    files.ensure()
    now = time.time()
    head = REVISIONS_KEPT + 100
    for revision in (0, 1, 2, 49, 50, 51, head - REVISIONS_KEPT + 1, head):
        directory = files.root / f"revision-{revision}-{revision:064x}"
        (directory / "files").mkdir(parents=True)
        (directory / "files" / "definitions.json").write_text("{}", encoding="utf-8")
        age = (REVISION_DAYS + 1) * DAY if revision != 51 else DAY  # Revision 51 is recent.
        os.utime(directory, (now - age, now - age))
    assert files.prune(head, now=now) == 4  # Revisions 1, 2, 49 and 50.
    left = sorted(int(path.name.split("-")[1]) for path in files.root.iterdir() if path.name.startswith("revision-"))
    assert left == [0, 51, head - REVISIONS_KEPT + 1, head]
