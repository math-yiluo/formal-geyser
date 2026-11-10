"""The coordination thread never waits for the content lock that archive moves and Library views hold."""
import json
import shutil
import stat
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from uuid import uuid4

import pytest

from formal_geyser.coordination import content as content_module
from formal_geyser.coordination.content import ContentError, NativeContentStore
from formal_geyser.coordination.repository import RepositoryStore
from formal_geyser.coordination.storage import BlobStore
from tests.coordination.publication_helpers import approve
from tests.coordination.test_archive_publication import archive_case as archive_case  # noqa: F401
from tests.coordination.test_archive_publication import quiet
from tests.coordination.test_native_content import snapshot
from tests.coordination.test_staging_preparation import submitted as submitted  # noqa: F401

needs_git = pytest.mark.skipif(shutil.which("git") is None, reason="Native Git is required")


def observe_lock(monkeypatch):
    entered = []
    original = NativeContentStore._locked

    @contextmanager
    def observed(self):
        entered.append(threading.current_thread().name)
        with original(self):
            yield
    monkeypatch.setattr(NativeContentStore, "_locked", observed)
    return entered


def test_task_reads_and_note_folders_do_not_wait_for_a_held_content_lock(tmp_path):
    content = NativeContentStore(tmp_path / ".task-based")
    task_id = uuid4().hex
    view = content.task_files(task_id, 1, {"title": "Locked", "objective": "Read without the lock.", "context": ""})
    held, release = threading.Event(), threading.Event()

    def holder():
        with content._locked():
            held.set()
            release.wait(30)
    thread = threading.Thread(target=holder)
    thread.start()
    assert held.wait(10)
    try:
        started = time.monotonic()
        assert content.read_task(task_id, 1, view.contract_sha256)["objective"] == "Read without the lock."
        content.worker_notes(uuid4().hex)
        assert time.monotonic() - started < 5
    finally:
        release.set()
        thread.join()


@needs_git
async def test_staging_on_the_coordination_thread_takes_no_content_lock(submitted, monkeypatch):
    entered = observe_lock(monkeypatch)
    await quiet(submitted.hub, submitted.author)
    assert not any(name.startswith("taskhub-db") for name in entered)


@needs_git
async def test_the_library_pointer_moves_off_the_coordination_thread(archive_case, monkeypatch):
    case = archive_case
    threads = []
    original = NativeContentStore.publish_current_view

    def observed(self, *args, **kwargs):
        threads.append(threading.current_thread().name)
        return original(self, *args, **kwargs)
    monkeypatch.setattr(NativeContentStore, "publish_current_view", observed)
    await approve(case.hub, case.reviewer)
    view = await case.hub.public_content()
    assert view.commit == case.hub._repository().head()
    assert threads and not any(name.startswith("taskhub-db") for name in threads)


def library(tmp_path):
    """A content store and a repository with one commit of a few Library files."""
    content = NativeContentStore(tmp_path / ".task-based")
    blobs = BlobStore(content.state_dir / "objects")
    repository = RepositoryStore(content.state_dir)
    files = {f"Math/F{index}.lean": f"def f{index} : Nat := {index}\n".encode() for index in range(8)}
    commit = repository.initialize(blobs, snapshot(blobs, files))["head"]
    return content, repository, blobs, commit


def observe_view_reads(monkeypatch):
    """One entry per full read of a Library view: whether the content lock was held."""
    reads, depth = [], threading.local()
    original_locked, original_verify = NativeContentStore._locked, content_module.verify_references

    @contextmanager
    def locked(self):
        with original_locked(self):
            depth.value = getattr(depth, "value", 0) + 1
            try:
                yield
            finally:
                depth.value -= 1

    def verify(root, references):
        reads.append(getattr(depth, "value", 0) > 0)
        return original_verify(root, references)
    monkeypatch.setattr(NativeContentStore, "_locked", locked)
    monkeypatch.setattr(content_module, "verify_references", verify)
    return reads


@needs_git
def test_a_verified_library_view_moves_the_pointer_without_being_read_again(tmp_path, monkeypatch):
    monkeypatch.setattr(content_module, "SETTLED_NANOSECONDS", 0)
    content, repository, blobs, commit = library(tmp_path)
    view = content.public_view(repository, blobs, commit)
    reads = observe_view_reads(monkeypatch)
    pointer = content.publish_current_view(view)
    assert json.loads(pointer.read_bytes())["commit"] == commit
    assert reads == []


@needs_git
def test_concurrent_pointer_moves_read_a_view_once_and_never_under_the_content_lock(tmp_path, monkeypatch):
    monkeypatch.setattr(content_module, "SETTLED_NANOSECONDS", 0)
    content, repository, blobs, commit = library(tmp_path)
    view = content.public_view(repository, blobs, commit)
    content._views.clear()  # As after a restart: the view was not verified in this process.
    reads = observe_view_reads(monkeypatch)
    with ThreadPoolExecutor(3) as pool:
        pointers = list(pool.map(lambda _: content.publish_current_view(view), range(3)))
    assert len(set(pointers)) == 1 and json.loads(pointers[0].read_bytes())["commit"] == commit
    assert reads == [False]


@needs_git
def test_an_existing_library_view_is_verified_without_the_content_lock(tmp_path, monkeypatch):
    content, repository, blobs, commit = library(tmp_path)
    content.public_view(repository, blobs, commit)
    content._views.clear()
    reads = observe_view_reads(monkeypatch)
    assert content.public_view(repository, blobs, commit).commit == commit
    assert reads == [False]


@needs_git
def test_a_library_view_changed_after_its_verification_never_becomes_current(tmp_path, monkeypatch):
    monkeypatch.setattr(content_module, "SETTLED_NANOSECONDS", 0)
    content, repository, blobs, commit = library(tmp_path)
    view = content.public_view(repository, blobs, commit)
    target = view.root / view.files[0].path
    target.chmod(stat.S_IREAD | stat.S_IWRITE)
    target.write_bytes(b"tampered")
    with pytest.raises(ContentError, match="integrity"):
        content.publish_current_view(view)
    assert not (content.library_root / "current.json").exists()


@needs_git
def test_a_just_written_small_view_is_read_once_when_its_pointer_moves(tmp_path, monkeypatch):
    content, repository, blobs, commit = library(tmp_path)
    view = content.public_view(repository, blobs, commit)
    reads = observe_view_reads(monkeypatch)
    content.publish_current_view(view)
    assert len(reads) == 1  # Under the lock while its metadata is unsettled, else before it.


@needs_git
def test_a_just_written_large_view_settles_and_its_pointer_move_reads_nothing(tmp_path, monkeypatch):
    monkeypatch.setattr(content_module, "SETTLE_WAIT_FILES", 1)
    monkeypatch.setattr(content_module, "SETTLED_NANOSECONDS", 200_000_000)
    content, repository, blobs, commit = library(tmp_path)
    view = content.public_view(repository, blobs, commit)  # Waits for the new view to settle.
    reads = observe_view_reads(monkeypatch)
    content.publish_current_view(view)
    assert reads == []
