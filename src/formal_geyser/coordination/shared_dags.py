"""Two shared planning DAGs, with one atomic revision and ordinary file bodies.

All active workers may update the pair, including reviewers and correspondence
activations. The graphs never grant mathematical acceptance or task authority.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO, cast
from uuid import uuid4

import portalocker

from .content import (
    ContentError,
    FileReference,
    NativeContentStore,
    _directory,
    _handle,
    _hash,
    _normal_path,
    _path,
    _plain,
    _read,
    _sha,
)
from .dag_check import GraphError, parse_graph, validate_dags
from .errors import HubError
from .graph_paths import GRAPH_DIRECTORY
from .models import Role
from .snapshot_limits import MAX_SNAPSHOT_BYTES
from .storage import canonical, digest

GRAPH_FILES = ("definitions.json", "proofs.json")
REVISIONS_KEPT = 200  # The newest shared DAG revisions always kept (DagFiles.prune).
REVISION_DAYS = 7  # Older revisions stay this long, as merge bases for updates read from them.
_REVISION = re.compile(r"revision-(\d+)-[0-9a-f]{64}")
CHECKER_PATH = ".private/taskhub-reference/check_dags.py"


class DagMergeConflict(GraphError):
    pass


def merge_dag_files(base: dict[str, bytes], current: dict[str, bytes], proposed: dict[str, bytes]) -> dict[str, bytes]:
    """Three-way merge disjoint nodes/edges; never choose a side on overlap."""
    merged = {}
    absent = object()
    for name in GRAPH_FILES:
        documents = [parse_graph(tree[name], name) for tree in (base, current, proposed)]
        result = {}
        for field in ("nodes", "edges"):
            def indexed(document):
                return {(item["id"] if field == "nodes" else (item["from"], item["to"])): item
                        for item in document[field]}
            before, now, ours = map(indexed, documents)
            combined = []
            for key in sorted(before.keys() | now.keys() | ours.keys()):
                b, c, n = (tree.get(key, absent) for tree in (before, now, ours))
                if n == b:
                    value = c
                elif c == b or c == n:
                    value = n
                else:
                    raise DagMergeConflict(f"Concurrent changes overlap at {name}:{field}:{key}")
                if value is not absent:
                    combined.append(value)
            result[field] = combined
        merged[name] = (canonical(result) + "\n").encode("utf8")
    # Disjoint changes can jointly introduce a cycle or delete an edge endpoint.
    validate_dags(*(parse_graph(merged[name], name) for name in GRAPH_FILES))
    return merged


@dataclass(frozen=True)
class PreparedDagUpdate:
    """A validated immutable candidate; never accepted from a worker payload."""

    reference: dict


class DagFiles:
    def __init__(self, content: NativeContentStore):
        self.content = content
        self.root = _path(content.project_root, GRAPH_DIRECTORY)

    def ensure(self) -> Path:
        return _directory(self.root)

    @staticmethod
    def capture(workspace: Path, source_dir: str) -> dict[str, bytes]:
        if (not isinstance(source_dir, str)
                or not (source_dir == ".private/dags" or source_dir.startswith(".private/dags/"))):
            raise ContentError("DAG source_dir must be workspace-relative .private/dags or a child directory")
        root = _path(workspace, source_dir, private=True)
        files = {}
        remaining = MAX_SNAPSHOT_BYTES
        for name in GRAPH_FILES:
            files[name] = _read(_path(root, name), max_bytes=remaining)
            remaining -= len(files[name])
        return files

    @staticmethod
    def validate(files: dict[str, bytes]) -> None:
        if set(files) != set(GRAPH_FILES):
            raise GraphError("Supply both definitions.json and proofs.json")
        if sum(map(len, files.values())) > MAX_SNAPSHOT_BYTES:
            raise GraphError("The shared DAG pair exceeds the native file byte envelope")
        validate_dags(*(parse_graph(files[name], name) for name in GRAPH_FILES))

    def reference(self, revision: int, content_key: str, manifest_sha256: str) -> dict:
        _hash(content_key)
        _hash(manifest_sha256)
        if type(revision) is not int or revision < 0:
            raise ContentError("Invalid shared DAG revision")
        directory = _path(self.root, f"revision-{revision}-{content_key}")
        return {"kind": "shared-dags", "status": "planning", "revision": revision,
                "content_key": content_key, "manifest_sha256": manifest_sha256,
                "root": str(directory / "files"), "manifest_path": str(directory / "manifest.json"),
                "definitions": str(directory / "files" / GRAPH_FILES[0]),
                "proofs": str(directory / "files" / GRAPH_FILES[1]),
                "current_path": str(self.root / "current.json")}

    def install(self, revision: int, files: dict[str, bytes], *, summary: str,
                worker_id: str | None = None, worker_name: str | None = None,
                task_id: str | None = None) -> dict:
        # Validate once, at the common storage boundary, before installing bytes.
        self.validate(files)
        metadata = {"kind": "shared-dags", "status": "planning", "revision": revision,
                    "author_id": worker_id, "author": worker_name, "task_id": task_id,
                    "summary": summary}
        key = digest({**metadata, "files": {name: _sha(body) for name, body in files.items()}})
        with self._installation_lock():
            view = self.content._tree(_path(self.root, f"revision-{revision}-{key}"), files, metadata)
        return self.reference(revision, key, view.manifest_sha256)

    def prune(self, head_revision: int, *, now: float | None = None) -> int:
        """Remove revisions no update can still merge from: below the REVISIONS_KEPT newest and older
        than REVISION_DAYS. Revision 0, the original pair, stays. An update made from a removed
        revision is refused as a revision conflict, to be read again and merged. In the Linux pilot
        2,630 revisions, each a full copy of the pair, held 8.7 GB; about 125 were made a day."""
        now = time.time() if now is None else now
        removed = 0
        with self._installation_lock():
            for directory in list(_normal_path(self.root).iterdir()):
                match = _REVISION.fullmatch(directory.name)
                if match is None or not directory.is_dir() or directory.is_symlink():
                    continue
                revision = int(match.group(1))
                if (revision == 0 or revision > head_revision - REVISIONS_KEPT
                        or now - directory.stat().st_mtime < REVISION_DAYS * 86400):
                    continue
                temporary = _plain(self.root / f".native-content-{uuid4().hex}")
                os.replace(directory, _normal_path(temporary))
                self.content._discard_temporary(temporary)
                removed += 1
        return removed

    @contextmanager
    def _installation_lock(self):
        # Graph installation never acquires the task-content mutex or lock.
        # Separate OS handles serialize both threads and other Hub processes.
        _directory(self.content.state_dir)
        lock = _path(self.content.state_dir, "graph-install.lock")
        with portalocker.Lock(str(_normal_path(lock)), mode="a+b", timeout=30) as stream:
            _handle(cast(BinaryIO, stream), lock)
            if os.fstat(stream.fileno()).st_size:
                raise ContentError("Shared DAG installation lock contains unexpected data")
            yield

    def _manifest(self, reference: dict) -> tuple[FileReference, ...]:
        body = _read(Path(reference["manifest_path"]), max_bytes=1048576)
        if _sha(body) != reference["manifest_sha256"]:
            raise ContentError("Shared DAG manifest differs from its recorded identity")
        manifest = json.loads(body)
        if (manifest.get("kind") != "shared-dags" or manifest.get("status") != "planning"
                or manifest.get("revision") != reference["revision"]):
            raise ContentError("Shared DAG manifest has a different identity")
        refs = tuple(FileReference.from_dict(item) for item in manifest["files"])
        if len(refs) != 2 or {item.path for item in refs} != set(GRAPH_FILES):
            raise ContentError("Shared DAG manifest must contain exactly two graph files")
        return refs

    def verify(self, reference: dict) -> None:
        """Inspect immutable bodies off the coordination thread."""
        reference = self.reference(reference["revision"], reference["content_key"],
                                   reference["manifest_sha256"])
        self.content._verify_tree(Path(reference["root"]), self._manifest(reference))

    def point(self, reference: dict) -> None:
        """Repair navigation under the ledger lock, without re-reading graph bodies.

        The caller serializes head selection and pointer replacement with SQLite.
        No global content lock is needed: candidate installation uses separate,
        immutable paths, and must not hold up this short coordination operation.
        """
        reference = self.reference(reference["revision"], reference["content_key"],
                                   reference["manifest_sha256"])
        self._manifest(reference)
        encoded = (canonical(reference) + "\n").encode("utf-8")
        pointer = self.root / "current.json"
        previous = _read(pointer, max_bytes=16384) if _normal_path(pointer).exists() else None
        if previous != encoded:
            self.content._replace_file(self.root, "current.json", encoded,
                                       _sha(previous) if previous is not None else None)


class SharedDags:
    if TYPE_CHECKING:
        content: Any
        db: Any
        _active: Any
        _role: Any
        _get: Any
        _event: Any
        workspace: Any
        _require_current_mutation: Any

    _dag_navigation_issue: str | None = None

    @property
    def dag_files(self) -> DagFiles:
        return DagFiles(self.content)

    def _dag_reference(self, c):
        row = c.execute("SELECT revision,content_key,manifest_sha256 FROM native_dag_head WHERE id=1").fetchone()
        return self.dag_files.reference(**dict(row)) if row is not None else None

    def _reconcile_dags(self, c):
        reference = self._dag_reference(c)
        try:
            if reference is None:
                empty = (canonical({"nodes": [], "edges": []}) + "\n").encode("utf-8")
                reference = self.dag_files.install(0, dict.fromkeys(GRAPH_FILES, empty),
                                                   summary="Initialize empty shared planning DAGs.")
                c.execute("INSERT INTO native_dag_head VALUES(1,?,?,?)", (
                    reference["revision"], reference["content_key"], reference["manifest_sha256"]))
            self.dag_files.point(reference)
            self._dag_navigation_issue = None
        except (OSError, ValueError, KeyError, TypeError) as exc:
            # Planning-file trouble must neither roll back an already saved head
            # nor disable task coordination. Never reset an existing head.
            self._dag_navigation_issue = f"{type(exc).__name__}: {exc}"[:2000]
        return reference

    async def reconcile_dags(self):
        # Read and point under the same cross-connection write transaction;
        # replaying an old receipt must never rewind the shared current pointer.
        return await self.db.call(self._reconcile_dags, write=True)

    async def dag_status(self):
        """Return authoritative paths and isolate graph integrity failures."""
        reference = await self.reconcile_dags()
        navigation_issue = self._dag_navigation_issue
        result = {"graphs": reference, "available": False,
                  "navigation": "pending" if navigation_issue else "current"}
        if navigation_issue:
            result["navigation_error"] = navigation_issue
        if reference is None:
            result["error"] = "Shared DAG initialization is unavailable; preserve existing files and repair storage."
            return result
        try:
            await asyncio.to_thread(self.dag_files.verify, reference)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            result["error"] = f"{type(exc).__name__}: {exc}"[:2000]
            result["recovery"] = ("Preserve the damaged revision. Restore exact files from a backup, "
                                  "or explicitly submit a checked replacement pair at the recorded revision.")
        else:
            result["available"] = True
        return result

    def _dag_update_base(self, c, p, actor, *, allow_stale=False):
        self._require_current_mutation(c, "dag.update", p, actor)
        self._role(actor, Role.WORKER)
        _, task = self._active(c, actor)
        worker = self._get(c, "workers", actor.worker_id)
        expected, summary = p.get("expected_revision"), p.get("summary")
        if type(expected) is not int or expected < 0:
            raise HubError("INVALID_INPUT", "Supply the current shared DAG revision", 422)
        if not isinstance(summary, str) or not summary.strip() or len(summary) > 4000:
            raise HubError("INVALID_INPUT", "Supply a concrete DAG update summary of 1 to 4000 characters", 422)
        head = self._dag_reference(c)
        if head is None or expected > head["revision"] or (not allow_stale and expected != head["revision"]):
            raise HubError("DAG_REVISION_CONFLICT", "Read current_graph_ref (or dag_status), merge your edits into that pair, and rerun the checker", 409,
                           details={"current_revision": head["revision"] if head else None,
                                    "current_graph_ref": head})
        return worker, task

    async def _prepare_dag_update(self, command, actor, principal, request_hash):
        def preflight(c):
            prior = c.execute("SELECT request_hash FROM receipts WHERE principal=? AND command=? AND key=?",
                              (principal, command.name, command.key)).fetchone()
            if prior:
                if prior["request_hash"] != request_hash:
                    raise HubError("IDEMPOTENCY_CONFLICT", "Idempotency key was reused with different data")
                return None  # Replay must not touch editable files or require live execution.
            worker, task = self._dag_update_base(c, command.payload, actor, allow_stale=True)
            head = self._dag_reference(c)
            expected = command.payload["expected_revision"]
            base = head
            if expected != head["revision"]:
                event = c.execute("SELECT payload_json FROM events WHERE kind='dags.updated' "
                                  "AND json_extract(payload_json,'$.graphs.revision')=? ORDER BY sequence DESC LIMIT 1",
                                  (expected,)).fetchone()
                base = json.loads(event[0])["graphs"] if event else None
            return worker, task, head, base

        base = await self.db.call(preflight)
        if base is None:
            return None
        worker, task, head, base_reference = base
        p = command.payload

        def prepare():
            files = self.dag_files.capture(self.workspace(actor.worker_id), p["source_dir"])
            self.dag_files.validate(files)
            if p["expected_revision"] != head["revision"]:
                old = base_reference
                if old is None and p["expected_revision"] == 0:
                    candidates = list(self.dag_files.root.glob("revision-0-*/manifest.json"))
                    if len(candidates) == 1:
                        manifest = json.loads(_read(candidates[0]))
                        old = self.dag_files.reference(0, candidates[0].parent.name.removeprefix("revision-0-"),
                                                      _sha(_read(candidates[0]))) if manifest.get("author_id") is None else None
                if old is None:
                    raise DagMergeConflict("The original committed DAG revision is unavailable")
                self.dag_files.verify(old)
                self.dag_files.verify(head)
                base_files = {name: _read(Path(old["root"]) / name) for name in GRAPH_FILES}
                current_files = {name: _read(Path(head["root"]) / name) for name in GRAPH_FILES}
                files = merge_dag_files(base_files, current_files, files)
            reference = self.dag_files.install(head["revision"] + 1, files, summary=p["summary"],
                worker_id=worker["id"], worker_name=worker["display_name"], task_id=task["id"])
            try:
                self.dag_files.prune(reference["revision"])
            except (OSError, ValueError):  # ContentError is a ValueError; the next update retries.
                pass
            return PreparedDagUpdate(reference)

        try:
            return await asyncio.to_thread(prepare)
        except DagMergeConflict as exc:
            raise HubError("DAG_REVISION_CONFLICT", str(exc), 409,
                           details={"current_revision": head["revision"], "current_graph_ref": head}) from exc
        except GraphError as exc:
            if p["expected_revision"] != head["revision"]:
                # The defect may be in the merge, not in either side: say which revision joined.
                raise HubError("INVALID_DAG", f"Merged with revision {head['revision']}, another update made "
                               f"since revision {p['expected_revision']}: {exc}", 422,
                               details={"current_revision": head["revision"], "current_graph_ref": head}) from exc
            raise HubError("INVALID_DAG", str(exc), 422) from exc

    def _commit_dag_update(self, c, p, actor, prepared: PreparedDagUpdate | None):
        effective = {**p, "expected_revision": prepared.reference["revision"] - 1} if prepared else p
        worker, task = self._dag_update_base(c, effective, actor)
        if prepared is None:
            raise HubError("INVALID_DAG", "The shared graph pair has not been prepared", 422)
        reference = prepared.reference
        c.execute("UPDATE native_dag_head SET revision=?,content_key=?,manifest_sha256=? WHERE id=1", (
            reference["revision"], reference["content_key"], reference["manifest_sha256"]))
        self._event(c, "dags.updated", "shared-dags", {"graphs": reference, "summary": p["summary"]},
                    task["id"], worker["id"])
        return reference


def prepare_dag_checker(workspace: Path) -> str:
    """Provide the exact backend validator as a standalone worker script."""
    from . import dag_check
    from .runtime.files import ScopedFiles

    text = Path(dag_check.__file__).read_text(encoding="utf-8")
    files = ScopedFiles(workspace)
    target = files.path(CHECKER_PATH, must_exist=False)
    if not _normal_path(target).is_file() or _read(target) != text.encode("utf-8"):
        files.write(CHECKER_PATH, text)
    return CHECKER_PATH
