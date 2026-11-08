"""Local Git mechanics for a reviewer's isolated integration workspace.

The public ref changes only in ``publish``. A freeze is a deterministic immutable
commit, so callers can journal it, validate it, and retry publication after a crash.
These operations perform no reasoning, remote transport, or domain validation.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import tempfile
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import portalocker

from .content import unsafe_characters, unsafe_component
from .draft_paths import is_draft_path
from .graph_paths import is_graph_path
from .snapshot_limits import MAX_SNAPSHOT_BYTES, MAX_SNAPSHOT_FILES, snapshot_bounds
from .storage import (
    SKIPPED_DIRECTORIES,
    BlobStore,
    atomic_write,
    canonical,
    on_coordination_thread,
    trusted_directory,
)

# What a blob snapshot leaves out of a checkout besides its own .git. A workspace holding any
# of these has work its snapshot lacks.
_UNSNAPSHOTTED = (SKIPPED_DIRECTORIES - {".git"}) | {"drafts", "graphs"}
_OID = re.compile(r"[0-9a-f]{40}\Z")
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_ZERO = "0" * 40
_MAX_FILES = MAX_SNAPSHOT_FILES
_MAX_BYTES = MAX_SNAPSHOT_BYTES
_MAX_OUTPUT = _MAX_BYTES + 32 * 1024 * 1024
_MAX_METADATA = 16 * 1024 * 1024
# The full ownership check walks the whole bare repository. A head read repeats it when an
# owned control file or folder changes, and at least once a minute off the coordination thread.
_OWNERSHIP_SECONDS = 60.0
# How long an operation waits for another one's repository lock before it fails.
LOCK_SECONDS = 30.0
_OWNERSHIP: dict[str, tuple[tuple, float]] = {}
_OWNERSHIP_LOCK = threading.Lock()
# Publications whose ownership, frozen receipt, commit parent and tree were checked before the
# transaction that publishes them, with the checked tree.
_VERIFIED_PUBLICATIONS: dict[tuple[str, str, str], str] = {}
# A separator alone is also a legitimate Markdown setext heading. Git's opener,
# closer, and optional diff3 ancestor marker identify unresolved conflict text.
_MARKERS = re.compile(rb"(?m)^(?:<{7,}|>{7,}|\|{7,})(?:[ \t].*)?\r?$")
_RESERVED = {".git", ".gitconfig", ".gitmodules", ".gitattributes", ".taskhub-integration", ".private"}


class RepositoryError(ValueError):
    """Unsafe repository input or a failed bounded local Git operation."""


class HeadMoved(RepositoryError):
    """The public branch no longer has the expected parent."""


class UnresolvedConflicts(RepositoryError):
    """Unmerged index entries or conflict markers still require review."""


class RepositoryBusy(TimeoutError):
    """Another operation kept the repository locked for the whole wait; a retry may succeed."""


def _safe_path(path: Path) -> Path:
    path = Path(os.path.abspath(path))
    for item in (path, *path.parents):
        if trusted_directory(item):
            break
        if item.is_symlink():
            raise RepositoryError(f"Linked repository paths are forbidden: {item}")
    return path


@contextmanager
def _file_lock(path: Path, holder: str) -> Iterator[None]:
    lock = portalocker.Lock(str(_safe_path(path)), timeout=LOCK_SECONDS)
    try:
        lock.acquire()
    except portalocker.LockException as error:
        raise RepositoryBusy(f"{holder} stayed locked by another operation for {LOCK_SECONDS:g} seconds") from error
    try:
        yield
    finally:
        lock.release()


def _reject_linked_tree(directory: Path) -> None:
    """Walk an owned directory, rejecting any link; entry types come from the listing."""
    pending = [os.fspath(directory)]
    while pending:
        with os.scandir(pending.pop()) as entries:
            for entry in entries:
                if entry.is_symlink():
                    raise RepositoryError(f"Linked repository paths are forbidden: {entry.path}")
                if entry.is_dir(follow_symlinks=False):
                    pending.append(entry.path)


# Snapshots of immutable commits, keyed by repository, blob store, and commit ID.
_SNAPSHOTS: dict[tuple[str, str, str], str] = {}


def _relative(value: str) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 1024:
        raise RepositoryError("Invalid repository path")
    if unsafe_characters(value):
        raise RepositoryError(f"Unsafe repository path: {value!r}")
    if is_draft_path(value):
        raise RepositoryError("Shared drafts cannot enter the formal source repository")
    if is_graph_path(value):
        raise RepositoryError("Shared graphs cannot enter the formal source repository")
    if any(unsafe_component(part, _RESERVED) for part in value.split("/")):
        raise RepositoryError(f"Unsafe repository path: {value!r}")
    return value


def _paths(values: list[str], *, max_files: int = _MAX_FILES) -> list[str]:
    if not isinstance(values, list) or len(values) > max_files:
        raise RepositoryError("Repository paths must be a bounded list")
    names: dict[str, str] = {}
    files: set[str] = set()
    for value in values:
        value = _relative(value)
        if value in files:
            raise RepositoryError(f"Duplicate repository path: {value}")
        files.add(value)
        pieces = value.split("/")
        for number in range(1, len(pieces) + 1):
            prefix = "/".join(pieces[:number])
            key = prefix.upper().casefold()
            if key in names and names[key] != prefix:
                raise RepositoryError("Portable names forbid case collisions")
            names[key] = prefix
    for value in files:
        if any("/".join(value.split("/")[:i]) in files
               for i in range(1, len(value.split("/")))):
            raise RepositoryError("A repository path cannot be both a file and a directory")
    return sorted(files)


class RepositoryStore:
    """An owned bare repository with a private worktree for each review session."""

    def __init__(self, state_dir: Path, branch: str = "main", executable: str = "git", *,
                 max_files: int = _MAX_FILES, max_bytes: int = _MAX_BYTES):
        self.max_files, self.max_bytes = snapshot_bounds(max_files, max_bytes)
        self.max_metadata_bytes = max(_MAX_METADATA, self.max_files * 1200 + 4096)
        self.max_output_bytes = self.max_bytes + max(32 * 1024 * 1024, self.max_metadata_bytes)
        if (not isinstance(branch, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]*", branch)
                or ".." in branch or "//" in branch or branch.endswith(("/", ".", ".lock"))
                or any(part.startswith(".") or part.endswith(".lock") for part in branch.split("/"))):
            raise RepositoryError("Invalid public branch name")
        resolved = shutil.which(executable)
        if resolved is None:
            raise RepositoryError("A native Git executable is required")
        self.executable = str(Path(resolved).absolute())
        self.state_dir = _safe_path(state_dir)
        self.path = self.state_dir / "repository.git"
        self.branch = branch
        self.ref = f"refs/heads/{branch}"
        self.control = self.state_dir / "repository-control"
        self._mutex = threading.RLock()

    @contextmanager
    def _locked(self) -> Iterator[None]:
        # The repository lock orders writes to the shared bare repository, its branch and its
        # control files. Each store has its own mutex, and the Hub makes a store per call: its
        # operations, like other processes', wait for each other at the file lock.
        with self._mutex:
            _safe_path(self.state_dir).mkdir(parents=True, exist_ok=True)
            _safe_path(self.control).mkdir(exist_ok=True)
            with _file_lock(self.control / "lock", "The Library repository"):
                yield

    @contextmanager
    def _session_locked(self, metadata: Path) -> Iterator[None]:
        # One integration checkout and its session record. Preparing, staging, freezing and
        # pruning it read and write that checkout for many seconds; they wait only for each
        # other there, and take the repository lock just for their writes to the bare
        # repository, so worker checkouts and publication do not wait for them.
        _safe_path(metadata.parent).mkdir(parents=True, exist_ok=True)
        with _file_lock(metadata.with_suffix(".lock"), "The integration checkout"):
            yield

    @staticmethod
    def _config(bare: bool) -> bytes:
        return ("[core]\n\trepositoryformatversion = 0\n\tfilemode = false\n"
                f"\tbare = {str(bare).lower()}\n\tlogallrefupdates = false\n"
                "\tsymlinks = false\n\tignorecase = false\n\tautocrlf = false\n").encode("ascii")

    @staticmethod
    def _write_json(path: Path, value: dict[str, Any]) -> None:
        _safe_path(path)
        atomic_write(path, canonical(value).encode("utf-8"), prefix=".repository-")

    def _read_json(self, path: Path) -> dict[str, Any]:
        try:
            raw = _safe_path(path).read_bytes()
            if len(raw) > self.max_metadata_bytes:
                raise RepositoryError("Oversized repository session metadata")
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise RepositoryError("Invalid repository session metadata")
            return value
        except (OSError, json.JSONDecodeError) as error:
            raise RepositoryError("Missing or invalid owned repository metadata") from error

    def _environment(self, index: Path | None = None) -> dict[str, str]:
        environment = {key: value for key, value in os.environ.items() if key in {"PATH", "TMPDIR"}}
        environment.update({
            "HOME": str(self.control),
            "XDG_CONFIG_HOME": str(self.control), "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_SYSTEM": os.devnull, "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_COUNT": "0", "GIT_TERMINAL_PROMPT": "0",
            "GIT_NO_REPLACE_OBJECTS": "1", "GIT_ATTR_NOSYSTEM": "1",
            "GIT_OPTIONAL_LOCKS": "0", "GCM_INTERACTIVE": "Never",
            "GIT_AUTHOR_NAME": "TaskHub", "GIT_AUTHOR_EMAIL": "taskhub@localhost",
            "GIT_COMMITTER_NAME": "TaskHub", "GIT_COMMITTER_EMAIL": "taskhub@localhost",
            "GIT_AUTHOR_DATE": "946684800 +0000", "GIT_COMMITTER_DATE": "946684800 +0000",
            "LC_ALL": "C", "LANG": "C",
        })
        if index is not None:
            environment["GIT_INDEX_FILE"] = str(index)
        return environment

    def _git(self, args: list[str], *, git_dir: Path | None = None,
             workspace: Path | None = None, body: bytes = b"", index: Path | None = None,
             allowed: tuple[int, ...] = (0,)) -> bytes:
        execution_root = workspace or (git_dir.parent if git_dir is not None else self.state_dir)
        command = [self.executable, "--no-pager", "--literal-pathspecs"]
        for value in (
            f"core.hooksPath={self.control / 'empty-hooks'}",
            f"core.attributesFile={self.control / 'empty-attributes'}",
            "core.autocrlf=false", "core.safecrlf=false", "core.symlinks=false",
            "core.fsmonitor=false", "core.untrackedCache=false",
            "core.checkRoundtripEncoding=", "core.quotePath=false", "core.pager=cat",
            "protocol.allow=never", "protocol.file.allow=never", "credential.helper=",
            "commit.gpgSign=false", "tag.gpgSign=false", "maintenance.auto=false",
            "gc.auto=0", "merge.renames=false", "merge.renormalize=false",
            "submodule.recurse=false", "fetch.recurseSubmodules=false",
        ):
            command.extend(["-c", value])
        if git_dir is not None:
            command.extend(["--git-dir", str(git_dir)])
        if workspace is not None:
            command.extend(["--work-tree", "."])
        command.extend(args)
        with tempfile.TemporaryFile() as incoming, tempfile.TemporaryFile() as outgoing, \
                tempfile.TemporaryFile() as errors:
            incoming.write(body)
            incoming.seek(0)
            try:
                process = subprocess.Popen(
                    command, cwd=execution_root, env=self._environment(index),
                    stdin=incoming, stdout=outgoing, stderr=errors,
                )
            except OSError as error:
                raise RepositoryError("Unable to start the native Git executable") from error
            deadline = time.monotonic() + 60
            failure: str | None = None
            while True:
                try:
                    # Returns as soon as Git exits; limits are checked between waits.
                    process.wait(timeout=0.05)
                    break
                except subprocess.TimeoutExpired:
                    pass
                if time.monotonic() > deadline:
                    failure = "Local Git operation timed out"
                if (os.fstat(outgoing.fileno()).st_size > self.max_output_bytes
                        or os.fstat(errors.fileno()).st_size > 1024 * 1024):
                    failure = "Local Git operation exceeded the output limit"
                if failure:
                    process.kill()
                    process.wait(timeout=5)
                    raise RepositoryError(failure)
            outgoing.seek(0)
            errors.seek(0)
            output_size = os.fstat(outgoing.fileno()).st_size
            if output_size > self.max_output_bytes:
                raise RepositoryError("Local Git operation exceeded the output limit")
            output = outgoing.read(output_size + 1)
            diagnostic = errors.read(1024 * 1024 + 1)
            if len(output) > self.max_output_bytes or len(diagnostic) > 1024 * 1024:
                raise RepositoryError("Local Git operation exceeded the output limit")
            if process.returncode not in allowed:
                detail = diagnostic.decode("utf-8", errors="replace")[:4000]
                raise RepositoryError(f"Git {args[0]} failed ({process.returncode}): {detail}")
            return output

    def _validate_git_dir(self, directory: Path, *, bare: bool) -> None:
        directory = _safe_path(directory)
        if not _safe_path(directory).is_dir():
            raise RepositoryError("Missing owned Git directory")
        if _safe_path(directory / "config").read_bytes() != self._config(bare):
            raise RepositoryError("Owned Git configuration was modified")
        _reject_linked_tree(directory)
        for name in ("config.worktree", "commondir", "gitdir", "shallow", "info/grafts",
                     "info/attributes", "objects/info/http-alternates"):
            if (directory / name).exists():
                raise RepositoryError(f"Unsafe Git configuration material: {name}")
        hooks = directory / "hooks"
        if hooks.exists() and any(hooks.iterdir()):
            raise RepositoryError("Git hooks are forbidden in managed repositories")
        alternates = directory / "objects" / "info" / "alternates"
        if bare:
            if alternates.exists():
                raise RepositoryError("The public object database cannot use alternates")
        elif not alternates.is_file() or alternates.read_bytes() != self._alternate():
            raise RepositoryError("The integration object database was redirected")

    def _alternate(self) -> bytes:
        return (str(self.path / "objects").replace("\\", "/") + "\n").encode("utf-8")

    def _owned(self) -> None:
        marker = self._read_json(self.control / "owner.json")
        if marker != {"format": 1, "repository": str(self.path), "branch": self.branch}:
            raise RepositoryError("Repository ownership or branch does not match")
        self._validate_git_dir(self.path, bare=True)
        if (self.path / "HEAD").read_bytes() != f"ref: {self.ref}\n".encode("ascii"):
            raise RepositoryError("The public repository HEAD was redirected")
        hooks = _safe_path(self.control / "empty-hooks")
        attributes = _safe_path(self.control / "empty-attributes")
        if not hooks.is_dir() or any(hooks.iterdir()) or attributes.read_bytes() != b"":
            raise RepositoryError("Managed Git safety configuration was modified")
        # Any operation's check also serves later head reads and reused snapshots, which then
        # need neither the lock nor another walk while the control files stay unchanged.
        self._remember_ownership()

    @staticmethod
    def _oid(value: str) -> str:
        if not isinstance(value, str) or not _OID.fullmatch(value):
            raise RepositoryError("A complete Git commit or object ID is required")
        return value

    def _head(self) -> str:
        # Git updates a loose ref by atomic rename; read it without a process.
        try:
            value = (self.path.joinpath(*self.ref.split("/"))).read_bytes()
        except FileNotFoundError:
            value = b""
        if len(value) == 41 and value.endswith(b"\n") and _OID.fullmatch(value[:40].decode("latin-1")):
            return value[:40].decode("ascii")
        return self._oid(self._git(["rev-parse", "--verify", self.ref],
                                   git_dir=self.path).decode("ascii").strip())

    def _ownership_identity(self) -> tuple | None:
        result = []
        # objects/info holds alternates; new object folders do not change it.
        for path in (self.control / "owner.json", self.control / "empty-hooks", self.control / "empty-attributes",
                     self.path, self.path / "config", self.path / "HEAD", self.path / "objects" / "info",
                     self.path / "refs"):
            try:
                info = os.stat(path, follow_symlinks=False)
            except OSError:
                return None
            if time.time_ns() - info.st_mtime_ns < 2_000_000_000:
                return None  # Changed too recently for its metadata to identify it.
            result.append((info.st_size, info.st_mtime_ns, info.st_ino))
        return tuple(result)

    def _ownership_current(self, *, any_age: bool = False) -> bool:
        identity = self._ownership_identity()
        if identity is None:
            return False
        with _OWNERSHIP_LOCK:
            cached = _OWNERSHIP.get(str(self.path))
        return (cached is not None and cached[0] == identity
                and (any_age or time.monotonic() - cached[1] < _OWNERSHIP_SECONDS))

    def _remember_ownership(self) -> None:
        identity = self._ownership_identity()
        if identity is not None:
            with _OWNERSHIP_LOCK:
                _OWNERSHIP[str(self.path)] = (identity, time.monotonic())

    def head(self) -> str:
        # While the owned control files keep their verified identity, the loose ref is read
        # without the repository lock: Git replaces it atomically. The coordination thread
        # reuses the last verification of an unchanged identity; others repeat it each minute.
        if self._ownership_current(any_age=on_coordination_thread()):
            return self._head()
        with self._locked():
            if not self._ownership_current():
                self._owned()
            return self._head()

    def _manifest(self, blobs: BlobStore, snapshot: str) -> dict[str, str]:
        try:
            raw = blobs.get(snapshot)
            if len(raw) > self.max_metadata_bytes:
                raise RepositoryError("Oversized source snapshot manifest")
            value = json.loads(raw)
        except (OSError, ValueError, TypeError) as error:
            raise RepositoryError("Invalid source snapshot") from error
        if not isinstance(value, dict) or set(value) != {"files"} or not isinstance(value["files"], list):
            raise RepositoryError("Invalid source snapshot manifest")
        if len(value["files"]) > self.max_files:
            raise RepositoryError(f"Repository snapshot exceeds the configured {self.max_files}-file limit")
        result: dict[str, str] = {}
        names = []
        for entry in value["files"]:
            if (not isinstance(entry, dict) or set(entry) != {"path", "sha256"}
                    or not isinstance(entry["sha256"], str) or not _HASH.fullmatch(entry["sha256"])):
                raise RepositoryError("Snapshots can contain only regular content-addressed files")
            path = _relative(entry["path"])
            names.append(path)
            result[path] = entry["sha256"]
        _paths(names, max_files=self.max_files)
        return result

    def _hash_bytes(self, body: bytes, directory: Path) -> str:
        if len(body) > self.max_bytes:
            raise RepositoryError("Repository file exceeds the snapshot size limit")
        return self._oid(self._git(["hash-object", "-w", "--no-filters", "--stdin"],
                                   git_dir=directory, body=body).decode("ascii").strip())

    def _tree(self, entries: dict[str, tuple[str, str]], directory: Path) -> str:
        _paths(list(entries), max_files=self.max_files)
        descriptor, name = tempfile.mkstemp(dir=self.control, prefix="index-")
        os.close(descriptor)
        os.unlink(name)
        index = Path(name)
        try:
            self._git(["read-tree", "--empty"], git_dir=directory, index=index)
            body = b"".join(f"{mode} {sha}\t{path}\0".encode("utf-8")
                            for path, (mode, sha) in sorted(entries.items()))
            self._git(["update-index", "-z", "--index-info"], git_dir=directory,
                      index=index, body=body)
            return self._oid(self._git(["write-tree"], git_dir=directory,
                                       index=index).decode("ascii").strip())
        finally:
            index.unlink(missing_ok=True)
            Path(name + ".lock").unlink(missing_ok=True)

    def _commit(self, directory: Path, tree: str, parent: str | None, message: str) -> str:
        args = ["commit-tree", self._oid(tree)]
        if parent is not None:
            args.extend(["-p", self._oid(parent)])
        return self._oid(self._git(args, git_dir=directory,
                                   body=message.encode("utf-8")).decode("ascii").strip())

    def _objects(self, directory: Path, objects: dict[str, str]) -> dict[str, bytes]:
        if not objects:
            return {}
        output = self._git(["cat-file", "--batch"], git_dir=directory,
                           body="".join(f"{self._oid(sha)}\n" for sha in objects).encode("ascii"))
        result = {}
        offset = 0
        for sha, kind in objects.items():
            end = output.find(b"\n", offset)
            header = output[offset:end].split(b" ")
            if (end < 0 or len(header) != 3 or header[:2] != [sha.encode(), kind.encode()]
                    or not header[2].isdigit()):
                raise RepositoryError("Invalid or missing Git object")
            size = int(header[2])
            value = output[end + 1:end + 1 + size]
            offset = end + 2 + size
            expected = hashlib.sha1(kind.encode() + b" " + str(size).encode() + b"\0" + value)
            if expected.hexdigest() != sha or output[offset - 1:offset] != b"\n":
                raise RepositoryError("Git object integrity failure")
            result[sha] = value
        if offset != len(output):
            raise RepositoryError("Unexpected Git object output")
        return result

    def _commit_tree(self, directory: Path, commit: str) -> str:
        body = self._objects(directory, {self._oid(commit): "commit"})[commit]
        first = body.split(b"\n", 1)[0]
        if not first.startswith(b"tree "):
            raise RepositoryError("Invalid Git commit")
        return self._oid(first[5:].decode("ascii"))

    def _entries(self, directory: Path, tree: str) -> tuple[dict[str, tuple[str, str]], dict[str, bytes]]:
        objects = {self._oid(tree): "tree"}
        raw = self._git(["ls-tree", "-r", "-t", "-z", "--full-tree", tree], git_dir=directory)
        entries = {}
        seen: set[str] = set()
        spelling: dict[str, str] = {}
        for record in raw.split(b"\0"):
            if not record:
                continue
            try:
                fields, name = record.split(b"\t", 1)
                mode, kind, sha = fields.decode("ascii").split(" ")
                path = _relative(name.decode("utf-8"))
            except (ValueError, UnicodeError) as error:
                raise RepositoryError("Invalid Git tree entry") from error
            self._oid(sha)
            key = path.upper().casefold()
            if path in seen or (key in spelling and spelling[key] != path):
                raise RepositoryError("Duplicate or case-colliding Git tree entries")
            seen.add(path)
            spelling[key] = path
            if mode == "040000" and kind == "tree":
                objects[sha] = kind
                continue
            if mode not in {"100644", "100755"} or kind != "blob":
                raise RepositoryError("Symlinks, submodules, and special Git files are forbidden")
            entries[path] = (mode, sha)
            objects[sha] = kind
        _paths(list(entries), max_files=self.max_files)
        contents = self._objects(directory, objects)
        if sum(len(contents[sha]) for _, sha in entries.values()) > self.max_bytes:
            raise RepositoryError("Repository snapshot exceeds the configured byte limit")
        return entries, contents

    def initialize(self, blobs: BlobStore, snapshot: str | None = None) -> dict[str, Any]:
        with self._locked():
            marker = self.control / "owner.json"
            if not marker.exists():
                if self.path.exists():
                    raise RepositoryError("Refusing to adopt an existing unowned Git repository")
                for directory in (self.control / "empty-hooks", self.control / "sessions",
                                  self.control / "publications"):
                    _safe_path(directory).mkdir(exist_ok=True)
                attributes = _safe_path(self.control / "empty-attributes")
                if attributes.exists() and attributes.read_bytes():
                    raise RepositoryError("Unexpected Git attributes in repository control")
                attributes.write_bytes(b"")
                self._git(["init", "--bare", "--template=", "--object-format=sha1",
                           f"--initial-branch={self.branch}", str(self.path)])
                (self.path / "config").write_bytes(self._config(True))
                self._write_json(marker, {"format": 1, "repository": str(self.path),
                                          "branch": self.branch})
            self._owned()
            if self._git(["rev-parse", "--verify", "--quiet", self.ref],
                         git_dir=self.path, allowed=(0, 1)):
                return {"head": self._head(), "branch": self.branch}
            files = {} if snapshot is None else self._manifest(blobs, snapshot)
            entries = {}
            total = 0
            for path, sha in files.items():
                content = blobs.get(sha)
                total += len(content)
                if total > self.max_bytes:
                    raise RepositoryError("Repository snapshot exceeds the configured byte limit")
                entries[path] = ("100644", self._hash_bytes(content, self.path))
            commit = self._commit(self.path, self._tree(entries, self.path), None,
                                  "TaskHub initial repository\n")
            self._git(["update-ref", self.ref, commit, _ZERO], git_dir=self.path)
            return {"head": commit, "branch": self.branch}

    def _snapshot(self, commit: str, blobs: BlobStore, directory: Path) -> str:
        entries, contents = self._entries(directory, self._commit_tree(directory, commit))
        return blobs.put_json({"files": [
            {"path": path, "sha256": blobs.put(contents[sha])}
            for path, (_, sha) in sorted(entries.items())
        ]})

    def snapshot(self, commit: str, blobs: BlobStore) -> str:
        # A commit's tree is immutable; reuse its snapshot while the blob remains. While the
        # verified control files are unchanged, the reuse takes no lock: worker checkouts
        # otherwise waited behind long integration work for an answer already known.
        key = (str(self.path), str(blobs.root), self._oid(commit))
        cached = _SNAPSHOTS.get(key)
        if cached is not None and blobs.has(cached) and self._ownership_current(any_age=True):
            return cached
        with self._locked():
            self._owned()
            cached = _SNAPSHOTS.get(key)
            if cached is not None and blobs.has(cached):
                return cached
            result = self._snapshot(commit, blobs, self.path)
            _SNAPSHOTS[key] = result
            return result

    def _session_path(self, workspace: Path) -> tuple[Path, Path]:
        workspace = _safe_path(workspace)
        if workspace.parent.name != ".taskhub-integration":
            raise RepositoryError("Review workspaces must be inside .taskhub-integration")
        _relative(workspace.name)
        key = hashlib.sha256(str(workspace).casefold().encode("utf-8")).hexdigest()
        return workspace, self.control / "sessions" / f"{key}.json"

    def _session(self, workspace: Path) -> tuple[Path, Path, dict[str, Any]]:
        self._owned()
        workspace, metadata = self._session_path(workspace)
        session = self._read_json(metadata)
        if (session.get("workspace") != str(workspace) or session.get("repository") != str(self.path)
                or session.get("phase") != "prepared"):
            raise RepositoryError("The integration workspace is not an owned prepared session")
        self._validate_git_dir(workspace / ".git", bare=False)
        if (workspace / ".git" / "HEAD").read_bytes() != (session["target_head"] + "\n").encode():
            raise RepositoryError("Integration HEAD changed outside the review protocol")
        merge_head = workspace / ".git" / "MERGE_HEAD"
        if merge_head.exists() and merge_head.read_bytes() != (session["candidate_commit"] + "\n").encode():
            raise RepositoryError("The integration merge was redirected")
        return workspace, metadata, session

    def _index(self, workspace: Path) -> tuple[dict[str, tuple[str, str]], list[str]]:
        raw = self._git(["ls-files", "--stage", "-z"], git_dir=workspace / ".git",
                        workspace=workspace)
        entries = {}
        conflicts = set()
        names = set()
        for record in raw.split(b"\0"):
            if not record:
                continue
            try:
                fields, name = record.split(b"\t", 1)
                mode, sha, stage = fields.decode("ascii").split(" ")
                path = _relative(name.decode("utf-8"))
            except (ValueError, UnicodeError) as error:
                raise RepositoryError("Invalid integration index") from error
            self._oid(sha)
            if mode not in {"100644", "100755"} or stage not in {"0", "1", "2", "3"}:
                raise RepositoryError("Symlinks, submodules, and special index entries are forbidden")
            names.add(path)
            if stage == "0":
                entries[path] = (mode, sha)
            else:
                conflicts.add(path)
        _paths(list(names), max_files=self.max_files)
        return entries, sorted(conflicts)

    @staticmethod
    def _install_workspace_file(path: Path, content: bytes) -> None:
        path = _safe_path(path)
        if path.exists():
            if not path.is_file() or path.read_bytes() != content:
                raise RepositoryError("An incomplete review workspace contains unexpected files")
            return
        atomic_write(path, content, prefix=".repository-")

    def _check_partial_checkout(self, workspace: Path, target: str) -> None:
        entries, contents = self._entries(self.path, self._commit_tree(self.path, target))
        directories = {"/".join(name.split("/")[:index]) for name in entries
                       for index in range(1, len(name.split("/")))}
        for base, dirs, files in os.walk(workspace, followlinks=False):
            if Path(base) == workspace:
                dirs.remove(".git")
            for name in dirs:
                path = _safe_path(Path(base) / name)
                if path.relative_to(workspace).as_posix() not in directories:
                    raise RepositoryError("Unexpected files in an incomplete review workspace")
            for name in files:
                path = _safe_path(Path(base) / name)
                relative = path.relative_to(workspace).as_posix()
                if (relative not in entries or not path.is_file()
                        or path.read_bytes() != contents[entries[relative][1]]):
                    raise RepositoryError("Unexpected files in an incomplete review workspace")
        indexed, conflicts = self._index(workspace)
        if conflicts or any(entries.get(name) != value for name, value in indexed.items()):
            raise RepositoryError("Unexpected index in an incomplete review workspace")

    def _complete_prepare(self, workspace: Path, metadata: Path,
                          session: dict[str, Any]) -> dict[str, Any]:
        git_dir = workspace / ".git"
        target, candidate = session["target_head"], session["candidate_commit"]
        if session["phase"] == "allocating":
            _safe_path(workspace.parent).mkdir(parents=True, exist_ok=True)
            workspace.mkdir(exist_ok=True)
            if any(item.name != ".git" for item in workspace.iterdir()):
                raise RepositoryError("Unexpected files in an incomplete review workspace")
            for relative in ("objects/info", "objects/pack", "refs/heads", "refs/tags"):
                _safe_path(git_dir / relative).mkdir(parents=True, exist_ok=True)
            # A minimal Git repository is installed without templates or inherited config.
            self._install_workspace_file(git_dir / "config", self._config(False))
            self._install_workspace_file(git_dir / "HEAD", (target + "\n").encode("ascii"))
            self._install_workspace_file(git_dir / "objects/info/alternates", self._alternate())
            self._validate_git_dir(git_dir, bare=False)
            session["phase"] = "checkout"
            self._write_json(metadata, session)
        if session["phase"] == "checkout":
            self._validate_git_dir(git_dir, bare=False)
            self._check_partial_checkout(workspace, target)
            self._git(["read-tree", "--reset", "-u", target], git_dir=git_dir, workspace=workspace)
            session["phase"] = "merging"
            self._write_json(metadata, session)
        if session["phase"] == "merging":
            self._validate_git_dir(git_dir, bare=False)
            merge_head = git_dir / "MERGE_HEAD"
            if not merge_head.exists():
                self._check_partial_checkout(workspace, target)
                self._git(["merge", "--no-commit", "--no-ff", "--no-edit", "--no-verify", candidate],
                          git_dir=git_dir, workspace=workspace, allowed=(0, 1))
            if not merge_head.is_file() or merge_head.read_bytes() != (candidate + "\n").encode():
                raise RepositoryError("Git did not prepare the requested candidate merge")
            self._index(workspace)
            session["phase"] = "prepared"
            self._write_json(metadata, session)
        self._session(workspace)
        return {**{name: session[name] for name in (
            "base_commit", "target_head", "candidate_commit", "workspace")},
            "conflicts": self._index(workspace)[1]}

    def prepare(self, blobs: BlobStore, candidate_snapshot: str, base_commit: str,
                workspace: Path, paths: list[str]) -> dict[str, Any]:
        workspace, metadata = self._session_path(workspace)
        with self._session_locked(metadata):
            self._owned()
            paths = _paths(paths, max_files=self.max_files)
            base_commit = self._oid(base_commit)
            if metadata.exists():
                old = self._read_json(metadata)
                if (old.get("repository") != str(self.path) or old.get("workspace") != str(workspace)
                        or old.get("base_commit") != base_commit
                        or old.get("candidate_snapshot") != candidate_snapshot
                        or old.get("paths") != paths
                        or old.get("phase") not in {"allocating", "checkout", "merging", "prepared"}):
                    raise RepositoryError("An integration workspace cannot be reused for another candidate")
                return self._complete_prepare(workspace, metadata, old)
            if workspace.exists():
                raise RepositoryError("Refusing to overwrite an existing unowned review workspace")
            source = self._manifest(blobs, candidate_snapshot)
            target_head = self._head()
            self._git(["merge-base", "--is-ancestor", base_commit, target_head], git_dir=self.path)
            entries, contents = self._entries(self.path, self._commit_tree(self.path, base_commit))
            total = sum(len(contents[sha]) for _, sha in entries.values())
            added: dict[str, bytes] = {}
            for path in paths:
                old_entry = entries.pop(path, None)
                if old_entry is not None:
                    total -= len(contents[old_entry[1]])
                if path in source:
                    content = blobs.get(source[path])
                    total += len(content)
                    if total > self.max_bytes:
                        raise RepositoryError("Repository snapshot exceeds the configured byte limit")
                    added[path] = content
            # Only the candidate's new objects enter the shared repository under its lock.
            with self._locked():
                for path, content in added.items():
                    entries[path] = ("100644", self._hash_bytes(content, self.path))
                tree = self._tree(entries, self.path)
                candidate = self._commit(self.path, tree, base_commit,
                                         f"TaskHub candidate {candidate_snapshot}\n{canonical(paths)}\n")
            # Validate the target before checkout. Never let Git materialize special paths.
            self._entries(self.path, self._commit_tree(self.path, target_head))
            session = {"format": 1, "phase": "allocating", "repository": str(self.path),
                       "workspace": str(workspace), "base_commit": base_commit,
                       "target_head": target_head, "candidate_commit": candidate,
                       "candidate_snapshot": candidate_snapshot, "paths": paths}
            self._write_json(metadata, session)
            return self._complete_prepare(workspace, metadata, session)

    def _working_bytes(self, workspace: Path, paths: list[str]) -> dict[str, bytes | None]:
        result: dict[str, bytes | None] = {}
        total = 0
        for name in _paths(paths, max_files=self.max_files):
            path = _safe_path(workspace / name)
            if not path.exists():
                result[name] = None
                continue
            if not path.is_file():
                raise RepositoryError(f"Tracked path is not a regular file: {name}")
            size = path.stat().st_size
            if size + total > self.max_bytes:
                raise RepositoryError("Repository snapshot exceeds the configured byte limit")
            with path.open("rb") as stream:
                body = stream.read(size + 1)
            if len(body) != size:
                raise RepositoryError("Repository source changed during capture")
            total += len(body)
            if total > self.max_bytes:
                raise RepositoryError("Repository snapshot exceeds the configured byte limit")
            if _MARKERS.search(body):
                raise UnresolvedConflicts(f"Conflict markers remain in {name}")
            result[name] = body
        return result

    def _stage(self, workspace: Path, contents: dict[str, bytes | None]) -> None:
        indexed, _ = self._index(workspace)
        # Authenticate existing blobs in one Git process and compare their exact
        # bytes. Reuse only their content; keep the original index normalization
        # for modes, conflict stages, and flags such as intent-to-add.
        existing = self._objects(workspace / ".git", {
            indexed[path][1]: "blob" for path, body in contents.items()
            if body is not None and path in indexed
        })
        records = []
        for path, body in contents.items():
            records.append(f"0 {_ZERO}\t{path}\0".encode("utf-8"))
            if body is not None:
                previous = indexed.get(path)
                sha = (previous[1] if previous is not None and existing[previous[1]] == body
                       else self._hash_bytes(body, workspace / ".git"))
                records.append(f"100644 {sha}\t{path}\0".encode("utf-8"))
        self._git(["update-index", "-z", "--index-info"], git_dir=workspace / ".git",
                  workspace=workspace, body=b"".join(records))

    def resolve(self, workspace: Path, paths: list[str]) -> dict[str, Any]:
        with self._session_locked(self._session_path(workspace)[1]):
            workspace, _, _ = self._session(workspace)
            self._stage(workspace, self._working_bytes(workspace, paths))
            return self._status(workspace)

    def _status(self, workspace: Path) -> dict[str, Any]:
        workspace, _, session = self._session(workspace)
        entries, conflicts = self._index(workspace)
        del entries
        raw = self._git(["status", "--porcelain=v1", "-z", "--untracked-files=all"],
                        git_dir=workspace / ".git", workspace=workspace)
        records = iter(raw.split(b"\0"))
        changed: list[str] = []
        untracked: list[str] = []
        for record in records:
            if not record:
                continue
            code = record[:2].decode("ascii")
            name = record[3:].decode("utf-8")
            (untracked if code == "??" else changed).append(name)
            if "R" in code or "C" in code:
                changed.append(next(records).decode("utf-8"))
        return {"workspace": str(workspace), "base_commit": session["base_commit"],
                "target_head": session["target_head"], "candidate_commit": session["candidate_commit"],
                "conflicts": conflicts, "changed_paths": sorted(set(changed)),
                "untracked": sorted(untracked), "finalized": "final" in session,
                "final": session.get("final")}

    def status(self, workspace: Path) -> dict[str, Any]:
        with self._session_locked(self._session_path(workspace)[1]):
            return self._status(workspace)

    def finalize(self, workspace: Path, blobs: BlobStore) -> dict[str, Any]:
        with self._session_locked(self._session_path(workspace)[1]):
            workspace, metadata, session = self._session(workspace)
            entries, conflicts = self._index(workspace)
            if conflicts:
                raise UnresolvedConflicts("Resolve and explicitly stage every conflict first")
            contents = self._working_bytes(workspace, list(entries))
            self._stage(workspace, contents)
            tree = self._oid(self._git(["write-tree"], git_dir=workspace / ".git",
                                       workspace=workspace).decode("ascii").strip())
            self._entries(workspace / ".git", tree)
            if self._working_bytes(workspace, list(entries)) != contents:
                raise RepositoryError("The integration workspace changed while it was being frozen")
            target = session["target_head"]
            candidate = session["candidate_commit"]
            commit = self._commit(
                workspace / ".git", tree, target,
                f"TaskHub reviewed integration\n\nTarget: {target}\nCandidate: {candidate}\n",
            )
            # Only new review objects travel back; alternates supply immutable ancestry.
            pack = self._git(["pack-objects", "--stdout", "--revs", "--no-reuse-delta"],
                             git_dir=workspace / ".git",
                             body=f"{commit}\n^{target}\n^{candidate}\n".encode("ascii"))
            with self._locked():
                self._git(["index-pack", "--stdin", "--fix-thin"], git_dir=self.path, body=pack)
            snapshot = self._snapshot(commit, blobs, self.path)
            # Once published, this commit is the head every worker checkout reads first.
            _SNAPSHOTS[(str(self.path), str(blobs.root), commit)] = snapshot
            changed = self._git(["diff-tree", "--no-commit-id", "--name-only", "--no-renames",
                                 "-r", "-z", target, commit], git_dir=self.path)
            result = {"commit": commit, "snapshot": snapshot, "target_head": target, "tree": tree,
                      "changed_paths": [part.decode("utf-8") for part in changed.split(b"\0") if part],
                      "files": blobs.get_json(snapshot)["files"]}
            with self._locked():  # Publication reads this record under the repository lock.
                self._write_json(self.control / "publications" / f"{commit}.json", result)
            session["final"] = result
            self._write_json(metadata, session)
            return result

    def prune_integration_workspace(self, workspace: Path, blobs: BlobStore) -> dict[str, Any]:
        """Remove an owned checkout that no open batch uses; its exact files stay recorded.

        A frozen checkout still equal to its frozen snapshot needs nothing more: that snapshot
        and its commit live outside it. Any other one, never frozen, changed after freezing, or
        left part-way through preparation when its batch was deferred, recovered, emptied or
        cancelled, is first read in full into an exact snapshot that the session receipt
        records, and only then removed. In the Linux pilot such checkouts were kept for good:
        40 of them, about 10 GB, on 2026-11-07. A checkout holding what a snapshot leaves out
        (private files, drafts, graphs, build directories) is kept, and so is a linked one.

        Only the session record is read and updated under the checkout's lock. The walk, the
        snapshot and the removal touch that ended checkout alone: under the repository lock
        they took over 30 seconds once other threads kept the interpreter busy, and worker
        checkouts waiting for the lock failed (Linux pilot).
        """
        workspace, metadata = self._session_path(workspace)
        if not workspace.exists():
            return {"workspace": str(workspace), "state": "already_absent", "bytes": 0}
        with self._session_locked(metadata):
            self._owned()
            session = self._read_json(metadata)
            if session.get("workspace") != str(workspace) or session.get("repository") != str(self.path):
                raise RepositoryError("Refusing to prune an unowned integration workspace")
            if not workspace.exists():
                return {"workspace": str(workspace), "state": "already_absent", "bytes": 0}
        final = session.get("final")
        total = 0
        for base, directories, names in os.walk(workspace, followlinks=False):
            for name in [*directories, *names]:
                path = Path(base) / name
                if path.is_symlink() or not path.resolve().is_relative_to(workspace):
                    raise RepositoryError("Refusing to prune a linked integration workspace")
                # Do not erase private or otherwise unsnapshotted work while reclaiming build copies.
                if name.casefold() in _UNSNAPSHOTTED:
                    return {"workspace": str(workspace), "state": "retained_extra_files", "bytes": 0}
                if path.is_file():
                    total += path.stat().st_size
        # Read in full: an exact snapshot stores every file the checkout holds (all but .git, whose
        # frozen objects the repository already has) before any of it is removed.
        snapshot = blobs.snapshot(workspace, max_files=self.max_files, max_bytes=self.max_bytes, exact=True)
        if final is not None and snapshot == final["snapshot"]:
            # The immutable snapshot and commit already live outside this checkout.
            record = {"workspace": str(workspace), "state": "reclaimed", "bytes": total,
                      "snapshot": snapshot, "commit": final["commit"]}
        else:
            # Its working files exist only here: the receipt keeps their snapshot.
            record = {"workspace": str(workspace), "bytes": total, "snapshot": snapshot,
                      "state": "reclaimed_unfrozen" if final is None else "reclaimed_modified",
                      "phase": session.get("phase")}
            if final is not None:
                record["commit"] = final["commit"]
        # Keep the session receipt, including the pruning record, for recovery.
        with self._session_locked(metadata):
            if self._read_json(metadata) != session:
                return {"workspace": str(workspace), "state": "retained_modified", "bytes": 0}
            self._write_json(metadata, {**session, "retention": record})
        def remove_readonly(function, path, error):
            target = Path(path)
            if (not isinstance(error, PermissionError) or target.is_symlink()
                    or not target.resolve().is_relative_to(workspace.resolve())):
                raise error
            target.chmod(stat.S_IWRITE)
            function(path)
        shutil.rmtree(workspace, onexc=remove_readonly)
        return record

    def _frozen_publication(self, commit: str, expected_head: str) -> dict[str, Any]:
        frozen = self._read_json(self.control / "publications" / f"{commit}.json")
        if frozen.get("commit") != commit or frozen.get("target_head") != expected_head:
            raise RepositoryError("Publication does not match a frozen owned integration")
        return frozen

    def _check_publication(self, commit: str, expected_head: str) -> str:
        frozen = self._frozen_publication(commit, expected_head)
        body = self._objects(self.path, {commit: "commit"})[commit]
        parents = [line[7:].decode("ascii") for line in body.split(b"\n\n", 1)[0].split(b"\n")
                   if line.startswith(b"parent ")]
        if parents != [expected_head] or self._commit_tree(self.path, commit) != frozen["tree"]:
            raise RepositoryError("Publication commit does not match its reviewed parent and tree")
        return frozen["tree"]

    def verify_publication(self, commit: str, expected_head: str) -> None:
        """Run publish()'s ownership, receipt, parent and tree checks before its transaction."""
        commit, expected_head = self._oid(commit), self._oid(expected_head)
        with self._locked():
            self._owned()
            tree = self._check_publication(commit, expected_head)
            with _OWNERSHIP_LOCK:
                _VERIFIED_PUBLICATIONS[(str(self.path), commit, expected_head)] = tree

    def publish(self, commit: str, expected_head: str) -> dict[str, Any]:
        with self._locked():
            commit, expected_head = self._oid(commit), self._oid(expected_head)
            with _OWNERSHIP_LOCK:
                tree = _VERIFIED_PUBLICATIONS.get((str(self.path), commit, expected_head))
            # Commits and trees are immutable Git objects. Checked before this transaction and with
            # ownership unchanged since, only the frozen receipt that authorizes them is reread.
            if (tree is None or not self._ownership_current(any_age=True)
                    or self._frozen_publication(commit, expected_head)["tree"] != tree):
                self._owned()
                self._check_publication(commit, expected_head)
            current = self._head()
            if current == commit:
                return {"head": commit, "branch": self.branch, "published": False}
            if current != expected_head:
                raise HeadMoved(f"Public HEAD changed from {expected_head} to {current}")
            try:
                self._git(["update-ref", self.ref, commit, expected_head], git_dir=self.path)
            except RepositoryError as error:
                current = self._head()
                if current == commit:
                    return {"head": commit, "branch": self.branch, "published": False}
                if current != expected_head:
                    raise HeadMoved(f"Public HEAD changed to {current}") from error
                raise
            return {"head": commit, "branch": self.branch, "published": True}
