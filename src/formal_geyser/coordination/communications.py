"""Native human correspondence and literature with a small delivery ledger."""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
from copy import deepcopy
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import NAMESPACE_URL, uuid4, uuid5

from .commands import CommandTable
from .errors import HubError
from .gateway_receipts import GATEWAY_SCHEMA, GatewayReceipts
from .literature import LiteratureFiles, fingerprint
from .models import Actor, Role
from .storage import atomic_write, canonical, collected_thread_call, digest

COMMUNICATION_SCHEMA = """
CREATE TABLE IF NOT EXISTS human_messages(
 sequence INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,
 worker_id TEXT NOT NULL REFERENCES workers(id),direction TEXT NOT NULL,
 request_id TEXT,path TEXT NOT NULL,sha256 TEXT NOT NULL,created_at INTEGER NOT NULL,
 acknowledged_offset INTEGER NOT NULL DEFAULT 0,acknowledged_at INTEGER);
CREATE INDEX IF NOT EXISTS human_worker_idx ON human_messages(worker_id,sequence);
CREATE TABLE IF NOT EXISTS human_deliveries(
 id TEXT PRIMARY KEY,worker_id TEXT NOT NULL,created_at INTEGER NOT NULL,acknowledged_at INTEGER);
CREATE TABLE IF NOT EXISTS human_delivery_items(
 delivery_id TEXT NOT NULL REFERENCES human_deliveries(id),message_id TEXT NOT NULL,
 start_offset INTEGER NOT NULL,end_offset INTEGER NOT NULL,PRIMARY KEY(delivery_id,message_id));
CREATE TABLE IF NOT EXISTS human_message_withdrawals(
 message_id TEXT PRIMARY KEY REFERENCES human_messages(id),worker_id TEXT NOT NULL,
 reason TEXT,withdrawn_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS external_requests(
 id TEXT PRIMARY KEY,request_key TEXT NOT NULL UNIQUE,kind TEXT NOT NULL,state TEXT NOT NULL,
 path TEXT NOT NULL,sha256 TEXT NOT NULL,source_id TEXT,created_at INTEGER NOT NULL,resolved_at INTEGER);
CREATE TABLE IF NOT EXISTS external_request_waiters(
 request_id TEXT NOT NULL REFERENCES external_requests(id),worker_id TEXT NOT NULL REFERENCES workers(id),
 PRIMARY KEY(request_id,worker_id));
CREATE TABLE IF NOT EXISTS external_request_resolutions(
 request_id TEXT PRIMARY KEY REFERENCES external_requests(id),response_id TEXT NOT NULL,
 response_hash TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS literature_entries(
 id TEXT PRIMARY KEY,path TEXT NOT NULL,sha256 TEXT NOT NULL,created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS delivery_transcript_updates(
 kind TEXT NOT NULL,delivery_id TEXT NOT NULL,input_id TEXT NOT NULL,state TEXT NOT NULL,
 turn_id TEXT,reason TEXT NOT NULL,applied INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(kind,delivery_id));
""" + GATEWAY_SCHEMA

# SQL: the human message `m` was not withdrawn by its sender.
NOT_WITHDRAWN = "NOT EXISTS(SELECT 1 FROM human_message_withdrawals x WHERE x.message_id=m.id)"

NATIVE_MESSAGE_INSTRUCTION = (
    "Read each native_message.path in full and verify its sha256 before taking any action that depends on it. "
    "Preserve the recorded sender authority. Delivery acknowledges the file reference, not a completed read."
)


def _identifier(value: Any, label: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value):
        raise HubError("INVALID_INPUT", f"Invalid {label}", 422)
    return value


def _text(value: Any, label: str, maximum: int = 131072) -> str:
    if not isinstance(value, str) or not value.strip() or len(value.encode("utf-8")) > maximum:
        raise HubError("INVALID_INPUT", f"{label} must be nonempty UTF-8 text within {maximum} bytes", 422)
    return value


def waiting_human_message(worker: str) -> str:
    """SQL: a person's directed message waits for the worker `worker` (an SQL expression): it
    is unacknowledged and not withdrawn, and no delivery that holds it is still being sent or
    uncertain."""
    return (f"EXISTS(SELECT 1 FROM human_messages m WHERE m.worker_id={worker} AND m.direction='to_worker' "
            "AND m.acknowledged_at IS NULL AND " + NOT_WITHDRAWN + " AND NOT EXISTS(SELECT 1 FROM human_delivery_items i "
            "JOIN message_delivery_holds h ON h.kind='human' AND h.delivery_id=i.delivery_id "
            "WHERE i.message_id=m.id AND h.state IN ('sending','uncertain')))")


def waiting_peer_message(worker: str) -> str:
    """SQL: a peer's directed message waits for the worker `worker`, as waiting_human_message."""
    return (f"EXISTS(SELECT 1 FROM peer_messages m WHERE m.recipient_id={worker} AND m.acknowledged_at IS NULL "
            "AND NOT EXISTS(SELECT 1 FROM peer_delivery_items i JOIN message_delivery_holds h "
            "ON h.delivery_id=i.delivery_id AND h.kind='peer' WHERE i.message_id=m.id "
            "AND h.state IN ('sending','uncertain')))")


def waiting_message(worker: str) -> str:
    """SQL: a directed message, from a person or a peer, waits for the worker `worker`."""
    return f"({waiting_human_message(worker)} OR {waiting_peer_message(worker)})"


# The correspondence commands' handlers (the message, literature and request families).
COMMUNICATIONS = CommandTable()


class Communications(GatewayReceipts):
    if TYPE_CHECKING:
        root: Path
        db: Any
        clock: Any
        _get: Any
        _role: Any
        _active: Any
        _peer_ack: Any
        _wake: Any
        _event: Any
        workspace: Any

    def _document_path(self, relative: str) -> Path:
        relative_path = Path(relative)
        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise HubError("CONTENT_PATH_INVALID", "Native document path must stay within its project")
        path = self.root / relative_path
        for parent in (path, *path.parents):
            if parent == self.root:
                break
            if parent.is_symlink():
                raise HubError("CONTENT_PATH_INVALID", "Native documents must not use linked paths")
        return path

    def _write_document(self, relative: str, metadata: dict, text: str) -> str:
        body = ("```json\n" + json.dumps(metadata, indent=2, ensure_ascii=False) + "\n```\n\n" + text + "\n").encode("utf-8")
        path = self._document_path(relative)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if path.read_bytes() != body:
                raise HubError("CONTENT_CONFLICT", "A native document ID already has different content")
            return hashlib.sha256(body).hexdigest()
        atomic_write(path, body, prefix=path.name + ".")
        return hashlib.sha256(body).hexdigest()

    def _read_document(self, row) -> dict:
        try:
            body = self._document_path(row["path"]).read_bytes()
        except FileNotFoundError as exc:
            raise HubError("CONTENT_UNAVAILABLE", "Native message or literature entry is missing", 503) from exc
        if hashlib.sha256(body).hexdigest() != row["sha256"]:
            raise HubError("CONTENT_CHANGED", "Native content differs from its recorded revision")
        header, separator, text = body.decode("utf-8").partition("\n```\n\n")
        if not separator or not header.startswith("```json\n"):
            raise HubError("CONTENT_INVALID", "Native document metadata is invalid")
        return {**json.loads(header[8:]), "text": text.removesuffix("\n"),
                "path": str(self._document_path(row["path"]))}

    def _message_sender(self, c, actor: Actor) -> dict:
        worker_id = actor.worker_id if actor.role == Role.WORKER else None
        if worker_id is not None:
            worker = self._get(c, "workers", worker_id)
            display_name = worker.get("display_name", worker_id)
        else:
            display_name = "User" if actor.role == Role.OPERATOR else "System"
        return {"sender_role": actor.role.value, "sender_worker_id": worker_id,
                "sender_display_name": display_name}

    def _read_message(self, c, row) -> dict:
        content = self._read_document(row)
        if "sender_role" not in content:
            # Old request responses may have come from either a worker or the
            # operator. Keep their files/hashes intact and do not invent authority.
            if content["direction"] == "to_user":
                sender = self._message_sender(c, Actor(role=Role.WORKER, worker_id=row["worker_id"]))
            elif content.get("request_id") is None:
                sender = self._message_sender(c, Actor(role=Role.OPERATOR))
            else:
                sender = {"sender_role": "unknown", "sender_worker_id": None,
                          "sender_display_name": "Unknown sender"}
            content.update(sender)
        content["native_message"] = {"path": content["path"], "sha256": row["sha256"], "format": "markdown"}
        return content

    def _human_message_identity(self, c, *, worker_id: str, direction: str, text: str,
                                message_id: str, sender: Actor, request_id: str | None = None) -> tuple:
        """Validate an exact retry before any request-resolution side effects."""
        self._get(c, "workers", worker_id)
        _text(message_id, "Stable message ID", 200)
        _text(text, "Message")
        origin = self._message_sender(c, sender)
        key = uuid5(NAMESPACE_URL, canonical(["human-message", worker_id, direction, message_id])).hex
        prior = c.execute("SELECT * FROM human_messages WHERE id=?", (key,)).fetchone()
        if prior:
            content = self._read_message(c, prior)
            changed_sender = content["sender_role"] != "unknown" and any(
                content.get(key) != origin[key] for key in ("sender_role", "sender_worker_id"))
            if content["text"] != text or content.get("request_id") != request_id or changed_sender:
                raise HubError("IDEMPOTENCY_CONFLICT", "Message ID was reused with different content")
            return key, origin, {**dict(prior), **content}
        return key, origin, None

    def _human_message(self, c, *, worker_id: str, direction: str, text: str,
                       message_id: str, sender: Actor, request_id: str | None = None) -> dict:
        key, origin, prior = self._human_message_identity(
            c, worker_id=worker_id, direction=direction, text=text, message_id=message_id,
            sender=sender, request_id=request_id)
        if prior is not None:
            return prior
        now = self.clock()
        relative = f"correspondence/{key}.md"
        metadata = {"id": key, "worker_id": worker_id, "direction": direction, **origin,
                    "request_id": request_id, "created_at": now}
        sha = self._write_document(relative, metadata, text)
        c.execute("INSERT INTO human_messages(id,worker_id,direction,request_id,path,sha256,created_at) "
                  "VALUES(?,?,?,?,?,?,?)", (key, worker_id, direction, request_id, relative, sha, now))
        if direction == "to_worker":
            wake = getattr(self, "_wake_for_message", None)
            if wake is not None:
                wake(c, worker_id, key)
        return {**metadata, "path": str(self.root / relative), "text": text,
                "native_message": {"path": str(self.root / relative), "sha256": sha, "format": "markdown"}}

    def _communication_command(self, c, name: str, p: dict, actor: Actor) -> dict:
        handler = COMMUNICATIONS.get(name)
        if handler is None:
            raise HubError("UNKNOWN_COMMAND", "Unknown correspondence operation", 422)
        return handler(self, c, name, p, actor)

    @COMMUNICATIONS.handles("message.send")
    def _on_message_send(self, c, name, p, actor):
        self._role(actor, Role.OPERATOR, Role.WORKER)
        if actor.role == Role.WORKER:
            attempt, _ = self._active(c, actor)
            worker_id, direction = attempt["worker_id"], "to_user"
            if p.get("worker_id") not in {None, worker_id}:
                raise HubError("FORBIDDEN", "Workers can only send their own human correspondence", 403)
        else:
            worker_id, direction = p["worker_id"], "to_worker"
            worker = self._get(c, "workers", worker_id)
            if worker.get("work_intention") == "retired":
                raise HubError("WORKER_RETIRED", "This worker has retired; its history remains available but it cannot receive new work")
        return self._human_message(c, worker_id=worker_id, direction=direction,
                                   text=p["text"], message_id=p["message_id"], sender=actor,
                                   request_id=p.get("request_id"))

    @COMMUNICATIONS.handles("message.ack")
    def _on_message_ack(self, c, name, p, actor):
        self._role(actor, Role.WORKER)
        self._active(c, actor)
        return self._human_ack(c, p["delivery_id"], str(actor.worker_id))

    @COMMUNICATIONS.handles("message.withdraw")
    def _on_message_withdraw(self, c, name, p, actor):
        """The operator withdraws a message to a worker that no turn has received: it is never
        delivered and wakes no one. Once a delivery of it was prepared it may already be in a
        turn, so it stays; so do replies to requests, which resolved them."""
        self._role(actor, Role.OPERATOR)
        row = c.execute("SELECT * FROM human_messages WHERE id=?", (p.get("message_id"),)).fetchone()
        if row is None:
            raise HubError("NOT_FOUND", "Message does not exist", 404)
        prior = c.execute("SELECT withdrawn_at FROM human_message_withdrawals WHERE message_id=?",
                          (row["id"],)).fetchone()
        if prior is not None:
            return {"message_id": row["id"], "worker_id": row["worker_id"], "withdrawn_at": prior["withdrawn_at"]}
        if (row["direction"] != "to_worker" or row["request_id"] is not None
                or self._read_message(c, row)["sender_role"] != Role.OPERATOR.value):
            raise HubError("INVALID_INPUT", "Only your own messages to workers can be withdrawn, "
                           "not replies to requests", 422)
        if row["acknowledged_offset"] or row["acknowledged_at"] is not None or c.execute(
                "SELECT 1 FROM human_delivery_items WHERE message_id=? LIMIT 1", (row["id"],)).fetchone():
            raise HubError("MESSAGE_DELIVERED", "This message has been delivered, or is being delivered, "
                           "and can no longer be withdrawn", 409)
        reason = None if p.get("reason") is None else _text(p["reason"], "Reason", 2000)
        now = self.clock()
        c.execute("INSERT INTO human_message_withdrawals VALUES(?,?,?,?)", (row["id"], row["worker_id"], reason, now))
        consume = getattr(self, "_consume_message_wake", None)
        if consume is not None:
            consume(c, row["worker_id"], row["id"])
        self._event(c, "message.withdrawn", row["id"], {"reason": reason}, worker_id=row["worker_id"])
        return {"message_id": row["id"], "worker_id": row["worker_id"], "withdrawn_at": now}

    @COMMUNICATIONS.handles("literature.request", "message.request")
    def _on_request(self, c, name, p, actor):
        self._role(actor, Role.WORKER)
        attempt, _ = self._active(c, actor)
        kind = "literature" if name == "literature.request" else "question"
        source_key = _text(p["request_key"], "Request key", 1000).strip()
        title, text = _text(p["title"], "Title", 1000), _text(p["text"], "Request")
        scope = ({"task_id": attempt["task_id"], "task_revision": attempt["task_revision"]}
                 if kind == "question" else {})
        # Historical unscoped questions retain their files, IDs and receipts.
        # New questions cannot silently reuse those ambiguous global records.
        key = digest({"kind": kind, "key": source_key, **scope})
        row = c.execute("SELECT * FROM external_requests WHERE request_key=?", (key,)).fetchone()
        if row is None:
            request_id = uuid5(NAMESPACE_URL, "external-request:" + key).hex
            path = f"correspondence/requests/{request_id}.md"
            meta = {"id": request_id, "kind": kind, "request_key": source_key,
                    **scope, "title": title, "created_at": self.clock()}
            sha = self._write_document(path, meta, text)
            c.execute("INSERT INTO external_requests VALUES(?,?,?,'pending',?,?,NULL,?,NULL)",
                      (request_id, key, kind, path, sha, meta["created_at"]))
            row = c.execute("SELECT * FROM external_requests WHERE id=?", (request_id,)).fetchone()
        content = self._read_document(row)
        if (content["kind"] != kind or content["request_key"] != source_key
                or content["title"] != title or content["text"] != text
                or any(content.get(field) != value for field, value in scope.items())):
            raise HubError("REQUEST_IDENTITY_CONFLICT", "Request key was reused with different identity or content")
        c.execute("INSERT OR IGNORE INTO external_request_waiters VALUES(?,?)", (row["id"], attempt["worker_id"]))
        return {**dict(row), **content,
                "wait_predicate": {"external_response_id": row["id"]}}

    @COMMUNICATIONS.handles("literature.register")
    def _on_literature_register(self, c, name, p, actor):
        self._role(actor, Role.WORKER, Role.OPERATOR)
        if actor.role == Role.WORKER:
            self._active(c, actor)
        source = Path(p["path"])
        if not source.is_absolute():
            base = self.workspace(str(actor.worker_id)) if actor.role == Role.WORKER else self.root
            p = {**p, "path": str(base / source)}
        return self._register_literature(c, p)

    @COMMUNICATIONS.handles("request.resolve")
    def _on_request_resolve(self, c, name, p, actor):
        self._role(actor, Role.OPERATOR, Role.WORKER)
        if actor.role == Role.WORKER:
            self._active(c, actor)
        row = c.execute("SELECT * FROM external_requests WHERE id=?", (p["request_id"],)).fetchone()
        if row is None:
            raise HubError("NOT_FOUND", "Request does not exist", 404)
        outcome = p.get("outcome", "fulfilled")
        if outcome not in {"fulfilled", "declined", "pending"}:
            raise HubError("INVALID_INPUT", "Request outcome must be fulfilled, declined, or pending", 422)
        if actor.role == Role.WORKER and (row["kind"] != "literature" or outcome != "fulfilled"):
            raise HubError("FORBIDDEN", "Workers may resolve literature only after registering an accessible source", 403)
        text = _text(p["text"], "Response")
        response_id = _text(p["message_id"], "Stable response ID", 200)
        source_id = p.get("source_id")
        source_path = p.get("source_path")
        registration = None
        if source_path is not None:
            self._role(actor, Role.OPERATOR)
            if row["kind"] != "literature" or outcome != "fulfilled" or source_id is not None:
                raise HubError("INVALID_INPUT", "A source path requires fulfilled literature without a source ID", 422)
            native_source = Path(_text(source_path, "Literature path", 32768))
            if not native_source.is_absolute():
                native_source = self.root / native_source
            observed = fingerprint(native_source)
            title = self._read_document(row)["title"]
            source_id = "provided-" + digest({"request_id": row["id"], "title": title,
                                              "source_sha256": observed["source_sha256"],
                                              "source_kind": observed["source_kind"]})
            registration = {"source_id": source_id, "title": title, "path": str(native_source), "copy": True,
                            "expected_fingerprint": observed}
        if row["kind"] == "literature" and outcome == "fulfilled":
            if registration is None:
                self._accessible_literature(c, source_id)
        elif source_id is not None:
            raise HubError("INVALID_INPUT", "Only fulfilled literature requests bind a source", 422)
        if row["state"] != "pending" and (row["state"] != outcome or row["source_id"] != source_id):
            raise HubError("REQUEST_RESOLVED", "The request already has a different terminal resolution")
        waiters = c.execute("SELECT worker_id FROM external_request_waiters WHERE request_id=?", (row["id"],)).fetchall()
        prior_messages = [self._human_message_identity(
            c, worker_id=waiter["worker_id"], direction="to_worker", text=text,
            message_id=response_id, sender=actor, request_id=row["id"])[2] for waiter in waiters]
        origin = self._message_sender(c, actor)
        response_hash = digest({"outcome": outcome, "source_id": source_id, "text": text,
                                "sender_role": origin["sender_role"], "sender_worker_id": origin["sender_worker_id"]})
        if row["state"] != "pending":
            resolution = c.execute("SELECT * FROM external_request_resolutions WHERE request_id=?", (row["id"],)).fetchone()
            if ((resolution is not None and (resolution["response_id"] != response_id
                                            or resolution["response_hash"] != response_hash))
                    or (resolution is None and (not prior_messages or not all(prior_messages)))):
                raise HubError("IDEMPOTENCY_CONFLICT", "A completed response is immutable; only its exact retry is allowed")
            return {"request_id": row["id"], "state": outcome, "source_id": source_id}
        if registration is not None:
            self._register_literature(c, registration)
        c.execute("UPDATE external_requests SET state=?,source_id=?,resolved_at=? WHERE id=?",
                  (outcome, source_id, self.clock() if outcome != "pending" else None, row["id"]))
        if outcome != "pending":
            c.execute("INSERT INTO external_request_resolutions VALUES(?,?,?)", (row["id"], response_id, response_hash))
        for waiter in waiters:
            self._human_message(c, worker_id=waiter["worker_id"], direction="to_worker",
                                text=text, message_id=response_id, sender=actor, request_id=row["id"])
        self._wake(c)
        return {"request_id": row["id"], "state": outcome, "source_id": source_id}

    @COMMUNICATIONS.handles("message.delivery.resolve")
    def _on_message_delivery_resolve(self, c, name, p, actor):
        self._role(actor, Role.OPERATOR)
        reason = _text(p["reason"], "Transport reconciliation evidence")
        hold = c.execute("SELECT * FROM message_delivery_holds WHERE kind=? AND delivery_id=?",
                         (p["kind"], p["delivery_id"])).fetchone()
        if hold is None:
            raise HubError("NOT_FOUND", "Uncertain delivery does not exist", 404)
        outcome = p["outcome"]
        if outcome not in {"delivered", "not_delivered"}:
            raise HubError("INVALID_INPUT", "Reconcile to delivered or not_delivered", 422)
        if hold["state"] != "uncertain" and hold["state"] != outcome:
            raise HubError("DELIVERY_CONFLICT", "Delivery was already reconciled differently")
        if outcome == "delivered":
            method = self._peer_ack if hold["kind"] == "peer" else self._human_ack
            method(c, hold["delivery_id"], hold["worker_id"])
        c.execute("UPDATE message_delivery_holds SET state=?,reason=? WHERE kind=? AND delivery_id=?",
                  (outcome, reason, hold["kind"], hold["delivery_id"]))
        if hold["input_id"]:
            c.execute("INSERT OR REPLACE INTO delivery_transcript_updates VALUES(?,?,?,?,?,?,0)",
                      (hold["kind"], hold["delivery_id"], hold["input_id"], outcome, hold["turn_id"], reason))
        refresh = getattr(self, "_refresh_message_wake", None)
        if refresh is not None:
            refresh(c, hold["worker_id"])
        return {"delivery_id": hold["delivery_id"], "state": outcome}

    def _register_literature(self, c, p: dict) -> dict:
        source_id = _identifier(p["source_id"], "source ID")
        p = {**p, "title": _text(p["title"], "Source title", 2000)}
        files = LiteratureFiles(self.root)
        prior = c.execute("SELECT * FROM literature_entries WHERE id=?", (source_id,)).fetchone()
        if prior is not None:
            record = self._read_literature(prior)
            proposed = {key: p[key] for key in ("title", "uri", "version", "citation") if key in p}
            if "description" in p:
                proposed["text"] = p.get("description") or p["title"]
            observed = fingerprint(Path(p["path"]))
            if (any(record.get(key) != value for key, value in proposed.items())
                    or record["source_sha256"] != observed["source_sha256"]):
                raise HubError("SOURCE_VERSION_CONFLICT", "Register changed literature under a new source ID")
            files.verify(record)
        else:
            path, sha, record = files.register(source_id, p)
            c.execute("INSERT INTO literature_entries VALUES(?,?,?,?)", (source_id, path, sha, self.clock()))
        files.add_pointer(record)
        return record

    def _read_literature(self, row) -> dict:
        return LiteratureFiles(self.root).read(row)

    def _accessible_literature(self, c, source_id: str | None) -> dict:
        row = c.execute("SELECT * FROM literature_entries WHERE id=?", (source_id,)).fetchone()
        if row is None:
            raise HubError("SOURCE_UNAVAILABLE", "Register an accessible local source before fulfilling the request")
        record = self._read_literature(row)
        LiteratureFiles.verify(record)
        return record

    def _external_wait_satisfied(self, c, predicate: dict) -> bool:
        """Whether the request a worker waits for is answered: fulfilled, and for literature with
        its source registered. SQL only: every wake-up checks it, on the coordination thread. The
        source was verified when the request was fulfilled; reading it verifies it again
        (accessible_literature), so a source changed since is reported to the worker then."""
        row = c.execute("SELECT state,kind,source_id FROM external_requests WHERE id=?",
                        (predicate.get("external_response_id"),)).fetchone()
        if row is None or row["state"] != "fulfilled":
            return False
        if row["kind"] == "literature":
            return c.execute("SELECT 1 FROM literature_entries WHERE id=?", (row["source_id"],)).fetchone() is not None
        return True

    async def human_history(self, worker_id: str, *, after: int = 0, limit: int = 100) -> dict:
        if after < 0 or not 1 <= limit <= 500:
            raise HubError("INVALID_INPUT", "Invalid message pagination", 422)
        def read(c):
            self._get(c, "workers", worker_id)
            rows = c.execute("SELECT m.*,x.withdrawn_at FROM human_messages m LEFT JOIN human_message_withdrawals x "
                             "ON x.message_id=m.id WHERE m.worker_id=? AND m.sequence>? ORDER BY m.sequence LIMIT ?",
                             (worker_id, after, limit + 1)).fetchall()
            messages = [{**dict(row), **self._read_message(c, row)} for row in rows[:limit]]
            updates = [dict(row) for row in c.execute(
                "SELECT id,acknowledged_at FROM human_messages WHERE worker_id=? AND acknowledged_at IS NOT NULL "
                "ORDER BY acknowledged_at DESC,sequence DESC LIMIT 500", (worker_id,))]
            withdrawals = [{"id": row[0], "withdrawn_at": row[1]} for row in c.execute(
                "SELECT message_id,withdrawn_at FROM human_message_withdrawals WHERE worker_id=? "
                "ORDER BY withdrawn_at DESC LIMIT 500", (worker_id,))]
            return {"messages": messages, "next_after": messages[-1]["sequence"] if messages else after,
                    "has_more": len(rows) > limit, "delivery_updates": updates, "withdrawals": withdrawals}
        return await self.db.call(read)

    async def pending_message_signal(self, worker_id: str, *, human: bool = False) -> bool:
        """Cheap runner hint; actual delivery still performs full authorization. `human`: only
        a person's message counts.

        Empty steering polls must not load task files, inspect the Lean cache,
        materialize conversations, or create delivery receipts. This query
        exposes no message content and does not acknowledge anything.
        """
        waiting = waiting_human_message if human else waiting_message

        def read(c):
            if c.execute("SELECT 1 FROM workers WHERE id=?", (worker_id,)).fetchone() is None:
                return False
            return bool(c.execute("SELECT " + waiting(":worker"), {"worker": worker_id}).fetchone()[0])
        return await self.db.read(read)

    async def human_unread(self, actor: Actor, *, worker_id: str | None = None,
                           max_bytes: int = 8192, reference_only: bool = False) -> dict:
        if not 512 <= max_bytes <= 262144:
            raise HubError("INVALID_INPUT", "Invalid human inbox page budget", 422)
        def prepare(c):
            self._role(actor, Role.WORKER, Role.RUNNER, Role.OPERATOR)
            if actor.role == Role.WORKER:
                self._active(c, actor)
                if worker_id not in {None, actor.worker_id}:
                    raise HubError("FORBIDDEN", "Only your own human inbox is accessible", 403)
                worker = str(actor.worker_id)
            else:
                worker = str(worker_id)
                self._get(c, "workers", worker)
            rows = c.execute("SELECT * FROM human_messages m WHERE worker_id=? AND direction='to_worker' "
                             "AND acknowledged_at IS NULL AND " + NOT_WITHDRAWN + " AND NOT EXISTS(SELECT 1 FROM human_delivery_items i "
                             "JOIN message_delivery_holds h ON h.kind='human' AND h.delivery_id=i.delivery_id "
                             "WHERE i.message_id=m.id AND h.state IN ('sending','uncertain')) ORDER BY sequence LIMIT 51", (worker,)).fetchall()
            delivery_id = uuid4().hex if rows and actor.role != Role.OPERATOR else None
            page: dict = {"worker_id": worker, "delivery_id": delivery_id, "messages": [], "has_more": False}
            if reference_only:
                page.update(delivery_mode="native_reference", instruction=NATIVE_MESSAGE_INSTRUCTION)
            for row in rows[:50]:
                content = self._read_message(c, row)
                text, start = content["text"], row["acknowledged_offset"]
                low, high = start, len(text)
                def chunk(end):
                    return {"id": row["id"], "sequence": row["sequence"], "request_id": row["request_id"],
                            **{key: content[key] for key in ("sender_role", "sender_worker_id", "sender_display_name")},
                            "native_message": content["native_message"],
                            "text": text[start:end], "offset": start, "next_offset": end, "complete": end == len(text)}
                if reference_only:
                    item = chunk(len(text))
                    del item["text"]
                    if len(canonical({**page, "messages": [*page["messages"], item]}).encode("utf-8")) > max_bytes:
                        if not page["messages"]:
                            raise HubError("MESSAGE_PAGE_TOO_SMALL", "Message metadata exceeds the delivery budget", 413)
                        break
                    page["messages"].append(item)
                    continue
                while low < high:
                    mid = (low + high + 1) // 2
                    if len(canonical({**page, "messages": [*page["messages"], chunk(mid)]}).encode("utf-8")) <= max_bytes:
                        low = mid
                    else:
                        high = mid - 1
                if low == start:
                    if not page["messages"]:
                        raise HubError("MESSAGE_PAGE_TOO_SMALL", "Message metadata exceeds the delivery budget", 413)
                    break
                page["messages"].append(chunk(low))
                if low < len(text):
                    break
            page["has_more"] = len(page["messages"]) < len(rows) or any(not m["complete"] for m in page["messages"])
            if delivery_id:
                c.execute("INSERT INTO human_deliveries VALUES(?,?,?,NULL)", (delivery_id, worker, self.clock()))
                c.executemany("INSERT INTO human_delivery_items VALUES(?,?,?,?)", [
                    (delivery_id, item["id"], item["offset"], item["next_offset"]) for item in page["messages"]])
            return page
        return await self.db.call(prepare, write=True)

    def _human_ack(self, c, delivery_id: str, worker_id: str) -> dict:
        row = c.execute("SELECT * FROM human_deliveries WHERE id=?", (delivery_id,)).fetchone()
        if row is None or row["worker_id"] != worker_id:
            raise HubError("FORBIDDEN", "Only the recipient can acknowledge a prepared human delivery", 403)
        if row["acknowledged_at"] is None:
            for item in c.execute("SELECT i.*,m.* FROM human_delivery_items i JOIN human_messages m ON m.id=i.message_id "
                                  "WHERE i.delivery_id=?", (delivery_id,)).fetchall():
                if item["worker_id"] != worker_id or item["acknowledged_offset"] < item["start_offset"]:
                    raise HubError("MESSAGE_ACK_GAP", "Acknowledgment cannot skip undelivered text")
                text = self._read_document(item)["text"]
                offset = max(item["acknowledged_offset"], item["end_offset"])
                c.execute("UPDATE human_messages SET acknowledged_offset=?,acknowledged_at=? WHERE id=?",
                          (offset, self.clock() if offset >= len(text) else None, item["message_id"]))
                consume = getattr(self, "_consume_message_wake", None)
                if offset >= len(text) and consume is not None:
                    consume(c, worker_id, item["message_id"])
            c.execute("UPDATE human_deliveries SET acknowledged_at=? WHERE id=?", (self.clock(), delivery_id))
        return {"delivery_id": delivery_id, "acknowledged": True}

    async def mark_delivery_uncertain(self, actor: Actor, kind: str, delivery_id: str,
                                      thread_id: str | None, turn_id: str | None, input_id: str | None,
                                      *, pending: bool = False) -> dict:
        if kind not in {"human", "peer"}:
            raise HubError("INVALID_INPUT", "Unknown inbox kind", 422)
        def mark(c):
            self._role(actor, Role.WORKER, Role.RUNNER)
            if actor.role == Role.WORKER:
                self._active(c, actor)
            table = "human_deliveries" if kind == "human" else "peer_deliveries"
            delivery = c.execute("SELECT * FROM " + table + " WHERE id=?", (delivery_id,)).fetchone()
            if delivery is None or (actor.role == Role.WORKER and delivery["worker_id"] != actor.worker_id):
                raise HubError("FORBIDDEN", "Delivery does not belong to the worker", 403)
            prior = c.execute("SELECT * FROM message_delivery_holds WHERE kind=? AND delivery_id=?",
                              (kind, delivery_id)).fetchone()
            state = "sending" if pending else "uncertain"
            if prior is not None:
                if pending:
                    raise HubError("DELIVERY_ALREADY_PREPARED", "An existing transport guard cannot authorize another send")
                if prior["input_id"] != input_id or prior["thread_id"] != thread_id:
                    raise HubError("DELIVERY_CONFLICT", "Prepared delivery already belongs to another input")
                if prior["state"] != "sending":
                    state = prior["state"]
                c.execute("UPDATE message_delivery_holds SET state=? WHERE kind=? AND delivery_id=?",
                          (state, kind, delivery_id))
            else:
                c.execute("INSERT INTO message_delivery_holds VALUES(?,?,?,?,?,?,?,NULL,?)",
                          (kind, delivery_id, delivery["worker_id"], thread_id, turn_id, input_id, state, self.clock()))
            return {"delivery_id": delivery_id, "state": state}
        return await self.db.call(mark, write=True)

    async def confirm_delivery_transport(self, actor: Actor, kind: str, delivery_id: str,
                                         accepted: bool, thread_id: str | None = None,
                                         turn_id: str | None = None, input_id: str | None = None,
                                         reason: str | None = None) -> dict:
        """Resolve a pre-send transport guard only after a definite native response, or after
        the native history records the input (`reason` then says so)."""
        self._role(actor, Role.RUNNER)
        if kind not in {"human", "peer"} or type(accepted) is not bool:
            raise HubError("INVALID_INPUT", "A definite native transport outcome is required", 422)
        def confirm(c):
            row = c.execute("SELECT * FROM message_delivery_holds WHERE kind=? AND delivery_id=?",
                            (kind, delivery_id)).fetchone()
            if row is None:
                raise HubError("DELIVERY_GUARD_REQUIRED", "Persist the exact transport guard before sending native input")
            if (input_id is not None and row["input_id"] != input_id) or (
                    thread_id is not None and row["thread_id"] != thread_id) or (
                    turn_id is not None and row["turn_id"] is not None and row["turn_id"] != turn_id):
                raise HubError("DELIVERY_CONFLICT", "The native response belongs to another prepared input")
            outcome = "delivered" if accepted else "not_delivered"
            if row["state"] not in {"sending", "uncertain", outcome}:
                raise HubError("DELIVERY_CONFLICT", "Native transport was already resolved differently")
            if accepted:
                method = self._human_ack if kind == "human" else self._peer_ack
                method(c, delivery_id, row["worker_id"])
            evidence = reason or ("Native RPC confirmed acceptance." if accepted
                                  else "Native RPC definitively rejected this input.")
            resolved_turn = turn_id or row["turn_id"]
            c.execute("UPDATE message_delivery_holds SET state=?,turn_id=?,reason=? WHERE kind=? AND delivery_id=?",
                      (outcome, resolved_turn, evidence, kind, delivery_id))
            if row["input_id"]:
                c.execute("INSERT OR REPLACE INTO delivery_transcript_updates VALUES(?,?,?,?,?,?,0)",
                          (kind, delivery_id, row["input_id"], outcome, resolved_turn, evidence))
            refresh = getattr(self, "_refresh_message_wake", None)
            if refresh is not None:
                refresh(c, row["worker_id"])
            return {"delivery_id": delivery_id, "state": outcome}
        result = await self.db.call(confirm, write=True)
        await self.reconcile_delivery_transcripts()
        return result

    async def reconcile_delivery_transcripts(self) -> int:
        """Apply committed delivery resolutions to the append-only visible history."""
        from .transcripts import TranscriptStore

        store = TranscriptStore(self.root)
        def reconcile(c):
            if c.in_transaction:
                raise RuntimeError("Transcript reconciliation must follow the ledger commit")
            rows = c.execute("SELECT * FROM delivery_transcript_updates WHERE applied=0").fetchall()
            applied, failed = 0, []
            for row in rows:
                try:
                    store.reconciled(row["input_id"], delivered=row["state"] == "delivered",
                                     turn_id=row["turn_id"], reason=row["reason"])
                except (HubError, OSError, ValueError, KeyError, TypeError) as error:
                    # Visible history only: the delivery outcome is already in the ledger.
                    # One unreadable record must not hold back every other worker.
                    failed.append({"input_id": row["input_id"], "error": type(error).__name__})
                    continue
                c.execute("UPDATE delivery_transcript_updates SET applied=1 WHERE kind=? AND delivery_id=?",
                          (row["kind"], row["delivery_id"]))
                applied += 1
            self.unapplied_transcript_updates = failed
            return applied
        return await self.db.call(reconcile)

    async def list_human_requests(self) -> list[dict]:
        def read(c):
            return [(dict(row), [w[0] for w in c.execute(
                "SELECT worker_id FROM external_request_waiters WHERE request_id=?", (row["id"],))])
                    for row in c.execute("SELECT * FROM external_requests ORDER BY created_at,id")]
        rows = await self.db.read(read)
        return [{**row, **(await self._verified_document(row)), "workers": workers} for row, workers in rows]

    async def _verified_document(self, row) -> dict:
        """Read an immutable request document once, off the coordination thread.

        The ledger records each document's SHA-256, so a verified read stays valid for that
        exact identity; a changed file is a new identity and is read and verified again.
        """
        cache = self.__dict__.setdefault("_document_cache", {})
        key = (row["path"], row["sha256"])
        if key not in cache:
            cache[key] = await asyncio.to_thread(self._read_document, row)
        return deepcopy(cache[key])

    async def list_literature(self) -> list[dict]:
        return await self.db.call(lambda c: [{**dict(row), **self._read_literature(row)} for row in c.execute(
            "SELECT * FROM literature_entries ORDER BY created_at,id")])

    async def accessible_literature(self, source_id: str) -> dict:
        """Resolve one exact native source for citation/validation, without re-registering it: its
        entry on a read connection, then its files verified off the coordination thread. In the
        Linux pilot verifying a 60,579-file source held that thread for 44 s."""
        def read(c):
            row = c.execute("SELECT * FROM literature_entries WHERE id=?", (source_id,)).fetchone()
            if row is None:
                raise HubError("SOURCE_UNAVAILABLE", "Register an accessible local source before fulfilling the request")
            return dict(row)
        row = await self.db.read(read)

        def verify():
            record = self._read_literature(row)
            LiteratureFiles.verify(record)
            return record
        return await collected_thread_call(verify)

    async def latest_operator_messages(self) -> dict[str, int]:
        """The newest message each worker addressed to the operator, for unread hints."""
        return await self.db.read(lambda c: {row[0]: row[1] for row in c.execute(
            "SELECT worker_id,MAX(sequence) FROM human_messages WHERE direction='to_user' GROUP BY worker_id")})

    async def delivery_holds(self) -> list[dict]:
        return await self.db.read(lambda c: [dict(row) for row in c.execute(
            "SELECT * FROM message_delivery_holds WHERE state='uncertain' ORDER BY created_at")])

    async def uncertain_deliveries(self, worker_id: str, thread_id: str) -> list[dict]:
        """One conversation's deliveries whose transport outcome is still unknown (SQL only)."""
        return await self.db.read(lambda c: [dict(row) for row in c.execute(
            "SELECT kind,delivery_id,turn_id,input_id FROM message_delivery_holds "
            "WHERE worker_id=? AND thread_id=? AND state='uncertain' ORDER BY created_at",
            (worker_id, thread_id))])
