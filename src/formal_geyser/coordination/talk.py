"""Project-readable, file-backed direct conversations between worker pairs.

Each pair of workers shares exactly one conversation; tasks never own talk.

The command hook writes only coordination metadata and an append intent. Call
``reconcile_peer_talk`` after committing commands. File-backed queries reconcile
and read within one serialized database operation, checking concurrent commits.
Private content never enters shared artifacts, events, or the blob store.
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import NAMESPACE_URL, uuid4, uuid5

from .communications import NATIVE_MESSAGE_INSTRUCTION
from .content import _normal_path
from .errors import HubError
from .models import Actor, Role
from .storage import canonical, digest
from .worker_names import worker_identity

MAX_TALK_TEXT_BYTES = 16384
PEER_TALK_SCHEMA = """
CREATE TABLE IF NOT EXISTS peer_conversations(
 id TEXT PRIMARY KEY,
 worker_low TEXT NOT NULL REFERENCES workers(id),worker_high TEXT NOT NULL REFERENCES workers(id),
 created_at INTEGER NOT NULL,UNIQUE(worker_low,worker_high),CHECK(worker_low<worker_high));
CREATE TABLE IF NOT EXISTS peer_messages(
 sequence INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,
 conversation_id TEXT NOT NULL REFERENCES peer_conversations(id),
 sender_id TEXT NOT NULL REFERENCES workers(id),recipient_id TEXT NOT NULL REFERENCES workers(id),
 client_message_id TEXT NOT NULL,request_hash TEXT NOT NULL,content_hash TEXT NOT NULL,
 text_length INTEGER NOT NULL,created_at INTEGER NOT NULL,
 acknowledged_offset INTEGER NOT NULL DEFAULT 0,acknowledged_at INTEGER,
 UNIQUE(sender_id,client_message_id),CHECK(sender_id<>recipient_id));
CREATE INDEX IF NOT EXISTS peer_unread_idx ON peer_messages(recipient_id,acknowledged_at,sequence);
CREATE INDEX IF NOT EXISTS peer_history_idx ON peer_messages(conversation_id,sequence);
-- A question, or an answer to one (talk.send kind); a message without a row is a notice.
CREATE TABLE IF NOT EXISTS peer_message_kinds(
 message_id TEXT PRIMARY KEY REFERENCES peer_messages(id),
 kind TEXT NOT NULL CHECK(kind IN ('question','answer')),answers TEXT REFERENCES peer_messages(id),
 CHECK((kind='answer')=(answers IS NOT NULL)));
CREATE INDEX IF NOT EXISTS peer_answer_idx ON peer_message_kinds(answers) WHERE answers IS NOT NULL;
CREATE TABLE IF NOT EXISTS peer_append_intents(
 message_id TEXT PRIMARY KEY REFERENCES peer_messages(id),content_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS peer_deliveries(
 id TEXT PRIMARY KEY,worker_id TEXT NOT NULL REFERENCES workers(id),
 created_at INTEGER NOT NULL,acknowledged_at INTEGER);
CREATE TABLE IF NOT EXISTS peer_delivery_items(
 delivery_id TEXT NOT NULL REFERENCES peer_deliveries(id),
 message_id TEXT NOT NULL REFERENCES peer_messages(id),
 start_offset INTEGER NOT NULL,end_offset INTEGER NOT NULL,
 PRIMARY KEY(delivery_id,message_id));
CREATE TABLE IF NOT EXISTS message_delivery_holds(
 kind TEXT NOT NULL,delivery_id TEXT NOT NULL,worker_id TEXT NOT NULL,
 thread_id TEXT,turn_id TEXT,input_id TEXT,state TEXT NOT NULL,
 reason TEXT,created_at INTEGER NOT NULL,PRIMARY KEY(kind,delivery_id));
-- A worker's unresolved deliveries: admission asks for each idle worker on every pass, which
-- without it read every hold (16,351 in the Linux pilot) for each one.
CREATE INDEX IF NOT EXISTS message_hold_worker_idx ON message_delivery_holds(worker_id,state);
"""


def _page_limits(limit: int, max_bytes: int) -> None:
    if type(limit) is not int or not 1 <= limit <= 100:
        raise HubError("INVALID_INPUT", "Talk pages require a limit from 1 to 100", 422)
    if type(max_bytes) is not int or not 512 <= max_bytes <= 65536:
        raise HubError("INVALID_INPUT", "Talk page bytes must be from 512 to 65536", 422)


MESSAGE_KINDS = ("notice", "question", "answer")


def _kind_fields(row) -> dict:
    """A question's or an answer's kind, and the question an answer answers; nothing for a notice."""
    if "kind" not in row.keys() or not row["kind"]:
        return {}
    return {"kind": row["kind"], **({"answers": row["answers"]} if row["answers"] else {})}


def _fields(payload: dict, required: set[str], optional: set[str] | None = None) -> None:
    if not required <= payload.keys() or payload.keys() - required - (optional or set()):
        raise HubError("INVALID_INPUT", "Unexpected or missing peer-talk fields", 422)


class PeerTalk:
    if TYPE_CHECKING:
        db: Any
        state_dir: Path
        clock: Any
        _active: Any
        _role: Any
        _get: Any

    def _peer_reader(self, c, actor: Actor, worker_id: str | None = None) -> str:
        self._role(actor, Role.OPERATOR, Role.WORKER, Role.RUNNER)
        if actor.role == Role.WORKER:
            self._active(c, actor)
            worker_id = worker_id or actor.worker_id
        if not worker_id:
            raise HubError("INVALID_INPUT", "Select a project worker inbox", 422)
        worker_id = getattr(self, "_resolve_worker_id", lambda _, value: value)(c, worker_id)
        self._get(c, "workers", worker_id)
        return str(worker_id)

    def _peer_conversation(self, c, conversation_id: str, actor: Actor) -> dict:
        self._role(actor, Role.OPERATOR, Role.WORKER)
        if actor.role == Role.WORKER:
            self._active(c, actor)
        row = c.execute("SELECT * FROM peer_conversations WHERE id=?", (conversation_id,)).fetchone()
        if row is None:
            raise HubError("NOT_FOUND", "Project conversation does not exist", 404)
        return dict(row)

    def _peer_talk_command(self, c, name: str, p: dict, actor: Actor) -> dict:
        """Transaction-only hook. Sender identity always comes from the fenced actor."""
        self._role(actor, Role.WORKER)
        attempt, _ = self._active(c, actor)
        sender = attempt["worker_id"]
        if name == "talk.ack":
            _fields(p, {"delivery_id"})
            return self._peer_ack(c, p["delivery_id"], sender)
        if name != "talk.send":
            raise HubError("UNKNOWN_COMMAND", "Unknown peer-talk operation", 422)
        _fields(p, {"message_id", "text"}, {"recipient_id", "kind", "answers"})
        kind, question = p.get("kind", "notice"), p.get("answers")
        if kind not in MESSAGE_KINDS:
            raise HubError("INVALID_INPUT", "A message is a notice, a question or an answer", 422)
        if (kind == "answer") != (question is not None):
            raise HubError("INVALID_INPUT", "An answer, and only an answer, names the question it answers "
                           "(answers: the question's message id)", 422)
        asker = self._asker(c, question, sender) if kind == "answer" else None
        recipient, client_id, text = p.get("recipient_id") or asker, p["message_id"], p["text"]
        if (not isinstance(recipient, str) or not recipient
                or not isinstance(client_id, str) or not client_id.strip() or len(client_id) > 200
                or not isinstance(text, str) or not text.strip()):
            raise HubError("INVALID_INPUT", "Recipient, stable message ID, and text are required", 422)
        recipient = getattr(self, "_resolve_worker_id", lambda _, value: value)(c, recipient)
        if asker is not None and recipient != asker:
            raise HubError("INVALID_RECIPIENT", "An answer goes to the worker who asked the question", 422)
        try:
            text_size = len(text.encode("utf-8"))
        except UnicodeError as exc:
            raise HubError("INVALID_INPUT", "Peer text must be valid UTF-8", 422) from exc
        if text_size > MAX_TALK_TEXT_BYTES:
            raise HubError("TALK_TOO_LARGE", "Peer text is limited to 16384 UTF-8 bytes", 413)
        if recipient == sender:
            raise HubError("INVALID_RECIPIENT", "Messages require another project worker", 422)
        self._get(c, "workers", recipient)
        # A notice keeps the hash it had before messages had kinds, so its replays still match.
        request_hash = digest({"recipient_id": recipient, "text": text,
                               **({"kind": kind, "answers": question} if kind != "notice" else {})})
        prior = c.execute("SELECT * FROM peer_messages WHERE sender_id=? AND client_message_id=?",
                          (sender, client_id)).fetchone()
        if prior is not None:
            if prior["request_hash"] != request_hash:
                raise HubError("IDEMPOTENCY_CONFLICT", "Message ID was reused with different data")
            return self._peer_send_receipt(self._peer_message(c, prior["id"]))
        now = self.clock()
        low, high = sorted((sender, recipient))
        conversation_id = uuid5(NAMESPACE_URL, canonical(["peer-talk", low, high])).hex
        message_id = uuid5(NAMESPACE_URL, canonical(["peer-message", sender, client_id])).hex
        c.execute("INSERT OR IGNORE INTO peer_conversations(id,worker_low,worker_high,created_at) "
                  "VALUES(?,?,?,?)", (conversation_id, low, high, now))
        content = {"schema_version": 1, "id": message_id, "conversation_id": conversation_id,
                   "sender_id": sender, "recipient_id": recipient,
                   "created_at": now, "text": text}
        encoded = canonical(content)
        c.execute(
            "INSERT INTO peer_messages(id,conversation_id,sender_id,recipient_id,client_message_id,"
            "request_hash,content_hash,text_length,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (message_id, conversation_id, sender, recipient, client_id, request_hash,
             hashlib.sha256(encoded.encode("utf-8")).hexdigest(), len(text), now))
        c.execute("INSERT INTO peer_append_intents VALUES(?,?)", (message_id, encoded))
        if kind != "notice":
            c.execute("INSERT INTO peer_message_kinds VALUES(?,?,?)", (message_id, kind, question))
        wake = getattr(self, "_wake_for_message", None)
        if wake is not None:
            # A notice leaves a waiting recipient waiting: it reads it when it next works.
            wake(c, recipient, message_id, notice=kind == "notice")
        answered = getattr(self, "_wake_answered", None)
        if kind == "answer" and answered is not None:
            answered(c, question)
        return self._peer_send_receipt(self._peer_message(c, message_id))

    @staticmethod
    def _peer_message(c, message_id: str):
        return c.execute("SELECT m.*,k.kind,k.answers FROM peer_messages m LEFT JOIN peer_message_kinds k "
                         "ON k.message_id=m.id WHERE m.id=?", (message_id,)).fetchone()

    @staticmethod
    def _asker(c, question_id, sender) -> str:
        """Who asked the question `question_id`, which only `sender`, its addressee, may answer."""
        row = c.execute("SELECT m.sender_id,m.recipient_id FROM peer_messages m JOIN peer_message_kinds k "
                        "ON k.message_id=m.id AND k.kind='question' WHERE m.id=?", (question_id,)).fetchone()
        if row is None:
            raise HubError("NOT_FOUND", "No question has this message id", 404)
        if row["recipient_id"] != sender:
            raise HubError("FORBIDDEN", "Only the worker a question was asked of answers it", 403)
        return str(row["sender_id"])

    def _peer_send_receipt(self, row) -> dict:
        return {**{key: row[key] for key in
                   ("id", "conversation_id", "sequence", "sender_id", "recipient_id", "created_at")},
                **_kind_fields(row), "native_message": self._peer_reference(row)}

    def _peer_reference(self, row) -> dict:
        return {"path": str(self._peer_file(row["conversation_id"], row["id"])),
                "sha256": row["content_hash"], "format": "json"}

    def _peer_file(self, conversation_id: str, message_id: str, *, create=False) -> Path:
        for value in (conversation_id, message_id):
            if len(value) != 32 or any(char not in "0123456789abcdef" for char in value):
                raise HubError("TALK_CONTENT_CORRUPT", "Invalid private transcript identifier")
        directory = _normal_path(self.state_dir)
        for part in ("private", "peer-talk", conversation_id):
            directory = directory / part
            if directory.is_symlink():
                raise HubError("TALK_CONTENT_CORRUPT", "Private transcripts cannot use linked paths")
            if create:
                directory.mkdir(exist_ok=True)
        target = directory / (message_id + ".json")
        if target.is_symlink():
            raise HubError("TALK_CONTENT_CORRUPT", "Private transcripts cannot use linked files")
        return target

    def _write_peer_intent(self, row) -> None:
        body = row["content_json"].encode("utf-8")
        if hashlib.sha256(body).hexdigest() != row["content_hash"]:
            raise HubError("TALK_CONTENT_CORRUPT", "Private append intent failed integrity checking")
        path = self._peer_file(row["conversation_id"], row["message_id"], create=True)
        if path.exists():
            if path.stat().st_size != len(body) or path.read_bytes() != body:
                raise HubError("TALK_CONTENT_CORRUPT", "Private transcript conflicts with append intent")
            return
        fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".pending-")
        temporary_path = Path(temporary)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary_path, path)
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if temporary_path.exists():
                temporary_path.unlink()

    def _reconcile_peer_talk(self, c) -> int:
        if c.in_transaction:
            raise RuntimeError("Private transcript reconciliation must follow transaction commit")
        count = 0
        while rows := c.execute(
                "SELECT i.message_id,i.content_json,m.conversation_id,m.content_hash "
                "FROM peer_append_intents i JOIN peer_messages m ON m.id=i.message_id "
                "ORDER BY m.sequence LIMIT 100").fetchall():
            for row in rows:
                self._write_peer_intent(row)
                # A crash before this atomic deletion replays the identical append safely.
                c.execute("DELETE FROM peer_append_intents WHERE message_id=?", (row["message_id"],))
                count += 1
        return count

    async def reconcile_peer_talk(self) -> int:
        """Recover committed append intents; never invoke this inside a write callback."""
        return await self.db.call(self._reconcile_peer_talk)

    def _peer_materialized_read(self, c, authorize, read) -> dict:
        """Read only after committed files are ready, in one database queue job.

        Files must be materialized outside a transaction: an uncommitted send
        must never leave a private transcript on rollback. Once reconciled,
        hold the writer lock through the read and any delivery receipt. Another
        connection may have committed while that lock was being acquired; in
        that case release it and reconcile its durable append intent first.
        The read callback rechecks authorization under the acquired lock.
        """
        while True:
            authorize(c)
            self._reconcile_peer_talk(c)
            c.execute("BEGIN IMMEDIATE")
            try:
                if c.execute("SELECT 1 FROM peer_append_intents LIMIT 1").fetchone():
                    c.execute("ROLLBACK")
                    continue
                result = read(c)
                c.execute("COMMIT")
                return result
            except BaseException:
                if c.in_transaction:
                    c.execute("ROLLBACK")
                raise

    def _peer_text(self, row) -> str:
        try:
            path = self._peer_file(row["conversation_id"], row["id"])
            # Escaped control characters can take six JSON bytes per UTF-8 byte.
            if path.stat().st_size > MAX_TALK_TEXT_BYTES * 6 + 4096:
                raise HubError("TALK_CONTENT_CORRUPT", "Private transcript exceeds its size bound")
            body = path.read_bytes()
        except FileNotFoundError as exc:
            raise HubError("TALK_CONTENT_UNAVAILABLE", "Private transcript file is missing", 503) from exc
        if hashlib.sha256(body).hexdigest() != row["content_hash"]:
            raise HubError("TALK_CONTENT_CORRUPT", "Private transcript failed integrity checking")
        content = json.loads(body)
        return str(content["text"])

    def _peer_chunk(self, row, text: str, offset: int, end: int) -> dict:
        return {"id": row["id"], "conversation_id": row["conversation_id"],
                "sequence": row["sequence"], "sender_id": row["sender_id"],
                "sender_role": "worker", **_kind_fields(row), "native_message": self._peer_reference(row),
                **{key: row[key] for key in ("sender_display_name", "recipient_display_name") if key in row.keys()},
                "created_at": row["created_at"], "text": text[offset:end],
                "offset": offset, "next_offset": end, "complete": end == len(text)}

    @staticmethod
    def _peer_named_row(c, row, fields=("sender_id", "recipient_id")) -> dict:
        result = dict(row)
        for field in fields:
            worker = c.execute("SELECT * FROM workers WHERE id=?", (row[field],)).fetchone()
            named = worker_identity(dict(worker)) if worker is not None else {}
            if named.get("display_name"):
                stem = field[:-3] if field.endswith("_id") else field
                result[stem + "_display_name"] = named["display_name"]
        return result

    def _fit_peer_chunk(self, page: dict, row, text: str, offset: int, max_bytes: int) -> dict | None:
        low, high = offset, len(text)
        while low < high:
            middle = (low + high + 1) // 2
            candidate = self._peer_chunk(row, text, offset, middle)
            test = {**page, "messages": [*page["messages"], candidate], "has_more": False}
            if len(canonical(test).encode("utf-8")) <= max_bytes:
                low = middle
            else:
                high = middle - 1
        return self._peer_chunk(row, text, offset, low) if low > offset else None

    async def peer_conversations(self, actor: Actor, *, worker_id: str | None = None,
                                 after_id: str = "", limit: int = 50,
                                 max_bytes: int = 8192) -> dict:
        _page_limits(limit, max_bytes)
        if (not isinstance(after_id, str) or (after_id and
                (len(after_id) != 32 or any(char not in "0123456789abcdef" for char in after_id)))):
            raise HubError("INVALID_INPUT", "Conversation cursor must be an exact conversation ID", 422)

        def read(c):
            worker = self._peer_reader(c, actor, worker_id)
            rows = c.execute(
                "SELECT p.*, (SELECT count(*) FROM peer_messages m WHERE m.conversation_id=p.id "
                "AND m.recipient_id=? AND m.acknowledged_at IS NULL) AS unread_count "
                "FROM peer_conversations p WHERE (worker_low=? OR worker_high=?) AND id>? "
                "ORDER BY id LIMIT ?", (worker, worker, worker, after_id, limit + 1)).fetchall()
            page: dict[str, Any] = {"worker_id": worker, "conversations": [],
                                    "next_after_id": after_id, "has_more": True}
            for row in rows[:limit]:
                named = self._peer_named_row(c, row, ("worker_low", "worker_high"))
                candidate = {**page, "conversations": [*page["conversations"], named],
                             "next_after_id": row["id"], "has_more": False}
                if len(canonical(candidate).encode("utf-8")) > max_bytes:
                    break
                page = candidate
            page["has_more"] = len(page["conversations"]) < len(rows)
            if rows and not page["conversations"]:
                raise HubError("TALK_PAGE_TOO_SMALL", "Talk metadata exceeds the page budget", 413)
            return page
        return await self.db.call(read)

    async def peer_history(self, conversation_id: str, actor: Actor, *, worker_id: str | None = None,
                           after_sequence: int = 0,
                           offset: int = 0, limit: int = 50, max_bytes: int = 8192) -> dict:
        """Participant/operator read, paged by the returned sequence and character offset."""
        _page_limits(limit, max_bytes)
        if (type(after_sequence) is not int or not 0 <= after_sequence <= 9223372036854775807
                or type(offset) is not int or offset < 0):
            raise HubError("INVALID_INPUT", "History cursors must be nonnegative integers", 422)

        def authorize(c):
            conversation = self._peer_conversation(c, conversation_id, actor)
            if worker_id is not None:
                selected = self._peer_reader(c, actor, worker_id)
                if selected not in {conversation["worker_low"], conversation["worker_high"]}:
                    raise HubError("FORBIDDEN", "Selected worker is not a conversation participant", 403)

        def read(c):
            authorize(c)
            rows = c.execute("SELECT m.*,k.kind,k.answers FROM peer_messages m LEFT JOIN peer_message_kinds k "
                             "ON k.message_id=m.id WHERE m.conversation_id=? AND m.sequence>? "
                             "ORDER BY m.sequence LIMIT ?",
                             (conversation_id, after_sequence, limit + 1)).fetchall()
            page: dict[str, Any] = {"conversation_id": conversation_id, "messages": [],
                                    "next_after_sequence": after_sequence, "next_offset": offset,
                                    "has_more": True}
            for row in rows[:limit]:
                text = self._peer_text(row)
                row = self._peer_named_row(c, row)
                start = offset if not page["messages"] else 0
                if start >= len(text):
                    raise HubError("INVALID_INPUT", "History offset is outside the message", 422)
                # Reserve cursor widths before fitting text so final metadata cannot overflow.
                fit = {**page, "next_after_sequence": row["sequence"], "next_offset": len(text)}
                chunk = self._fit_peer_chunk(fit, row, text, start, max_bytes)
                if chunk is None:
                    break
                page["messages"].append(chunk)
                page["next_offset"] = 0 if chunk["complete"] else chunk["next_offset"]
                if chunk["complete"]:
                    page["next_after_sequence"] = row["sequence"]
                else:
                    break
            page["has_more"] = (len(page["messages"]) < len(rows)
                                or bool(page["messages"] and not page["messages"][-1]["complete"]))
            if rows and not page["messages"]:
                raise HubError("TALK_PAGE_TOO_SMALL", "Talk metadata exceeds the page budget", 413)
            if not rows and offset:
                raise HubError("INVALID_INPUT", "History offset requires a message", 422)
            return page
        return await self.db.call(lambda c: self._peer_materialized_read(c, authorize, read))

    async def peer_unread(self, actor: Actor, *, worker_id: str | None = None,
                          limit: int = 50, max_bytes: int = 8192,
                          reference_only: bool = False) -> dict:
        """Prepare bounded delivery without acknowledgment; operators only inspect the inbox."""
        _page_limits(limit, max_bytes)

        def authorize(c):
            return self._peer_reader(c, actor, worker_id)

        def prepare(c):
            worker = authorize(c)
            rows = c.execute("SELECT m.*,k.kind,k.answers FROM peer_messages m "
                             "LEFT JOIN peer_message_kinds k ON k.message_id=m.id WHERE m.recipient_id=? "
                             "AND acknowledged_at IS NULL AND NOT EXISTS("
                             "SELECT 1 FROM peer_delivery_items i JOIN message_delivery_holds h "
                             "ON h.delivery_id=i.delivery_id AND h.kind='peer' "
                             "WHERE i.message_id=m.id AND h.state IN ('sending','uncertain')) ORDER BY sequence LIMIT ?",
                             (worker, limit + 1)).fetchall()
            delivery = (uuid4().hex if rows and (actor.role == Role.RUNNER or
                        actor.role == Role.WORKER and actor.worker_id == worker) else None)
            page: dict[str, Any] = {"worker_id": worker, "delivery_id": delivery,
                                    "messages": [], "has_more": True}
            if reference_only:
                page.update(delivery_mode="native_reference", instruction=NATIVE_MESSAGE_INSTRUCTION)
            for row in rows[:limit]:
                text = self._peer_text(row)
                row = self._peer_named_row(c, row)
                if reference_only:
                    chunk = self._peer_chunk(row, text, row["acknowledged_offset"], len(text))
                    del chunk["text"]
                    if len(canonical({**page, "messages": [*page["messages"], chunk]}).encode("utf-8")) > max_bytes:
                        break
                else:
                    fitted = self._fit_peer_chunk(page, row, text, row["acknowledged_offset"], max_bytes)
                    if fitted is None:
                        break
                    chunk = fitted
                page["messages"].append(chunk)
                if not chunk["complete"]:
                    break
            page["has_more"] = (len(page["messages"]) < len(rows)
                                or bool(page["messages"] and not page["messages"][-1]["complete"]))
            if rows and not page["messages"]:
                raise HubError("TALK_PAGE_TOO_SMALL", "Talk metadata exceeds the page budget", 413)
            if delivery is not None:
                c.execute("INSERT INTO peer_deliveries VALUES(?,?,?,NULL)",
                          (delivery, worker, self.clock()))
                c.executemany("INSERT INTO peer_delivery_items VALUES(?,?,?,?)", [
                    (delivery, item["id"], item["offset"], item["next_offset"])
                    for item in page["messages"]])
            return page
        return await self.db.call(lambda c: self._peer_materialized_read(c, authorize, prepare))

    def _peer_ack(self, c, delivery_id: str, worker_id: str) -> dict:
        if not isinstance(delivery_id, str) or not delivery_id:
            raise HubError("INVALID_INPUT", "A prepared peer delivery ID is required", 422)
        delivery = c.execute("SELECT * FROM peer_deliveries WHERE id=?", (delivery_id,)).fetchone()
        if delivery is None or delivery["worker_id"] != worker_id:
            raise HubError("FORBIDDEN", "Only the recipient can acknowledge a prepared delivery", 403)
        if delivery["acknowledged_at"] is None:
            for row in c.execute(
                    "SELECT i.*,m.acknowledged_offset,m.text_length,m.recipient_id "
                    "FROM peer_delivery_items i JOIN peer_messages m ON m.id=i.message_id "
                    "WHERE i.delivery_id=?", (delivery_id,)).fetchall():
                if row["recipient_id"] != worker_id or row["acknowledged_offset"] < row["start_offset"]:
                    raise HubError("TALK_ACK_GAP", "Delivery cannot skip undelivered text")
                offset = max(row["acknowledged_offset"], row["end_offset"])
                c.execute("UPDATE peer_messages SET acknowledged_offset=?,"
                          "acknowledged_at=CASE WHEN ?>=text_length THEN COALESCE(acknowledged_at,?) "
                          "ELSE acknowledged_at END WHERE id=?",
                          (offset, offset, self.clock(), row["message_id"]))
                consume = getattr(self, "_consume_message_wake", None)
                if offset >= row["text_length"] and consume is not None:
                    consume(c, worker_id, row["message_id"])
            c.execute("UPDATE peer_deliveries SET acknowledged_at=? WHERE id=?",
                      (self.clock(), delivery_id))
        return {"delivery_id": delivery_id, "acknowledged": True}
