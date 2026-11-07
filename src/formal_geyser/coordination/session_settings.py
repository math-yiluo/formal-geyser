"""What a new conversation starts with: its backend, model and effort.

The operator chooses a model and effort for workers on each backend, for reviewers and for the
Integrator (on Codex), the backend of the next new worker, and the backend an existing worker
moves to, and each account's number of concurrent sessions. Every model, effort and backend
choice applies only to conversations that begin after it. A role's new model and effort reach
the new conversations of workers that already exist only when the operator applies them to
existing workers; otherwise each of those keeps, for its new conversations on that backend,
what they started with until then (worker_kept_models). A conversation keeps the
backend, model and effort it began with for as long as it is continued, after an interruption
too. A worker moves to its requested backend when its next new conversation begins (a new
task, a rotation, a new batch) and that account can start a session; until then the request
waits.

No role takes another's model or effort, and none has a default: each is chosen in project.toml
or by the operator, and a run starts only once every role has both (Hub._require_choices).
"""
from __future__ import annotations

import json
import re
import time
from typing import TYPE_CHECKING, Any, get_args

from .errors import HubError
from .models import BACKENDS, Actor, Effort, Role

# session_limits holds each account's limit as the operator set it (-1: no limit). It replaced
# concurrency_preferences, in which 0 set no limit and -1 could not be stored: that table's rows
# move to it once, 0 becoming -1, and it stays empty.
# role_models holds each role's choice. It replaced role_model_preferences, in which reviewers and
# the Integrator shared the review row: that table's rows move to it once, the review row giving
# the Integrator its own as well, and it stays empty.
SESSION_SETTINGS_SCHEMA = """
CREATE TABLE IF NOT EXISTS role_model_preferences(
 role TEXT NOT NULL CHECK(role IN ('worker','review')),
 backend TEXT NOT NULL CHECK(backend IN ('codex','claude')),
 model TEXT,effort TEXT,updated_at INTEGER NOT NULL,PRIMARY KEY(role,backend));
CREATE TABLE IF NOT EXISTS role_models(
 role TEXT NOT NULL CHECK(role IN ('worker','review','integrator')),
 backend TEXT NOT NULL CHECK(backend IN ('codex','claude')),
 model TEXT,effort TEXT,updated_at INTEGER NOT NULL,PRIMARY KEY(role,backend));
INSERT OR IGNORE INTO role_models(role,backend,model,effort,updated_at)
 SELECT role,backend,model,effort,updated_at FROM role_model_preferences;
INSERT OR IGNORE INTO role_models(role,backend,model,effort,updated_at)
 SELECT 'integrator',backend,model,effort,updated_at FROM role_model_preferences WHERE role='review';
DELETE FROM role_model_preferences;
CREATE TABLE IF NOT EXISTS session_limits(
 backend TEXT PRIMARY KEY CHECK(backend IN ('codex','claude')),
 sessions INTEGER NOT NULL CHECK(sessions>=-1),updated_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS concurrency_preferences(
 backend TEXT PRIMARY KEY CHECK(backend IN ('codex','claude')),
 sessions INTEGER NOT NULL CHECK(sessions>=0),updated_at INTEGER NOT NULL);
INSERT OR IGNORE INTO session_limits(backend,sessions,updated_at)
 SELECT backend,CASE WHEN sessions=0 THEN -1 ELSE sessions END,updated_at FROM concurrency_preferences;
DELETE FROM concurrency_preferences;
CREATE TABLE IF NOT EXISTS worker_backend_requests(
 worker_id TEXT PRIMARY KEY REFERENCES workers(id),
 backend TEXT NOT NULL CHECK(backend IN ('codex','claude')),requested_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS worker_kept_models(
 worker_id TEXT NOT NULL REFERENCES workers(id),
 backend TEXT NOT NULL CHECK(backend IN ('codex','claude')),
 model TEXT,effort TEXT,kept_at INTEGER NOT NULL,PRIMARY KEY(worker_id,backend));
"""
# Each role has its own choice; reviewers and the Integrator run on Codex.
ROLES = {"ordinary": "worker", "review": "review", "integrator": "integrator"}
ROLE_BACKENDS = {"worker": BACKENDS, "review": ("codex",), "integrator": ("codex",)}
ROLE_EFFORT_KEYS = {"review": "review_effort", "integrator": "integrator_effort"}
# Codex lists each model's efforts in its catalog; Claude Code takes these.
CODEX_EFFORT = re.compile(r"[a-z]{1,20}")
# Admission reads the session limits many times a second; a change applies within this.
LIMITS_SECONDS = 2.0


def bound_settings(worker: dict) -> dict | None:
    """The model and effort the worker's current conversation began with; None without one."""
    if not worker.get("thread_id"):
        return None
    try:
        binding = json.loads(worker.get("profile_json") or "{}")
    except (TypeError, ValueError):
        return None
    settings = binding.get("settings") if isinstance(binding, dict) else None
    if isinstance(settings, dict) and settings.get("effort"):
        return {"model": settings.get("model"), "effort": settings["effort"]}
    profile = binding.get("codex_profile") if isinstance(binding, dict) else None
    if isinstance(profile, dict) and profile.get("effort"):
        # Bound before conversations kept their own settings: the project's, by role.
        return {"model": profile.get("model"),
                "effort": profile.get(ROLE_EFFORT_KEYS.get(str(worker.get("kind")), "effort")) or profile["effort"]}
    return None


class SessionSettings:
    if TYPE_CHECKING:
        config: Any
        db: Any
        clock: Any
        _get: Any
        _event: Any
        _role: Any
        _normalize_worker_fields: Any
        _dispatch_holds: Any
        _backend_full: Any

    _session_limits: tuple[float, dict[str, int]] | None = None

    def _session_limit(self, c, backend: str) -> int:
        """How many sessions may run at once on this backend's account (-1: no limit; 0: no
        worker session): the operator's setting, else the project's."""
        cached = self._session_limits
        if cached is None or time.monotonic() - cached[0] > LIMITS_SECONDS:
            cached = self._session_limits = (time.monotonic(), {
                row["backend"]: row["sessions"] for row in c.execute("SELECT backend,sessions FROM session_limits")})
        limits = cached[1]
        return limits[backend] if backend in limits else getattr(self.config.concurrency, backend)

    def _session_limits_view(self, c) -> dict:
        saved = {row["backend"] for row in c.execute("SELECT backend FROM session_limits")}
        return {backend: {"limit": self._session_limit(c, backend),
                          "source": "operator" if backend in saved else "config"} for backend in BACKENDS}

    def _configured_role(self, kind: str, backend: str) -> dict:
        """What project.toml chooses for this role on this backend: workers in [codex] or
        [claude], reviewers in [reviewers], the Integrator in [integrator] (or the older
        review_effort and integrator_effort). None for what it leaves open."""
        config = self.config.backend_config(backend)
        if kind in ("review", "integrator"):
            section = self.config.reviewers if kind == "review" else self.config.integrator
            return {"model": section.model, "effort": section.effort or config.effort_for(kind)}
        return {"model": config.model, "effort": config.effort}

    def _new_session_settings(self, c, kind: str, backend: str, worker_id: str | None = None) -> dict:
        """The model and effort a new conversation of this role starts with on this backend; for
        a worker that kept earlier ones, those. None where neither the operator nor project.toml
        chose."""
        role = ROLES.get(kind)
        row = c.execute("SELECT model,effort FROM worker_kept_models WHERE worker_id=? AND backend=?",
                        (worker_id, backend)).fetchone() if worker_id else None
        if row is None and role:
            row = c.execute("SELECT model,effort FROM role_models WHERE role=? AND backend=?",
                            (role, backend)).fetchone()
        configured = self._configured_role(kind, backend)
        return {"model": (row["model"] if row else None) or configured["model"],
                "effort": (row["effort"] if row else None) or configured["effort"]}

    def _role_models(self, c) -> dict:
        """Each role's model and effort per backend for new conversations: the operator's
        choice, else the project configuration; and how many existing workers keep earlier ones."""
        saved = {(row["role"], row["backend"]) for row in c.execute("SELECT role,backend FROM role_models")}
        kept: dict[tuple[str, str], int] = {}
        for row in c.execute("SELECT w.kind,k.backend,COUNT(*) AS count FROM worker_kept_models k JOIN workers w "
                             "ON w.id=k.worker_id AND w.backend=k.backend WHERE w.work_intention<>'retired' "
                             "AND w.state<>'retired' GROUP BY w.kind,k.backend"):
            if row["kind"] in ROLES:
                key = (ROLES[row["kind"]], row["backend"])
                kept[key] = kept.get(key, 0) + row["count"]
        return {role: {backend: {**self._new_session_settings(c, kind, backend),
                                 "source": "operator" if (role, backend) in saved
                                 else "config" if any(self._configured_role(kind, backend).values()) else None,
                                 "kept_by_existing": kept.get((role, backend), 0)}
                       for backend in ROLE_BACKENDS[role]}
                for role, kind in (("worker", "ordinary"), ("review", "review"), ("integrator", "integrator"))}

    def _claude_workers_possible(self) -> bool:
        """Whether Claude workers can start on this machine, so that their model must be chosen."""
        from .runtime.claude import claude_unavailable
        return claude_unavailable(self.config.claude) is None

    def _missing_role_choices(self, c) -> list[str]:
        """The worker, reviewer and Integrator choices still to make, in words."""
        roles = [("Workers on Codex", "ordinary", "codex"), ("Reviewers", "review", "codex"),
                 ("The Integrator", "integrator", "codex")]
        if self._claude_workers_possible():
            roles.insert(1, ("Workers on Claude", "ordinary", "claude"))
        missing = []
        for label, kind, backend in roles:
            chosen = self._new_session_settings(c, kind, backend)
            missing += [f"{label}: {field}" for field in ("model", "effort") if not chosen[field]]
        return missing

    async def session_settings(self, worker_id: str) -> dict:
        """The backend, role, model and effort of the worker's next turn: those its current
        conversation began with, or those a new conversation starts with now (`new`)."""
        def read(c):
            worker = self._get(c, "workers", worker_id)
            bound = bound_settings(worker)
            settings = bound or self._new_session_settings(c, worker["kind"], worker["backend"], worker["id"])
            return {"backend": worker["backend"], "kind": worker["kind"], **settings, "new": bound is None}
        return await self.db.read(read)

    def _adopt_requested_backend(self, c, worker_id: str) -> None:
        """A worker about to begin a new conversation moves to its requested backend when that
        account can start a session now (not held; for a worker, not full)."""
        row = c.execute("SELECT r.backend,w.backend AS current,w.thread_id,w.kind FROM worker_backend_requests r "
                        "JOIN workers w ON w.id=r.worker_id WHERE r.worker_id=?", (worker_id,)).fetchone()
        if row is None or row["thread_id"] is not None:
            return
        target = row["backend"]
        if target != row["current"]:
            if target in self._dispatch_holds(c) or (row["kind"] == "ordinary" and self._backend_full(c, target)):
                return
            c.execute("UPDATE workers SET backend=? WHERE id=?", (target, worker_id))
            self._event(c, "worker.backend_switched", worker_id, {"from": row["current"], "to": target},
                        worker_id=worker_id)
        c.execute("DELETE FROM worker_backend_requests WHERE worker_id=?", (worker_id,))

    def _session_settings_command(self, c, name: str, p: dict, actor: Actor):
        self._role(actor, Role.OPERATOR)
        if name == "runtime.set_role_model":
            role, backend, model, effort = p.get("role"), p.get("backend"), p.get("model"), p.get("effort")
            if role not in ROLE_BACKENDS:
                raise HubError("INVALID_INPUT", "Expected the worker, review or integrator role", 422)
            if backend not in ROLE_BACKENDS[role]:
                raise HubError("INVALID_INPUT", "Reviewers and the Integrator run on Codex" if role != "worker"
                               else "Expected the codex or claude backend", 422)
            if model is not None and (not isinstance(model, str) or not model.strip() or len(model) > 200
                                      or any(ch.isspace() for ch in model)):
                raise HubError("INVALID_INPUT", "Expected a model name, or null for the project's", 422)
            if effort is not None and (effort not in get_args(Effort) if backend == "claude"
                                       else not isinstance(effort, str) or not CODEX_EFFORT.fullmatch(effort)):
                raise HubError("INVALID_INPUT", "Expected an effort level, or null for the project's", 422)
            existing = p.get("apply_to_existing", False)
            if not isinstance(existing, bool):
                raise HubError("INVALID_INPUT", "Expected apply_to_existing to be true or false", 422)
            kinds = [kind for kind, name in ROLES.items() if name == role]
            before = {kind: self._new_session_settings(c, kind, backend) for kind in kinds}
            if model is None and effort is None:
                c.execute("DELETE FROM role_models WHERE role=? AND backend=?", (role, backend))
            else:
                c.execute("INSERT INTO role_models(role,backend,model,effort,updated_at) VALUES(?,?,?,?,?) "
                          "ON CONFLICT(role,backend) DO UPDATE SET model=excluded.model,effort=excluded.effort,"
                          "updated_at=excluded.updated_at", (role, backend, model, effort, self.clock()))
            if existing:
                c.execute(f"DELETE FROM worker_kept_models WHERE backend=? AND worker_id IN "
                          f"(SELECT id FROM workers WHERE kind IN ({','.join('?' * len(kinds))}))", (backend, *kinds))
            else:
                # Workers on this backend now keep what their new conversations started with until
                # now; one that already keeps earlier choices keeps those. A worker that moves here
                # later starts with the current choice.
                for kind in kinds:
                    if self._new_session_settings(c, kind, backend) != before[kind]:
                        c.execute("INSERT OR IGNORE INTO worker_kept_models(worker_id,backend,model,effort,kept_at) "
                                  "SELECT id,?,?,?,? FROM workers WHERE kind=? AND backend=? "
                                  "AND work_intention<>'retired' AND state<>'retired'",
                                  (backend, before[kind]["model"], before[kind]["effort"], self.clock(), kind, backend))
            self._event(c, "runtime.role_model_selected", "runtime",
                        {"role": role, "backend": backend, "model": model, "effort": effort,
                         "apply_to_existing": existing})
            return self._role_models(c)
        if name == "runtime.set_concurrency":
            backend, sessions = p.get("backend"), p.get("sessions")
            if backend not in BACKENDS:
                raise HubError("INVALID_INPUT", "Expected the codex or claude backend", 422)
            if sessions is not None and (not isinstance(sessions, int) or isinstance(sessions, bool)
                                         or not -1 <= sessions <= 1024):
                raise HubError("INVALID_INPUT", "Expected 0 to 1024 sessions, -1 for no limit, "
                               "or null for the project's", 422)
            if sessions is None:
                c.execute("DELETE FROM session_limits WHERE backend=?", (backend,))
            else:
                c.execute("INSERT INTO session_limits(backend,sessions,updated_at) VALUES(?,?,?) "
                          "ON CONFLICT(backend) DO UPDATE SET sessions=excluded.sessions,updated_at=excluded.updated_at",
                          (backend, sessions, self.clock()))
            self._event(c, "runtime.concurrency_selected", "runtime", {"backend": backend, "sessions": sessions})
            self._session_limits = None
            view = self._session_limits_view(c)
            self._session_limits = None  # Read again once this change is committed.
            return view
        if name == "worker.select_backend":
            self._normalize_worker_fields(c, p, "worker_id")
            worker = self._get(c, "workers", p["worker_id"])
            backend = p.get("backend")
            if backend not in BACKENDS:
                raise HubError("INVALID_INPUT", "Expected the codex or claude backend", 422)
            if worker["kind"] != "ordinary":
                raise HubError("INVALID_INPUT", "Only workers switch backends; reviewers, the Integrator "
                               "and the Dispatcher run on Codex", 422)
            if worker["work_intention"] == "retired" or worker["state"] == "retired":
                raise HubError("WORKER_RETIRED", "A retired worker starts no new conversation", 409)
            if backend == "claude" and backend != worker["backend"]:
                from .runtime.claude import claude_unavailable
                problem = claude_unavailable(self.config.claude)
                if problem:
                    raise HubError("BACKEND_UNAVAILABLE", problem, 409)
            c.execute("DELETE FROM worker_backend_requests WHERE worker_id=?", (worker["id"],))
            if backend != worker["backend"]:
                c.execute("INSERT INTO worker_backend_requests(worker_id,backend,requested_at) VALUES(?,?,?)",
                          (worker["id"], backend, self.clock()))
            self._event(c, "worker.backend_requested", worker["id"], {"backend": backend, "current": worker["backend"]},
                        worker_id=worker["id"])
            if worker["active_attempt"] is None:
                # A worker between conversations moves at once; a running one when it next begins one.
                self._adopt_requested_backend(c, worker["id"])
            current = self._get(c, "workers", worker["id"])
            pending = c.execute("SELECT backend FROM worker_backend_requests WHERE worker_id=?",
                                (worker["id"],)).fetchone()
            return {"worker_id": worker["id"], "backend": current["backend"],
                    "requested_backend": pending["backend"] if pending else None}
        return None
