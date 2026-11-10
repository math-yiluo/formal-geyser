from __future__ import annotations

import re
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class Role(StrEnum):
    OPERATOR = "operator"
    WORKER = "worker"
    RUNNER = "runner"
    VALIDATOR = "validator"


class Actor(StrictModel):
    role: Role
    worker_id: str | None = None
    attempt_id: str | None = None
    fencing_token: int | None = None


class Limits(StrictModel):
    packet_bytes: int = Field(default=32768, ge=512)
    delta_bytes: int = Field(default=8192, ge=256)


# The one Codex version whose app-server protocol this runtime was audited against.
AUDITED_CODEX_VERSION = "0.160.0"


class RoleEfforts(StrictModel):
    """A reviewer's and the Integrator's effort as a project chose it before the [reviewers] and
    [integrator] sections, which take precedence. Neither takes the workers' `effort` any more."""
    review_effort: Literal["low", "medium", "high", "xhigh", "max"] | None = None
    integrator_effort: Literal["low", "medium", "high", "xhigh", "max"] | None = None

    def effort_for(self, kind: str) -> str | None:
        """This section's effort for a role: a reviewer's or the Integrator's own, else the workers'."""
        if kind in ("review", "integrator"):
            return self.review_effort if kind == "review" else self.integrator_effort
        return getattr(self, "effort")


def profile_entries(value: list[str], label: str) -> list[str]:
    """Profile names: distinct, and usable as a directory name."""
    if len(set(value)) != len(value):
        raise ValueError(f"{label} profile names must be distinct")
    for name in value:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,31}", name):
            raise ValueError(f"A {label} profile name is 1 to 32 letters, digits, dots, dashes or underscores")
    return value


class CodexConfig(RoleEfforts):
    home: str | None = Field(default=None, min_length=1)
    executable: str = "codex"
    version: str = AUDITED_CODEX_VERSION
    # The workers' model and effort on Codex. No default: each is chosen when the project is
    # initialized (or in Project setup), and a run starts only once every role has both.
    model: str | None = None
    effort: str | None = None
    stop_grace_seconds: float = Field(default=10.0, gt=0)
    max_frame_bytes: int = Field(default=67108864, ge=1024)
    # Accounts used one at a time, in order of preference (see codex_profiles): the first is
    # ``home`` itself, each other one ``home/profiles/<name>``. Empty: ``home`` alone, and a usage
    # limit holds Codex until the operator resumes.
    profiles: list[str] = Field(default_factory=list, max_length=32)

    @field_validator("profiles")
    @classmethod
    def profile_entries(cls, value: list[str]) -> list[str]:
        return profile_entries(value, "Codex")


Backend = Literal["codex", "claude"]
BACKENDS: tuple[str, ...] = ("codex", "claude")


class ClaudeConfig(RoleEfforts):
    """Claude Code driven through the pinned Agent SDK, billed only to a Claude subscription.

    ``home`` is the CLAUDE_CONFIG_DIR in which the operator signed in once with ``/login``;
    further sign-ins are ``profiles``. The runtime never reads or forwards a credential and
    refuses API-key billing.
    """
    home: str | None = Field(default=None, min_length=1)
    # None selects the Claude Code binary bundled with the pinned SDK.
    executable: str | None = Field(default=None, min_length=1)
    sdk_version: str = "0.2.163"
    cli_version: str = "2.1.286"
    # The workers' model and effort on Claude; no default, as for Codex.
    model: str | None = None
    effort: Literal["low", "medium", "high", "xhigh", "max"] | None = None
    # Hosts sandboxed commands may reach; "*" allows every non-local host.
    allowed_domains: list[str] = Field(default_factory=lambda: ["*"])
    stop_grace_seconds: float = Field(default=10.0, gt=0)
    max_frame_bytes: int = Field(default=67108864, ge=1024)
    # Subscription sign-ins used one at a time, in order of preference (see claude_profiles): the
    # first is ``home`` itself, each other one ``home/profiles/<name>``. Empty: ``home`` alone.
    profiles: list[str] = Field(default_factory=list, max_length=32)

    @field_validator("profiles")
    @classmethod
    def profile_entries(cls, value: list[str]) -> list[str]:
        return profile_entries(value, "Claude")

    @field_validator("allowed_domains")
    @classmethod
    def domain_entries(cls, value: list[str]) -> list[str]:
        for entry in value:
            if not entry or len(entry) > 253 or any(ch.isspace() or ch in "/\\\"'" for ch in entry):
                raise ValueError("Allowed domains must be host names, wildcard patterns, or '*'")
        return value


class RepositoryConfig(StrictModel):
    enabled: bool = False
    branch: str = Field(default="main", min_length=1, max_length=200)
    executable: str = Field(default="git", min_length=1, max_length=4000)

    @field_validator("branch")
    @classmethod
    def branch_name(cls, value: str) -> str:
        from .git_transport import validate_branch

        return validate_branch(value)


class GitSyncConfig(StrictModel):
    enabled: bool = False
    remote: str | None = Field(default=None, min_length=1, max_length=4000)
    poll_seconds: float = Field(default=15.0, ge=1, le=3600)
    timeout_seconds: float = Field(default=60.0, ge=1, le=600)
    credential_helper: str | None = Field(default=None, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")

    @field_validator("remote")
    @classmethod
    def remote_location(cls, value: str | None) -> str | None:
        if value is None:
            return None
        from .git_transport import normalize_remote

        return normalize_remote(value)

    @model_validator(mode="after")
    def configured_remote(self):
        if self.enabled and self.remote is None:
            raise ValueError("Enabled Git synchronization requires a configured remote")
        return self


class LeanServiceConfig(StrictModel):
    """The project's Lean service, which starts every Lean and Lake process (lean_service)."""
    threads: int | None = Field(default=None, ge=1, le=1024)  # Thread capacity; default: logical cores.
    reserve_gib: float = Field(default=4, ge=0, allow_inf_nan=False)  # Kept free for the system and agents.
    commit_reserve_gib: float = Field(default=8, ge=0, allow_inf_nan=False)
    low_watermark_gib: float = Field(default=3, ge=0, allow_inf_nan=False)  # Below it nothing new starts.
    critical_watermark_gib: float = Field(default=1.5, ge=0, allow_inf_nan=False)  # Below it a job is suspended.
    per_owner: int = Field(default=2, ge=1, le=64)  # Running jobs per worker within a class.
    aging_minutes: float = Field(default=20, gt=0, allow_inf_nan=False)
    # Blocking validations that may run at once ahead of interactive checks.
    validation_slots: int = Field(default=4, ge=0, le=64)
    # The most threads a validation starts with. Its build restores the Library from Lake's cache
    # and compiles the few modules a candidate changes: in the Linux pilot, of 5,236 validations
    # in three days, the median used 0.43 cores, the 99th percentile 0.93, the most 7.3, yet each
    # was given threads for all 18,000 modules, so one or two held the machine at a time.
    validation_threads: int = Field(default=4, ge=1, le=1024)
    # The most Lake builds at once, of every lane. Each holds a build slot with a full copy of the
    # Library's build: in the Linux pilot 9.6 GB a slot, and 13 builds at once filled the disk.
    # A build past it waits, first come first served, for a slot to free; 0: no limit.
    build_slots: int = Field(default=8, ge=0, le=1024)
    # Worker processes for each check's reading, syncing and hashing of the Library sources and
    # build outputs; 0 runs that work in the service's own threads, which share one interpreter:
    # in the Linux pilot it held the service at one core while the machine was 80% idle.
    bookkeeping_processes: int = Field(default=8, ge=0, le=64)
    sandbox: Literal["codex", "none"] = "codex"
    warm_gib: float = Field(default=6, ge=0, allow_inf_nan=False)  # Library .olean pages kept resident.
    sessions: int = Field(default=6, ge=0, le=64)  # Resident development-check sessions; 0 turns them off.
    session_idle_minutes: float = Field(default=10, gt=0, allow_inf_nan=False)
    # A development check, or a validation, that has not ended after this long is stopped and
    # ends unavailable (0: no limit); its suspended time does not count. Lean's heartbeat limit
    # bounds each declaration's elaboration; this bounds what it cannot, such as a kernel
    # reduction, and the inspection and kernel replay that run without heartbeats. In the Linux
    # pilot the longest of 1,198 development checks took 11 minutes, of 210 validations 13.
    check_minutes: float = Field(default=30, ge=0, allow_inf_nan=False)
    validation_minutes: float = Field(default=60, ge=0, allow_inf_nan=False)
    # A check one of whose Lean processes holds more private memory than this (resident or
    # swapped out) is stopped and ends unavailable, as one past its time (0: no limit). In the
    # Linux pilot two inspections grew past 8 GB, the machine's memory ran out, every check was
    # stopped and queued again, and they started over first: no check ended for 12 minutes. Of
    # 11,500 checks there that ended, the largest held 13.9 GB in all, all but three less than 9.
    process_memory_gib: float = Field(default=12, ge=0, allow_inf_nan=False)
    # The most memory the Lean service and every process it starts may hold together, which the
    # kernel enforces on its scope (MemoryMax): past it the kernel reclaims and then ends one of
    # the service's own processes, never TaskHub's. Absent: 60% of the machine's memory; 0: no
    # bound. On the pilot on 2026-11-09 the memory ran out with Lean, TaskHub and the workers'
    # processes all scored alike, and the kernel ended TaskHub, and with it every worker.
    service_memory_gib: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    # How long a submission waits for its validation before the validation continues in the
    # background and its result arrives as a notification.
    validation_wait_seconds: float = Field(default=90, ge=0, allow_inf_nan=False)
    # Days a finished check keeps its result (for its caller and for reuse by content) and its
    # specification; after them only its telemetry stays. In the Linux pilot every check that ran
    # again with the same content did so within a day of the first, and seven days of results
    # held 64 GB (lean_service/store.py).
    result_days: float = Field(default=2, gt=0, le=90, allow_inf_nan=False)
    # Each hour, at most this much of the idle build slots' identical outputs is compared and
    # shared on disk where the filesystem shares extents (XFS with reflink, btrfs; not ext4):
    # each slot holds a full copy of the Library's build (lake_dedupe.py). 0 turns it off.
    share_outputs_gib: float = Field(default=32, ge=0, allow_inf_nan=False)


Effort = Literal["low", "medium", "high", "xhigh", "max"]


class DecisionConfig(StrictModel):
    """The Dispatcher's model, which decides task assignment and recruitment, each decision in a
    new, small conversation without tools. No default; the operator's recorded choice overrides it."""
    model: str | None = Field(default=None, min_length=1, max_length=200)
    effort: Effort | None = None


class RoleModelConfig(StrictModel):
    """The model and effort of a Codex role's new conversations: the reviewers', the Integrator's.
    No default; the operator's recorded choice overrides it."""
    model: str | None = Field(default=None, min_length=1, max_length=200)
    effort: str | None = Field(default=None, min_length=1, max_length=100)


RotationMode = Literal["conservative", "aggressive"]


class RotationConfig(StrictModel):
    """When a conversation is replaced by a new one that continues the same task. Conservative
    rotates a conversation idle for idle_minutes; aggressive also rotates one whose context
    exceeds context_tokens. The operator's recorded choice overrides the mode."""
    mode: RotationMode = "conservative"
    idle_minutes: int = Field(default=40, ge=1, le=24 * 60)
    context_tokens: int = Field(default=100_000, ge=20_000, le=1_000_000)


class AssignmentConfig(StrictModel):
    """The backend and rotation mode each new agent gets, by role. A list applies by worker
    number and repeats: Worker 0 takes its first entry, Worker 1 the next. What this leaves
    open follows the operator's backend choice and the project's rotation mode; the operator
    can still change an agent's mode later. Reviewers and the Integrator run on Codex, whatever
    backend workers use: their models are chosen for Codex only (session_settings), so
    reviewer_backend and integrator_backend, kept for older project files, may only say so."""
    worker_backends: list[Backend] = Field(default_factory=list, max_length=64)
    worker_rotation: list[RotationMode] = Field(default_factory=list, max_length=64)
    reviewer_backend: Backend | None = None
    reviewer_rotation: RotationMode | None = None
    integrator_backend: Backend | None = None

    @field_validator("reviewer_backend", "integrator_backend")
    @classmethod
    def codex_roles(cls, value: Backend | None) -> Backend | None:
        if value not in (None, "codex"):
            raise ValueError("Reviewers and the Integrator run on Codex; leave this out or set it to \"codex\"")
        return value

    def backend(self, kind: str, number: int | None) -> Backend | None:
        if kind == "ordinary":
            return self.worker_backends[number % len(self.worker_backends)] if (
                self.worker_backends and number is not None) else None
        return None

    def rotation(self, kind: str, number: int | None) -> RotationMode | None:
        if kind == "review":
            return self.reviewer_rotation
        if kind != "ordinary" or not self.worker_rotation or number is None:
            return None
        return self.worker_rotation[number % len(self.worker_rotation)]


class ConcurrencyConfig(StrictModel):
    """How many sessions may run at once on each backend's account; -1 sets no limit, and 0
    starts no worker session. An ordinary worker beyond it waits for a free one. Reviewers and
    the Integrator are admitted anyway, since accepted work waits on them, and count; decision
    helpers do not count.
    While dispatch ramps up, an account's ordinary worker sessions start launch_seconds after
    its previous one, so a large project does not start all at once: until the account first
    fills, or launch_ramp_minutes after the service starts or the run starts or resumes
    (0: until it fills), whichever comes first."""
    codex: int = Field(default=-1, ge=-1, le=1024)
    claude: int = Field(default=-1, ge=-1, le=1024)
    launch_seconds: float = Field(default=0, ge=0, le=3600, allow_inf_nan=False)
    launch_ramp_minutes: float = Field(default=10, ge=0, le=24 * 60, allow_inf_nan=False)


class WaitConfig(StrictModel):
    """How long a wait for a person's answer (a worker's or the operator's) lasts before its worker
    reconsiders. No default; the operator's recorded choice overrides it."""
    answer_deadline_minutes: int | None = Field(default=None, ge=1, le=30 * 24 * 60)


class WakerConfig(StrictModel):
    """The model and effort of the waker (waker.py), which judges whether a subscribed task's newly
    published results meet a waiting worker's need. Chosen when the project is initialized; the
    operator's recorded choice overrides it. While neither chose one, wake decisions wait."""
    model: str | None = Field(default=None, min_length=1, max_length=200)
    effort: Effort | None = None


class ProjectConfig(StrictModel):
    schema_version: Literal[1] = 1
    name: str = "FormalGeyser"
    domain: str = "general"
    collaboration_mode: Literal["collaborative"] = "collaborative"
    limits: Limits = Field(default_factory=Limits)
    codex: CodexConfig = Field(default_factory=CodexConfig)
    claude: ClaudeConfig = Field(default_factory=ClaudeConfig)
    # The initial choice for fresh workers; the operator's recorded switch overrides it.
    # The assignment plan, where it names a backend, overrides both.
    new_worker_backend: Backend = "codex"
    assignment: AssignmentConfig = Field(default_factory=AssignmentConfig)
    concurrency: ConcurrencyConfig = Field(default_factory=ConcurrencyConfig)
    repository: RepositoryConfig = Field(default_factory=RepositoryConfig)
    sync: GitSyncConfig = Field(default_factory=GitSyncConfig)
    environment: dict[str, Any] = Field(default_factory=dict)
    lean_service: LeanServiceConfig = Field(default_factory=LeanServiceConfig)
    decisions: DecisionConfig = Field(default_factory=DecisionConfig)
    rotation: RotationConfig = Field(default_factory=RotationConfig)
    waits: WaitConfig = Field(default_factory=WaitConfig)
    waker: WakerConfig = Field(default_factory=WakerConfig)
    reviewers: RoleModelConfig = Field(default_factory=RoleModelConfig)
    integrator: RoleModelConfig = Field(default_factory=RoleModelConfig)

    @model_validator(mode="after")
    def sync_repository(self):
        if self.sync.enabled and not self.repository.enabled:
            raise ValueError("Git synchronization requires the managed repository to be enabled")
        return self

    def backend_config(self, backend: str) -> CodexConfig | ClaudeConfig:
        if backend == "codex":
            return self.codex
        if backend == "claude":
            return self.claude
        raise ValueError(f"Unknown native backend: {backend}")

    def runtime_profile(self, backend: str) -> dict[str, Any]:
        """The identity a native session is bound to; Codex keeps its historical shape."""
        if backend == "claude":
            return {"backend": "claude", **self.claude.model_dump(mode="json")}
        return self.backend_config(backend).model_dump(mode="json")


def _ignored_policy(schema: dict[str, Any]) -> None:
    # A call that still names the former field validates; the field is dropped before use.
    schema.setdefault("properties", {})["policy"] = {
        "type": "string", "description": "Ignored: a requirement no longer names a policy."}


class Requirement(StrictModel):
    """A dependency on an artifact or on a task revision. It holds once the artifact's newest version
    (or, for a task, an output of the completed task) is accepted, whatever kind it is: the kind is
    the provider's own (lean.definition, lean.statement, lean.proof), never chosen by the one who
    waits. Until 2026-11-07 a requirement named a policy too, and a wrong one never held."""
    model_config = ConfigDict(extra="forbid", strict=True, json_schema_extra=_ignored_policy)
    key: str = Field(min_length=1, max_length=128)
    task_id: str | None = None
    task_revision: int | None = Field(default=None, ge=1)
    artifact_id: str | None = None

    @model_validator(mode="before")
    @classmethod
    def legacy_policy(cls, value):
        # Stored contracts and predicates, and habits, still carry the former field.
        if isinstance(value, dict) and "policy" in value:
            value = {key: item for key, item in value.items() if key != "policy"}
        return value

    @model_validator(mode="after")
    def one_provider(self):
        if (self.task_id is None) == (self.artifact_id is None):
            raise ValueError("Specify exactly one task or artifact provider")
        if self.task_id and self.task_revision is None:
            raise ValueError("Task providers require an exact revision")
        return self


# A task objective or context that is only a file name, such as "objective.md": the stored
# task.json names its own files so, and a contract copied from one would name no target.
CONTRACT_FILE_NAME = re.compile(r"\s*[\w./-]{1,200}\.(?:md|markdown|txt|json)\s*", re.ASCII)


def file_name_fields(contract: Any) -> list[str]:
    """The objective or context fields of a contract (model or mapping) that are a file name."""
    fields = []
    for field in ("objective", "context"):
        value = contract.get(field) if isinstance(contract, dict) else getattr(contract, field, None)
        if isinstance(value, str) and CONTRACT_FILE_NAME.fullmatch(value):
            fields.append(field)
    return fields


class TaskContract(StrictModel):
    title: str = Field(min_length=1, max_length=300)
    objective: str = Field(min_length=1, max_length=20000)
    domain: str = "general"
    kind: str = "work"
    acceptance_policy: str = "general.peer@1"
    inputs: list[str] = Field(default_factory=list, max_length=200)
    requirements: list[Requirement] = Field(default_factory=list, max_length=128)
    constraints: list[str] = Field(default_factory=list, max_length=100)
    context: str = Field(default="", max_length=40000)
    priority: int = Field(default=1, ge=0, le=3)
    payload: dict[str, Any] = Field(default_factory=dict)


class Subscription(StrictModel):
    """A wait on another existing task: the waker, after each integration that publishes Lean from
    it, judges whether what the task published meets `need`, and then ends the wait."""
    task_id: str = Field(min_length=1, max_length=64)
    need: str = Field(min_length=1, max_length=2000)  # The results awaited, in words.


class Predicate(StrictModel):
    """What ends a pause. A task subscription (W1) is judged by the waker and ends the wait whatever
    `mode` says; the own-submission results (W2: validations, publications,
    completion_declaration_id) and a person's answer (W3: answers from workers, external_response_id
    from the operator) combine by `mode`. A wait for an answer also ends at the project's answer
    deadline. Since 2026-11-07 a pause takes no `requirements`: stored waits keep theirs."""
    mode: Literal["all", "any"] = "all"
    subscriptions: list[Subscription] = Field(default_factory=list, max_length=20)
    # Staged candidates whose machine validation result, whatever its verdict, is awaited.
    validations: list[str] = Field(default_factory=list, max_length=20)
    # Candidates of the waiter's own task whose outcome is awaited: published by the Integrator,
    # or rejected (in review or machine validation), deferred, withdrawn or superseded.
    publications: list[str] = Field(default_factory=list, max_length=20)
    # Questions this worker sent with talk_send (their message ids), each awaiting its answer.
    answers: list[str] = Field(default_factory=list, max_length=20)
    external_response_id: str | None = Field(default=None, min_length=1)
    completion_declaration_id: str | None = Field(default=None, min_length=1)

    def has_condition(self) -> bool:
        return bool(self.subscriptions or self.validations or self.publications or self.answers
                    or self.external_response_id or self.completion_declaration_id)

    def awaits_answer(self) -> bool:
        return bool(self.answers or self.external_response_id)


class Checkpoint(StrictModel):
    summary: str = Field(min_length=1, max_length=20000)
    next_action: str = Field(min_length=1, max_length=5000)
    input_refs: list[str] = Field(default_factory=list, max_length=200)


class ValidationReport(StrictModel):
    verdict: Literal["accepted", "rejected", "needs_operator", "unavailable"]
    policy: str
    details: dict[str, Any] = Field(default_factory=dict)
    qualifications: list[str] = Field(default_factory=list)


class Command(StrictModel):
    name: str
    key: str = Field(min_length=1, max_length=200)
    payload: dict[str, Any] = Field(default_factory=dict)
