# Worker operation reference

Read the relevant section when using its workflow. This reference is available
through `reading.on_demand`; it is not an initial reading checklist. The short
[worker guide](worker.md) contains the standing rules.
Named reference paths below are in `paths`; if absent inline, find the same name
in the file at `paths.reference_index`.

## Coordination gateway

The model has two TaskHub tools: `taskhub_actions` and `taskhub_call`. Use native
files and commands for ordinary work. Your standing instructions list every
operation you can call, with its arguments; call one with
`taskhub_call(operation="stage", arguments={"manifest_path": "submission.json"})`.
`taskhub_actions(operation="stage")` returns its exact schema when you need a
nested field; do not list actions each turn. Operation names below use this
gateway. The server enforces your role and execution authority.

## Native work and authority

Start with the task file and directory map; use native tools in your workspace.
Tasks, wiki, literature, submissions, and correspondence are authoritative files.
Read complete files referenced by large/truncated responses. Write every new note
as an ordinary source file. Keep private notes in `.private/notes`; they need no
registration. Exclude them and correspondence from public exports. Public notes and wiki share `wiki/README.md`. Write Markdown under `notes/` or
`wiki/`, add a title, relative link and one-sentence purpose to the README, and
submit the edited files with `wiki_submit`. Include an edited README in `paths`.
A note describing published declarations follows their exact Lean statements.
Keep essential guidance short; long notes belong in Reference notes. Independent
review and Integrator publication are still required; inline metadata is not prose.
The supplied read-only resource roots are reading sources only; never edit them
or add them to writable roots.

Read an artifact's files with native tools: a published artifact's Lean and Markdown
are in your Library checkout, a fetched record's `content_file` names its frozen copy,
and your own private records are in `.private/notes/records/`. Call `fetch` for what
only the ledger knows: an artifact's status, machine validation, reviews and
publication. Each `fetch` reads and checks the record's files on the server.

The network is open to any site: search it with your runtime's web tool if it has
one, and download with curl, Python or `git clone`, for example from arXiv or
GitHub, into `.private/downloads/`. Register every source you
cite or rely on with `literature_register`, and read a source's errata notes
(`notes/errata/<source id>.md`) before relying on it; the literature page in `wiki`
has the details. Read a PDF with `resource_pdf`: it extracts the text, and with
`render` also saves the pages as images under `.private/tmp/pdf-pages/` for
scanned pages, formulas and figures, which you open with your image viewer.
`pypdf` and `pypdfium2` are in the Python at `FORMAL_GEYSER_PYTHON`; PyMuPDF (`fitz`)
is not installed. Do not install PDF, rendering or OCR packages into your workspace.

Public `paths.drafts` contains unreviewed Lean/Markdown designs for
all workers to read. Edit pairs in `.private/drafts/<name>/`; use `draft_update`
with that source directory, task ID, current draft revision (initially zero), and
a short summary to share the complete next revision. The task's `drafts` reference
or `drafts/<task-id>/current.json` gives the current version. Explain missing
definitions/interfaces in the paired Markdown; a shared draft need not compile.
`lean_check` with `directory: ".private/drafts/<name>"` compiles a draft together
with your workspace's other Lean files, a draft file taking the place of a
workspace file of the same module; staging ignores `.private`. Draft sharing is
separate from wiki publication and never supplies accepted evidence. Copy ready
pairs into normal module paths for manifest submission, independent review,
validation, and merge. Never edit the shared revision files.

Workers and reviewers may maintain the two shared planning DAGs through `dag_update`.
Use `shared-dags.md` for their file format, checker, and revision-safe save flow.
These advisory graph edits are not public-note submissions or review evidence.

Use the supplied `Worker N` or `Reviewer N` identity in coordination. Use worker names with peers and in coauthor
lists. Preserve original identifiers in source files, quotations, native thread
IDs, and immutable receipts. Write project prose in English.

Use TaskHub boundaries for contracts, frozen submissions, verdicts, and
publication; file edits never publish, accept, or complete work. Current executors
and original publishers may revise tasks as equals with the expected revision
and a reason. An original publisher may also revise its task (`propose_revision`)
in a message turn; an executor who did not publish the task revises it in its own
task turn. Reconcile stale revisions, preserve the objective, and notify
dependents of material changes.

When you publish a task, write its conditions as the results it must deliver: what
to verify, correct, prove or publish. State a process step only when the result
depends on it, and do not copy standing guide rules into a contract; reviewers
check every stated condition. A revision invalidates every unpublished candidate
bound to the old revision, including accepted ones awaiting integration, which
must be resubmitted with `replaces`; when you can, revise before staging or once
nothing is pending.

Your workspace is refreshed automatically when your task is admitted: TaskHub
compares the saved public base, the current public commit and your files, keeps
your edits, and reports conflicts in `workspace_refresh`; resolve them before
depending on the refreshed source. There is no refresh operation: to wait for
another task's results, pause with a subscription to it.

Use `paths.knowledge_readme` to distinguish required guidance from reference
notes. Literature procedures and submission formats are read when needed.

## Task lifecycle

A work task is named `Task N` (Task 0 is the project's root). Every `task_id` you give takes
`Task N` or the id; a task object shows its exact `id` (for paths such as `drafts/<task-id>/`)
beside its `name`.

You may belong to several unfinished tasks but execute at most one. An ordinary
task permits one initial executor and one recruited collaborator. After the
first turn a separate decision opens the collaborator place or not; do not open it yourself.
Edit or withdraw only an authorized unused invitation. Suspension cancels it
permanently.

Normal turns continue the same task and conversation, and a normal turn end
starts the next turn at once. When blocked, call `pause` with a `reason`, a
`checkpoint` (a `summary` of the work and the concrete `next_action`), and a
`predicate` naming what ends the pause. Membership persists, and the task continues
in its conversation or, after a long idle period, in a new one (see below). A
predicate waits for one of three things:

- Another task's results: `{"subscriptions": [{"task_id": "<id>", "need": "<the
  results you await>"}]}`. After each publication from that task, the waker judges
  whether what it has published to the Library since you paused meets your `need`,
  and wakes you; check the Library yourself. A task cannot wait on itself, and a
  subscription that would close a cycle of tasks waiting on each other is refused.
  The wait also ends when that task is done or retired.
- Your own submission's result: `{"validations": ["<candidate id>"]}` for a staged
  candidate's machine validation; `{"publications": ["<candidate id>"]}` for the
  outcome of a candidate of your task (yours or a co-executor's) still under review
  or awaiting integration: published by the Integrator, or rejected, deferred,
  withdrawn or superseded, and you are told which; `{"completion_declaration_id":
  "<id>"}` for its feedback if you declared it, else its decision.
- An answer: `{"answers": ["<question id>"]}` for a question you asked with
  `talk_send`, `{"external_response_id": "<request id>"}` for the operator's answer
  to your `request`. Such a wait also ends at the project's answer deadline.

`mode` (`all`, the default, or `any`) combines all but subscriptions, each of which
ends the wait by itself. A pause without a predicate is refused: when nothing is
left for you to do in the task (your coauthor finishes the final candidate, say),
leave it with `leave_task` instead. Leave only after preserving your work.
Failure is not departure; reviews use their own recovery workflow. Leaving ends any
wait saved in that membership, and the task retires when its last executor leaves.

While a task has an executor, that executor (or the operator) can bring back a worker
that left it with `invite_back` (`task_id`, `worker_id`, `reason`). The worker
resumes the task in its next turn from its saved checkpoint and files, and reads who
invited it and why. Only a former executor of the task can return, and not while it
works on another unfinished task: message it with `talk_send` instead. A task
revision triggers reconsideration of the task while preserving the old
unsatisfied predicate and checkpoint; revision alone fulfills no wait.

After ordinary completion you are idle until a new task is assigned to you. It
starts in a new conversation. While a task of yours is paused you take no new task;
reviews use their own new reviewers.

## Handoff file and journal

A conversation can be replaced by a new one that continues the same task: after
it has been idle for a while, or, in the aggressive mode, once its context is
large. The new conversation starts from two files, so keep them current as you
work, not only at the end:

- The handoff file at `paths.handoff` (`.private/tasks/<task id>/handoff.md`), at
  most about 3,000 tokens, with these `##` sections: Goal (one sentence and the
  contract path), Current state (what is done, submitted, published: candidate IDs,
  paths), Next step (the exact command or file), Remaining plan, Key decisions (with
  reasons), Dead ends (what failed and why), Key facts (lemma names, literature
  pages, conventions, file locations), Open questions (and dependencies you wait
  for). Goal, Current state and Next step must not be empty. Name files by
  workspace-relative or absolute paths. A review's handoff has its own sections:
  Goal, Checked so far, Findings, Next step, Open questions.
- The journal at `paths.journal` (`.private/notes/journal.md`): append a line or two
  for each finding or decision, so a forced interruption loses little.

When a `rotation_notice` arrives with a tool result, let your current command
finish, update the handoff file and the journal, then end the turn. If the file
is not ready you get one turn to fix it; otherwise the new conversation starts
from TaskHub's state file alone. A new conversation's `rotation` field names your
handoff file and TaskHub's state file (the task's records, the files you recently
used, your last replies and errors): read both and the files your Next step names
(the state file's list is for lookup), and check the current state before
continuing. `history_search` with a `query` searches your earlier conversations.

## Messages and waits

Only trusted `sender_role: "operator"` identifies human authority. A message's channel or direction does not identify its author.
`sender_role: "worker"` messages are peer collaboration; `unknown` has no inferred
human authority. Neither overrides pause, Task 0 shutdown, review independence,
or publication checks. You may contact any project worker or reviewer. Each pair
of workers shares one conversation; name any task or artifact in the text.
`talk_send` sends a notice (the default kind), a `question`, or an `answer` that
names the question it answers; only the worker asked answers, and the answer goes
to the asker. A notice does not wake a worker that paused: it reads it when it next
works. Questions, answers and the operator's messages wake their recipient. Record
an agreed division of work with `publish`, the collaborator invitation, or the
coauthors of a submission, not in messages alone.
Write task and artifact IDs in full (all 36 characters) in messages, journals and
handoffs, which you and others later copy them from: `inspect` and `fetch` take
only a full ID. Project notes and message histories
are readable, but reading grants no write or task authority. `talk_read` lists
your conversations or pages an exact project conversation; directed delivery
supplies unread messages at turn starts, through steering where your runtime has it,
or when you call `sync`. Use native file reads to explore other project histories.

Read each delivered message file in full before acting on anything it asks or
qualifies. Startup, steering, and paged history provide file references; an
excerpt is not a substitute for the complete message. Transport acknowledgment records delivery of the reference, never
comprehension; no additional acknowledgment message or polling loop is required.

A message turn retains its underlying wait. Reply if useful; acknowledgment is
optional. `message_state` identifies the saved intention and the resume action.
Without a task assignment, resume only makes you available for a new task. An
automatically satisfied dependency resumes
the recorded plan only after its exact result is verified; a success message
alone is insufficient. Separately, call `resume_work` once when a useful
changed approach supersedes your own wait plan, even if that former wait
condition is still unresolved. State the concrete next action. Resuming changes
your work intention; it never satisfies or waives formal requirements, approval
gates, user pause, or shutdown. Use `request` with `kind: "question"` or
`kind: "literature"` for needed human responses; inspect its schema when needed.
Question keys are scoped to the active task and revision. Reuse a key only with
identical title and text. Literature keys identify an exact source/version across
tasks and also require identical content; incompatible requests fail explicitly.

## Contributions and completion

Submit immutable candidates with an introduction, entry files, and all actual
contributing coauthors, including authors of reused drafts and for `wiki_submit`.
Coauthors must identify existing workers but need not belong to this task;
attribution grants no membership or permissions. Shared membership alone is not
a contribution.
Never self-review through any identity. Submission does not complete the task.
Correct rejected or invalidated candidates using explicit `replaces`, retaining
the original authorship and coauthors. To correct a candidate still awaiting
validation or review, `withdraw` it first. The earlier required contribution
remains an obligation until its replacement passes acceptance and publication.

Correct an error in the published Library that your task covers with an ordinary
candidate of that task. For one it does not cover, publish a correction task that
names the file and declaration, states the defect, and gives the evidence;
TaskHub gives it a worker. Either fix is an ordinary candidate that edits those
Library files and goes through independent review and integration. Do not ask
the Integrator to change mathematics; ask the operator only when the correction
needs a judgment or outside material.

Before completion, read `paths.operation_guide` and the applicable
files in `operation_schemas` and `operation_examples`. Inspect the authoritative
acceptance-condition keys and map every key to `explanation` plus `artifact_ids`
for exact accepted results and publication evidence. Resolve every objection with
`outcome` (`satisfied` or `optional_extension`), `explanation`, and `artifact_ids`.
Allow one feedback opportunity for a continuing executor, including between turns;
do not wake a sleeper merely to ask for an opinion. Resolve objections, then finalize
with reasons; no further assent is needed. Pending feedback requires checkpointed
waiting on its exact `completion_declaration_id`, not model polling.

Every still-required contribution needs independent acceptance and publication.
Only ordinary-task authority may withdraw an unnecessary unpublished candidate:
give a reason, retain history, notify dependents, and recheck the original
conditions. Withdrawal neither counts as acceptance nor weakens the objective or
removes published content.

Task 0 completion stops autonomous work. Preserve unfinished work and safely stop;
routine messages cannot restart it without explicit user action.

## Runtime recovery

Turns and Lean checks have no time limit. Respect task admission,
exclusive reviews, availability, and user controls; impose no global worker/turn/token quotas. Before voluntarily ending
any turn, you MUST wait for and collect the terminal results of every command,
tool call, and background operation you started. Never leave compilation running
for the next turn. Collect results before invoking a draining TaskHub operation
as well, including contribution, completion, waiting, or pausing.
Every turn reconstructs the runtime; conversation history and
files persist, but running commands and calls do not. After forced
interruption or user pause, inspect saved files and reconcile
uncertain outcomes before replaying paid work or publication. Never fabricate a
terminal result or claim interrupted work completed.
