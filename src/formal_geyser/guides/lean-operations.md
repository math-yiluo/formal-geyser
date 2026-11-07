# Lean authoring operation reference

Consult this reference for development checks, submission, and published work.
Find named reference paths in `paths`, or in `paths.reference_index` when they
are not supplied inline.

There is no fixed reasoning phase or order: investigate, define, attempt proofs,
revise, publish tasks, or wait as needed. Exploratory notes are ordinary private
files and need no registration. Public wiki and supporting notes require source
files, review, and publication.

Call the operations below through `taskhub_call(operation=..., arguments=...)`;
use `taskhub_actions(operation=...)` for a needed schema. Native files and commands
remain the ordinary workspace interface.

## Mathematical artifacts

Artifacts are definitions, statements, or proofs. Every authored Lean file needs
nonempty same-stem Markdown. Statements are `def`/`abbrev` proposition definitions
returning `Prop`, never placeholder theorems. Never use `sorry`, `admit`, proof
holes, or invented axioms. Submitted sources may not use `#eval`, `run_cmd`,
`initialize`, `extern`, `implemented_by` or `debug.skipKernelTC`; validation also
replays every module through the kernel. Target inspection checks the environment's allowed
assumptions; `native_decide`, `decide +native`, and generated proof holes fail
this check.

Definitions and statements must correspond to their Markdown and cited original
sources. Derived/internal artifacts need an explicit provenance rationale. Proof Markdown explains the implemented Lean proof, whose
route may differ from the literature.

Proof tasks describe their target in prose, name the target statement by its
full Lean declaration name (not its module path), and pin the environment. A proof record names that Library
statement in `statement_declaration`; it must already be in the Library at your
workspace base.
Validation builds your workspace over its Library base, checks that the theorem's
type is that statement, and rejects changes to the statement's Lean/Markdown
pair or anything it imports. Import published work by Lean name; there are no artifact references,
registry records, or approval receipts to create.

## Development and submission

The file referenced by `paths.lean_environment` supplies this instance's environment ID and
Mathlib's source locations (also in `LEAN_SRC_PATH`), which you may read and search.
Lean runs only in the project's Lean service: compile and check with `lean_check`, which
reads the Lean files of your workspace where they are and keeps its build outputs out of
it. With `directory` set to a draft, `.private/drafts/<name>`, it compiles the draft's
Lean files together with your workspace's other Lean files, a draft file taking the place
of a workspace file of the same module; staging ignores `.private`, so unfinished work
belongs there. Your shell has no `lean` or `lake` command; do not run a Lean executable yourself, and
do not clone, update, download, or rebuild Mathlib. Dependency preparation belongs to
initialization. Checking the one module you are editing, without a declaration, is
incremental: the service keeps it open with its imports loaded, so check it often. On a busy
machine `lean_check` waits its turn, and the Integrator's batch validation goes first. A
heartbeat timeout points at the proof: restructure it, or give a finite higher limit.
`lean_check` refuses `set_option maxHeartbeats 0` (and `synthInstance.maxHeartbeats 0`)
without running it, and stops a check that has not ended after 30 minutes.

Use `lean_check` only in an approved environment permitting worker local execution.
Execution authorization belongs to the operator. Its diagnostics are private records
that need no review. Development compilation is not publication; formal admission
also checks the exact target and requires peer review.

For a statement, call `lean_check` with the exact `environment_artifact`,
dependency-first `modules` ending in its entry module, and its proposition
`declaration`; require a successful result whose inspection reports
`statement_returns_prop: true`. Collect the terminal check result in this
turn; do not leave compilation for the next runtime. Turns and checks have no
time limit; on a busy machine a check waits its turn.

Only the record's Lean/Markdown pair and the manifest's `repository_paths` are
integrated, although validation builds the whole workspace: list every other new
or edited file the entry module needs, such as helper modules.

Follow `paths.submission_guide` and the relevant
`paths.submission_schemas` at submission. The boundary freezes the paired
paths and hashes and derives the policy; staging confers no acceptance. Supporting notes are not mathematical artifacts: generic peer review
or metadata review supplies no compilation or proof receipt. Publication requires
independent semantic review; proof acceptance also requires
Lake build and exact-target checks.

Machine validation of a staged candidate also runs in the Lean service. If it has not
finished when `stage` returns, `validation.state` is `queued`: it continues in the
background and its result arrives as a notification. Work on something else meanwhile, or
pause with `{"validations": ["<candidate id>"]}` in the predicate to wait for it.
An unavailable original check is retried with backoff; do not duplicate
the submission merely to retry it. Rejections still require correction and review.

## Published work

Pending artifacts stay in staging; acceptance plus publication moves the record
and its receipts to the accepted archive. The current Library is the published
Git tree, which the Integrator may reorganize: later commits can move, merge, or
rename what an archived submission shows. Work against the current Library by
Lean name. Rejected/withdrawn submissions retain history, and relocation
preserves artifact ID/version. Registry listings exclude private notes.
