# TaskHub operation reference

Named reference paths are supplied under `paths`. If a path is absent inline,
find its name in the file referenced by `paths.reference_index`. Read only the
format or procedure needed for the current operation.

Use this reference in every domain when submitting public notes, preparing
whole-task completion, or resolving its feedback. It is available through `paths.operation_guide`,
`operation_schemas`, and `operation_examples`. Read the applicable files with
native tools. Schemas describe input formats; they confer no authority or
acceptance. All example IDs and findings are placeholders to replace with real
accepted results and verified explanations.

The only TaskHub entrypoints are `taskhub_actions` and `taskhub_call`. Obtain a
needed operation's current schema with `taskhub_actions(operation="stage")` and
invoke it with `taskhub_call(operation="stage", arguments={...})`. Names below
identify operations. Reuse retained current schemas and read only the applicable
native format files; discovery is not a per-turn ritual. Reference filenames
such as `schemas/complete.json` use the operation names.

## Public Lean/Markdown drafts

Use `draft_update` to share a complete directory of draft pairs from
`.private/drafts/<name>/`. Supply `task_id`, `source_dir`, `expected_revision`
(zero initially), and a short `summary`. Find the current independent draft
revision in the task's `drafts` reference or public `drafts/<task-id>/current.json`.
Only ordinary task maintainers update it; all workers can read the immutable
files at `paths.drafts`, or use the read-only `drafts` resource.
Missing definitions may prevent compilation; explain them and their intended
interfaces in same-stem Markdown. No compiler, artifact registration, review,
or task completion is triggered by sharing. Never treat a draft as accepted
dependency evidence. Copy ready pairs into normal project module paths and use
the existing manifest submission, review, validation, and merge workflow.

## Public notes and wiki

Private notes are ordinary files under `.private/notes` and need no registration.
For public knowledge, write Markdown under `notes/` or `wiki/`. Add a title,
relative link and one-sentence purpose in `wiki/README.md`; keep required guidance
short and put long notes in Reference notes. Submit edited paths, including an
edited README, with `wiki_submit`:

```json
{
  "operation": "wiki_submit",
  "arguments": {
    "paths": ["notes/approach.md", "wiki/README.md"],
    "summary": "Record the reusable approach and its limitations.",
    "coauthors": []
  }
}
```

`coauthors` is optional and names only existing workers of this instance by `Worker N`
name. Attribution grants no task permissions. Supply `replaces` to correct an
exact rejected candidate. `wiki_submit` freezes this contribution and keeps the
activation working; it does not complete the task. Independent review and
Integrator publication remain required. Public Markdown-only integration skips
Lean. Notes cannot publish Lean or its paired Markdown; use the domain submission
guide for mathematical artifacts. Files, not inline metadata, contain the prose.

## Declare completion

Call `taskhub_call(operation="inspect", arguments={"task_id": "the-current-task-id", "include_completion": true})`
first, without a historical `revision`. Its
`completion.acceptance_conditions` object supplies the exact keys required in `evidence`.
Include every key, with no extras: the original objective, constraints,
requirements, and acceptance conditions, plus current additions. Preserve
original conditions even when later execution instructions change. Never infer
keys from prose or reduce the objective to the work already completed.

Each value has exactly the authored fields `explanation` (nonempty, at most 10000
characters) and `artifact_ids` (one to 100 exact accepted artifact IDs). Explain
how those results satisfy that condition. TaskHub resolves their exact hashes,
independent review receipts, publication commits, and validation evidence; do not
invent or copy receipt objects into this authored format.

For a task whose inspected keys are exactly `objective` and `acceptance:0`:

```json
{
  "task_id": "task-id",
  "revision": 1,
  "evidence": {
    "objective": {
      "explanation": "The accepted publication fulfills the complete objective.",
      "artifact_ids": ["accepted-artifact-id"]
    },
    "acceptance:0": {
      "explanation": "The published result satisfies this exact condition.",
      "artifact_ids": ["accepted-artifact-id"]
    }
  },
  "reason": "Every inspected condition is supported by accepted publications."
}
```

Use `schemas/complete.json` and `examples/complete.json` here.
`schemas/completion-evidence.json` describes the evidence object alone. Pass the
object as `arguments` in `taskhub_call(operation="complete", arguments=...)`;
this operation has no file-path input.
It opens a completion declaration and returns its ID; it does not close the task.
Submission, successful compilation, or a pending review cannot supply accepted
completion evidence. Every still-required contribution needs independent
acceptance and successful publication, and all original and current requirements
must still hold. Historical acceptance of removed Library content is insufficient.

## Feedback and finalization

An active collaborator receives one feedback opportunity. If it is pending,
checkpoint and wait on the exact `completion_declaration_id`; do not poll the
model or repeatedly redeclare completion. The initiator resolves feedback and
decides with operation `completion_finalize`. A changed task revision requires fresh
inspection and evaluation.

For `decision: "close"`, map every exact string in the declaration's
`feedback.conditions` to one `resolution` entry. Use `{}` if there are none.
Each entry requires `outcome`, `explanation`, and nonempty `artifact_ids`:

```json
{
  "acceptance:0": {
    "outcome": "satisfied",
    "explanation": "The accepted publication contains the result requested by this objection.",
    "artifact_ids": ["accepted-artifact-id"]
  }
}
```

`outcome` is exactly `satisfied` or `optional_extension`. For `satisfied`, show
where the accepted results fulfill the disputed requirement. For
`optional_extension`, explain why the unchanged requirement is already fulfilled
and why the suggestion exceeds it; cite the accepted results demonstrating that
fulfillment. The extension may become another task. Neither outcome can waive a
requirement, treat an unaccepted result as accepted, or substitute assertion for
evidence. A real unmet condition means `decision: "continue"` and further work.

Read `schemas/completion-resolution.json` for the map and
`schemas/completion_finalize.json` for the operation arguments. The examples
directory contains complete argument objects for both resolution outcomes. Pass
the chosen object through `taskhub_call(operation="completion_finalize", arguments=...)`. Include a
nonempty evidence-based `reason`. Optional `evidence` uses the same per-condition
format; when omitted the backend rechecks the declaration's saved evidence.
Finalization rechecks requirements and publication gates. No further peer assent
is needed after evidence resolves the one feedback opportunity.

## Turn boundaries

Before voluntarily ending any turn, you MUST wait for and collect the terminal
results of every tool cell, session, and background operation you started.
Compilation must finish and its result be collected in the same turn. Every turn
reconstructs the runtime; conversation history and files persist, but running
tool handles do not. Turns and checks have no time limit.
Forced interruption or user pause uses recovery:
inspect saved files and reconcile uncertain outcomes before replaying work.
Never fabricate a terminal result or claim interrupted work completed.
