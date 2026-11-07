# Submitting a contribution

Named reference paths are supplied under `paths`. If a path is absent inline,
find its name in the file referenced by `paths.reference_index`. Read only the
format or procedure needed for the current operation.

Write the source files in your workspace. Every authored Lean file needs nonempty
same-stem Markdown. Write a UTF-8 JSON manifest, then use one of these boundaries:

- `taskhub_call(operation="stage", arguments={"manifest_path": "submission.json"})` freezes a candidate for
  review while you continue working.
- `taskhub_call(operation="contribute", arguments={"manifest_path": "submission.json"})`
  records contribution intent and drains your activation. The candidate is frozen
  after execution stops. It does not declare the task complete.
- Operation `complete` declares whole-task completion separately, with evidence from
  accepted and merged contributions. Read `paths.operation_guide`
  and its schema/examples for the per-condition evidence and objection-resolution
  formats. It does not submit a candidate.

The names below are gateway operations. Use `taskhub_actions(operation=...)` for
a needed schema and `taskhub_call(operation=..., arguments=...)` to invoke it.
Reuse retained current schemas rather than rediscovering them each turn.

Paths are relative to your workspace, including paths inside the manifest, even
if the manifest lives in a subdirectory. Keep source in files, not tool arguments.
The manifest is limited to one MiB. Never mix `manifest_path` with inline fields.
This generated reference directory is private runtime material, excluded from
candidate snapshots and publication. Keep your own notes in `.private/notes`.

## Manifest

```json
{
  "kind": "lean.definition",
  "introduction": "Identify the contribution and the files where review should begin.",
  "data": {
    "name": "Contribution name",
    "entry_module": "Project.Definitions",
    "declaration": "Project.selectedDefinition",
    "description": "Explain the intended mathematical meaning.",
    "provenance": "source",
    "sources": [
      {
        "literature_id": "source-id-from-literature-index",
        "content_sha256": "replace-with-the-exact-source-content-hash",
        "locator": "Section and definition number"
      }
    ]
  },
  "coauthors": ["Worker 1"]
}
```

This is a format template, not a ready mathematical task or a valid submission.
Use real names, files, citation IDs, hashes, and locations. For this entry module,
the required pair is `Project/Definitions.lean` and `Project/Definitions.md`.
List all actual contributing coauthors, including the authors of reused drafts,
whether or not they belong to this task. Use existing worker identities; attribution
does not grant task membership or permissions. These identities affect independent
review eligibility. Shared membership alone is not a contribution.
The same rule applies to wiki and supporting-note submissions.

Optional `data.qualifications` holds author scope notes for the reviewer to assess
against the task. It does not automatically block machine validation. Explain
essential assumptions and scope in the paired Markdown as well; the metadata is
retained with the artifact and is not automatically inserted into Library files.
Reviewer and validation qualifications still mean unresolved verification.
Optional `repository_paths` lists the other new or edited files to integrate.
Only the record's Lean/Markdown pair and these paths reach the Library, although
validation builds the whole workspace: list every helper module and edited
Library file the entry module needs. One candidate may list up to 10,000 paths, so a
large supporting development (such as a port of an external source) goes into a
single candidate rather than being split across records of another kind.
Optional `input_refs` lists accepted artifact IDs the record refers to that the task does not
already supply; in practice only an approved environment (`environment_artifact`). Library
results are imported by Lean name, never referenced. `replaces` identifies an earlier
rejected, withdrawn, or review-invalidated candidate whose obligation this
corrected contribution replaces. Invalidated review does not discharge the
required contribution; its recorded replacement must still be accepted and merged.

Read only the relevant file at `paths.submission_schemas` (`schemas/native/`). Each JSON Schema
describes one complete native manifest. It is reference material, not acceptance
evidence; cross-record relationships, provenance, source bytes, and task authority
are checked by the backend. `formalizectl schema TYPE` also shows the complete
normalized record format, including fields the submission boundary fills.

## Mathematical records

Only definition, statement, and proof records are mathematical artifacts.

| Kind | Author supplies | Boundary derives and checks |
| --- | --- | --- |
| `lean.definition` | Name, description, module/declaration, exact citations or explicit derived/internal provenance | Paired file paths/hashes, snapshot, unambiguous environment and version, policy |
| `lean.statement` | Name, statement, module/proposition declaration, provenance | The same pairing, snapshot, environment and policy |
| `lean.proof` | Name, module/theorem declaration, `statement_declaration` naming the Library statement it proves, paired Markdown explaining the proof | Task/revision, snapshot, environment and pairing |

Statements use `def` or `abbrev` returning `Prop`, never placeholder theorems.
Do not use `sorry`, `admit`, or new axioms. Published mathematics has no artifact
identity: import what you need from the current Library by Lean name.

### Check a statement

Use an exact approved environment that permits worker local execution. Put the
statement's entry module last in the dependency-first module order and explicitly
supply its proposition declaration:

```json
{
  "environment_artifact": "approved-environment-id",
  "modules": ["Project.Definitions", "Project.Statement"],
  "declaration": "Project.selectedStatement"
}
```

Pass this object as `arguments` to `taskhub_call(operation="lean_check", arguments=...)`.
Replace the placeholder environment, modules,
and declaration with the actual source being submitted; omit modules that are
not needed. Supply `directory` only when the sources live in that workspace
subdirectory. Wait for and collect the terminal result in the same turn.

Require successful compilation **and inspection**: the returned `ok` must be
`true`, `inspection` must be present, and `inspection.statement_returns_prop`
must be `true`. If the result is unavailable, failed, or missing this metadata,
inspect the report and resolve the issue before submission. Never infer success
from diagnostic text. This development check is never independent acceptance.

Normally omit `bundle`, policy, and derived hashes. You may specify authoritative
values explicitly, but any conflict is rejected rather than silently repaired.
An ambiguous environment must be resolved first. No default relaxes review,
compilation, source correspondence, or independent proof verification.

## Proof targets

Only a proof task takes a proof candidate. A task with another policy, such as a
`lean.peer@1` program, takes definitions and statements but no proofs: publish a
focused `lean.proof@1` task for each statement it needs proved and stage the
proof there. That is the intended route, not duplicate work.

A proof task states its target in prose and pins `environment_artifact` in its
payload and `inputs`. Find the statement in the current Library and name it in
the manifest's `data.statement_declaration`. Validation builds your workspace over
its recorded Library base, checks that the theorem's type is that statement, and
rejects any change to the statement's Lean/Markdown pair or the pairs it imports.

Write task contracts as JSON files and use operation `publish` or
`propose_revision` with `contract_path`. A contract has `title`, `objective`,
`domain`, `kind`, `acceptance_policy`, `inputs`, `requirements`, `constraints`,
`context`, `priority`, and `payload`. `domain` is the project's, `"lean"`; a task
file without it gets it. `objective` and `context` are each either the Markdown
text itself or the relative path of an adjacent Markdown file that holds it (for
example `"objective": "objective.md"` beside the task file); a named file that does
not exist is refused. A proof task uses `acceptance_policy` `lean.proof@1`, pins
`payload.environment_artifact` (also listed in `inputs`), and names the target
statement's declaration in its objective, for example under a `## Target
statement` heading. Give the declaration's full Lean name as the Library file
defines it, such as `Langlands.TwistAdmissibilityCompatibility`, not the path of
its module, such as `Langlands.Representation.Twist.AdmissibilityStatement`.
Task revisions still require the
exact current task ID, revision, and revision reason. Changing a proof target requires a real task
revision and fresh validation; it is not a manifest edit that silently changes
the requested theorem.

## Notes and literature

Every new note has an ordinary source file. Private exploration notes stay in
`.private/notes` without registration. Public notes and wiki share `wiki/README.md`.
Write public Markdown under `notes/` or `wiki/`; add or update its title, relative
link and one-sentence purpose in the README. Keep the Essential section short;
long literature audits and theory surveys belong in Reference notes.

Use `wiki_submit` with the edited Markdown `paths`, a short `summary`, and all
actual contributing `coauthors`. Include `wiki/README.md` when updating entries;
supply `replaces` when correcting a rejected candidate. If the Library has no
README yet, copy the starter at `paths.knowledge_readme` and its linked starter
guides into your workspace and include them in the first submission. Preserve
existing entries when updating the README. Literature follows its linked guide.

Independent review is required. The existing Integrator merges public notes in a
separate batch and maintains links. A public Markdown-only diff skips Lean;
notes cannot publish Lean or its paired Markdown. File edits alone do not publish.
No inline note bodies, extra knowledge schema or reading acknowledgments are needed.

Environment and compiler environment registration require operator authority.

## Review and revision

Staging is not acceptance. An independent reviewer must compare cited original
literature, Lean and paired Markdown at precise locations for definitions and
statements. Proof review checks Lean/Markdown agreement and the target statement; the
implementation need not follow the literature's proof route.
The reviewer rejects mathematical errors and mistranslations, or accepts the
contribution. The dedicated Integrator handles meaning-preserving integration
with fresh validation. Missing evidence or
unavailable tooling remains unresolved, not an invented approval or rejection.
Reviewers use `paths.semantic_review_guide`,
`semantic_review_schema`, and `semantic_review_example` for the complete report
format. Exact paired paths and line ranges and all cited-source tuples are
mandatory for mathematical acceptance.

Only successful review and integration publish to the current Library and retain
the accepted artifact archive. Required unresolved submissions block completion.
Correct a rejected candidate in your files and submit a new manifest with
`replaces`; frozen prior submissions and their recorded findings remain intact.
