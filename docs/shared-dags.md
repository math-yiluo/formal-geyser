# Shared planning DAGs

The project has two shared graphs, `definitions.json` and `proofs.json`. Every
active worker may maintain both, including an unrelated task's worker, a reviewer,
or a message-only activation. Decision helpers have no tools and are not workers:
they neither read nor edit them. User pause/stop and fencing still
apply. No task owns a graph or a node.

## Files and native workflow

At the run root, `graphs/current.json` identifies one revision and exact paths to
both files under `graphs/revision-<n>-<identity>/files/`. Revision zero is an empty
pair. `dag_status` returns the coordinator's current reference and verifies its
files; follow both paths from that one response to avoid mixing versions. The
pointer is a recoverable navigation convenience and can lag after an I/O failure.
These are the same two global DAGs across revisions, not per-task graphs.
The immutable versions retain authorship, source task, summary, and file hashes.
Each update removes the revisions below the 200 newest that are more than 7 days old; revision
zero stays. They are kept only as merge bases: an update read from a removed revision is refused
with `DAG_REVISION_CONFLICT`, to be read again and merged. The Linux pilot kept every one of 2,630
revisions, each a full copy of the pair, 8.7 GB.
The existing SQLite coordinator retains only one head identity and normal command
receipts; graph bodies are ordinary JSON files. Graphs remain outside Library and
formal source snapshots. Existing public drafts retain their separate workflow.

Copy the current pair to `.private/dags/edit/` in the worker workspace, edit it,
and run the supplied standalone script:

```text
python .private/taskhub-reference/check_dags.py .private/dags/edit/definitions.json .private/dags/edit/proofs.json
```

The checkout also provides `scripts/check_dags.py`, and an installed package can
run `python -m formal_geyser.coordination.dag_check FILE FILE`.
The script uses only Python's standard library, does not access referenced sources,
and exits zero on success or nonzero with specific diagnostics on failure.

Then call the existing two-tool gateway (discover `dag_update` as needed):

```json
{
  "operation": "dag_update",
  "arguments": {
    "source_dir": ".private/dags/edit",
    "expected_revision": 0,
    "summary": "Add the prerequisite needed by the proposed construction."
  }
}
```

`expected_revision` must be the pair's actual read revision. Both files are saved
together. The backend reads the proposed bytes, invokes the same validator,
compares the shared revision under the existing transaction, and commits one
new head. Stale saves fail with `DAG_REVISION_CONFLICT`; reread, merge, check, and
use a new call rather than overwriting another worker. Bad graph contents fail
with `INVALID_DAG`. Exact retries return their original receipt even if the
editable files changed or disappeared. Pointer recovery always publishes the
latest committed head, never the receipt's older revision. A pointer failure
after commit does not change the successful receipt. Conflict errors include
`details.current_revision` and `details.current_graph_ref`, so a stale navigation
file cannot trap a worker in repeated conflicts. `dag_status` retries navigation
repair and reports file availability separately.

Reads, schema/cycle validation, and immutable candidate installation run outside
the SQLite coordination transaction. The short commit rechecks the worker's
authority and global revision, then records the head and receipt atomically.
The combined byte allowance is enforced as each file is read, before allocation.
A concurrent winner can leave an unreferenced immutable candidate; it never
becomes the current graph. Missing or damaged graph files do not prevent TaskHub
startup or other tasks. Existing identities and files are preserved for repair;
there is no automatic reset or rollback to an older graph.

When native writes are unavailable, `workspace_copy` streams each source from the
`graphs` resource root into `.private/dags/`. `workspace_write` supports local
UTF-8 edits using a Unicode `offset`, `delete_count`, and the exact current hash.
The inserted text is bounded per call; the existing file can exceed 1 MiB. Normal
full writes retain their 1 MiB bound. Message-only activations can use these
fallbacks only inside `.private/dags/`; decision helpers cannot write or copy.

## Graph format

Both documents use these fields; put definition nodes in definitions.json and
theorem/lemma nodes in proofs.json. Theorems include statements written as `def`
returning `Prop`. An unimplemented mathematical object is a valid node.

```json
{
  "nodes": [
    {"id": "D", "title": "Target definition", "references": ["drafts/path/D.md"]},
    {"id": "E", "title": "Required foundation", "description": "The carrier and operations required by D."}
  ],
  "edges": [
    {"from": "D", "to": "E", "reason": "Use E's carrier and operations in D."}
  ]
}
```

Only `id` and `title` are required on a node. Optional `description` and `references`
contain prose and file/source links. Only `from` and `to` are required on an edge;
`reason` explains the needed interface or result. An edge belongs to its source
node's document and can target a node in either document. IDs are case-sensitive
and unique across the pair. JSON keys and edges must not be duplicated. No
handwritten verified/current/accepted flags are supported.

The checker rejects malformed or unexpected fields, dangling references, self
loops, and directed cycles across the entire pair, and reports a cycle path.
It uses iterative traversal rather than Python recursion. Each pair is bounded
by the existing native file byte envelope, with no separate small node-count cap.
It does not inspect mathematical meaning, compile Lean, or check file links.

## Meaning and prompts

These graphs describe expected dependencies of current construction/proof routes.
They do not automatically create tasks, waits, artifact references, or acceptance
evidence. Actual Lean dependency extraction is deferred. Existing mathematical
verification and publication remain authoritative. Keep alternative routes and
full explanations in Markdown rather than asserting all alternatives are required.

The packaged `shared-dags.md` guide is available on demand to ordinary, review, and
correspondence workers. `paths` locates `graphs` and the current shared-DAG pointer;
the `dag_checker` path is also available through `paths.reference_index`. Workers
refresh through `dag_status` before editing and must check locally before saving.
The coordinator independently rechecks all saves, including those where a worker
forgot the local check. A server-side graph check is never mathematical approval.
Workers can also read the built-in `graphs` resource, alongside public `drafts`.
