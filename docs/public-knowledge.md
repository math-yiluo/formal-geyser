# Public knowledge

## One entry point

`wiki/README.md` combines navigation and reading guidance. Public Markdown under
`wiki/` and `notes/` belongs to the same knowledge area. Each linked page has a
title and one-sentence purpose. The short **Essential** section contains general
instructions and important lessons to read before exploration. **Reference notes**
contains long literature audits, theory surveys and other task-specific material.
Workers consult these when relevant and reuse guidance already in their history.
There is no separate index.md or knowledge-maintainer worker.

The runtime provides `paths.knowledge_readme`, `paths.wiki` and `paths.public_notes`.
It never injects page text, change lists or mandatory full-wiki reading packages.
No wiki read/delivery tables, acknowledgments or per-worker frozen page copies.
Required runtime operation guides and directed-message delivery remain separate.

New Lean libraries seed the README and short starter pages. Imported files at
those paths are preserved. Before a project publishes its own README, the native
entry points to starter guidance in the instance's `wiki/` directory. On first
publication, include the README and any linked starter guides in the workspace.
Published knowledge lives in the Library; edit a workspace copy for review.
Private notes remain in `.private/notes` and are not exported as public knowledge.

## Contribution and integration

1. Author a useful note, record assumptions/limitations and source references,
   and add or update its entry in the README. Use relative Markdown links, such
   as `[Topic](../notes/topic.md) — What this page helps with.`
2. Call `wiki_submit` with edited Markdown paths (including an edited README),
   summary and optional coauthors. The source files are the content, not a manifest
   body or a database text field. Mathematical Lean/Markdown pairs remain artifacts.
3. An independent reviewer checks accuracy, citations, reading level and the entry.
   A note cannot establish mathematical acceptance or modify a Lean pair.
4. When idle, the same singleton Integrator takes a separate public-note batch
   without waiting for five items or thirteen minutes. It preserves both authors'
   entries when README edits conflict and repairs links. It never interrupts an
   already active mathematical batch; user pauses and account holds still apply.
5. Validation compares the recorded base snapshot and the exact final tree. Only
   changes to unpaired public Markdown under `wiki/` or `notes/` take the document
   route: check nonempty UTF-8 pages and the presence of the README.
   Reading classification, summaries, links and scientific accuracy remain
   review/integration responsibilities. There is no new Markdown schema.
   Lean, paired Markdown, build files and other changes retain domain validation.
6. Publish the validated snapshot through the existing current-head and process
   quiescence checks. Only actual publication satisfies waiting work.

Documentation batches can still wait behind an active batch or on their own
review/conflicts. This change removes needless Lean work, not those dependencies.
