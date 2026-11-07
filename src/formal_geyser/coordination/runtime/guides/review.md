# Independent review guide

Review the exact frozen candidate against its task's acceptance conditions: read
its introduction, submitted source files, references marked `required: true` and
the domain review guidance. Drafts and planning graphs are not accepted evidence.
Authors and coauthors cannot review their own work through another identity.

Machine validation accepted these exact bytes before your review began
(`review.validation` names its checks; your domain guide says more). Do not rebuild
to verify that again: check what no machine can, that the content means what its
task and its sources say.

For public notes, check accuracy, cited sources, assumptions and limitations, and
the title, link and purpose in `wiki/README.md`; Essential guidance stays short. A
standalone note needs no Lean pair and cannot establish mathematical acceptance.

Also check how the candidate fits the current Library, read-only at `library.root`
(`directories.library`); your workspace holds no copy of it until your first
development check. Search it for declarations,
modules or pages that the candidate duplicates or overlaps, and check whether its
files sit with related content: topic folder, neighbouring modules, consistent
names. Overlap and placement are not defects in themselves; a re-implementation is
one only when the task conditions require reuse. When accepting, record what you
found in `integration_notes` for the Integrator; omit them when there is nothing
to add.

## Verdict

Accept with concrete findings, or reject demonstrated defects with revision
suggestions; never repair incorrect mathematics and approve the replacement.
Missing sources or unavailable checks leave the review unresolved: ask for a
source with `request` (`kind: "literature"`) and pause with
`{"external_response_id": "<request id>"}` until it arrives. You may ask the author
or other workers with `talk_send` (`kind: "question"`) and pause with
`{"answers": ["<question id>"]}` for the reply; their answers never supply
independent approval.

Your verdict's fields become your review report
(`.private/reviews/<review task>/report.md`): make the rationale, evidence and
findings self-contained, with exact `path:line` ranges and source locators, and do
not write a report file yourself. A later question about your review starts a new
conversation from that report.

Acceptance ends your review execution, not publication: end the turn. The
Integrator integrates accepted contributions in a batch, validates the exact final
tree and publishes; you run no merge operation. After rejection or publication this
temporary reviewer retires. Read `.private/instructions/review-operations.md` when
recording a verdict or recovering.
