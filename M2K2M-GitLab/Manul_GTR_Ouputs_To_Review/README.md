# GtR patent outputs for manual review — v4.12 full review

Regenerated on 12 October 2026 from `gtr_patstat_linkage_autumn_2025_v4_12_full_review.db`, opened read-only.

| File | One row represents | Rows | Projects | Unresolved outputs represented |
| --- | --- | ---: | ---: | ---: |
| `01_projects_without_any_links.csv` | One project without an accepted application or family link | **365** | **365** | **446**, listed inside project rows |
| `02_unresolved_outputs_on_partially_linked_projects.csv` | One unresolved output on a project with at least one linked output | **318** | **203** | **318** |

The groups do not overlap: **446 + 318 = 764 unresolved outputs across 568 projects**.
The full database contains 5,357 patent outputs across 2,714 projects: 4,593 outputs
and 2,349 projects are linked. Of the linked outputs, 4,541 have specific-application
links and 52 have family-only links. The database has 365 unlinked projects.

## What changed since the supplied working-copy package

File 1 fell from 382 projects / 465 outputs to 365 projects / 446 outputs.
File 2 still has 318 outputs across 203 projects, but its membership changed:
**two old focus outputs left and two different focus outputs entered**. The new
rows include their current linked siblings, targets and evidence. Counts alone
would hide this change. The selections were recalculated from the completed database.

## Selection and source context

An output is linked only when an accepted application or family row exists.
Parsed identifiers and candidate patent records alone do not count. Fully covered
projects are excluded. File 1 is not a list of every UKRI project without patents.

File 1 retains all unresolved outputs on each project in numbered fields and
`UnresolvedOutputsJSON`. File 2 retains the focus output in `UnresolvedOutputJSON`,
other unresolved output IDs and all current linked siblings in `LinkedOutputsJSON`.
These JSON records contain original output fields, parsed references, identifier
resolution/candidates, accepted application records/evidence and family decisions.
Reviewed v4.12 decisions are included where relevant in linked-output context.

Use the output's own reference, description, title and corroboration to propose a
link. Shared project membership or a shared title alone is not sufficient. A
family-only decision does not assign every member application to the output.
Repeated linked-sibling lists are context: do not sum them as unique linked outputs.

## People, organisations and CRNs

The original project's people/organisation context was retained and checked against
the full-build database. Available source records, relationships and nullable
`org_crn` fields remain in JSON, including leading zeros and canonical IDs.
A company number provides organisation context, not proof of patent ownership.
Project-level `output_impact_summary` is not included in these files: that evidence
remains in GtR Enhanced and is planned for the next audit.

## Review fields

Editable fields: `Reviewer`, `ReviewStatus`, `ReviewedAt`, `ReviewDecision`,
`OutputIDsReviewed`, `ProposedTargetLevel`, `ProposedApplnIDs`, `ProposedFamilyType`,
`ProposedFamilyIDs`, `RelatedLinkedOutputIDsUsed`, `EvidenceURLs`, `ReviewerNotes`.

Statuses: `not_reviewed`, `in_progress`, `reviewed`. Suggested decisions:
`application_link_proposed`, `family_link_proposed`, `needs_more_evidence`,
`no_identifiable_patent`, `possible_output_classification_issue`.
Record supporting sources and uncertainties. Keep source/context fields unchanged.
No proposals entered in CSVs are automatically accepted in the database.
Review annotations are retained only for the same ReviewID; new focus rows start
unreviewed. No non-default review annotations were present on surviving supplied rows.

## CSV format and continuation fields

UTF-8 with BOM, comma-separated, all fields quoted. In Excel use **Data → From
Text/CSV** and preserve IDs, references, CRNs and URLs as text. Enable filters and
wrap text. Use a CSV-aware reader to count records because cells contain line breaks.

Continuation columns are at most **30,000 characters per cell**. Join `_Part01`,
`_Part02`, etc. in numerical order **without separators** before parsing JSON.
No source text is truncated. The main field layout is retained; continuation counts
are recalculated, so the number of parts can differ from the older package.

| File | Continuation field | Parts |
| --- | --- | ---: |
| 1 | ProjectPeopleJSON | 3 |
| 1 | ProjectOrganisationsJSON | 5 |
| 2 | ProjectOrganisationsSummary | 3 |
| 2 | ProjectPeopleJSON | 3 |
| 2 | ProjectOrganisationsJSON | 12 |
| 2 | LinkedOutputsReviewText | 1 |
| 2 | LinkedOutputsJSON | 4 |

## Verification and provenance

Checks passed: 365 unique project rows representing 446 unresolved outputs;
318 unique focus outputs on 203 partially linked projects; disjoint complete
coverage of the database's 764 unresolved outputs; current project counters;
exact linked-sibling populations; continuation reconstruction; and 32,209 repeated
person/organisation context records compared with the current database.
All source output payloads and accepted-link evidence were read from the completed
full-build database. All CSV cells fit within 30,000 characters.

Tool version: `4.12-reviewed-full-build`.

Build completed UTC: `2026-10-12T18:53:21.590013+00:00`.

Source SHA-256: `fc815649c24b8284c6385c0836fe5272c418b06e8c266b4d9eb42ebb755d6f94`.

## GitLab destination

Replace these three files in `GTR_PATSTAT_Linkage/Manul_GTR_Ouputs_To_Review/`.
The original folder spelling is retained. The ZIP contains this folder with exactly
two CSVs and this README, with no database, checkpoint or macOS metadata files.
