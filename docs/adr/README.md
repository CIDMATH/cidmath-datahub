# Architecture Decision Records

This directory holds Architecture Decision Records (ADRs) for the CIDMATH Data Hub. Each ADR captures a single architectural or design decision: the context that motivated it, the decision itself, the alternatives considered, and the consequences accepted.

## Why ADRs exist

Every architectural decision feels obvious in retrospect, but six months later nobody remembers the reasoning. Without a written record, decisions get relitigated when new contributors arrive, sometimes reversed in ignorance of why they existed. An ADR is the answer to "we already thought about that — here's what we decided, and why."

ADRs are also the diff history of the architecture. When a decision no longer fits, you don't delete the old ADR; you write a new one that references it and marks the old one as **Superseded**.

## When to write one

Write an ADR for any decision that:

- Will be hard to reverse (schema names, catalog structure, bundle layout)
- Future contributors might second-guess without context
- Reflects a real trade-off between alternatives rather than an obvious best path
- Establishes a convention others will be expected to follow

You do **not** need an ADR for routine implementation choices (which lint rule, what to name a local variable, how to structure a single function).

## Format

Each ADR is a short markdown file — three to five paragraphs is the target. Filename pattern: `NNNN-short-kebab-case-title.md`. Numbers are sequential and never reused.

### Template

```markdown
# NNNN — Short title in sentence case

## Status
Proposed | Accepted | Superseded by NNNN — YYYY-MM-DD

## Context
What problem you were facing. What constraints applied. Why this decision had
to be made. Two or three paragraphs of background.

## Decision
What you chose. Be specific. Include code snippets, schema fragments, or
diagrams if they clarify.

## Alternatives considered
What else was on the table and why each was rejected. One bullet or short
paragraph per alternative. The point is to show the decision was deliberate.

## Consequences
What becomes easier, what becomes harder, what risks you've accepted. Include
both positive and negative consequences honestly.
```

### Status values

- **Proposed** — drafted, not yet accepted. Used during PR review.
- **Accepted** — current decision, in effect.
- **Superseded by NNNN** — replaced by a later ADR. Both files stay in the repo; the new ADR explains what changed.

## Workflow

1. A new architectural decision comes up in discussion.
2. The person proposing the decision writes an ADR in **Proposed** status and opens a PR.
3. Reviewers discuss the ADR (not the implementation) until consensus.
4. Merge moves status to **Accepted**. Implementation work proceeds.
5. If the decision later changes, a new ADR is written that explicitly supersedes the old one. The old ADR's status is updated to **Superseded by NNNN**.

## Index

| # | Title | Status |
|---|---|---|
| [0001](0001-layering-vocabulary.md) | Layering vocabulary: raw / processed / analysis | Accepted |
| [0002](0002-schema-is-subject-not-source.md) | Schema represents a subject area, not a source | Accepted |
| [0003](0003-catalog-split-source-vs-integrated.md) | Source-aligned and integrated/modeled data live in separate catalogs | Accepted (revised) |
| [0004](0004-monorepo-bundle-per-domain.md) | Monorepo with multiple Databricks Asset Bundles, one per subject area | Accepted (revised) |
| [0005](0005-discovery-tags-and-dataset-catalog.md) | Data discovery via Unity Catalog tags and a curated dataset catalog | Accepted (revised) |
| [0006](0006-table-and-column-naming.md) | Table and column naming conventions | Accepted |
| [0007](0007-update-semantics-and-history.md) | Table update semantics and history handling | Accepted |
| [0008](0008-catalog-metadata-schema-design.md) | Catalog metadata schema design: universal base plus per-domain extensions | Accepted (revised) |
| [0009](0009-data-quality-framework.md) | Data quality framework | Accepted (revised) |
| [0010](0010-observability-and-alerting.md) | Observability and alerting | Accepted |
| [0011](0011-testing-strategy.md) | Testing strategy | Accepted |
| [0012](0012-identity-and-deploy-authorization.md) | Identity, service principals, and deploy authorization | Accepted (revised) |
| [0013](0013-documentation-standards.md) | Documentation standards | Accepted (revised) |
| [0014](0014-reference-data-scope.md) | Reference data: scope, bundle structure, and standardization via informational foreign keys | Accepted (revised) |
| [0015](0015-integrated-table-naming.md) | Integrated catalog table naming (Kimball suffixes for derived analytical content; reference tables unsuffixed) | Accepted |
| [0016](0016-ci-enforcement-policy.md) | CI enforcement policy (hybrid — controlled-vocabulary membership and dataset_catalog row presence enforced; everything else documented-only) | Accepted |
| [0017](0017-bootstrap-lessons.md) | Bootstrap lessons: friction we hit getting the platform live | Accepted |
| [0018](0018-access-groups-and-grant-model.md) | Access groups and the grant model (engineer / reader tiers; which bundle grants what) | Accepted |
| [0019](0019-analyst-discovery-view.md) | Analyst-facing discovery view (curated `discovery.datasets` over `_ops` via view ownership chain) | Accepted |
| [0020](0020-geography-reference.md) | Geography reference: IPUMS NHGIS source, vintaged snapshots + crosswalks, lean attributes + companion WKB geometry | Accepted |
| [0021](0021-geography-crosswalks.md) | Geography crosswalks: ship NHGIS bg-sourced 2010↔2020 files as published, normalized into one long-form table | Accepted |

## Future ADRs (backlog)

The following are known gaps to be addressed when the need is concrete. Captured here so they're not forgotten and so contributors know we deliberately deferred them rather than missed them.

| Anticipated # | Topic | Likely trigger |
|---|---|---|
| TBD | **Pipeline standardization and modular composition** | After 2-3 pipelines exist and patterns can be extracted from real code. Will codify the reference-pipeline pattern, common modules, and how to compose source ingestion + processing + analysis steps consistently. |
| TBD | Schema evolution and breaking-change policy | First time a source adds a column or we need to rename one |
| TBD | Data retention and VACUUM policy | When storage cost or commercial-data contract expiry forces the question |
| TBD | Notebook-to-pipeline graduation policy | First exploratory notebook that becomes a candidate for productionization |
| TBD | Per-domain metadata extension ADRs | One per domain extension as it lands (e.g., `dataset_surveillance` schema) |
| TBD | Type system conventions | Emerges from first pipeline (decimal vs. double, identity columns, etc.) |
| TBD | Performance: partitioning, clustering, optimize cadence | When a real workload demands tuning |
| TBD | Lakehouse Monitoring rollout | After 2-3 stable analysis-layer tables exist |
| TBD | Delta Sharing for external consumers | First external partner request |
| TBD | Cost management and budget alerts | Once meaningful spend accrues |
| TBD | Logging conventions | Stub now; formalize when patterns diverge |

## Further reading

- Michael Nygard's original ADR post: https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions
- ADR GitHub organization (templates and tooling): https://adr.github.io
