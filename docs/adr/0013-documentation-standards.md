# 0013 — Documentation standards

## Status
Accepted — 2026-05-15

## Context
A data hub's documentation is part of its product surface. Users read table comments before querying; engineers read pipeline READMEs before debugging; on-call reads runbooks at 2am. Documentation that exists in someone's head, in a stale wiki, or only in commit messages doesn't serve any of those audiences.

We've already established several documentation surfaces in passing:

- `CLAUDE.md` at the repo root (ADR 0004 mentions it; it's been drafted in the setup checklist).
- `docs/adr/` for architecture decisions (this ADR series).
- `_ops.dataset_catalog` for table-level metadata (ADR 0005, 0008).
- Runbooks at `docs/runbooks/` (ADR 0010).
- Incident writeups at `docs/incidents/` (ADR 0010).

What hasn't been pinned down: per-bundle READMEs, table and column comments in Unity Catalog, Python docstring conventions, onboarding documentation, what gets auto-generated vs. hand-written, and how documentation is kept in sync with code.

This ADR sets the bar for what documentation is required, where it lives, and who maintains it.

## Decision

### Required documentation surfaces

| Surface | Lives at | Maintained by | Audience |
|---|---|---|---|
| **Repo README** | `README.md` | Platform team | New contributors, drive-by visitors |
| **CLAUDE.md** | `CLAUDE.md` | Platform team | Claude agent + new contributors |
| **ADRs** | `docs/adr/NNNN-*.md` | Whoever proposes a decision | Anyone making future architectural decisions |
| **Onboarding** | `docs/onboarding.md` | Platform team | New team members |
| **Operations** | `docs/operations.md` | Platform team | On-call, anyone deploying |
| **Runbooks** | `docs/runbooks/<scenario>.md` | Whoever first hits the scenario | On-call during incidents |
| **Incident writeups** | `docs/incidents/YYYY-MM-DD-*.md` | The on-call who handled it | Future on-call, retrospective |
| **Per-bundle README** | `bundles/<subject>/README.md` | The subject's owner | Engineers working on that subject |
| **UC table comments** | `COMMENT ON TABLE`, set in pipeline definitions | Pipeline author | Anyone browsing the catalog |
| **UC column comments** | Set in pipeline schema declarations | Pipeline author | SQL writers |
| **Python docstrings** | Inline in `src/cidmath_datahub/` | Module author | Engineers reading/calling the code |
| **Generated table reference** | Markdown rendered from `_ops.dataset_catalog_full` | Auto-generated nightly | Discovery surface |

### Repo README structure (template)

The top-level `README.md` is the entry point. Sections, in order:

1. **What this is.** Two-paragraph overview: CIDMATH Data Hub, what it serves, who maintains it.
2. **Quick start.** "If you just want to query the data, do X. If you want to contribute, do Y."
3. **Architecture.** A short diagram or description of catalogs / bundles / shared package. Links to ADRs 0001, 0002, 0003, 0004.
4. **Repo layout.** ASCII tree mirroring the actual structure.
5. **Getting started for contributors.** Links to `docs/onboarding.md`.
6. **Operations.** Links to `docs/operations.md`.
7. **Conventions.** Links to ADR README index.
8. **Contact.** Owners, Teams channel (Data Hub in CIDMATH Team Site), escalation paths.

Length target: under 500 lines. Deep content lives in `docs/`; the README points there.

### Per-bundle READMEs

Every `bundles/<subject>/` has a `README.md` covering:

1. **Subject overview.** What public-health topic this bundle covers.
2. **Datasets included.** Bulleted list with one-line descriptions; references to `_ops.dataset_catalog` rows.
3. **Pipelines.** Each pipeline with: source(s), cadence, freshness SLA, owner.
4. **Known issues / gotchas.** Anything weird about this subject's data.
5. **Contact.** Owner email; Teams channel for questions.

Length: 1-3 pages. New subjects start with a stub README that's filled in as the first pipeline lands.

### Unity Catalog comments (required, enforced)

**Every table** at any layer must have a `COMMENT` set when registered. The comment describes:
- What the table represents
- Grain (one row per *what*)
- Layer and update semantics in plain English

Example:

```sql
COMMENT ON TABLE ecdh_dev.wastewater.sample_concentration IS
  'Unified wastewater sample concentrations across all ingested sources. One row per (facility_id, sample_collected_at, analyte). Analysis layer, MERGE upsert on the composite key.';
```

**Every column on every analysis-layer table** must have a comment. Comments describe:
- What the value represents
- Unit, where applicable
- Special values or encoding

```sql
ALTER TABLE ecdh_dev.wastewater.sample_concentration
  ALTER COLUMN concentration_copies_per_ml COMMENT
  'SARS-CoV-2 RNA concentration in copies per milliliter, normalized to flow volume. Negative values indicate below detection limit (encoded as -1).';
```

For raw and processed layers, table comments are required but column comments are optional (they often mirror the upstream source's documentation; a link in the table comment to the source data dictionary is sufficient).

**Enforcement:** documented-only per ADR 0016 (hybrid CI enforcement policy). Missing comments on analysis-layer tables and columns are surfaced as lint-style CI warnings (visible in PR comments) but do not block the merge. Code review is responsible for catching consistent omissions. Rationale for not CI-gating: comments are taste-driven (what makes a good comment is judgment-driven, not mechanically checkable), and missing comments are visible in catalog browsing in a way that creates social pressure to add them.

### Python docstrings (Google style)

Every public function, class, and module in `src/cidmath_datahub/` has a docstring. Style: Google ([example](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html)).

Minimum content:

```python
def schema_for(subject: str, layer: Layer) -> str:
    """Return the Unity Catalog schema name for a subject + layer combination.

    Implements the bare-subject convention from ADR 0001: analysis-layer
    schemas drop the suffix; raw and processed layers use `<subject>_<layer>`.

    Args:
        subject: Subject area identifier in snake_case (e.g., "wastewater").
        layer: One of Layer.RAW, Layer.PROCESSED, Layer.ANALYSIS.

    Returns:
        The schema name string (e.g., "wastewater_raw", "wastewater").

    Examples:
        >>> schema_for("wastewater", Layer.RAW)
        'wastewater_raw'
        >>> schema_for("wastewater", Layer.ANALYSIS)
        'wastewater'
    """
```

Private functions (`_prefixed`) get a one-line docstring describing intent; full Args/Returns/Examples optional.

Bundle entrypoints in `bundles/<subject>/src/` have a module docstring covering inputs, outputs, and where the orchestration is defined. They don't need extensive function-level docs — they're thin wrappers around `src/cidmath_datahub/`.

### Runbook conventions

Runbooks at `docs/runbooks/<scenario>.md` follow a consistent structure:

1. **Scenario.** Two sentences: "When you see X, this is the runbook to follow."
2. **Triage steps.** Numbered list. The first three steps should narrow scope (which pipeline, which table, when did it start).
3. **Common causes.** Bullet list with cause → diagnostic command.
4. **Resolution paths.** For each common cause, the fix or escalation.
5. **Post-resolution.** What to update, who to notify, whether an incident writeup is required.

Initial runbook backlog (created as scenarios emerge):

- `docs/runbooks/pipeline-failed.md`
- `docs/runbooks/freshness-sla-missed.md`
- `docs/runbooks/dq-fail-spike.md`
- `docs/runbooks/cost-anomaly.md`
- `docs/runbooks/rotate-sp-credentials.md` (referenced in ADR 0012)

Don't pre-write runbooks for scenarios that haven't occurred. The first time an incident happens, the on-call writes the runbook.

### Incident writeups

Per ADR 0010, prod-paging incidents get a writeup at `docs/incidents/YYYY-MM-DD-short-name.md`. Five-paragraph maximum, covering: what happened, impact, resolution, follow-ups, lessons learned.

No formal post-mortem ceremony at this stage. The writeup itself is the artifact.

### Onboarding documentation

`docs/onboarding.md` is the new-contributor's first-week guide:

1. **Workspace access.** How to get added to Databricks, the `cidmath-data-engineers` group, GitHub org.
2. **Local setup.** Python version, `databricks` CLI install, repo clone, `pip install -e .`, IDE recommendations.
3. **First deploy.** Step-by-step: deploy the `_platform` bundle to your personal dev target.
4. **Reading list.** Which ADRs to read first (CLAUDE.md, 0001, 0004, 0006, 0008).
5. **First contribution.** A small starter task (e.g., add a row to `_ops.taxonomy_domain`) that touches the workflow without high stakes.

Length: 1-2 pages plus links.

### Operations documentation

`docs/operations.md` is the manual for running the system:

1. **Deploying.** Dev (any contributor), prod (tag + approval flow).
2. **Triggering pipelines manually.** When and how.
3. **Pausing a pipeline.** For maintenance or known upstream issues.
4. **Reading the alerting dashboards.** What signals mean what.
5. **Common operations.** Vacuum, optimize, schema evolution, adding a new bundle.
6. **Emergency procedures.** Rollback, halt deploys, contacts.

### Auto-generated documentation

A nightly Job in `_platform` generates a Markdown table-reference doc from `_ops.dataset_catalog_full`. Output: one Markdown file per subject, listing every table with its description, grain, layer, update semantics, refresh cadence, owner. Stored as a Delta table view and optionally rendered to `docs/generated/` for static-site consumption.

We're not standing up a docs site (Sphinx, mkdocs) at this stage. The repo's Markdown is browsable on GitHub. If a richer site becomes warranted (external audience, search, navigation), it's a future ADR.

### What is NOT required (explicitly)

- A central wiki outside the repo. Wikis drift; in-repo Markdown lives with the code.
- Sphinx-generated HTML docs. Add when there's external audience demand.
- Architecture diagrams in PlantUML/Mermaid. Encouraged but optional — text descriptions are fine.
- Tutorial walkthroughs. Examples in docstrings cover the need.

### Maintenance

- **Each PR that adds or changes a public function updates its docstring.** Caught in code review.
- **Each PR that adds a table updates the `COMMENT ON TABLE` and column comments.** Caught by CI.
- **Each PR that materially changes architecture either creates a new ADR or updates an existing one.** Caught in code review.
- **Runbooks and operations docs update opportunistically.** The on-call who finds a doc wrong fixes it as part of incident response.

Documentation isn't a separate phase. It's a PR requirement like tests and code review.

## Alternatives considered
- **Comprehensive Sphinx-generated docs site from day one.** Rejected. Adds tooling overhead and a build step; payoff is for external audiences we don't have yet.
- **CI-enforce UC comments (block on missing).** Considered, rejected per ADR 0016's hybrid CI policy. Comment quality is taste-driven, not mechanically checkable; an empty `COMMENT ON TABLE` passes a presence check while serving no one. Lint-style warnings + code review handles the case well enough at our scale.
- **Detailed post-mortem template for every incident.** Rejected. Five-paragraph cap is deliberate; cargo-cult post-mortems drain energy and discourage writing them at all.
- **Auto-generate ADRs from commit messages.** Rejected. ADRs require thought; commit messages don't capture the alternatives-considered structure that makes ADRs valuable.
- **Wiki (Notion, Confluence, etc.) for non-code docs.** Rejected. Wikis drift from code; in-repo Markdown stays close to what it documents and is versioned with it.

## Consequences
- **Browsing the catalog is informative.** UC table and column comments give users enough context to know whether a table is what they need.
- **Onboarding is structured.** A new contributor has a clear first-week path.
- **On-call has runbooks for the common cases.** Reduces 2am cognitive load.
- **Generated docs reduce duplication.** Table reference is a query, not a hand-maintained list.
- **No CI gate on docstrings or onboarding docs.** Trust + review handles it. CI catches what's mechanically checkable (UC comments).
- **Documentation lives with the code.** Single source of truth, versioned with the thing it documents.
- **Maintenance is distributed.** No "docs team" — each PR maintains its own docs.
- **Some upfront cost.** First-pass onboarding doc and per-bundle README templates take effort. Pay once, save many times.
