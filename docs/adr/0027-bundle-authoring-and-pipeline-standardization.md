# 0027 — Bundle authoring and pipeline standardization

## Status
Accepted — 2026-05-31. **Amended by ADR 0037 (2026-06-22):** the two validation patterns this ADR's
`run_build` docstring contrasts (in-memory *validate-then-write* for reference vs *write-then-query*
for conformance) are **unified** — all builds use one **gated write-then-validate**: validate the
raw/processed staging (query-based, `TableDQ`) and gate the promote to the canonical, which gives the
"never land a bad table" safety at any scale (incl. large reference like census block). In-memory
validation is now an optional fast-path for tiny data, not a co-equal pattern. The `run_build` seam
stays validation-agnostic; only the recommended default changed. See ADR 0037 decision 8.

## Context
Two layers of standardization are already strong: **conventions** (CLAUDE.md + ADRs 0001–0026 cover catalogs, schemas, naming, update semantics, DQ, grants, discovery, job-vs-LDP) and **shared library primitives** (`common/`: `dq.DQRecorder`, `registration.register_dataset`, `grants`, `logging`, `vocabularies`; `reference/` domain logic). The ADR 0011 rule — thin entrypoints in `bundles/<x>/src/`, testable logic in `src/cidmath_datahub/` — is followed.

What is **not** standardized is *bundle authoring* itself. Every build entrypoint hand-rolls the same outer lifecycle:

```
SparkSession → ensure schema/table → new_run_id → with DQRecorder(...) as recorder:
    extract / transform / write / recorder.record(...)            # order varies
→ register_dataset(...) → grant_schema_*(...)
```

This is consistent today only because the same author carried the pattern in their head. The skeleton is copy-pasted, not codified, and it has already drifted in defensible-but-undocumented ways (e.g. weather conforms *write-then-query-validate* while geography *validates-in-memory-then-writes*), and is easy to get subtly wrong (forget to register, forget a grant, fail to flush DQ on the error path). Standing up a new subject means copying an existing bundle and changing ~30 things — exactly the manual edits that produced this week's rename/audit-column cleanups.

This matters now because the project intends to **open development to others** (per the standardization review) and because the ADR backlog's "Pipeline standardization and modular composition" item — partially addressed by ADR 0023 (shared GADM IO) and ADR 0026 (job-vs-LDP) — named its trigger as "once a non-geography subject bundle exists to generalize from." Weather is now that exemplar. The tacit authoring pattern needs to become explicit before a third bundle copies the un-templated one.

## Decision
Standardize bundle authoring along three complementary mechanisms, each doing the part it is good at — **template = consistent birth, seam = consistent structure, CI/review = consistent life** — while deliberately *not* prescribing the transform logic, DQ strategy, table shape, or update semantics.

### 1. A canonical orchestration seam — `common/pipeline.run_build`
A thin function owns the invariant lifecycle; the caller supplies the variable parts as hooks (`ensure`, `work`, `register`, `grant`). Canonical phase order: `ensure → [DQ context: work] → register → grant`. Guarantees that were previously per-author:

- The DQ buffer is flushed on **both** the success and failure paths (the `DQRecorder` context manager).
- `register` and `grant` run **only if `work` succeeded**, so a failed build never publishes catalog metadata or opens grants.
- `register=None` is the **explicit, logged** opt-out for engineer-only staging layers (raw) that aren't catalogued — a visible choice, not an omission.

The seam is deliberately agnostic about what happens *inside* `work`: the validate-then-write (in-memory DQ, typical of reference builds) vs write-then-query-validate (query-based DQ, typical of source conformance) choice is legitimate and layer-dependent, so it stays in the hook. The seam standardizes the lifecycle *around* the work, not the work.

### 2. Thin-entrypoint contract (reinforces ADR 0011)
A `bundles/<subject>/src/build_*.py` entrypoint should: parse args, construct its `ensure`/`work`/`register`/`grant` hooks (delegating real logic to unit-tested modules in `src/cidmath_datahub/`), and call `run_build`. No transform logic in the entrypoint.

### 3. A Databricks Asset Bundle template + authoring guide
A custom DAB template (`databricks bundle init`) scaffolds a new subject bundle from a few prompts — `subject_name`, `provider_code` (ADR 0006), layers (raw/processed/analysis), `update_semantics` (ADR 0007), `execution_model` (job vs LDP, ADR 0026). It emits the **structure** (`databricks.yml`, `resources/<job>.yml` with vars wired and no auto-run, a thin `build_*.py` stub that calls `run_build` with `TODO` hooks, `README.md`, a `tests/` stub, and the `deploy-<subject>.yml` workflow) — **not** behavioral/source-specific logic. Paired with a `docs/authoring-a-bundle.md` walkthrough. The template scaffolds day-one correctness; it is explicitly *not* an ongoing-conformance mechanism (see Consequences).

## Alternatives considered
- **A heavy base-class framework** (an abstract `Build` class every entrypoint subclasses, owning extract/transform/write/DQ). Rejected: too prescriptive — it would force one DQ strategy and table-write shape, fighting the legitimate reference-vs-source differences. The function-with-hooks seam standardizes the lifecycle without constraining the work.
- **Documentation only (no seam, no template).** Rejected: prose conventions haven't prevented the skeleton drift we already see; contributors would still copy-paste and diverge, pushing standardization onto review.
- **Cookiecutter (or similar) for scaffolding.** Rejected in favor of the native DAB template — no extra dependency, integrates with the CLI the team already uses, and renders directly into the bundle layout.
- **Build the template first.** Rejected as a sequencing error: it would freeze today's copy-pasted skeleton into `.tmpl` form. The seam + this ADR come first so the template scaffolds entrypoints that *call `run_build`*, not 300-line skeletons to edit.
- **One execution model for all bundles.** Out of scope here; settled by ADR 0026 (job vs LDP per table by work shape).

## Consequences
- **`common/pipeline.run_build` ships now** (with unit tests for phase order, the `register=None` opt-out, and the failure path skipping register/grant). It is the seam the template and future bundles target.
- **Existing bundles retrofit incrementally, not urgently.** Geography and weather keep working as-is; they move onto `run_build` opportunistically (weather only *after* the in-flight backfill completes — never refactor a running pipeline's code mid-run). The seam is additive.
- **Three mechanisms, three scopes.** The template gives a correct *starting point* but never re-applies, so it does not enforce ongoing conformance; the seam enforces lifecycle *structure*; CI + review enforce *ongoing* conformance. Treating the template as sufficient on its own would be a mistake — hence the seam and the (future) CI guardrails.
- **Lower review burden for external contributors.** A generated bundle that already calls `run_build` and wires the shared primitives means reviewers check the *logic and DQ*, not whether the author re-derived the skeleton correctly.
- **Follow-on work (tracked, not done here):** the DAB template + `databricks_template_schema.json`; `docs/authoring-a-bundle.md`; a `weather/README.md` (the exemplar bundle currently lacks one); and optional CI guardrails (wire the deferred ADR 0016 rule 4 — `_ops.dataset_catalog` row presence — plus a thin-entrypoint check). These close the backlog "Pipeline standardization and modular composition" item, which this ADR supersedes as the home for that decision.
- **The in-memory-vs-query DQ choice is now documented**, not accidental: reference/small-cardinality builds validate in memory before writing; large/source-conformance builds write then query-validate. Both are valid; the seam accommodates either inside `work`.
