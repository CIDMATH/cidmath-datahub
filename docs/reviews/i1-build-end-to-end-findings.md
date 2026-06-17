# I1 — One build end-to-end (→ the #1 shared-builder decision) — findings

**Date:** 2026-06-16 · **Specimen:** `bundles/_reference/src/build_ndc.py` (734 lines), traced
through `common/pipeline.run_build`, `common/grants`, `common/registration`, compared against
`build_cvx.py` / `build_icd10cm.py`. **Method:** repo-level. Part of the SDE review plan.

## Verdict
The `run_build` seam (ADR 0027) is a clean, well-scoped orchestration boundary — it owns the
*lifecycle* and nothing more, exactly as intended. But **everything between the pure parser and
`run_build` is hand-rolled per build, and ~40–50% of each entrypoint is recurring skeleton or
mechanical mapping.** That copy-paste surface is the root cause of the inconsistencies the earlier
passes found (`loaded_at`/`ingested_at`, `snapshot_replace`/`full_refresh`, `_get_secret` copies).
I1 confirms the #1 shared builder is worth doing — and surfaces one new inconsistency (uneven DQ-helper
adoption).

## What recurs (the copy-paste surface) vs what's genuinely per-source

| Recurring skeleton (copy-pasted, near-identical) | Genuinely per-source (belongs to the source) |
|---|---|
| `_table_has_column` — **byte-identical** in ndc/cvx/icd | The Spark `StructType` (the data shape) |
| `_write_table` — the `DELETE`+`append` snapshot-replace (the non-atomic one ADR 0034 fixes) | The **parser** (already isolated in `reference/*.py`, ADR 0011 — good) |
| `_create_current_view` — near-identical (and ADR 0034 drops these) | The specific **DQ business rules** (the checks themselves) |
| `_ensure` — `CREATE SCHEMA IF NOT EXISTS` (+ `CREATE VOLUME`) | The **metadata values** (license, URLs, descriptions, `known_limitations`) |
| `_grant` — `grant_schema_reader`×2 + `verify`×2 (+ volume grants) | The vintage key name/type (`snapshot_date`/`edition_year`/`loinc_version`) |
| `_register` **scaffolding** — the `common={...}` dict + `register_dataset` calls | — |
| `main()` argparse — catalog/groups/source-url/snapshot|edition/no-views | — |
| IO helpers — fetch/zip/`_persist_snapshot`/`_get_secret` (secret copy from I3) | — |
| **row-dict construction** — the `[{...} for r in records]` that re-lists every schema field by hand | — |

The row-dict construction is the sharpest DRY smell: each build declares its columns **twice** —
once as the `StructType`, once as the dict comprehension — so a field added to one and missed in the
other is a latent bug the type system won't catch.

## New inconsistency this pass found
**Uneven DQ-helper adoption.** ADR 0029 introduced `TableDQ` to single-source the
record→severity→raise pattern, but `build_ndc.py` hand-rolls ~210 lines of raw
`ctx.recorder.record(...)` calls instead. So the very boilerplate `TableDQ` was built to remove is
still copy-pasted in at least one build. (Worth a quick audit of which builds use `TableDQ` vs raw
`record()` — part of U3.)

## Recommendation for the #1 decision (for your approval)
Build a **config/composition** reference-table builder that sits *above* `run_build` — not a base
class. The source supplies a spec; the builder owns the invariant skeleton and bakes in the
already-made decisions (ADR 0034 atomic `replaceWhere` + `vintage_snapshot`, no `_current` views,
`ingested_at`, `TableDQ`). Adopting it therefore *fixes* the inconsistencies by construction.

Illustrative shape (not final):
```python
@dataclass(frozen=True)
class ReferenceTableSpec:
    schema: str                 # "codes"
    table: str                  # "ndc_product"
    spark_schema: StructType    # the one declaration; rows derived from it
    vintage_key: str            # "snapshot_date" | "edition_year" | "loinc_version"
    parse: Callable[..., list]  # pure, from reference/*.py
    dq:    Callable[[BuildContext, list], None]   # injected business rules (TableDQ-based)
    catalog_meta: ReferenceMeta # license/urls/description/known_limitations values
    source: SourceSpec | None   # optional download: url / volume snapshot / secret scope+key

def build_reference_table(specs: list[ReferenceTableSpec], catalog, groups, ...) -> None:
    # owns: _ensure DDL, atomic per-vintage write, register scaffolding, grant+verify,
    #       IO (fetch/zip/volume/secret), argparse — wired through run_build.
```

- **Scope:** everything between the parser and `run_build` — write, register-scaffolding, grant+verify,
  ensure-DDL, IO, argparse, row-derivation. **Out:** the parser (stays pure) and the DQ business rules
  (injected) — the two places real variation lives, kept as injection points to avoid over-abstraction.
- **Style:** config object + a function (composition), matching the functional `run_build` seam.
- **Proving ground:** greenfield on **ICD-10-PCS** (next code system, public, simplest, mirrors
  icd10cm), then a **hard case** (the 2-table NDC or the faceted+secret LOINC) *before* declaring it
  the standard — then backport cvx/loinc/ndc/icd opportunistically (the 0034 reclass + `loaded_at`
  rename already touch those, so fold the migration in there).

### Pre-mortem (#1 is high-stakes)
The failure mode is **over-abstraction**: the sources genuinely differ (LOINC faceted + licensed
download + 2 tables; ICD hierarchy + Apr-1 mid-year overlay; NDC 2 linked tables + Volume snapshot;
CVX Volume snapshot). A too-rigid builder gets bypassed with escape hatches and ends up worse than
the copy-paste. Mitigation: the builder owns **only** the invariant skeleton; parse, DQ, and schema
stay injected; and it must clear a hard case (NDC 2-table *or* LOINC) before becoming mandatory — if
it can't express those without contortion, shrink its scope to just write+register+grant and leave
IO/argparse per-build.

## Suggested next step
Approve the scope/style/proving-ground above (or adjust), then I draft it as an ADR (≈0036) and a
first implementation against ICD-10-PCS. Until then nothing changes — this is a recommendation, not
a build.
