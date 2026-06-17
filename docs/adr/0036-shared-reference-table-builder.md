# 0036 — Shared reference-table builder (`ReferenceTableSpec` over `run_build`)

## Status
Proposed. Extends ADR 0027 (the `run_build` seam); builds on 0011 (pure logic modules), 0029
(`TableDQ`), 0008 / 0018 (registration + grants). Bakes in 0034 (vintage-snapshot semantics, atomic
per-vintage write, no `_current` views) and the `ingested_at` audit-column standard (ADR 0006);
relates to 0035 (the `(geoid, geo_vintage)` conformance contract, for later fact builds). Triggered
by the I1 review — see `docs/reviews/i1-build-end-to-end-findings.md`.

## Context
`run_build` (ADR 0027) standardized the build *lifecycle* (`ensure → [DQ] work → register →
grant`), but everything *between the pure parser and `run_build`* is still hand-rolled per
entrypoint. The I1 review quantified it on `build_ndc.py`: ~40–50% of each build is recurring
skeleton or mechanical mapping — `_table_has_column` (byte-identical across builds), `_write_table`,
`_create_current_view`, the `_ensure` DDL, `_grant`+`verify`, the `_register` scaffolding, `main()`
argparse, the IO helpers (download / zip / Volume snapshot / `_get_secret`), and a row-dict
construction that re-declares every Spark-schema field by hand.

That copy-paste surface is the **demonstrated root cause** of the drift the SDE review kept finding:
`loaded_at` vs `ingested_at` (D1), `snapshot_replace` vs `full_refresh` (0034), `_get_secret` copied
across builds (I3), uneven `TableDQ` adoption (I1 — `build_ndc` hand-rolls ~210 lines of raw
`record()`), and the schema-declared-twice DRY hazard. Enforcing conventions by "ADR + CI + code
review" on hand-rolled builds keeps drifting; the fix is to make them hold **by construction**.

## Decision
1. **A config/composition builder, not a base class.** Introduce `ReferenceTableSpec` plus
   `build_reference_table(specs, ...)` that sits *above* `run_build`. Composition + injected
   callbacks fits the functional seam and stays unit-testable; inheritance is rejected.

2. **The builder owns the invariant skeleton:** the `ensure` DDL (schema, optional Volume); the
   **atomic per-vintage write** (`replaceWhere`, `vintage_snapshot`; first build seeds — ADR 0034);
   the `register` scaffolding (assemble the `DatasetCatalog`/`DatasetEngineering` entries from the
   spec, defaulting `update_semantics="vintage_snapshot"`); `grant` + `verify` (reader/engineer
   tiers, optional `READ VOLUME`); the IO helpers (download / zip / Volume snapshot / a single
   `_get_secret` consolidated into `common/`); `argparse`/`main`; and **row derivation from the
   Spark schema** (eliminating the hand-listed dict).

3. **The source injects what genuinely varies:** the Spark `StructType`; the pure parser
   (`reference/*.py`, ADR 0011); the **DQ business rules** as a callback built on `TableDQ` (ADR
   0029); the metadata values (license / URLs / description / `known_limitations`); the vintage key
   (name + type); and an optional source spec (URL / Volume / secret scope+key).

4. **Adoption fixes drift by construction.** A build on the builder gets `ingested_at`,
   `vintage_snapshot` + atomic `replaceWhere` + immutability, **no `_current` views**, and `TableDQ`
   for the common checks — automatically, because the builder is the only place that logic lives.

5. **Multi-table sources pass a list of specs sharing one run** (NDC product+package; LOINC
   core+map_to).

6. **Proving ground, then opportunistic backport.** Build the builder and prove it **greenfield on
   ICD-10-PCS** (simplest, public, mirrors `codes.icd10cm`). Then exercise a **hard case** (the
   2-table NDC *or* the faceted+secret LOINC) **before** the builder becomes mandatory. Migrate the
   existing builds (cvx / loinc / ndc / icd) opportunistically — folding each into the ADR-0034
   reclass + `loaded_at`→`ingested_at` rename that already touch them. Existing builds keep working
   until migrated.

## Alternatives considered
- **Base-class / inheritance builder.** Rejected: doesn't fit the functional `run_build` seam;
  composition with injected callbacks is more flexible and testable.
- **Keep ADR + CI + code-review enforcing conventions on hand-rolled builds.** Rejected: it
  demonstrably drifts — the SDE review findings *are* the evidence. Conventions should hold by
  construction.
- **A bigger framework that also owns parse and DQ.** Rejected (over-abstraction): parse and DQ are
  exactly where real cross-source variation lives, so they stay injected (see the pre-mortem).
- **Do nothing / defer.** Rejected: the per-build cost compounds with every queued code system
  (ICD-10-PCS, RxNorm, SNOMED, HCPCS).

## Consequences
- New module (e.g. `common/reference_builder.py`) + `ReferenceTableSpec`; `_get_secret` consolidated
  into `common/secrets.py` (closes I3 SHOULD-FIX #2).
- New reference builds become "a parser + a spec" instead of ~700 lines, and inherit the converged
  conventions rather than re-typing them.
- Incremental migration; each backport is the natural moment to apply the 0034 reclass and the audit
  rename. No big-bang.
- **Risk — over-abstraction** (I1 pre-mortem): the sources genuinely differ (LOINC faceted +
  licensed download + 2 tables; ICD hierarchy + Apr-1 overlay; NDC 2 linked tables + Volume; CVX
  Volume snapshot). Mitigation: parse / DQ / schema stay injected, and the builder must clear a hard
  case before becoming mandatory — if it can't express NDC-2-table or LOINC without contortion,
  shrink its scope to write + register + grant and leave IO/argparse per-build.
- The builder becomes the single place to later add the deferred `vintage_key` metadata (0034), the
  `(geoid, geo_vintage)` FK contract (0035) for fact builds, and schema-derived row construction.

## Implementation notes (non-normative)
```python
@dataclass(frozen=True)
class ReferenceTableSpec:
    schema: str                 # "codes"
    table: str                  # "icd10pcs"
    spark_schema: StructType    # the single declaration; rows derived from it
    vintage_key: str            # "edition_year" | "snapshot_date" | "loinc_version"
    parse: Callable[..., list]  # pure, from reference/*.py
    dq:    Callable[[BuildContext, list], None]   # injected, TableDQ-based
    catalog_meta: ReferenceMeta # license / urls / description / known_limitations
    source: SourceSpec | None   # optional: url / volume snapshot / secret scope+key

def build_reference_table(specs: list[ReferenceTableSpec], catalog, groups, ...) -> None:
    ...  # wires _ensure / atomic write / register / grant+verify / IO / argparse via run_build
```
Build the builder and prove it on ICD-10-PCS in one PR; do not backport existing builds in the same
PR.

### Registration gaps to close (from the I2 review — `docs/reviews/i2-ops-metadata-model-findings.md`)
Because the builder owns the registration scaffolding, it is the single place to fix the `_ops`
writer⊂schema gaps the I2 review found — close them here rather than as N per-build edits:
- **Populate the consequential `dataset_catalog` columns** that `DatasetCatalogEntry` currently omits
  — at least `refresh_cadence` / `reporting_lag` / `revision_cadence` (ADR 0007's source-behaviour
  mechanism, presently unreachable) — and drop or defer the remaining always-null columns from the
  DDL + the `discovery.datasets` view so the analyst surface stops advertising perpetually-null fields.
- **Write `pipeline_runs` from `run_build`** (`run_id` / `pipeline_reference` / start / end / status /
  `tables_written` / `triggered_by`): the table exists but has **no writer**, and the
  `common.pipeline_runs` helper its DDL comment references does not exist. Closes the run-history /
  observability gap (also tracked under O1).
- **Make `freshness_sla_hours` and `history_table` settable** on `DatasetEngineeringEntry` —
  freshness alerting, and `history_table` is the `merge_scd2_side` contract (ADR 0007) that becomes
  required when ADR 0034's SCD2 escalation first lands.
- **Validate the controlled-vocabulary fields centrally.** Today only `update_semantics` /
  `materialization_type` are validated at construction. Add `layer` (reconcile first: code uses
  `reference`, the DDL comment says `model`), `access_tier` (`open` in code vs `public` in the DDL),
  `subject`, `source_provider_code`, `spatial_resolution` — to `vocabularies.py` + dataclass
  `__post_init__`, and confirm CI (ADR 0016) covers them.
