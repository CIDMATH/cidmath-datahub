# 0037 — Reference-data ingestion: uniform source→model path, processed stage by complexity

## Status
Proposed. **Amends ADR 0014** (reference now uses the landing→model path, not one-shot-to-model) and
**ADR 0030** (ICD hierarchy: flat → layered, additive); **extends ADR 0003's framing** (the source
catalog is the **raw/landing layer**, origin-agnostic — not exclusively externally-sourced);
**simplifies/extends ADR 0036** (one placement model + an optional processed stage). Relates to 0001
(layering), 0021/0028 (geography), 0032 (raw Volume snapshots), 0034 (vintage model), 0035
(`(geoid, geo_vintage)` conformance for augmenting inputs), 0039 (Volume payload landing for all
extracted sources — the raw layer now lands via a Volume first).

## Context
Reference builds historically went straight to the **model** catalog in one step. That (a) is a soft
deviation from ADR 0003 for sourced data (externally-sourced data landing directly in the integrated
catalog with no raw layer), and (b) oversimplifies hierarchical / multi-grain reference data (one
script does download + parse + derive-structure + enrich + write; the flat output can't express the
source's structure).

Two refinements emerged while designing this:
- **RUCA** (a flat rural-urban code that augments geography and must land where geography's processed
  step can join it same-catalog) showed **placement and workflow are separate axes**: placement
  should be uniform; only whether there's a *processed* stage is complexity-driven.
- For **continuity** — one place data lands and one way it's processed — even purely **logic-generated**
  reference (e.g. `time`) should follow the same path rather than be a model-only special case. Its
  "raw" is simply the generator's output landing in the raw/landing layer instead of a fetched file.

So the model is: **one path for all reference data**; the source catalog is the raw/landing layer
(origin-agnostic); the only thing that varies is whether a *processed* stage sits between raw and the
promoted canonical.

## Decision
1. **One uniform path for all reference data:** `raw (source catalog) → [processed (source catalog)]
   → canonical (model catalog)`. Raw lands in `ecdh_<env>.<subject>_raw` — whether **fetched** from an
   external source *or* **produced by internal logic** (the generator's output is the raw landing).
   The canonical/enriched table is promoted to `ecdh_model_<env>.<subject>.<table>`. Flow is
   landing→model only, never model→source. Every layer is vintage-stamped (`vintage_snapshot`, ADR
   0034). Per **ADR 0039** the raw layer lands in two steps: every **fetched** source's payload first
   lands verbatim in a source-catalog **Volume**, and the raw Delta table is built 1:1 from it
   (generalizing ADR 0032's revise-in-place snapshot to *all* extracted sources, for fetch-avoidance +
   fidelity); **generated** reference has no Volume (its raw = the generator output).

2. **The processed stage is optional, gated by complexity** — this is the only thing "tier" decides
   (not the catalog):
   - **Simple** (flat, no hierarchy/multi-grain): `raw → promote canonical`. No processed stage.
     E.g. CVX, MVX, ICD-10-PCS, HCPCS, a state list, RUCA, and most generated tables.
   - **Complex**: `raw → processed → promote canonical`; the processed stage derives level/grain
     tables and enriches. E.g. ICD-9/10-CM, geography, LOINC Parts. (A *generated* table can also be
     complex — e.g. `time` may derive `epi_week` from the generated calendar in a processed step.)

   **Criterion** for a processed stage — any one: internal **hierarchy/levels**; **multiple
   constituent grains or files**; **one source → multiple downstream shapes**. None → simple.

3. **Generated reference uses the same path** — no carve-out. Its raw is the logic-produced output
   landing in `<subject>_raw` (no external artifact, no Volume snapshot needed), then promoted (and
   processed if complex) exactly like sourced data. This is what makes the source catalog the
   origin-agnostic raw/landing layer (extending ADR 0003's framing).

4. **Augmenting inputs land raw in the consuming subject's `*_raw` (the RUCA rule).** A simple
   reference that augments a complex subject (RUCA / SVI / ADI / urbanicity on geography) lands its
   raw in *that subject's* `<subject>_raw` so the processed step joins it same-catalog; promote a
   standalone canonical to the model catalog if it's also wanted as a direct lookup, else denormalize.
   Coded to a geography vintage → the ADR 0035 `(geoid, geo_vintage)` contract applies to its keys.

5. **Layering *adds*, doesn't replace, the flat canonical.** The canonical flat/enriched table stays
   the conformance workhorse; processed level/grain tables are additive analytical structure.

6. **The shared builder (ADR 0036) is one path with an optional processed stage.**
   `build_reference_table` always does `raw (source) → [processed (source)] → promote canonical
   (model) → register → grant`; simple subjects skip the processed stage; generated subjects run a
   generator in place of a fetch for the raw step. No separate single-step / multi-stage / model-only
   variants. Conventions inherited by construction: atomic `replaceWhere`, `ingested_at`, no
   `_current` views, `TableDQ`, schema-declared-once, pure logic (ADR 0011).

7. **Serving form — the model-catalog canonical is the *enriched* (denormalized) dimension; the
   normalized levels stay in processed** ("normalize in the build layer, serve a star"). For a
   reference *dimension* with internal hierarchy or parents, the processed stage holds the normalized
   per-level tables (engineer-only), and the **one** table promoted to the model catalog is the
   enriched form: child rows carry the parent **keys *and* denormalized parent attributes** (labels).
   There is **no separate lean-base + `_enriched` view** (this amends ADR 0028) — matching how
   `codes.icd10cm` already denormalizes chapter/block onto the code row (ADR 0030). Keep the parent
   **keys** on the canonical so it stays joinable/conformant and re-derivable. **Bounds:** (a) this is
   a *dimension* pattern — **facts stay thin and FK to dimensions**, never denormalize dimension
   attributes onto fact rows; (b) **enrich at *every* grain, including census block** — block rows
   should carry the full parent chain (block group, tract, county, state) keys + labels, because
   carrying that geographic context is the point. Large grains (block ~8M rows) are a
   **storage/clustering *awareness*** flag, **not** a reason to skip enrichment: cluster well (e.g. by
   `(geo_level, vintage)` / parent geoid), keep geometry in the companion `boundary` table (off the
   enriched row), and accept the bounded denormalization cost — only revisit if a measured cost
   actually bites. (c) **the enrichment join runs in `processed`, against same-source-catalog tables**
   — never by reading the model catalog (decision 1). So a child level that enriches from parent
   levels (block group ← tract/county/state, for IDs **and** labels) requires those parents to be
   present in the **source** catalog: a complex multi-level subject is therefore migrated
   **parents-first** — each parent's `processed` table must exist before its children build. For such
   subjects this is a **rebuild**, not a raw-layer backport (see Consequences); do **not** instead
   join the model-catalog canonicals at promote time (a rejected alternative — it reintroduces a
   model→source-shaped dependency and splits enrichment across two layers).

8. **Validation: validate the staging, gate the promote — one pattern at every size.** DQ runs as a
   **query-based** check over the raw/processed staging (engineer-only), and the **promote to the
   canonical is gated** on it passing — so the consumer-facing canonical never lands bad data, at any
   scale (a 290-row code set and an ~8M-row block table use the same flow). The layering's staging
   *is* the "never land a bad table" mechanism the in-memory pre-write pattern provided, now uniform —
   this **supersedes the reference-vs-conformance validation split of ADR 0027**. **In-memory
   validation** (pure checks on parsed records before the raw write) is a permitted **optional
   fast-path for genuinely small data** — not a separate architectural pattern, and it needs no
   parallel DQ helper; `TableDQ` (query-based) is the single DQ family. The pure parse + check *logic*
   stays pure and unit-tested (ADR 0011); only *where* it executes (driver vs Spark) varies by scale.

## Alternatives considered
- **Tier *placement* too** (simple → model-only; complex → source). Rejected: RUCA showed placement ≠
  workflow; two placement models is a needless special case and leaves simple reference deviating
  from ADR 0003.
- **Carve out generated reference as model-only** (an earlier version of this ADR). Rejected for
  **continuity**: a single landing zone + one flow is simpler to teach and build than a
  sourced-vs-generated fork, and it keeps the builder to one shape. Accepted costs: the source
  catalog now holds some internally-generated data (the "raw/landing layer" reframing), and for
  generated/flat tables raw ≈ canonical so the promote is near a copy + an extra table. Bounded at
  this scale.
- **Keep all reference one-shot in the model catalog (status quo).** Rejected: deviates from 0003 and
  oversimplifies complex data.

## Consequences
- **One rule, zero forks:** every reference subject is `raw → [processed if complex] → canonical`;
  raw always lands in the source/landing catalog, canonical always in the model catalog. No
  sourced-vs-generated and no simple-vs-complex *placement* branching to get wrong.
- Reference data conforms to ADR 0003 as reframed (source catalog = raw/landing layer; model catalog =
  integrated/promoted).
- **Cost, stated honestly:** for flat or generated tables raw ≈ canonical, so the promote adds a near-
  copy table + `_ops` row + grants for little transformation. Accepted as the price of one coherent
  pattern; the source catalog also now contains internally-generated reference (a semantic stretch we
  take on deliberately).
- **Migration stakes depend on cross-level enrichment:**
  - **Flat / single-grain subjects = a low-stakes backport, not a rebuild:** existing model-only
    reference — `codes.cvx`, `codes.ndc_*`, `codes.loinc*`, `codes.icd10pcs`, the icd9/10-cm
    canonicals, **and generated `time`** — gains a `<subject>_raw` landing layer in the source catalog
    (data reproducible / regenerable; canonicals unaffected).
  - **Complex subjects whose children enrich from parents = a rebuild** (decision 7 bound c). Geography
    is the first: its levels must be migrated **parents-first** into the source catalog (as
    `<source>_*`, e.g. `us_census_state`), the old non-layered model tables **dropped**, and each level
    **re-promoted enriched** through the builder. Because this drops and rebuilds the *live* integrated
    dimension, it needs a **cutover plan**: do the swap **per-level atomically**, and verify the
    re-promoted canonical matches the pre-migration row counts + keys (no silent loss) before dropping
    the old table; downstream FK consumers are exposed during the window.
  - RUCA (being built now via the old methodology) is re-homed under decision 4.
- **Templates re-simplify** to one sourced/generated reference path (processed stage optional),
  retiring the "simple→task / complex→layered" routing.
- Amends 0014; amends 0030; extends 0003's framing and 0036.

## Implementation notes (non-normative)
Decision tree for a reference subject:
```
raw → <subject>_raw (source/landing catalog)
   • fetched (external source) or generated (run the generator) — same step
Needs a processed stage? (hierarchy / multi-grain / multiple downstream shapes)
   ├─ no  → promote canonical → model catalog
   └─ yes → processed (derive levels/grains, enrich) → promote canonical → model catalog
(Augmenting a complex subject? land raw in THAT subject's <subject>_raw — decision 4.)
```
Proving grounds (both prove the processed-stage path): **geography block-group + block** (multi-grain)
and **ICD-10-CM relayered** (hierarchy). ICD-10-PCS already exercised the no-processed-stage path; it
+ the other model-only tables (incl. `time`) need only the raw-layer backport. Sequence geography
first — it builds the ADR 0036 builder — but note it is **not greenfield**: it is a parents-first
**migration** of the existing levels (state → county → tract → block-group → block; decision 7 bound c
+ the cutover note above), since block-group/block enrich from their parents same-catalog. Then the
ICD-10-CM relayer reuses the builder.

## Applied — codes backport wave 1 (ICD-10-PCS + ICD-9 Procedures)
The `codes` subject's model-only medical-code builds are being folded onto `build_reference` the same
way RUCA was (ADR 0038 delta 6). **Wave 1 (this change):** the two flat, public, single-payload
builds — `codes.icd10pcs` and `codes.icd9_procedures` — are done. Each now: lands its CMS zip verbatim
in the source-catalog Volume `ecdh_<env>.codes_raw._landing` (ADR 0039, `PER_VINTAGE_IMMUTABLE`,
fetch-once), parses into the 1:1 raw table `ecdh_<env>.codes_raw.<table>`, and promotes the canonical
`ecdh_model_<env>.codes.<table>` from raw via the builder (per-edition atomic `replaceWhere`, `_ops`
registration, grants — all builder-owned). These are the **first builds to use a non-default
`vintage_column` (`edition_year`)**, exercising the builder's generic per-vintage write path. The
hand-rolled `run_build` write/register/grant and the `_current` views are removed (ADR 0034: "current"
= `MAX(edition_year)` / the live idiom, matching the RUCA fold). Canonical schemas + rows are
unchanged — a build-mechanism fold-in with data parity; consumers unaffected. The pure parsers in
`reference/icd10pcs.py` / `reference/icd9_procedures.py` are reused unchanged. **Deferred to later
waves:** CVX/NDC (revise-in-place + Volume relocation), the authenticated sets (LOINC/RxNorm/SNOMED),
and the multi-source hierarchical ICD-CM.
