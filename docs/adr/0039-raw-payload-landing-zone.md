# 0039 — Raw payload landing zone: Volume-first ingestion for all extracted sources

## Status
Proposed.

**Extends and partially supersedes ADR 0032** (generalizes the raw Volume snapshot from
revise-in-place sources to *all extracted* sources; adds the re-fetch-cost rationale 0032
did not weigh; formalizes a retention-mode taxonomy). 0032's revise-in-place snapshot
survives as one retention mode; its stance that vintage-reproducible sources keep *no*
local copy (re-pull on demand) is superseded. **Refines ADR 0037** (the raw layer is now
`source → Volume payload → raw Delta`). Relates to 0034 (vintage model), 0036 (shared
builder), 0007 (update semantics).

## Context
ADR 0032 established a raw immutable **Volume snapshot** for *revise-in-place* sources
whose history is valuable (CVX, CDC weekly), and deliberately chose **not** to keep a local
copy for *vintage-reproducible* sources (geography/Census), reasoning they can be re-pulled
on demand. Two things sharpen that trade-off now that builds are moving onto the shared
builder (ADR 0036) with a 1:1 raw Delta layer (ADR 0037):

1. **Re-fetch is not free for reproducible sources.** IPUMS NHGIS extracts are slow async
   submit→poll→download; CPT is throttled to 10 calls/day; a rebuild, a raw-schema change,
   or a reprocess re-hits the API for no new data. (The first `us_state` layered runs
   re-submitted the IPUMS extract *every* run.) A persisted payload makes every rebuild,
   reprocess, or parser fix **zero-API**.
2. **Fidelity, audit, and replay matter for every source, not just revise-in-place.** The
   verbatim payload is the reproducible record and lets us re-parse history if the parser
   improves — valuable even when the source is technically re-pullable.

And extraction is not only files: REST/JSON and query payloads are ephemeral and can revise
silently, so they warrant archiving **as much as or more than** files.

## Decision
1. **Every extracted source payload lands verbatim in a Unity Catalog Volume before any
   parsing** — a file (zip/CSV/XML/shapefile) *or* an API/query payload (JSON response,
   paginated payloads, a serialized query result) — stored as **format-faithfully as the
   extraction allows**, engineer-only. The 1:1 raw Delta table (ADR 0037) is then built
   **from the Volume payload**, not by re-fetching. **Generated** reference (no extraction
   from an external source, e.g. `time`) has **no Volume** — its raw table is built from the
   generator; provenance is the generator code + version.

2. **Landing retention mode is chosen from the same source-behavior analysis that picks the
   table's `update_semantics`** (ADR 0007). It is a parallel axis, not a new judgment:

   | Source behavior (→ table semantics) | Volume retention mode | Fetch behavior |
   |---|---|---|
   | Immutable vintaged (→ `vintage_snapshot`) | **one payload set per vintage**, immutable | fetch once **per `(landing, vintage)`**; skip if that combo is already present |
   | Revise-in-place (→ `snapshot_replace`/SCD2) | **timestamped snapshot per extraction**, never overwrite a date | fetch each run (= 0032's mechanism) |
   | Incremental/append (→ `append_only`/merge) | **payload per extraction batch/window** | fetch each window |
   | Generated (no extraction) | **none** | n/a |

3. **Volume placement follows ADR 0037 (source catalog).** One landing Volume per subject's
   raw layer, engineer-only: `/Volumes/<source_catalog>/<subject>_raw/_landing/...`, with
   `vintage=<v>/` (immutable) or `snapshot_date=<YYYY-MM-DD>/` (revise-in-place) /
   batch-window partitioning per the retention mode, created `IF NOT EXISTS` by the build's
   ensure phase. (This relocates 0032's `codes.cvx_raw` Volume from the *model* catalog to
   the *source* catalog, consistent with reference data now landing source-first.)

4. **Builder API (ADR 0036).** A `RawLanding` splits its single `acquire` into
   `fetch_to_volume(ctx, vintage)` (idempotent; honors the retention mode — skips when an
   immutable vintage's payload is already present) and `read_from_volume(ctx, vintage) →
   DataFrame`, plus a `landing_retention` mode. `build_reference` gains a **Phase 0**
   (ensure Volume payloads) before Phase A (write raw Delta); the raw Delta write reads from
   the Volume. Phase 0 is skipped for generated sources.

5. **Two representations of raw, deliberately** — Volume bytes (archive / fidelity / replay /
   refetch-avoidance) and the raw Delta table (queryable / validatable bronze). Both are 1:1
   with source; neither is accidental duplication.

## Alternatives considered
- **Keep ADR 0032 as-is** (Volume only for revise-in-place; re-pull reproducible sources).
  Rejected: ignores the real re-fetch cost on slow/throttled sources and forfeits
  fidelity/replay for reproducible ones — both of which the build rework surfaced.
- **Volume only, no raw Delta table** (parse files straight from the Volume in `processed`).
  Rejected: the raw Delta gives a stable schema, queryability, and a DQ gate; re-parsing
  shapefiles/JSON from the Volume on every downstream read is fragile and slow.
- **Persist files but not API/query payloads.** Rejected: ephemeral API responses are the
  payloads *most* in need of an archived, reproducible copy.
- **Content-addressed (hash) storage instead of vintage/date paths.** Deferred: the
  vintage/snapshot-date partitioning is simpler and matches the table keys; revisit if
  dedup across identical re-pulls becomes worthwhile.

## Consequences
- **Rebuilds, reprocessing, and parser fixes run zero-API** off the Volume; throttled
  sources (CPT, 10/day) become tractable; slow IPUMS extracts are fetched once per vintage.
- **`us_state` (already built) gets retrofitted**: today it re-downloads NHGIS to a temp dir
  every run; it becomes fetch-once-to-Volume (per-vintage immutable) + read-from-Volume.
  County/tract/zcta/block-group/block are built on this from the start.
- **Storage grows**, bounded by the retention mode: immutable-vintage keeps one copy per
  vintage; revise-in-place grows one snapshot per run (as 0032 already accepted); the
  size/frequency criterion (0032) still routes large/frequent sources toward SCD2 on the
  *table* side.
- **The builder gains a Phase 0** and `RawLanding` gains the fetch/read split + retention
  mode; every migrated build implements `fetch_to_volume`/`read_from_volume`.
- **ADR 0032 is narrowed** to "the revise-in-place table-tracking half" (snapshot_replace/
  SCD2 keyed by `snapshot_date`) — orthogonal to and still valid alongside this ADR, which
  owns the Volume landing for all sources. CVX, when built, uses this ADR's snapshot_per_run
  Volume mode + 0032's in-table snapshot_replace.
- **Generated reference (`time`) is unaffected** — no Volume; built from the generator.
- Requires the source catalog to host engineer-only Volumes under each `<subject>_raw`
  schema (a grants/governance addition, parallel to the raw/processed schema grants).

## Implementation notes (non-normative)
Sequencing: amend the ADR 0036 builder (`RawLanding` fetch/read split + `landing_retention`;
`build_reference` Phase 0) → retrofit `us_state` (per-vintage-immutable Volume) and confirm
fetch-once in dev → then county/tract inherit it. The 0037 raw definition and the
README/0036 docs get the `source → Volume → raw Delta` refinement folded in at the same time.
