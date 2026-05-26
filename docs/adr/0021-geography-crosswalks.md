# 0021 — Geography crosswalks: ship NHGIS as published

## Status
Accepted — 2026-05-26

## Context
ADR 0020 declared `geography.crosswalk` as the cross-vintage translation surface but deferred the question of which crosswalks to ship and how to shape the table. The slice-1 wiring surfaced the constraint that drives this decision: NHGIS publishes **no direct same-level crosswalk** — no tract→tract, no zcta→zcta, no county→county. Its 2010↔2020 crosswalks are sourced *from block groups* and distributed to bg, tract, and county targets in both directions: six file sets total. Building same-level crosswalks (e.g. tract→tract) from these requires aggregating by source population denominators, which live in the `population` schema and would introduce a cross-schema dependency in the reference layer.

We also need a stable shape that can host crosswalks for multiple source/target levels and multiple weight kinds. The NHGIS files carry an area-proportion column (`parea`) plus interpolation weights — typically `wt_pop`, `wt_hh`, `wt_fam`, `wt_adult` — with the exact set varying per file depending on what was computable from the source denominators (e.g. `wt_fam` falls back to a different basis when a source bg has no families).

## Decision
We ship the NHGIS bg-sourced 2010↔2020 crosswalks **as published**, normalized into a single long-form `geography.crosswalk` table. All six directions are included (bg2010 → {bg, tract, county} 2020 and the reverse), with all weight kinds NHGIS provides for each file. The table shape:

| column | type | notes |
|---|---|---|
| `source_level` | string | always `"bg"` for slice 2b; the column generalizes to future source levels |
| `source_vintage` | int | TIGER basis year of the source unit |
| `source_geoid` | string | derived from source GISJOIN via `gisjoin_to_geoid` |
| `source_gisjoin` | string | NHGIS source key (what the file natively uses) |
| `target_level` | string | `"bg"`, `"tract"`, or `"county"` |
| `target_vintage` | int | TIGER basis year of the target unit |
| `target_geoid` | string | derived from target GISJOIN |
| `target_gisjoin` | string | NHGIS target key |
| `weight_kind` | string | controlled vocabulary: `pop`, `hh`, `fam`, `adult`, `area` (=`parea`) |
| `weight` | double | interpolation weight (one row per source × target × weight_kind) |

One row per (source unit, target unit, weight_kind) — long form. Consumers pick the row set matching their data's source level, vintage, and target via `WHERE source_level = … AND source_vintage = … AND target_level = … AND target_vintage = … AND weight_kind = …`. DQ: weights sum to ~1 per source unit within each `(source_level, source_vintage, target_level, target_vintage, weight_kind)` group, using the existing `validate_crosswalk_weights` helper extended with the group keys. The table is Liquid-Clustered by `(source_level, source_vintage, target_level, target_vintage)` so the dominant filter pattern prunes hard. Download uses the NHGIS supplemental-data convention via `ipums.get(url, stream=True)`; no extract submission is required for crosswalks.

The build runs as a **separate `build_crosswalk.py` entrypoint and `crosswalk_job.yml`**. This decouples crosswalk refresh cadence from the boundary refresh, keeps `build_geography.py` from growing further, and lets the crosswalk job carry its own (lighter) dependencies — no geospatial libraries, just `ipumspy` and `pandas`. Same access posture as the rest of `geography` (reader tier for both groups via ADR 0018/0020 — already applied at schema level).

## Alternatives considered
- **Derive same-level crosswalks (tract→tract, zcta→zcta, county→county)** by aggregating the bg-sourced files with source population denominators. Rejected: cross-schema dependency on `population`, more derivation logic and its own DQ surface, and divergence from what NHGIS actually publishes — leaving consumers unable to compare our derived weights against the upstream artifact. The "as-published" approach is faithful and lets consumers derive same-level crosswalks themselves when they have the population data and the analytical intent.
- **Wide table** with one column per weight kind (`wt_pop`, `wt_hh`, …) rather than long form. Rejected: brittle if NHGIS adds or removes weight kinds; `weight_kind` as a column makes group-by/filter natural and the schema doesn't have to evolve when the file set does.
- **Ship only bg→bg as the atomic, and derive bg/tract/county targets via views**. Rejected: the bg→tract and bg→county files NHGIS publishes are pre-aggregated correctly by NHGIS using block-level populations we don't have to re-derive. Shipping them directly is both more useful and more faithful.

## Consequences
- A single conformed cross-vintage translation surface — long-form, multi-level, multi-weight-kind — without a cross-schema dependency on `population`.
- Data volume is meaningful but tractable: six file sets × hundreds of thousands to low millions of source-target pairs × multiple weight kinds; estimate low single-digit millions of rows total. Liquid Clustering on the dominant filter columns keeps slices cheap.
- The table doesn't translate same-level data (tract→tract etc.) directly. Consumers who need that derivation compose with the bg-sourced rows themselves (e.g. allocate bg-level data to target tracts, then aggregate to source tracts) — the same workflow anyone using NHGIS crosswalks already follows.
- Crosswalk file structure (exact column names per file, weight kinds present) is **verified at download time**. NHGIS occasionally adds or removes weight kinds across releases; the long-form table absorbs that without schema changes, and the loader maps whatever weight columns the file carries into the controlled `weight_kind` vocabulary.
- Separate `build_crosswalk.py` + `crosswalk_job.yml` means crosswalk and boundary refreshes can run on different cadences, and the crosswalk job's env is lighter (no geospatial libraries). It's a new approval-gated job YAML; no new `databricks-common.yml` variables (reuses `ipums_secret_*`).
