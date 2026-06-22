# 0028 — Geography hierarchical-filter views

## Status
Accepted — 2026-05-31. **Amended by ADR 0037 (2026-06-22):** for the *serving form*, the enriched
data is now the **promoted model-catalog canonical** — parent attributes denormalized onto the child
dimension, computed in the processed stage — **not a view over a lean base**. So the `*_enriched`
views are retired in favour of one enriched canonical per level. 0028's intent (filter children by a
readable parent; parent keys denormalized onto children) stands; only the lean-base-plus-view
*mechanism* is superseded. See ADR 0037 decision 7.

## Context
A very common end-user need is filtering a census geography by a *higher* level: "all counties in Georgia", "all census tracts in Fulton County, GA". The `geography` reference already supports this **by parent code** with no join, because the parent geoids are denormalized onto the child tables (ADR 0020): `us_county` carries `state_geoid`, and `us_tract` carries both `state_geoid` and `county_geoid`. So `us_county WHERE state_geoid = '13'` and `us_tract WHERE county_geoid = '13121'` are single-table filters today.

What is missing is the **human-readable** parent. The parent *display* attributes — state name, USPS abbreviation (`GA`), HHS region, county name — are not on the child rows. So filtering by the readable parent ("in Georgia", "in Fulton County") instead of the code still requires hand-writing the hierarchy joins (`us_tract` → `us_county` → `us_state`), and `us_tract` has no `name` of its own at all. That join burden falls on every analyst and every dashboard query, which is exactly the kind of friction that matters as we open the data to a broader audience.

(ZCTAs are the exception: they cross county and state boundaries and have no single nesting parent, which is why `us_zcta` has no parent geoid. They are out of scope here.)

## Decision
Provide **convenience views** that denormalize the stable parent display attributes onto each child level, rather than denormalizing columns into the canonical base tables. Owned by the `_reference` bundle, in the `geography` schema of the integrated catalog:

- **`geography.us_county_enriched`** = `us_county` + `state_name`, `state_stusps`, `state_hhs_region` (from `us_state`).
- **`geography.us_tract_enriched`** = `us_tract` + `county_name` + `state_name`, `state_stusps`, `state_hhs_region` (from `us_county` and `us_state`).

Each view `SELECT <child>.*` (every base column preserved; the child's own `name` stays `name`, the parent's is aliased `state_name`/`county_name`) and joins up the hierarchy **vintage-keyed** (`… AND child.vintage = parent.vintage`), so a query filters by `vintage` plus any readable parent attribute on one object: `us_tract_enriched WHERE vintage = 2020 AND state_stusps = 'GA' AND county_name = 'Fulton County'`.

The joins are **INNER**: every child has a valid parent (FK integrity is already a blocking DQ guarantee in the entity builds, ADR 0023 P0-3). The view-build job asserts `count(view) == count(base)` per view as a blocking DQ check, so any orphaned parent reference fails loudly rather than silently dropping child rows.

View SQL is single-sourced in `cidmath_datahub.reference.geography.us_enriched_view_definitions` (pure, unit-tested). The build entrypoint `bundles/_reference/src/build_geography_views.py` is the first to use the shared `run_build` seam (ADR 0027): `ensure → [DQ: create views + rowcount parity] → register → grant`. The views register in `_ops.dataset_catalog` with `materialization_type='view'`, `is_hosted=false`, and `derived_from` listing the base + parent tables, so they appear in `discovery.datasets`. They inherit the geography schema's reader-tier grants (ADR 0018).

## Alternatives considered
- **Denormalize parent labels onto the base tables** (`state_stusps`/`state_name` columns on `us_county`; `county_name`/`state_*` on `us_tract`). Best raw scan ergonomics, but it duplicates parent attributes into the canonical reference tables — redundancy that must be kept consistent at build time, and a change to tables other systems treat as authoritative. Rejected in favor of views, which deliver the same single-object filtering with zero base-table redundancy and zero extra storage/refresh cost.
- **A single flat `us_hierarchy` dimension** (tract + its county + state attributes, one wide table). Convenient for the fully-nested US case but materializes a large redundant table and bakes in one traversal; the per-level views compose more naturally and cost nothing. Rejected for now; revisit only if a wide flat dim is specifically requested.
- **Do nothing — document the join pattern.** Rejected: the join burden recurs for every analyst/dashboard, and `us_tract` having no name makes the two-hop join non-obvious. The ergonomic win is cheap to provide.

## Consequences
- **Single-object filtering by readable parent.** `geography.us_county_enriched` / `us_tract_enriched` let users filter by state name/USPS/HHS region and county name without joins, while code-based filtering on the base tables (`state_geoid`/`county_geoid`) continues to work unchanged.
- **Canonical tables stay normalized.** No redundant columns on `us_state`/`us_county`/`us_tract`; the views carry the denormalization and cost nothing to store. They recompute on read (a view), so they are always consistent with the base tables.
- **Vintage-correct.** Joins are vintage-keyed, so the enriched rows never mix a child of one vintage with a parent of another; filter on `vintage` as usual.
- **Discoverable + governed.** Registered in the catalog (as views) and covered by geography's reader grants; surfaced in `discovery.datasets`.
- **First `run_build` adopter.** This entrypoint exercises the ADR 0027 seam in production, a useful proof before retrofitting weather/geography.
- **Pattern, not one-off.** When ZCTA→place or international subnational hierarchies warrant the same treatment, add analogous `_enriched` views; the `us_enriched_view_definitions` helper is the place to extend. Deploy order: after `build_geography`.
