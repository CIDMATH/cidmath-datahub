# 0032 — Source-history preservation for revise-in-place sources

## Status
Proposed — 2026-06-15

(Proposed ahead of the `codes.cvx` implementation, which is the first application and
pins the remaining specifics — the Volume path/naming and retention. Moves to Accepted
when that PR merges.)

## Context
Most of our sources are **vintage-reproducible**: Census/geography ships dated vintages
that stay downloadable, so re-fetching reconstructs any past state and we keep no history
of our own (ADR 0024 re-pulls a vintage on demand). Some sources are not. The CDC IIS
**CVX** vaccine code set is **revised in place** — codes change status (`Active` →
`Inactive`), descriptions are edited, new codes appear — and CDC publishes only the
*current* list with a per-code "Last Updated" date. There is no published archive of
past states; once upstream revises, the as-published list for an earlier date is gone.
The same shape is coming for CDC **surveillance** data (FluView / NNDSS-style weekly
reporting), where the revision/backfill history is itself an analytical signal.

ADR 0007 defined the update-semantics vocabulary (`append_only`, `snapshot_replace`,
`merge_upsert`, `merge_scd2`, `full_refresh`, …) but did not establish *how* we preserve
history for sources that don't preserve their own. Two stacked questions decide whether
we capture history at all, and how:

1. **Does the source preserve its own history?** Census: yes (re-pullable). CVX / CDC
   weekly: no.
2. **Is the revision history analytically valuable?** Weather NOAA prelim→scaled: no —
   they're corrections to absorb, so `weather_raw` uses `merge_upsert` and keeps none.
   CVX status changes / surveillance backfill: yes.

When history is **not** source-preserved **and** is valuable, we must capture it
ourselves. This ADR sets that convention. `codes.cvx` is the first application: tiny
(~hundreds of codes), revised rarely, refreshed on an annual schedule.

## Decision
For a revise-in-place source whose history has value, capture it with a **paired**
mechanism:

1. **Raw immutable Volume snapshot.** Each run writes the as-pulled raw payload
   *verbatim* to a Unity Catalog **Volume**, date-stamped and **never overwritten**
   (`<source>/<source>_<YYYY-MM-DD>.<ext>`; if the date's file exists, skip and log).
   This is the full-fidelity record and the durable source of truth, since upstream
   cannot reproduce past states. It also lets us re-parse history if our parser improves.
2. **In-table revision tracking via `snapshot_replace`.** The table is keyed by
   `(natural_key, snapshot_date)`; each run DELETEs only its own `snapshot_date` rows and
   appends, leaving all prior snapshots intact — the exact per-vintage replace geography
   uses (ADR 0024), with `snapshot_date` as the vintage key. Every snapshot stays
   directly queryable; "current" is the latest `snapshot_date`, optionally surfaced as a
   `<table>_current` view.

Use **this snapshot pattern** when the dataset is **small and changes infrequently** —
retaining whole snapshots is cheap and far simpler than effective-dated rows. Use
**SCD2** (`merge_scd2`, ADR 0007) instead when the dataset is **large and/or revised
frequently per-record**, where storing a full copy per run would bloat; in that case
still snapshot the raw payload to the Volume, but track the processed grain as SCD2.

First application — `codes.cvx`: annual schedule; raw `XML-new` to a managed Volume in
the `codes` schema; table keyed by `(cvx_code, snapshot_date)`, `snapshot_replace`,
registered with `update_semantics="snapshot_replace"`. Retention: keep all snapshots
(the data is tiny); revisit per-source if snapshots ever grow large.

## Alternatives considered
- **SCD2 as the primary mechanism.** Rejected *for small/rare-change sources* like CVX:
  effective-dated rows + current-flags are more machinery than warranted when a whole
  snapshot is a few hundred rows. Kept as the documented choice for large/frequently
  revised sources.
- **`full_refresh`, no history.** Rejected: it overwrites the only copy of each
  as-published state, which (unlike Census) cannot be re-fetched — discarding exactly the
  analytical signal (what was active/coded when).
- **Volume snapshot only (no in-table tracking).** Rejected as the sole mechanism: raw
  payloads aren't directly queryable, so every point-in-time question means re-parsing
  files. The in-table `snapshot_date` gives direct SQL; the Volume gives fidelity and
  replay. Pairing them is cheap at this size.
- **Re-pull on demand (the geography-vintage approach).** Not available — the source does
  not preserve history, which is the whole reason for this ADR.

## Consequences
- **Point-in-time becomes a plain query** (`WHERE snapshot_date = …`), and the raw
  payloads are auditable and replayable. Consumers wanting "current" filter to the latest
  `snapshot_date` (or use the optional `<table>_current` view) — a small ergonomic cost.
- **Storage grows by one snapshot per run.** Fine for small/infrequent sources; the
  decision criterion above routes large/frequent sources to SCD2 before this bites.
- **New standing convention:** a UC Volume per such source for raw snapshots, with
  date-stamp immutability (never overwrite a date). Volume location/naming and retention
  are pinned by the first implementation (`codes.cvx`) and generalized here.
- **`snapshot_replace` gets real use.** Note `codes.icd10` and the geography builds
  register `update_semantics="full_refresh"` despite doing per-edition/per-vintage
  replace; CVX uses the precise `snapshot_replace`. This ADR doesn't retrofit those, but
  flags the inconsistency for a future cleanup.
- **Surveillance subjects inherit the pattern.** When the first CDC weekly source lands,
  it reuses the raw-Volume-snapshot half directly; its processed grain likely chooses
  SCD2 over whole-snapshot retention per the size/frequency criterion.
