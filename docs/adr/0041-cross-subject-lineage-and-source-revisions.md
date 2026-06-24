# 0041 — Cross-subject lineage, propagation, and source revisions

## Status
Proposed. Extends ADR 0040 (orchestration granularity), 0039 (Volume landing + retention),
0034 (vintage_snapshot / immutability), 0032 (source history), 0037 (layering; enriched
canonical), 0035 (fact–geography conformance), 0008 (`_ops` metadata), 0010 (alerting).

Closes the gaps 0040 left implicit. 0040 decided *where the task boundaries are*; it
assumed the dependency graph is intra-job and that vintages are immutable. Both assumptions
break the moment a consumer in another job conforms to a dimension, or a source re-releases
a vintage. Without the mechanisms here, those breakages surface as **silent staleness**
(wrong-but-green) rather than loud failure.

## Context — the four root gaps

1. **The true dependency DAG spans jobs; orchestration and repair only see *inside* a
   job.** `depends_on` enforces parents-first within a job. Across the subject=job boundary
   (RUCA→geography, every fact→geography/time/codes) ordering is unenforced and
   partial-refresh fan-out *stops at the boundary* — re-running `us_county` re-runs its
   intra-job descendants but leaves cross-job consumers silently stale. No tool sees the
   whole graph, so "what is downstream of geography" is not even queryable.

2. **The propagation rule (0040) has no mechanism.** Nothing records that a consumer
   conformed against a specific *version* of an upstream, and nothing detects that the
   upstream has since moved. So "re-derive downstream on upstream change" can never fire.

3. **"Immutable vintage" + skip-if-complete assumes sources never re-release a vintage —
   they do.** Census/TIGER boundary corrections, ERS RUCA editions, errata. Today
   `per_vintage_immutable` skips re-fetch when the vintage dir exists, and `replaceWhere`
   would overwrite with no record of change. Source corrections silently vanish —
   contradicting the 0039/0032 history-preservation goal.

4. **Denormalized enriched canonicals turn a parent change into a write-time fan-out with
   no detector.** A `state_name` correction silently staleys the denormalized copy in every
   descendant level (and cross-job consumer) until each re-derives. Retiring the `_enriched`
   views (0028) removed the always-live-join safety net. (An instance of gap 2.)

## Decision

### D1 — Cross-job dependencies are declared, ordered, and asserted
- **Declare** the cross-subject edges in a single dependency manifest (a checked-in
  `dependencies.yml` / `_ops.subject_dependencies` table: `consumer_build → upstream_table`)
  so the full graph — not just per-job DAGs — is queryable for fan-out and impact analysis.
- **Order** first-build and scheduled-freshness edges with explicit `Run Job` tasks
  (hard edges), not "run geography first by convention." Soft "read the persisted table"
  is permitted only for steady-state reruns of an already-built, unchanged upstream.
- **Assert** in every consumer's derive a precondition: each declared upstream exists and
  is at the expected `(vintage, revision)`; otherwise **fail loud** rather than conform to a
  missing or stale parent. This is the cross-job analogue of the in-job parent-FK check.

### D2 — Build lineage is recorded; staleness is detectable
- On every promote, record per `(table, vintage, revision)` in `_ops`: the Delta commit
  `table_version`, a `content_fingerprint` (hash of the published rows or the source
  fingerprint it derived from), and `built_at`.
- Each consumer records, at conform time, the `(upstream_table, version, fingerprint)` set
  it built against (`_ops.conformance_lineage`).
- A cheap **staleness check** (view + a scheduled job, ADR 0010 alerting) flags any consumer
  whose recorded upstream version ≠ the upstream's current version. This is the mechanism
  that lets 0040's propagation rule actually fire, and it makes gap-4 fan-out *visible*
  instead of silent. Re-derive is then a targeted re-run of the flagged consumers' derive.

### D3 — Source revisions are first-class; immutability is per `(vintage, revision)`
- The Volume landing stores a **source fingerprint** (ETag / Last-Modified / content hash)
  alongside each payload. `skip-if-complete` compares the **fingerprint**, not mere
  directory existence — a changed source for an already-landed vintage is detected.
- A genuine re-release bumps a **`vintage_revision`** and is a **deliberate, reviewed
  re-land** (not an auto-overwrite). `vintage_snapshot` immutability (0034) is redefined as
  immutable per `(vintage, revision)`; the prior revision is retained (Volume + a retained
  Delta snapshot), preserving history per 0032/0039.
- Default fetch never silently overwrites a landed `(vintage, revision)`; re-landing the
  *same* fingerprint is a no-op, a *different* fingerprint requires an explicit revision bump.

### D4 — Gate fail-mode is defined; downstream blocks rather than reads stale
- The DQ-gated promote (0036 Phase A→B) is **atomic across the vintages in a run**:
  all promote or none. No mixed-vintage canonical.
- On a gate **skip** (Phase A DQ failed), the canonical **retains last-good** and the build
  fails the task (loud). Downstream consumers, via the D1 precondition + D2 staleness check,
  **block on the freshness assertion** rather than silently conforming to last-good.

### D5 — Volume class governs retention and destructive ops
- Classify every landing Volume: **re-creatable** (immutable / re-fetchable sources) vs
  **history-of-record** (`snapshot_per_run` / `merge_scd2` — the only copy of revise-in-place
  history).
- `DROP VOLUME` (the stale-ownership fix) is permitted only on **re-creatable** volumes;
  history-of-record volumes are never dropped by tooling or runbook, and are backed up.
- Retention/vacuum policy per mode: immutable = keep all `(vintage, revision)`;
  snapshot-per-run = retain per policy (e.g. N snapshots / M days) then archive, not delete.

### D6 — Capture concurrency is bounded
- Jobs whose capture has revise-in-place semantics set `max_concurrent_runs: 1` so
  overlapping triggers cannot interleave or double-capture snapshots.

### D7 — Skip provably-unchanged rebuilds (a payoff of D2/D3)
- The Volume already skips the *fetch* when the payload for a `(table, vintage[, revision])`
  is complete. The same idea extends one layer down to the *derive*: skip the
  process/write/promote of `(output, vintage)` iff a stored **build signature** equals the
  current one. The signature is a hash of everything that determines the output:
  `{source-payload fingerprint(s) (D3), derive code/version, upstream parent versions (D2)}`.
  Deterministic derive + identical inputs ⇒ identical output, so the skip is provably
  equivalent to rewriting — not a heuristic.
- **Why not "skip if the vintage rows already exist":** immutable *source* ≠ immutable
  *output*. A derive code change (new enriched column, fixed row-builder, changed tolerance)
  or an upstream parent rebuild changes the correct output while the source vintage is
  unchanged; a rows-exist skip would silently freeze the build against those changes — the
  wrong-but-green failure this ADR exists to prevent.
- The signature is recorded **only after a successful promote** (mirroring `_FETCH_COMPLETE`
  being written only after a complete fetch), so a half-finished run is never skipped.
- **Not** an operator `--skip-unchanged`/`--force` flag: that pushes the "did anything
  change?" judgment onto whoever runs the job (who won't know a parent rebuilt). The
  signature decides. Most valuable at block-group/block scale, where reprocessing 8M-row
  geometry is real compute; inert/cheap below it.

## Consequences
- **Geography (now) is unaffected** — intra-job, immutable, single source. D1–D6 are inert
  until the first cross-job consumer (RUCA) and the first revise-in-place source (a fact).
- **Before RUCA:** D1 (declare + order + assert the geography→RUCA edge) and the
  FK-against-**canonical** rule apply; D3 matters only if a geography vintage is corrected.
- **Before facts/surveillance:** D2 (lineage + staleness) and D4 (gate fail-mode) are
  prerequisites — they are what keep conformance correct under dimension change; D5/D6 apply
  to the revise-in-place capture.
- `_ops` gains `subject_dependencies`, `conformance_lineage`, and per-table
  `version`/`fingerprint`/`revision` columns (additive; via the setup job, not data
  pipelines — consistent with the no-silent-drift rule).
- A `vintage_revision` column joins `vintage` across landing, raw, processed, canonical, and
  the fact `geo_vintage` conformance key (0035).

## Alternatives considered
- **Keep cross-job deps soft ("just run upstream first").** Rejected: unenforced ordering +
  invisible fan-out is exactly the silent-staleness failure this ADR exists to prevent.
- **One mega-job for all subjects so `depends_on` covers everything.** Rejected: collapses
  the subject=job boundary (0040), couples unrelated cadences/envs, and makes every refresh
  a whole-warehouse rebuild.
- **Treat every vintage re-release as a brand-new vintage.** Rejected: conflates "a new
  reference period" with "a correction to an existing one"; breaks crosswalks and the
  conformance key. `(vintage, revision)` keeps the distinction.
- **Live-join enrichment instead of denormalized canonical (un-retire `_enriched`).**
  Rejected as the default (0037/0028 chose materialized for read-perf), but D2 makes the
  materialized staleness *detectable*, which is the property the live join gave for free.
