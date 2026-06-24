# 0040 — Pipeline orchestration granularity: per-level tasks and the capture/derive seam

## Status
Proposed. Builds on ADR 0036 (the shared builder's Phase 0 / A / B), 0027 (`run_build`),
0037 (layering; augmenting inputs), 0039 (Volume landing + retention modes), 0032 (source
history), 0007/0034 (update semantics). Decides what becomes a Databricks **job task**
versus what stays a step *inside* a build. **ADR 0041** closes the cross-job /
mutable-source gaps this ADR leaves implicit (lineage, propagation mechanism, source
revisions, gate fail-mode).

## Context
The layered builder runs a build as a single task whose phases are: **capture**
(source → Volume → 1:1 raw, ADR 0039 Phase 0 + the raw write) → **derive**
(raw → [processed] → promoted canonical, Phases A/B). Two granularity questions follow:

1. **Across builds within/between subjects** — how is ordering expressed?
2. **Within a build** — when should the capture/derive phases become *separate tasks*
   rather than one combined task?

The geography migration already answered (1): one task per level, ordered parents-first
via `depends_on` (state → county → tract → …). This ADR settles (2), and the cross-subject
version of (1), with a single rule.

## Decision

1. **Orchestration lives in the job DAG; computation lives in the builder.** `depends_on`
   owns ordering/retry/parallelism; `build_<…>` owns how a build runs. (Adopted in the
   geography DAG.)

2. **The unit of orchestration is the build (a level, or a subject's output set), wired
   parents-first by `depends_on`.** Intra-subject dependencies (tract needs county's
   processed table) are task edges, not in-process coupling.

3. **The capture/derive seam exists in the builder as phases; promote it to a *task*
   boundary only when capture and derive have *different triggers*.** Triggers:
   - **Cadence** — a revise-in-place source (`snapshot_per_run` / `merge_scd2`, ADR
     0032/0039) must *capture* on the source's revision schedule (each run pins an
     otherwise-lost snapshot), while *derive* runs on its own, slower schedule.
   - **Upstream cross-subject dependency on derive** — an augmenting input or a
     fact→dimension conformance makes *derive* depend on another subject's raw/canonical,
     while *capture* depends on nothing. A whole-build edge is then too coarse: it
     serializes the independent capture behind the upstream and misstates the real
     dependency. Splitting lets the edge land precisely (`upstream → this.derive`).
   - **Propagation / re-derive** — when an upstream reference changes (new vintage,
     correction), downstream consumers must **re-derive (re-conform), not re-capture**.
     With the seam this is a targeted re-run of *derive* tasks. Without it, re-running a
     whole build re-captures — and for a revise-in-place source that **mints a spurious
     new snapshot**, corrupting history. So here combining is *incorrect*, not merely slow.
   - **Compute profile / governance** (secondary) — IO-bound capture vs heavy-Spark
     derive wanting different clusters; or an approval/identity gate on the canonical
     publish.

4. **When all triggers are shared, keep capture + derive in one task.** A single stable
   source, intra-subject ordering already handled by per-level tasks, uniform compute, no
   gate → combined. The "don't re-fetch / don't redo work" benefit in this case is
   delivered by the **Volume cache + idempotent `replaceWhere`** (the caching layer), not
   by splitting tasks.

5. **The middle stages (raw→processed, processed→canonical) stay fused as "derive,"** with
   **one sanctioned exception: a governance/publish gate** may split the final *publish*
   (staging→canonical promote) out of derive as its own task, when promotion needs approval
   or a distinct identity. Absent that, the derivation chain is not split further — splitting
   it for compute alone is a non-goal (size is handled by per-`(level, vintage)` chunked
   writes, ADR 0020). This resolves the apparent tension between the "governance" trigger in
   decision 3 and the "derive stays fused" rule: governance is the *only* thing that splits
   the publish out.

## Consequences

- **Geography stays combined, per-level.** Geography levels are **root dimensions (pure
  producers)** — they consume no other subject's derive, their source is stable/immutable
  (`vintage_snapshot`), compute is uniform, no gate. So no capture/derive split, for any
  level. (Current design is correct as built.)
- **Augmenting inputs (RUCA, SVI, ADI, urbanicity) are their own builds**, `depends_on`
  the geography level they extend, producing **standalone `(geoid, vintage)` extension
  tables** (e.g. `us_ruca_tract`) that join the geography canonical — *not* columns on the
  geography canonical, and *not* a modification of the geography build. So adding RUCA does
  **not** rework `us_tract`.
- **Fact/surveillance subjects are where the capture/derive split lands.** Capture runs
  independently and ASAP (critical for revise-in-place sources); derive `depends_on` the
  dimension canonicals (geography/time/codes); a dimension change fans out to fact
  *re-derives*, not re-captures.
- **Builder support:** the phases already exist (0/A/B); enabling the split is exposing
  `capture` and `derive` as independently invocable entrypoints (a `--phase` analogous to
  the `--level` selector). Build only when the first split-warranting consumer lands.

## Alternatives considered
- **Always split every stage into tasks.** Rejected: DAG explosion (≈stages × levels),
  the validate→promote gate spread across tasks, inter-task re-init overhead — all for
  stages that share a lifecycle in the common (rebuild-from-stable-source) case.
- **Never split (one task per build, always).** Rejected: breaks revise-in-place capture
  cadence, over-serializes cross-subject dependencies, and makes "re-derive on upstream
  change" re-capture — which mints spurious snapshots for history-tracked sources.
- **Split on data size/compute alone.** Rejected as the *primary* rule (it's a secondary
  trigger); size is handled first by per-`(level, vintage)` chunked writes (ADR 0020).
