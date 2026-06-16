# 0034 — Vintage-retained snapshot semantics: atomic per-vintage replace, immutable vintages, and the SCD2 escalation rule

## Status
Proposed — amends ADR 0007 (adds one `update_semantics` value and a usage rule). Relates to
0024 (geography vintaging), 0032 (source-history preservation), 0016 (controlled-vocabulary
CI). Removes the ad-hoc `<table>_current` views shipped on cvx/loinc/ndc.

## Context
Most reference / code-system tables share one update model: each upstream release is stamped
with a **vintage discriminator** and retained; a rebuild replaces only the rows for the
vintage(s) it rebuilt. Confirmed in `codes.cvx`, `codes.ndc_product` / `codes.ndc_package`,
`codes.loinc` / `codes.loinc_map_to`, `codes.icd9cm`, `codes.icd10cm`, and the geography
vintage tables use the same `vintage`-keyed model.

Four problems surfaced in review:

1. **No name for the model, so the labels are wrong.** ADR 0007's `snapshot_replace` means
   "fully replaces table contents with the latest snapshot" (no retention); `full_refresh`
   means "recomputed from upstream every run." Neither matches "retain every stamped vintage,
   replace in place per vintage." Authors split: `snapshot_replace` on cvx/loinc/ndc,
   `full_refresh` on icd + geography — both contradicting their own definitions.

2. **The write is not atomic.** Every table does `DELETE FROM t WHERE <vintage> IN (...)`
   then a separate `append`. A reader between the two statements sees the vintage half-removed;
   a failure between them loses it. Delta supports this atomically.

3. **The `<table>_current` views answered the wrong question.** cvx/loinc/ndc grew
   `WHERE vintage_key = MAX(vintage_key)` views ("latest loaded"), applied inconsistently
   (none on icd/geography), with `loinc_current` taking `MAX()` over a *string* version
   (mis-orders once versions stop being lexically sortable). More fundamentally, "current" is
   already answerable from the `vintage` column; a separate object adds surface without adding
   capability.

4. **The boundary between "this model suffices" and "you need SCD2" was undefined.** Working
   the Connecticut county→planning-region case showed it: if a revision arrives as a *new
   vintage*, the retained-vintage set is itself a full history and answers point-in-time
   questions by filtering `vintage`. But if an *existing* vintage is restated in place, the
   per-vintage replace overwrites the prior content (recoverable only via Delta time travel,
   which ADR 0007 rejects for analytical history) — silently losing history and orphaning any
   facts coded to the superseded rows. Nothing recorded which case applies.

## Decision
1. **Name the model and make the write atomic: `update_semantics = "vintage_snapshot"`.**
   Add it to `UpdateSemantics` (`common/vocabularies.py`). Definition: the table retains all
   stamped vintages; each run **atomically** replaces only the vintage(s) it rebuilt via Delta
   `replaceWhere` —
   `df.write.mode("overwrite").option("replaceWhere", "<vintage_key> IN (...)").saveAsTable(t)`
   — and the first build seeds the schema. This replaces the non-atomic `DELETE` + `append` in
   every build. Reclassify the tables that do this (`cvx`, `ndc_product`/`ndc_package`,
   `loinc`/`loinc_map_to`, `icd9cm`, `icd10cm`, geography vintage tables) off their current
   `snapshot_replace` / `full_refresh` labels. Keep 0007's `snapshot_replace` for true
   whole-table replacement and `full_refresh` for whole-table recompute (crosswalks, enriched
   views).

2. **Vintages are immutable; `replaceWhere` is for idempotent re-runs, not revisions.** A
   corrected or re-based release gets a **new vintage key** — it is never written as a
   `replaceWhere` overwrite of an existing vintage's content. `replaceWhere` exists only to make
   re-running *the same logical pull* idempotent. When this holds, the retained-vintage set is a
   complete history: every state ever published remains queryable under its own vintage. It
   follows that:
   - **"Latest vintage" is `MAX(vintage)`; "current operative" is the `live` accessor (Currency
     model, #7).** No validity columns are needed, and a still-valid older vintage (2010
     geography, an older ICD edition) is **never** treated as wrong or end-dated just because a
     newer vintage exists — it remains the correct answer for data coded to it, and consumers
     select it by `vintage` explicitly.
   - **Accepted in-place refinement (not a violation):** where a within-vintage update is
     deliberately collapsed and the superseded state is neither analytically needed nor lost
     (it is reproducible from source) — e.g. ICD-10-CM overlaying the Apr-1 mid-year update onto
     the Oct-1 base under one `edition_year` (ADR 0030) — that is a documented, intentional
     refinement, not a trigger for #4.

3. **No vintage-grain effective-dating, and no `<table>_current` views.** Drop the cvx/loinc/ndc
   `_current` views (and their `_ops` registrations); this also deletes the `loinc_current`
   ordering bug. **`effective_start` / `effective_end` are reserved strictly for SCD2** —
   row-level revision validity, `NULL` end = the current version — and appear only on a table
   that adopts the escalation in #4. They are deliberately *not* used at the vintage grain:
   doing so would overload the SCD2 names for vintage sequencing and would end-date vintages
   that are still valid (2010 does not stop being the correct 2010 geography when 2020 lands).

4. **SCD2 escalation rule, with a named trigger.** Escalate a table from `vintage_snapshot` to
   ADR 0007 `merge_scd2` / `merge_scd2_side` **if and only if an existing key can be restated in
   place with content you must later reconstruct** — i.e. you need point-in-time history of a
   revised vintage, `_change_type='delete'` tombstones for removed entities, or as-of-date
   joins that survive a restatement. If instead each revision can be expressed as a new vintage
   key, stay in `vintage_snapshot`; it already preserves the history. **Nothing in the system
   requires SCD2 today** (geography mints a new TIGER-basis vintage rather than mutating an
   existing one; CVX/NDC key on `snapshot_date`, so every pull is a new vintage). This is a
   readiness rule for the first genuinely revise-in-place-with-history source.

5. **Real-world applicability — the "in force" axis — is separate, and deferred.** A release
   published before it takes effect (ICD-10-CM FY2027 issued in summer 2026; 2020 boundaries
   shipped in 2019) has a real-world *effective date* — when it actually comes into force —
   distinct from the `vintage` label, the currency vocabulary (#7), and SCD2 row-revision
   validity. This is the one concept the term "in force" fits precisely: if it is ever stored it
   gets its own column (`in_force_from`, optionally `in_force_through`) — **not**
   `effective_start`/`effective_end` (those are SCD2), and **not** the currency accessors. For
   ICD it is derivable from the fiscal calendar, so there is no column today; deferred until a
   concrete need.

6. **Declaring the vintage key in metadata is deferred (YAGNI).** With the `_current` views gone,
   no automated consumer reads "which column is the vintage." Record it in the table comment;
   add a `vintage_key` field to `DatasetEngineeringEntry` only when a real consumer exists.

7. **Currency model — settle the vocabulary now; build none of it until a source needs it.**
   Two *orthogonal* currency axes, plus a composite accessor built from them:
   - **`is_current`** (revision axis) — SCD2-native: the latest non-superseded revision of an
     entity. Stored only where a table is SCD2; absent on `vintage_snapshot` tables (immutable
     vintages carry no in-table revisions). `effective_start`/`effective_end` (#3) belong to
     this axis only.
   - **`is_latest_vintage`** (epoch axis) — `vintage = MAX(vintage)`. **Derived, never stored** —
     a stored flag would force rewriting older vintages on each load, breaking #2. It means
     "newest epoch," not "older epochs are wrong."
   - **`live`** (composite accessor) — "the data as it operatively stands now" =
     `is_latest_vintage` ∧ (`is_current` where the table is SCD2). Exposed as one standardized
     predicate; materialize as `<table>_live` views only if/when a consumer needs the handle,
     and then uniformly (not ad hoc). `live` is for current-state consumption; joining to
     historically-coded facts still selects `vintage` explicitly — `live` does not replace that.

   Keep the axes atomic — do not fuse them into a single flag: each answers a distinct question
   ("all revisions of the live vintage" vs "the current revision of a *historical* vintage")
   that a fused flag forecloses, and a fused flag's meaning would shift by table type. Do not name
   the currency concept `effective` (collides with the SCD2 date columns) or `in_force` —
   `in_force` denotes a real-world effective date, which is the separate applicability axis (#5),
   not "latest operative." **Nothing here is built today:** every current table is pure `vintage_snapshot`
   with immutable keys, so "current operative" is simply `MAX(vintage)`. This is the convention
   the first SCD2 or revise-in-place source inherits, not present work.

## Alternatives considered
- **`effective_start` / `effective_end` at the vintage grain** (an earlier version of this
  draft). Rejected: it overloads the SCD2 column names for vintage sequencing, end-dates
  vintages that are still valid, and conflates real-world applicability with row-revision
  validity. `vintage` already answers the currency questions; SCD2 dates have a precise,
  different meaning that this would muddy.
- **Keep the `_current` views (fixed)** or a materialized `is_current` flag. Rejected:
  confusing, inconsistently applied, and `MAX`-based "current" embeds the wrong concept; a flag
  also goes stale. A `vintage` filter is simpler and correct.
- **Keep the non-atomic `DELETE` + `append`.** Rejected: not crash-safe; exposes a half-replaced
  vintage to readers. `replaceWhere` is atomic.
- **Reuse `snapshot_replace` / leave `full_refresh`.** Rejected: both contradict their 0007
  definitions for retention-keeping, per-vintage tables.
- **Force a single `vintage` column name everywhere.** Rejected: collapses date-, fiscal-year-,
  and string-versioned vintages into one name and type, losing natural-key semantics.
- **SCD2 for all code systems now.** Rejected: write amplification and query complexity for
  sources that just need the latest/in-effect release; SCD2 is the opt-in escalation.

## Consequences
- `UpdateSemantics` gains a value; `scripts/ci/check_conventions.py` and the
  controlled-vocabulary CI (ADR 0016) must accept it.
- The reclassification is **registration-only with no new columns and no rebuild** — a label
  change plus the `replaceWhere` write swap. (The separate `loaded_at`→`ingested_at` audit
  cleanup is the only data-column change, tracked in its own issue.) This is materially cleaner
  than the earlier draft, which added effective-date columns.
- `replaceWhere` replaces `DELETE` + `append` per build — small, behavior-preserving, crash-safe.
- cvx/loinc/ndc `_current` views are dropped; the `loinc_current` ordering bug is gone.
- A standardized currency vocabulary — `is_current` (SCD2 revision), `is_latest_vintage`
  (`MAX(vintage)`, derived), and the composite `live` accessor (#7) — is documented for
  contributors, with implementation deferred until a source needs SCD2 or current-state views,
  so the distinction is inherited rather than rediscovered.
- The immutability principle + the SCD2 trigger give contributors a clear, testable rule for the
  next revise-in-place source; the Connecticut case is the worked example (model the change as a
  new vintage, never an in-place overwrite — and if a source genuinely overwrites a key whose
  history you need, that is the SCD2 signal).
- Effective-dating semantics stay reserved and unambiguous for SCD2; future-dated applicability,
  if needed, gets its own named column.
- The shared reference-table builder (the root-cause refactor discussed alongside this ADR)
  becomes simpler on the temporal side: it owns the atomic per-vintage write and an immutability
  guard, and nothing more.
