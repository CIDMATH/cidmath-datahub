# U4 — `vocabularies.py` + `check_conventions.py` (ADR 0016) — findings

**Date:** 2026-06-22 · **Scope:** `src/cidmath_datahub/common/vocabularies.py`,
`scripts/ci/check_conventions.py`. Part of the SDE review plan; picks up I2 finding #4.

## Verdict
Both are well-built (AST-based scan, pure/unit-testable, enum↔mirror integrity check, honestly-marked
deferred stubs). But U4 surfaces the **most concrete blocker in the review**: the `vintage_snapshot`
value the merged ADRs require is **not in the vocabulary**, so ADR 0034/0036/0037 are not yet
implementable — and several `dataset_catalog` vocab fields are validated by neither the dataclass nor
CI (confirming I2 #4).

## What's solid
- `vocabularies.py` is the single source of truth (enums + mirror value-sets); `check_vocabulary_integrity`
  asserts enum↔mirror consistency and namespace formatting.
- `check_conventions.py` is **AST-based** (not regex): it validates enum member access
  (`DQSeverity.FAIL`) and string literals under controlled-vocab keys (`update_semantics="..."`,
  `severity=`, `category=`, `materialization_type=`) across `src/` + `bundles/`. Pure
  `scan_source_for_vocab_errors` → unit-testable. Turns vocab typos into CI failures, not 40-minute
  job failures.
- Deferred rules (3 tag values, 4 catalog-row presence) are **explicit stubs**, not silent no-ops.

## Findings

### MUST-FIX (and it's the first step to implement 0034/0036/0037)
1. **`vintage_snapshot` is missing from `UpdateSemantics`.** The enum has `append_only`,
   `snapshot_replace`, `merge_upsert`, `merge_scd2`, `merge_scd2_side`, `incremental_compute`,
   `full_refresh` — **no `vintage_snapshot`**. ADR 0034 (merged) introduced it and its consequences
   said "UpdateSemantics gains a value; CI must accept it" — but that was never done. Consequences
   today: any build setting `update_semantics="vintage_snapshot"` **fails twice** — at
   `DatasetEngineeringEntry.__post_init__` (validates against `UPDATE_SEMANTICS_VALUES`) and at the CI
   scan (validates the literal). So **0034/0037 — and the proving grounds, the RUCA migration, every
   reclass — are blocked until `VINTAGE_SNAPSHOT = "vintage_snapshot"` is added** to the enum (a
   one-line, additive change; the mirror set + CI pick it up automatically). Good news: this proves
   CI is doing its job (it would correctly block a premature usage); it just needs the enum updated
   first. **This is the unblocking first commit of the builder work.** — **RESOLVED 2026-06-22:**
   `VINTAGE_SNAPSHOT = "vintage_snapshot"` added to `UpdateSemantics`; the mirror set, the CI scan,
   and `DatasetEngineeringEntry` validation all derive from the enum, so it propagated automatically.
   0034 / 0037 / the proving grounds / the RUCA migration are unblocked.

### SHOULD-FIX
2. **`layer` / `access_tier` / `subject` / `source_provider_code` / `spatial_resolution` are
   validated by neither the dataclass nor CI** (confirms I2 #4 from the CI side). `_DECLARED_STRING_KEYS`
   only covers `severity` / `category` / `update_semantics` / `materialization_type`. So the `layer`
   drift (`reference` in code vs DDL comment `model`) and `access_tier` (`open` vs DDL `public`) are
   invisible to CI. → Add a `Layer` enum (reconcile the set first), decide `access_tier`'s governing
   vocab (it's a *tag namespace* today, taxonomy-governed — but the catalog column isn't checked
   against it), and add the consequential ones to `_DECLARED_STRING_KEYS` + `DatasetCatalogEntry`
   validation. (Pairs with the I2/0036 registration-gap work.)
3. **`incremental_compute` is in the enum but not in ADR 0007's documented set** (drift the other
   direction — code has a value the ADR doesn't list). Reconcile: add it to 0007 if intended, else
   remove.

### CONSIDER (low)
4. **The scan only catches string *literals* under known keys + enum member access.** Values passed
   via variables, f-strings, or `**dict` unpacking escape it (a small false-negative surface). Fine
   for the common literal case; note it.
5. **Rules 3 & 4 remain deferred.** Rule 4 (catalog-row presence) is the more valuable to wire now —
   it would catch the I2 "writer⊂schema" / missing-registration class — but it needs the CI SP to read
   `_ops`. Tracked as documented-only / review-enforced.

## Ties
- **0034/0036/0037:** finding #1 is the literal prerequisite to implement them — add the enum value
  first. **I2 / 0036:** findings #2/#3 are the vocab half of the registration gaps already folded into
  ADR 0036's scope; the `Layer` enum + the extra validated keys belong there.
