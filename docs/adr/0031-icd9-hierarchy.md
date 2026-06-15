# 0031 — ICD-9-CM hierarchy sourcing, and the shared code-system hierarchy contract

## Status
Accepted — 2026-06-08

## Context
`codes.icd10cm` (ADR 0030) preserves the ICD-10-CM classification tree on the node table — adjacency (`parent_icd10cm_code`), a materialized path (`ancestor_codes`), depth (`node_level`), and denormalized chapter/block — so subtree selection and chapter rollups are one query. We now add `codes.icd9cm` so surveillance/clinical data coded before the 2015-10-01 ICD-10 transition can be conformed and rolled up the same way. The payoff of having both is cross-2015 analysis; for that to work the two tables must be **query-compatible** — identical `array_contains` subtree and chapter-rollup semantics — even though their sources and code structures are genuinely different.

ICD-9-CM is **frozen** (final update FY2014; valid for US coding through 2015-09-30), distributed by NCHS as **Rich Text Format** files (not a fixed-width order file or a tabular XML), with a different code structure: three-to-five-digit numeric categories (`250` → `250.0` → `250.00`), plus the V (`V01`–`V91`) and E (`E000`–`E999`) supplementary classifications. There is no tabular XML, no seventh-character mechanism, and no mid-year update. So neither ADR 0030's XML-primary tree-sourcing nor `icd10cm.py`'s order-file/overlay machinery applies; the implementation must be its own.

## Decision
Two decisions: a shared **contract** (so the tables interoperate) and the ICD-9 **sourcing** (how its tree is built).

**(1) The code-system hierarchy contract — documented, not coded.** Every code-system reference table (`codes.icd10cm`, now `codes.icd9cm`, and any future one) carries the same hierarchy columns with the same semantics:

```
parent_<sys>_code  STRING          -- nearest existing ancestor in the edition; NULL at a top-level category
node_level         INT             -- depth in the adjacency tree; node_level == len(ancestor_codes)
ancestor_codes     ARRAY<STRING>   -- root -> parent path (ordered), e.g. ["250","250.0"] for 250.00
chapter_code       STRING
chapter_name       STRING
block_code         STRING
block_name         STRING
```

This guarantees `WHERE array_contains(ancestor_codes, '250')` selects a subtree and `GROUP BY chapter_code` rolls up identically across code systems. `icd9`'s hierarchy unit tests mirror `icd10`'s against this contract. The contract is the *only* thing the two share; the modules do **not** share code (see Alternatives), so the contract lives here as documentation, not in a base class.

**(2) ICD-9 adjacency from the prefix rule (primary); chapter from a static frozen map; block from Appendix E.** ICD-9-CM codes nest cleanly by **string prefix** — `250` ⊂ `250.0` ⊂ `250.00`, `V30` ⊂ `V30.00`, `E812` ⊂ `E812.0` — so the longest-existing-prefix rule reconstructs the tree *directly* from the edition's own code set, with no external tree source needed. This **inverts ADR 0030**, where the XML was the authoritative tree and the prefix rule was only the fallback for seventh-character codes: ICD-9 has neither seventh-character expansions nor a tabular XML, so the prefix rule is the natural *primary*. For the labels not derivable from codes: **chapter** comes from a small **static range map baked into `icd9cm.py`** (the 17 ICD-9 chapters by 3-digit range, plus the V and E supplementary classifications), and **block** (the finer "List of Three-Digit Categories" sections) comes from **Appendix E** (`DC_3D` RTF), shipped per-edition in the same NCHS distribution. `reference/icd9cm.py` is standalone; `reference/icd10cm.py` is left untouched. `is_billable` is derived as leaf-of-set (a code is billable iff no more-specific code has it as a prefix — the ICD-9-CM "code to highest specificity" rule). Editions are pure annual base releases (frozen, no mid-year overlay). V and E codes are in scope; Volume 3 procedure codes (`PTAB`) and the ICD-9↔ICD-10 GEM crosswalk are separate issues.

> **Amendment (2026-06-12, during implementation).** Original plan sourced *both* chapter and block from Appendix E. On the first dev run the codes/descriptions/adjacency built correctly but all chapter/block columns came back null — `striprtf`'s rendering of `DC_3D` didn't match the parser's assumed layout. Since ICD-9-CM is **frozen**, its 17 chapters (and the V/E classifications) are fixed and authoritative, so chapter is now assigned from a static range map rather than parsed out of the RTF — robust, and it **resolves the V/E open question** below (they map to chapters `V` and `E`). Block still comes from Appendix E; the block parser is being tuned against the real `DC_3D` text. This narrows — but does not fully overturn — the "no static range table" rejection: static is used only for the frozen, coarse chapter level, not the finer block sections.

## Alternatives considered
- **Unify `icd9` and `icd10` into one module/table.** Rejected. Parsing (RTF vs. fixed-width/XML), code normalization (decimal after the 3rd char, except E after the 4th), source handling, and tree-sourcing all differ; unifying would mean `if code_system == ...` branching throughout. Separate scripts plus a shared *contract* keeps each readable. Code-level extraction of a generic graph step is deferred to the rule-of-three (a third code system, or when the GEM crosswalk must walk both trees) — the same discipline used for `gadm.py` / `registration.py`.
- **A static chapter/block range table** (cf. an earlier geography idea). Rejected: Appendix E is authoritative, per-edition, and ships in the same NCHS distribution, so there is no reason to hand-maintain ranges.
- **XML-primary like ICD-10.** Not applicable: ICD-9-CM has no tabular XML, and its codes nest by pure string prefix, so the prefix rule is correct as primary rather than fallback.

## Consequences
- **`codes.icd9cm` is query-compatible with `codes.icd10cm`** (same subtree/rollup semantics by construction), enabling cross-2015 analysis once both are loaded; the GEM crosswalk that bridges the code sets is its own table/issue (`codes.icd9_icd10_gem`).
- **Simpler than ICD-10**: no second source download for the tree, no XML, no XML-vs-prefix cross-check — justified because ICD-9 nests cleanly. The only extra source is Appendix E for labels.
- **`is_billable` is approximate by design** (leaf-of-set); it matches the ICD-9-CM highest-specificity billing rule, and DQ flags any oddities (e.g., a three-digit code billable despite having subdivisions).
- **Resolved during implementation:** the V- and E-code chapter question is settled by the static chapter map (amendment above) — V and E are their own supplementary chapters (`V`/`E`), assigned directly rather than depending on whether `DC_3D` enumerates them. Their finer *block* labels still depend on Appendix E coverage and are flagged (WARN, `find_unmapped_blocks`) where absent. Recorded in `known_limitations`.
- **Frozen source**: no update machinery is ported from `icd10cm.py`; the edition list is a parameterized set of historical annual base releases.
