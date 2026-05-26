# 0015 — Integrated catalog table naming

## Status
Accepted — 2026-05-15

## Context
ADR 0003 originally stated that integrated-catalog tables would not carry Kimball-style `fact_*`/`dim_*` prefixes, with role captured via Unity Catalog tags instead. The rationale was that concept-named schemas would "carry the role" and that shorter names read better. On reflection, that argument doesn't hold for the integrated catalog as actually designed:

- Concept-named schemas mix roles. `surveillance` will hold facts (`case_report`, `lab_test`) *and* analytical dims (`facility`, `case_status`). `pathogen` will hold canonical reference (`taxonomy`) *and* analytical outputs (parameter estimates).
- UC tags don't appear where users look. SQL autocomplete shows table names; catalog explorer leads with names; downstream tools read names. Encoding role in tags requires every consumer to make a separate query to discover it.
- Kimball convention is the dominant idiom in data warehousing literature and tooling. Anyone with DW experience reads `county_dim` / `case_report_fact` and immediately knows what to expect. Inventing a different convention costs every consumer cognitive load forever.

ADR 0014 introduced a related question: reference data (`geography.county`, `time.epi_week`) lives in the integrated catalog but isn't "derived analytical content" — it's canonical truth. Does it get a Kimball suffix or not? And what about reference data with SCD2 behavior — does the time-variance change anything?

This ADR settles both questions.

## Decision

### Two orthogonal axes, two separate decisions

| Axis | Determined by | Captured in |
|---|---|---|
| **Canonical vs. derived** — is this externally-defined authoritative truth, or an output computed from data? | Who owns/produced it (`_reference` bundle vs. any other bundle) | Naming suffix (the rule below) |
| **Update behavior** — does this change over time, and if so, how? | Source behavior + materialization choice | `update_semantics` field on `_ops.dataset_engineering` (per ADR 0007) |

These are independent. A table can be canonical AND time-varying (e.g., `geography.county` is Census-defined but Census occasionally revises it). A table can be derived AND mostly-static (e.g., a one-time analytical projection). Treat them separately.

### Naming rule for the integrated catalog

| Kind of content | Owner | Suffix |
|---|---|---|
| Reference data (canonical/authoritative; computed deterministically or sourced from an authoritative external standard) | `_reference` bundle | **No suffix.** Example: `geography.county`, `time.epi_week`, `codes.loinc`, `pathogen.taxonomy`. |
| Derived analytical fact tables (measurements, events, transactions) | Any non-`_reference` bundle | **`_fact` suffix.** Example: `surveillance.case_report_fact`, `surveillance.lab_test_fact`, `pathogen.parameter_estimate_fact`. |
| Derived analytical dimension tables (entities that facts reference, but which are produced from data rather than canonical reference) | Any non-`_reference` bundle | **`_dim` suffix.** Example: `surveillance.facility_dim` (built from claims data), `population.cohort_dim` (derived study cohort). |
| Bridge / junction tables for many-to-many relationships | Any bundle | **`_bridge` suffix.** Example: `surveillance.case_to_lab_bridge`, `pathogen.taxonomy_to_icd10_bridge`. |

The canonical-vs-derived test: *if the value in this table changed, would the change need to be defended empirically (we re-ran the analysis and got a different number) or definitionally (the authoritative source revised the definition)?* Empirical → derived → suffix. Definitional → canonical → no suffix.

### SCD behavior is recorded separately

SCD type (1, 2, 4) is captured in `_ops.dataset_engineering.update_semantics` (per ADR 0007). It does **not** affect the naming convention:

- `geography.county` uses `merge_scd2` because Census revises county definitions. Still reference. Still no suffix.
- `surveillance.facility_dim` may use `merge_scd2` because facility ownership changes over time. Still derived. Still `_dim` suffix.

Companion history tables (when `merge_scd2_side` is the chosen pattern) follow ADR 0007's `<table>_history` naming. So a derived analytical dim with side-history is `surveillance.facility_dim` plus `surveillance.facility_dim_history`. A reference table with side-history is `geography.county` plus `geography.county_history`.

### Source-specific vs. conformed facts

Some facts are produced from a single source with that source's specific methodology. Others unify multiple sources into a conformed measurement. Naming records the distinction:

- **Source-specific fact:** `<provider_code>_<concept>_fact` — e.g., `surveillance.epic_cosmos_vaccine_coverage_fact`, `surveillance.kp_member_encounter_fact`. The provider code comes from ADR 0006's list. Used when the fact's methodology, definitions, or scope is specific to one source and hasn't been (yet) reconciled with other sources.
- **Conformed fact:** `<concept>_fact` — e.g., `surveillance.vaccine_coverage_fact`. Used when multiple sources have been reconciled to a single shared definition and the table represents the unified view.

The progression from source-specific to conformed is itself meaningful — the naming change records that conformance has been achieved. When a conformed table is created, the source-specific tables typically remain (for traceability and for use cases that need source-specific granularity); the conformed table is the canonical answer.

For derived analytical *dimensions* the source prefix is rarely needed because dimensional definitions tend to be naturally conformable. Default: `<concept>_dim`. Use source prefix only if a source-specific dim materially differs from a future conformed version.

### Surrogate key conventions

For derived analytical content using surrogate keys:

- **Fact tables:** surrogate primary key column named `<table_base>_sk` (e.g., `case_report_sk` on `case_report_fact`). Foreign keys to dims use the dim's natural key when one exists (e.g., `county_fips`), or the dim's surrogate (`facility_sk`) when not.
- **Derived dim tables:** primary key is the natural key when one exists (e.g., `cohort_id` on `cohort_dim`); use a surrogate `<table_base>_sk` when no natural key exists or when SCD2 history needs to disambiguate versions of the same natural entity.
- **Reference tables:** primary key is the canonical identifier from the source standard (`county_fips`, `epi_week_id`, `loinc_code`). Never a surrogate — the canonical identifier IS the key.

Surrogate columns are integer-typed (`bigint`) and generated via Delta `GENERATED ALWAYS AS IDENTITY` when supported, otherwise via a deterministic hash of the natural key components.

### Versioning of analytical outputs

Some analytical outputs need version tracking (e.g., a re-estimation of parameters using a new methodology). Two valid patterns:

- **Single table with `methodology_version` column** — preferred when versions coexist analytically and consumers may query any version. Filter on the column.
- **Separate tables with `_v1`/`_v2` suffix on the base name** — used when methodology change is incompatible enough that consumers shouldn't accidentally mix versions. Example: `pathogen.parameter_estimate_fact` becomes `pathogen.parameter_estimate_fact_v2` when a methodology breaking change occurs; the old table is retained and deprecated.

The expectation is that `_v2` versioning is rare. Most methodology evolution should be SCD2 or columnar versioning, not table-level versioning.

### What we don't do

- **No `tbl_`, `vw_`, `mv_` prefixes.** UC tracks object type natively; prefixes add noise.
- **No suffix on reference tables, even when they have SCD2 behavior.** See the orthogonal-axes rule above.
- **No `fact_*` / `dim_*` *prefixes* (vs. suffixes).** Prefix-style sorts all facts together and all dims together when scanning a schema, which sounds nice but the more common scan order is by concept (case_report_*, lab_test_*). Suffix keeps concept-first.
- **No mixed conventions within a schema.** All facts in a given schema use the same suffix style; all dims likewise. Inconsistency is the failure mode that costs cognitive load.

### Examples — end-to-end

```
ecdh_model_dev.geography.state                              # reference, mostly-static
ecdh_model_dev.geography.county                             # reference, time-varying (SCD2)
ecdh_model_dev.geography.county_history                     # SCD2 side-history companion
ecdh_model_dev.geography.zcta                               # reference, time-varying
ecdh_model_dev.geography.crosswalk_zcta_to_county           # reference structural
ecdh_model_dev.geography.hhs_region                         # reference, static

ecdh_model_dev.time.calendar_date                           # reference, static
ecdh_model_dev.time.epi_week                                # reference, static

ecdh_model_dev.codes.loinc                                  # reference, slow-changing
ecdh_model_dev.codes.cvx                                    # reference, slow-changing

ecdh_model_dev.pathogen.taxonomy                            # reference, static structural
ecdh_model_dev.pathogen.icd10_mapping                       # reference, static structural
ecdh_model_dev.pathogen.parameter_estimate_fact             # derived analytical (R0, GI, VE)
ecdh_model_dev.pathogen.parameter_estimate_fact_v2          # rare: methodology version break

ecdh_model_dev.surveillance.epic_cosmos_vaccine_coverage_fact   # source-specific fact
ecdh_model_dev.surveillance.kp_member_encounter_fact            # source-specific fact
ecdh_model_dev.surveillance.case_report_fact                    # conformed fact
ecdh_model_dev.surveillance.lab_test_fact                       # conformed fact
ecdh_model_dev.surveillance.facility_dim                        # derived dim (from claims)
ecdh_model_dev.surveillance.facility_dim_history                # SCD2 side-history
ecdh_model_dev.surveillance.case_status_dim                     # derived dim
ecdh_model_dev.surveillance.case_to_lab_bridge                  # bridge for many-to-many

ecdh_model_dev.population.cohort_dim                            # derived dim (study cohort)
```

## Alternatives considered
- **No suffixes (the original ADR 0003 stance).** Rejected. Roles aren't discoverable at a glance; mixing facts and dims in a concept schema becomes confusing fast.
- **Suffix everything in the integrated catalog including reference (`county_dim`, `epi_week_dim`).** Rejected. Loses the canonical-vs-derived distinction. Makes reference tables look like analytical projections, which they aren't.
- **Prefixes instead of suffixes (`dim_county`, `fact_case_report`).** Rejected. Sorts all dims together when scanning a schema, but the more useful scan order is by concept. Suffix preserves concept-first ordering.
- **Source-encoded as a column instead of in the name.** Rejected for source-specific facts because a source-specific fact table typically *only* contains data from one source. Encoding source in a column would always be a constant, adding no value.
- **Keep the `kimball_role:*` UC tag namespace as additional metadata.** Rejected as unnecessary duplication once names carry the role. A separate ADR (0005 revision) drops the `kimball_role:*` namespace.

## Consequences
- **At-a-glance role discovery.** Scanning a schema's table list reveals which are facts, which are dims, which are bridges, and which are canonical reference.
- **Canonical-vs-derived is visible.** Anyone glancing at the catalog can tell whether `geography.county` is authoritative (no suffix → yes) or whether they're looking at a derived projection.
- **Source-specific vs. conformed is encoded in the name.** A consumer can tell whether they're getting the canonical conformed truth or a source-specific view.
- **Lines up with standard data warehousing literature.** Onboarding is easier; tool integrations are easier.
- **Slightly longer names.** `surveillance.case_report_fact` is six characters longer than `surveillance.case_report`. Acceptable trade-off.
- **The `kimball_role:*` UC tag becomes redundant and is dropped** (see ADR 0005 revision).
- **SCD behavior is orthogonal to naming.** Contributors can choose update semantics independently. No naming gymnastics when a reference table needs SCD2 or when a derived dim doesn't need history.
- **One more rule to learn at onboarding.** The canonical-vs-derived test is straightforward but is an additional concept. Documented in CLAUDE.md and reinforced through code review.

**Implementation refinement (2026-05-26, country prefix).** Once ADR 0022 introduced global tables (`geography.country`, `geography.country_subdivision`, `geography.subnational`) alongside the existing US-specific tables, the unprefixed US table names became ambiguous. Per the ADR 0006 country-prefix refinement, US-specific reference tables now carry a `us_` prefix: `geography.us_state`, `geography.us_county`, `geography.us_tract`, `geography.us_zcta`, `geography.us_hhs_region`, `geography.us_crosswalk`. The "no Kimball suffix on reference tables" rule from this ADR still holds — `us_state` is still unsuffixed (reference, not Kimball derived). The examples in this ADR (`geography.county`, `geography.state`) predate the rename; substitute `geography.us_county`, `geography.us_state` when reading them today.

**Implementation refinement (2026-05-26, country prefix).** Once ADR 0022 introduced global tables (`geography.country`, `geography.country_subdivision`, `geography.subnational`) alongside the existing US-specific tables, the unprefixed US table names became ambiguous. Per the ADR 0006 country-prefix refinement, US-specific reference tables now carry a `us_` prefix: `geography.us_state`, `geography.us_county`, `geography.us_tract`, `geography.us_zcta`, `geography.us_hhs_region`, `geography.us_crosswalk`. The "no Kimball suffix on reference tables" rule from this ADR still holds — `us_state` is still unsuffixed (reference, not Kimball derived). The examples in this ADR (`geography.county`, `geography.state`) predate the rename; substitute `geography.us_county`, `geography.us_state` when reading them today.
