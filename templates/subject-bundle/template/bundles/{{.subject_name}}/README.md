# `{{.subject_name}}` subject bundle

Source-aligned subject bundle for **{{.subject_name}}** (ADR 0004). Writes to `ecdh_<env>`: `{{.subject_name}}_raw`, `{{.subject_name}}_processed`, and (when built) the bare `{{.subject_name}}` analysis schema. Deploys after `_platform` and `_reference`.

## Tables

| Schema.table | Layer | Update semantics | Source |
|---|---|---|---|
| `{{.subject_name}}_raw.{{.provider_code}}_{{.primary_dataset}}` | raw | merge_upsert | {{.provider_code}} (TODO: source URL + docs) |
| `{{.subject_name}}_processed.{{.provider_code}}_{{.primary_dataset}}` | processed | {{.update_semantics}} | conformed from raw |

## Structure (ADR 0011 / 0027)

- `databricks.yml` — bundle definition; includes `../../databricks-common.yml`.
- `resources/*.yml` — one job per build step (deploy-only; trigger from the Databricks UI).
- `src/build_{{.primary_dataset}}_*.py` — thin entrypoints; each wires `ensure`/`work`/`register`/`grant` hooks and calls `cidmath_datahub.common.pipeline.run_build`.
- Parse/transform/conform logic lives in `src/cidmath_datahub/{{.subject_name}}/{{.primary_dataset}}.py` — unit-tested, no Spark.

## Build order

1. `[{{.subject_name}}] build_{{.primary_dataset}}_raw` — land the source faithfully.
2. `[{{.subject_name}}] build_{{.primary_dataset}}_processed` — conform to geography/time, register `_ops` metadata, apply grants.

Jobs are deploy-only; trigger from the Databricks UI. Deploys run via `.github/workflows/deploy-{{.subject_name}}.yml` (copy from `deploy-weather.yml`) on merge to main.

## Status

Scaffolded from `templates/subject-bundle`. **TODO:** implement the entrypoint hooks and the logic module, add real DQ checks, and register the processed table. See `docs/authoring-a-bundle.md`.

## Contact

- **Owner:** Connor Van Meter (connor.vanmeter@emory.edu)
