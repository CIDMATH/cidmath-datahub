# CIDMATH Data Hub

A Databricks-based platform for ingesting, transforming, and sharing infectious disease modeling and analytics data, deployed via Databricks Asset Bundles (DAB).

The hub serves Emory's Center for Infectious Disease Modeling and Analytics Training Hub (CIDMATH) and partners. It consolidates public, commercial, and partner data sources into a unified environment supporting reproducible analytics, collaborative modeling, and scalable data pipelines.

## Quick start

**To query the data** (analyst / researcher):

The catalog `ecdh_prod` holds source-aligned data organized by subject area (e.g., `ecdh_prod.wastewater`, `ecdh_prod.vaccine`). The catalog `ecdh_model_prod` holds canonical reference data (`geography`, `time`, code systems). Browse Unity Catalog in your Databricks workspace to find what's available, or query `ecdh_prod._ops.dataset_catalog_full` for rich metadata about every dataset.

**To contribute** (engineer):

See [`docs/onboarding.md`](docs/onboarding.md) for first-time setup, including local Databricks CLI configuration and `pip install -e .[dev]`. Read [`CLAUDE.md`](CLAUDE.md) for conventions and [`docs/adr/README.md`](docs/adr/README.md) for the architectural decisions.

## Architecture at a glance

- **Two source-aligned catalogs:** `ecdh_dev` and `ecdh_prod`. Schemas follow `<subject>_raw`, `<subject>_processed`, and `<subject>` (analysis layer, no suffix). See [ADR 0001](docs/adr/0001-layering-vocabulary.md) and [ADR 0002](docs/adr/0002-schema-is-subject-not-source.md).
- **Two integrated/modeled catalogs:** `ecdh_model_dev` and `ecdh_model_prod`. Schemas are concepts (`geography`, `time`, `pathogen`, `surveillance`). Reference tables carry no suffix; derived analytical tables use Kimball-style `_fact`/`_dim`/`_bridge` suffixes. See [ADR 0003](docs/adr/0003-catalog-split-source-vs-integrated.md) and [ADR 0015](docs/adr/0015-integrated-table-naming.md).
- **Monorepo with multiple Databricks Asset Bundles:** one `_platform` bundle for infrastructure, one `_reference` bundle for canonical reference data, one bundle per subject area. See [ADR 0004](docs/adr/0004-monorepo-bundle-per-domain.md) and [ADR 0014](docs/adr/0014-reference-data-scope.md).
- **Shared Python package:** `src/cidmath_datahub/` built into a wheel that each bundle installs as a library.
- **Operational metadata schema:** `_ops` in each catalog holds the dataset catalog, engineering metadata, DQ results, and other cross-cutting tables. See [ADR 0008](docs/adr/0008-catalog-metadata-schema-design.md).

## Repo layout

```
cidmath-datahub/
├─ README.md
├─ CLAUDE.md                       # context for Claude / new contributors
├─ pyproject.toml                  # builds cidmath_datahub wheel
├─ databricks-common.yml           # shared workspace, targets, variables, tags
├─ bundles/
│  ├─ _platform/                   # infrastructure: schemas, _ops tables, grants
│  ├─ _reference/                  # canonical reference data (added in Week 2)
│  └─ <subject>/                   # one per subject area
├─ src/
│  └─ cidmath_datahub/             # shared Python package
│     ├─ common/                   # naming, IO, logging, UC helpers, secrets
│     ├─ ingest/                   # ingestion patterns
│     ├─ transforms/               # transformation utilities
│     └─ dq/                       # data quality framework
├─ tests/
│  ├─ unit/                        # pure logic
│  ├─ data/                        # DataFrame transforms with local Spark
│  ├─ integration/                 # end-to-end against dev workspace
│  └─ fixtures/                    # synthetic test data
├─ docs/
│  ├─ adr/                         # architecture decision records
│  ├─ onboarding.md
│  ├─ operations.md
│  └─ runbooks/
└─ .github/workflows/
   ├─ ci.yml
   ├─ deploy-platform.yml
   ├─ deploy-reference.yml         # added when _reference lands
   └─ deploy-domain.yml            # added when first subject bundle lands
```

## Operations

See [`docs/operations.md`](docs/operations.md) for prerequisites (one-time manual setup an account admin performs before first deploy), deploy procedures, and emergency contacts.

## Conventions and decisions

All architectural conventions live in [`docs/adr/`](docs/adr/README.md). Sixteen ADRs cover layering, schema structure, catalog organization, bundle structure, discovery, naming, update semantics, metadata, data quality, observability, testing, identity, documentation, reference data, integrated table naming, and CI enforcement policy.

## Contact

- **Owner:** Connor Van Meter (connor.vanmeter@emory.edu)
- **Team chat:** Microsoft Teams — `Data Hub` channel in the CIDMATH Team Site
- **Funded by:** Insight Net cooperative agreement CDC-RFA-FT-23-0069 (CDC's Center for Forecasting and Outbreak Analytics)
