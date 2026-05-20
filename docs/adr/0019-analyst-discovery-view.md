# 0019 — Analyst-facing discovery view

## Status
Accepted — 2026-05-20

## Context
The dataset catalog — the rich, EpiPortal-style metadata describing every dataset (what it is, why it matters, its coverage, cadence, limitations, how to access it) — lives in the `_ops` schema (ADR 0008). `_ops` is internal and engineer-only by design (ADR 0018): reader-tier groups like `ecdh-analysts` get no access to it. That creates a gap the moment a real consumer arrives. An analyst exploring the `time` reference data can query the data but has no way to *discover* what else exists, because the catalog that would tell them is in a schema they can't see.

ADR 0018 anticipated this and deferred it: "a curated, analyst-facing discovery surface can be exposed later as an explicit view in a non-`_ops` schema if demand appears." Demand has now appeared (the first analyst use case), so this ADR makes that decision concrete.

The core requirement is to expose *metadata about datasets* (not the data, and not the operational/engineering internals) to reader-tier groups, without weakening the rule that `_ops` stays engineer-only.

## Decision
Add a `discovery` schema (no leading underscore — it is reader-facing, not internal) to each catalog, containing a single curated view, `discovery.datasets`, that selects an analyst-relevant subset of columns from `_ops.dataset_catalog` joined to `_ops.dataset_engineering`. Reader-tier groups receive `USE SCHEMA` + `SELECT` on `discovery`; they still receive nothing on `_ops`.

This relies on **Unity Catalog's view ownership chain**: the `_platform` deploy service principal owns both the `_ops` base tables (it created them) and the `discovery.datasets` view (it creates it in the setup job). When a principal queries the view, UC checks privileges along the ownership chain rather than requiring the querying principal to hold `SELECT` on the underlying `_ops` tables. So an analyst with `SELECT` on the view (and `USE CATALOG`/`USE SCHEMA` to reach it) reads curated catalog metadata while having zero access to `_ops` itself.

The view's column list is deliberately curated for a data consumer: identity and classification (`full_table_name`, `subject`, `layer`), the descriptive narrative (`description`, `public_health_relevance`, `known_limitations`, `missingness_notes`, `data_suppression_notes`), coverage and cadence (temporal/spatial/demographic resolution and coverage, `refresh_cadence`, `reporting_lag`, `revision_cadence`), access path (`source_url`, documentation links, `license`, `dua_required`, `dua_reference`, `access_tier`, `is_hosted`), and freshness (`update_semantics`, `last_refresh_at`, `dq_status_last`). Internal plumbing — pipeline references, ingestion watermarks, schema versions, the free-form `domain_metadata_misc` map, and maintainer contact details — is intentionally omitted. The view is created per-catalog by the same `setup_ops_tables.py` job that creates `_ops`, so it stays in lockstep with the catalog and is re-asserted idempotently on every deploy.

## Alternatives considered
- **Grant analysts read access to `_ops` directly.** Rejected: it breaks the engineer-only boundary on `_ops` (ADR 0018) and would expose engineering internals and operational tables (`dq_results`, `pipeline_runs`) that consumers shouldn't see.
- **Reuse the existing `_ops.dataset_catalog_full` view.** Rejected for the analyst surface: it lives in `_ops` (so the same access problem) and includes engineering columns. It remains the engineer-facing composite; `discovery.datasets` is the consumer-facing one.
- **Row-filter the discovery view to only datasets the analyst can query.** Rejected for now: discoverability of what *exists* (including DUA-gated datasets the analyst could request) is valuable, and the view exposes metadata, not data. Row/column-level restriction can be layered on later if a specific dataset's existence is itself sensitive.
- **A single cross-catalog discovery view.** Deferred: the catalog is per-catalog today, mirroring `_ops`. A unified view spanning `ecdh_<env>` and `ecdh_model_<env>` can be added if analysts find the per-catalog split awkward.

## Consequences
- **Analysts can browse the catalog** by querying `ecdh_model_<env>.discovery.datasets` (and `ecdh_<env>.discovery.datasets`) as soon as the `_platform` setup job re-runs, with no change to the `_ops` access boundary.
- **A new schema convention exists.** `discovery` is the first schema that is neither a concept schema (per CLAUDE.md) nor internal (`_ops`); it is the designated home for reader-facing, curated views over operational metadata. Future consumer-facing surfaces belong here.
- **The curated column list is a maintenance point.** When `dataset_catalog` gains a column that consumers should see, the view must be updated to surface it. This is a deliberate, reviewable choice rather than `SELECT *`.
- **Ownership discipline matters.** The exposure pattern depends on the deploy SP owning both the base tables and the view. If a human ever creates these objects locally (ADR 0017's ownership pitfall), the chain can break and analysts may lose access until ownership is corrected. CI as the canonical deploy path keeps this consistent.
