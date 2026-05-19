# 0001 — Layering vocabulary: raw / processed / analysis

## Status
Accepted — 2026-05-15

## Context
Databricks documentation and tutorials use the "medallion architecture" vocabulary — bronze, silver, gold — for the three canonical data layers. This is the idiomatic choice and has the practical advantage that every Databricks doc, example, and example DAB our team reads will use it.

However, the CIDMATH Data Hub's user base is not primarily Databricks engineers. It includes epidemiologists, public health practitioners, modelers, and downstream STLT partners who will browse the Unity Catalog directly to find data. The medallion metaphor is not self-explanatory: "what is gold data?" is a reasonable question and the answer ("the highest-quality, analysis-ready layer") requires explanation every time. Color metaphors also imply ordinal quality rather than functional purpose, which can be misleading.

A secondary concern: the "gold" layer in canonical Databricks usage covers both *aggregated/summarized* data and *denormalized analytical marts*. Calling it "aggregate" overstates the aggregation. Calling it "analysis" describes the actual purpose: this is the layer you read to do analysis.

## Decision
Use three layers named for purpose, not for metaphor:

| Layer | Purpose |
|---|---|
| **raw** | Source data ingested as-is. Faithful copy of the source. |
| **processed** | Cleaned, typed, deduplicated, conformed within a single source. |
| **analysis** | Subject-level, analysis-ready, user-facing. Multiple sources unified here. |

The analysis layer drops the suffix in schema names. Raw and processed schemas are named `<subject>_raw` and `<subject>_processed`; the analysis-layer schema is named `<subject>` (no suffix). End users see `ecdh_prod.wastewater.*` rather than `ecdh_prod.wastewater_analysis.*` and don't have to ask what "analysis" means or worry about the raw/processed plumbing.

The bare-subject convention also signals access tier without additional grants logic: anything ending in `_raw` or `_processed` is internal/engineer-facing; anything with no suffix is user-facing.

## Alternatives considered
- **bronze/silver/gold.** Rejected primarily for user comprehension. Secondary concern: "gold" misnames what the top layer actually does.
- **landing/staging/curated.** Rejected because "staging" implies temporary or pre-production, which is not what the processed layer is — processed tables are durable, governed, queryable artifacts.
- **raw/cleaned/analytic.** Close to the chosen vocabulary. Preferred "processed" over "cleaned" because the second layer does more than clean: it types, deduplicates, conforms, and applies schema constraints. "Cleaning" undersells the work.
- **All three layers carry a suffix (`wastewater_analysis`).** Rejected because the analysis layer is the most-read; making its name longer adds friction for the audience that matters most.

## Consequences
- **Better for non-technical users.** Schema names read naturally without Databricks context.
- **Diverges from Databricks documentation.** Anyone reading Databricks tutorials needs to translate bronze→raw, silver→processed, gold→analysis. The translation is documented in `CLAUDE.md` and is straightforward.
- **Suffixes double as access-tier signals.** Default grant model: engineers get `USE SCHEMA` + `SELECT` on `*_raw` and `*_processed`; end users get those on bare-subject schemas only. One pattern covers every subject — no per-schema bespoke ACLs.
- **"Analysis" is harder to confuse with "ML model output."** If we later add ML-derived tables, they live in their own schemas (e.g., in the integrated catalog or in a `<subject>_models` schema if needed), not in the analysis layer.
- **Renaming layers later is expensive.** If we change our minds, every schema, every grant, every reference in code/docs/ADRs has to change. This decision is durable by design.
