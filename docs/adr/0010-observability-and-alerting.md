# 0010 — Observability and alerting

## Status
Accepted — 2026-05-15

## Context
A data hub that breaks silently is unusable: consumers query stale or missing data, modelers use bad inputs, the team finds out from a chat message asking "is something wrong?" rather than from instrumentation. ADR 0009 introduced the DQ framework that captures *what* is wrong; this ADR addresses *who finds out, when, and through what channel.*

The observability surface needs to cover four signals:

1. **Pipeline run status** — succeeded, failed, partially failed, still running.
2. **Data freshness** — has each table been refreshed within its expected cadence?
3. **Data quality outcomes** — are DQ checks passing? How severe are the failures?
4. **Cost and resource use** — DBU consumption per pipeline, per environment, per project.

Databricks provides primitives for each: job notifications, the LDP event log, Lakehouse Monitoring (for analysis-layer profiling), system tables (`system.billing.usage`, `system.access.audit`), Databricks SQL alerts, Lakehouse Monitoring dashboards. The question is how we use them coherently — what gets alerted, where alerts go, who responds.

The team is small. Over-alerting creates fatigue and alerts get ignored. Under-alerting means real failures slip past. The right calibration depends on user impact: an alert is worth sending if a human's response would change the outcome.

## Decision

### What we monitor

| Signal | Source | Frequency |
|---|---|---|
| Pipeline run failures | Databricks job/pipeline events | Per run |
| Pipeline duration anomalies | Databricks job/pipeline events | Per run; threshold = 2× rolling 30-day median |
| Table freshness SLA violations | `_ops.dataset_engineering.last_refresh_at` vs. expected cadence | Hourly check |
| DQ severity = `fail` events | `_ops.dq_results` | Per run (immediate) |
| DQ severity = `quarantine` rate above threshold | `_ops.dq_results` aggregations | Daily roll-up |
| Cost anomalies | `system.billing.usage` | Daily roll-up; threshold = 2× trailing 7-day average |
| Schema drift in source data | DQ schema checks | Per run |
| Catalog completeness (missing dataset_catalog rows) | CI + nightly audit | Daily |

### Alert routing

**Primary channel: Microsoft Teams.** Single channel — "Data Hub" within the "CIDMATH Team Site" team. All alert severities post here, with differentiation conveyed in the message itself (severity color bar, label in the title, `<at>Everyone</at>` mention reserved for paging-severity).

**Secondary channel: Email.** Paging-severity alerts and the daily digest are also delivered to `connor.vanmeter@emory.edu` via Databricks-native email notifications on jobs/pipelines. Additional recipients can be added per bundle as the team grows. Non-paging alerts do not go to email by default (avoids inbox noise).

**Three severity tiers, conveyed in the Teams message:**

- **Page** — red color bar in the Adaptive Card; title prefixed `[PAGE]`; `<at>Everyone</at>` mention. Terse single-line description with a link to the failing run / dashboard. Also sent to email.
- **Alert** — yellow color bar; title prefixed `[ALERT]`; no mention. Teams only.
- **Info / Digest** — gray color bar; title prefixed `[INFO]` or posted as the daily digest at 8am ET. Daily digest is also sent to email.

**No SMS or phone paging** at this stage. Reassess if 24/7 ops becomes a real requirement.

**Routing per signal:**

| Signal | Teams severity | Email? |
|---|---|---|
| Pipeline `fail` (prod) | Page | Yes |
| Pipeline `fail` (dev) | Alert | No |
| DQ severity `fail` (prod) | Page | Yes |
| DQ severity `quarantine` above threshold (prod) | Alert | No |
| Freshness SLA violation (prod) | Page | Yes |
| Freshness SLA violation (dev) | Alert | No |
| Cost anomaly | Info (in daily digest) | Yes (digest) |
| Schema drift | Alert | No |
| Catalog completeness gaps | Info (in daily digest) | Yes (digest) |

Per-domain alert recipients are declared in each bundle's `databricks.yml` via a `notifications` section. Email recipients are listed directly; the Teams webhook URL is referenced from a bundle variable defined in `databricks-common.yml`. This lets a domain bundle add domain-specific email recipients (e.g., the wastewater bundle could add a wastewater-team distribution list) without changing the Teams routing.

### Freshness SLAs

Every materialized table optionally declares a freshness SLA in `_ops.dataset_engineering`:

| Column | Type | Purpose |
|---|---|---|
| `freshness_sla_hours` | int | If non-null, the max hours since last successful refresh before the table is considered stale |
| `freshness_check_paused` | boolean | Lets engineers temporarily silence freshness alerts during planned maintenance |

A scheduled Job in `_platform` runs hourly: for every table with a non-null `freshness_sla_hours`, compute `current_timestamp() - last_refresh_at` and alert if it exceeds the SLA. Tables without an SLA are not checked.

Defaults to suggest in dataset declarations:

| Layer | Suggested default `freshness_sla_hours` |
|---|---|
| raw | 2× the source's reporting cadence (e.g., daily source → 48 hours) |
| processed | 1× the source's reporting cadence + reporting_lag |
| analysis | Same as upstream processed, unless aggregating multiple sources |

### Dashboards

Three Databricks SQL dashboards owned by `_platform`, refreshed every 15 minutes:

1. **Pipeline health.** Per-bundle, per-pipeline run status over the last 7 days. Failure rate, duration trends, last successful run.
2. **Data quality trends.** Per-table DQ severity counts over the last 30 days. Trend charts for warn/quarantine/fail rates.
3. **Cost and capacity.** DBU consumption per bundle, per pipeline, per environment. Trailing 30 days. Tagged by `cost_center` from ADR's tag schema.

Domain bundles can ship additional dashboards in `bundles/<subject>/dashboards/` for subject-specific health views.

### Incident response (lightweight at this stage)

- Each domain bundle declares an `owner` in `databricks.yml`. The owner is the first responder for alerts from that bundle.
- A "primary on-call" role rotates weekly across the data team for paging-severity events. The on-call's job is triage, not necessarily fix — they escalate to the domain owner if the issue isn't in `_platform`. *(Assumption: this presumes a team with multiple members. If practice is solo or near-solo for now, this becomes "alerts go to the named on-call email recipient and that person triages.")*
- Runbooks live in `docs/runbooks/` (one per common incident class: pipeline failed, freshness SLA missed, DQ fail spike, cost anomaly). Each runbook covers triage steps, common causes, and resolution paths.
- Post-incident: any prod-paging incident requires a brief writeup in `docs/incidents/YYYY-MM-DD-short-name.md` covering what happened, impact, resolution, and any follow-up actions. Five-paragraph max.

### Lakehouse Monitoring integration (deferred)

Lakehouse Monitoring is Databricks' built-in capability for data profiling and drift detection on Delta tables. It's well-suited for analysis-layer tables once stable. The plan:

- Phase 1 (now): Custom DQ checks (ADR 0009) + dashboards over `_ops.dq_results`. This is what we build day 1.
- Phase 2 (after first 2-3 pipelines): Enable Lakehouse Monitoring on key analysis-layer tables for distribution and drift signals. Alert on drift exceeding a threshold.

Defer Phase 2 until we have stable analysis tables to monitor; drift detection on still-evolving schemas produces noise.

### Audit logging

Use the Databricks-managed `system.access.audit` system table for workspace audit events (logins, grant changes, pipeline runs). Retention is managed by Databricks; we don't duplicate.

For data-level audit (who queried this table, when), defer — UC's query history covers it for now and we'll evaluate fuller solutions if compliance requires.

## Alternatives considered
- **Set up Datadog/PagerDuty from day one.** Rejected. Adds an external dependency and licensing cost without proportional benefit at our scale. Databricks-native + Teams + email covers the actual need. Revisit if 24/7 ops or external SLA reporting becomes a requirement.
- **No alerting until we're sure what to alert on.** Rejected. Pipelines will fail in week one; without alerts, we won't know until a consumer complains. Start with conservative alerts; tune down what's noisy.
- **Alert on every DQ warn.** Rejected. Guarantees alert fatigue. `warn` aggregates into the daily digest and trend dashboard, not real-time alerts.
- **Alert via email only.** Rejected. Teams is faster, more visible, and team is already in it. Email is supplementary for paging-severity and digest.
- **Two-channel Teams setup (separate "Pages" channel).** Considered. Cleaner separation of paging from non-paging, slightly more setup (a second webhook). Started with single channel; revisit if alert fatigue is observed in the first month.

## Consequences
- **Failures surface within minutes via Teams (and email for paging).** The team finds out before consumers do.
- **Freshness SLAs are explicit and per-table.** Each pipeline's `databricks.yml` (or the engineering metadata row) sets the SLA; alerts are deterministic, not opinion-based.
- **The DQ framework (0009) is fully leveraged.** `dq_results` rows drive real-time and aggregated alerts, plus dashboards. The schema designed in 0009 pays for itself here.
- **Initial alerting is conservative; tuning is expected.** Some alerts will be noisy in the first month. The post-incident process (and just routine team retro) is the feedback loop for tuning thresholds.
- **No external observability vendor.** Reduced complexity and cost at the cost of Datadog-class capabilities. Acceptable trade-off for the team's scale.
- **Runbooks and incident writeups are required but lightweight.** Five-paragraph cap discourages cargo-culting. The point is institutional memory, not paperwork.
- **Lakehouse Monitoring is a deliberate Phase 2.** Avoids deploying drift detection on tables that are themselves drifting because they're new.
- **Per-domain alert routing via bundles.** Each bundle declares its owner and channel overrides. A `wastewater` bundle's failures page the wastewater team plus the on-call; a `_platform` failure pages the data team broadly.
