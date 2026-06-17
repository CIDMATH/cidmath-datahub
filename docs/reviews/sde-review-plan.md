# SDE Incremental Review Plan — CIDMATH Data Hub (triage-first, risk-ordered)

A senior-data-engineer review of what's in the system today. **Top-down triage first, then
bottom-up depth where the risk actually is** — not an exhaustive module-by-module crawl. You
invoke one pass at a time; each is read-only (findings, not code changes) and sized for a
focused session.

Scope: the reference / `codes` / geography / time data we have now. Re-run the relevant passes
when non-reference (surveillance / fact-style) sources land — same rubric, different specifics.

## How to use
- **Start with T0 (triage).** It maps the big bets, scores each area by risk, and outputs a
  *prioritized, pruned* run order for the passes below — including which to **skip**. Everything
  after T0 is a menu pulled in that order, not a checklist to complete.
- Invoke a pass by ID: **"run review T0"**, then "run U1", etc.
- Every pass is read-only: findings graded **must-fix / should-fix / consider / nit**, with
  what's working called out, plus optional ADR/issue drafts — nothing applied without your go.
- **High-stakes passes** (anything T0 marks high-risk, plus the #1 decision) get two extra
  steps: a **pre-mortem** ("imagine this broke in production in six months — why?") and, on
  request, an **independent verification subagent** with fresh context as a second set of eyes.
  This counters single-reviewer blind spots (the reviewer helped shape some of this, e.g. ADR
  0034); it is not a substitute for a human co-reviewer on the biggest calls.

## Review rubric (applied every pass)
correctness · simplicity / DRY · single responsibility · robustness (errors, atomicity,
idempotency, partial-failure) · security / least-privilege · testability & coverage ·
consistency with ADRs & conventions · performance / operability · the over/under-built test
(YAGNI vs a real gap).

## Risk grading (drives the ordering)
Each area is scored **blast radius** (local & cheap → systemic & hard-to-reverse) × **likelihood
of being wrong** (settled → smelly / untested / recently churned). High-risk → review deep and
early; low-risk → skim or skip. T0 assigns these scores; the rest of the plan follows them.

---

## ▶ START HERE — T0: Architecture & Risk Triage (top-down)
A single rapid sweep at altitude, **before** any deep pass, so expensive architectural mistakes
surface before time is sunk into unit details. It looks across — not into — these big bets:
catalog / schema / layering (ADR 0001–0003 / 0015) · the data model & grain · update-semantics
& history (0007 + 0034) · cross-source conformance & FK strategy (0023 / 0025) · build & deploy
architecture (0004 / 0026 / 0027) · security & governance posture (0012 / 0018 / 0033) · and the
actual data inventory (what tables exist, sizes, freshness). For each it assigns blast radius ×
likelihood, and it re-frames the **#1 shared-builder** question with evidence.

**Output:** a one-page risk map + a pruned, prioritized run order ("do these N passes in this
order; skip these as low-risk; here are the 2–3 things that worried me most"). T0's output
overrides the default lean below.

## The pass menu — pull in T0's priority order; prune freely

### Units (a module in isolation)
- **U1 — `common/pipeline.py` (`run_build`, ADR 0027):** ensure→work→register→grant contract;
  partial-failure behavior; idempotency; whether it's the right seam for the shared builder.
- **U2 — `common/registration.py` + entry dataclasses (ADR 0008):** required vs optional fields;
  insert vs upsert / idempotency; how 0034's reclass + a future `vintage_key` land; validation.
- **U3 — `common/dq.py` (`TableDQ`, ADR 0029):** check builders; severity→record→raise; WARN vs
  FAIL consistency; coverage gaps; the bespoke-check escape hatch.
- **U4 — `common/vocabularies.py` + `scripts/ci/check_conventions.py` (ADR 0016):** what CI
  enforces vs merely documents; scanner robustness / false-negatives; the `update_semantics`
  enum (0034 touch point).
- **U5 — reference pure-logic modules (ADR 0011):** parse/validate consistency across sources;
  `_get_secret`; purity; shared-helper candidates.

### Integrations (units wired together)
- **I1 — one build end-to-end** (e.g. `build_ndc.py`): entrypoint→run_build→write→register→
  grant→DQ; quantify the copy-paste surface vs source-specific bits. **Feeds the #1 decision.**
- **I2 — `_ops` metadata model:** `dataset_catalog` / `dataset_engineering` / `dq_results` +
  `discovery.datasets` (ADR 0008 / 0019); schema, relationships, ownership chain, drift.
- **I3 — secret-scoped downloads** (ipums / loinc / umls, ADR 0012): consistency, security, the
  shared-UTS-helper question for RxNorm/SNOMED.
- **I4 — grants + deploy auth** (ADR 0012 / 0018 / 0033): least privilege, catalog-vs-schema
  split, the drift check, the Actions→UI flow.

### System / architecture (the whole in combination)
- **S1 — catalog / schema / layering** (0001 / 0002 / 0003 / 0015).
- **S2 — naming-convention conformance sweep** (0006).
- **S3 — update-semantics & history post-0034** (0007 + 0034).
- **S4 — testing strategy & coverage** (0011 / 0016).
- **S5 — bundle / monorepo / deploy + CI matrix** (0004 / 0026 / 0027).
- **S6 — cross-source conformance** (0023 / 0025; FKs to geography/time; conform overrides).
- **S7 — ADR corpus coherence** (all ADRs; supersession chains, contradictions, code-vs-ADR
  drift). Natural capstone.

### Data — the data itself, not the code
- **D1 — data profiling & DQ-as-outcome:** profile the loaded tables (null rates, cardinalities,
  distributions, duplicates, suspicious values, freshness); judge whether the DQ checks are the
  *right* ones and what silent issues they miss. For a data hub this is first-class, not
  optional. (Uses the `data:explore-data` / `data:validate-data` skills.)

### Operations — can it run, fail, and recover
- **O1 — operational readiness:** failure modes & recovery; observability / alerting (ADR 0010);
  idempotent re-runs and backfill/replay in practice; cost; runbook coverage. Does the pipeline
  survive a bad day?

### Decision
- **#1 — shared reference-table builder:** scope (write+register only vs also download/parse),
  style (config/composition vs inheritance), proving ground (greenfield `icd10pcs` vs refactor
  `cvx`) — decided with evidence from T0 + I1 + I2.

## Default lean (only if T0 surfaces nothing surprising)
T0 → the high-risk items T0 flags → I1 + I2 → **#1 decision** → D1 (trust the data before going
deeper) → remaining U / S / O passes in risk order → S7 capstone. **T0's risk map overrides
this.**

## Progress
- [ ] **T0 — Architecture & risk triage (START HERE)**
- [ ] U1 — run_build seam · [ ] U2 — registration / `_ops` writers · [ ] U3 — TableDQ ·
      [ ] U4 — vocabularies + check_conventions · [ ] U5 — reference pure-logic consistency
- [x] I1 — one build end-to-end (done 2026-06-16, see i1-build-end-to-end-findings.md) · [ ] I2 — `_ops` model · [x] I3 — secret downloads ·
      [x] I4 — grants + deploy auth — both done 2026-06-16 (see i4-i3-security-governance-findings.md; no must-fix; secret-ACL-as-code follow-up drafted)
- [ ] **#1 — DECISION: shared reference-table builder** — recommendation ready (see I1 findings: config/composition `ReferenceTableSpec`, greenfield on ICD-10-PCS then a hard case); awaiting your approval of scope/style/proving-ground
- [ ] S1 — layering · [ ] S2 — naming · [ ] S3 — semantics/history (0007+0034) ·
      [ ] S4 — testing · [ ] S5 — bundle/deploy · [ ] S6 — conformance · [ ] S7 — ADR coherence
- [x] D1 — data profiling & DQ-as-outcome (done 2026-06-16 — see d1-data-profiling-findings.md; surfaced the conformance-vintage must-fix → ADR 0035 draft)
- [ ] O1 — operational readiness
