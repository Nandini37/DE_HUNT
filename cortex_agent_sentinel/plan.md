# PLAN.md — Drafting an Improvement Plan

Write one plan per issue. Do not group multiple issues into a single plan.
Complete the diagnosis checklist in [DETECT.md](DETECT.md) §5 before opening this file.
Attach cost tier and estimated USD from [COST.md](COST.md) to the plan header.

---

## 1. Before Writing the Plan

Answer these questions. Do not proceed until you can answer all of them:

- What exactly is broken or slow? (query text, job name, table name)
- What is the measurable symptom? (X mins elapsed, $Y cost, N failed rows)
- What is the root cause? (use the diagnosis checklist from [DETECT.md](DETECT.md) §5)
- What is the minimum change that fixes it?
- What could go wrong with that change? What is the mitigation?
- How will you prove it worked? (before/after query profile, DMF pass, 3 successful runs)

If you cannot answer all six, stop and ask.

---

## 2. Plan Template

Use this exact structure. Fill every section. Do not leave any section blank.

```
## Improvement Plan — [Job Name / Query ID]

Issue Type     : [Failed Job | Long-Running Query | Expensive Query | DMF Failure]
Detected At    : [timestamp]
Duration       : [X mins]
Estimated Cost : [$Y]
Cost Tier      : [CRITICAL | HIGH | MEDIUM | LOW]
Warehouse      : [name / size]

### Root Cause
[Specific cause. Reference the actual query text, data volume, join type,
 scan pattern, or error message. Be precise — "slow query" is not a root cause.]

### Proposed Changes
1. [Change] — Expected impact: [measurable outcome, e.g. "reduces full scan to 14-day window"]
2. [Change] — Expected impact: [measurable outcome]
3. [Change] — Expected impact: [measurable outcome]

### Files / Objects to Modify
- [file path or SQL object] — [what changes and why]

### Risks & Tradeoffs
- [Risk]: [mitigation]

### Success Criteria
- [ ] Query duration < 60 mins in dev (measured via Snowflake query profile)
- [ ] Estimated cost reduces by ≥ 30% vs baseline run
- [ ] DMF checks pass after change
- [ ] All existing Streamlit dashboard assertions pass
- [ ] No regression in downstream job dependencies
```

---

## 3. Fix Lookup Table

Use this as your starting point for root cause → fix mapping.

| Symptom | Root Cause | Fix |
|---|---|---|
| Full table scan, high `gb_scanned` | No date filter / no clustering key | Add `WHERE date_col >= DATEADD(...)` and cluster table on that column |
| Slow JOIN, high row count | Missing join key index, high cardinality, or fan-out | Verify join cols are clustering keys; check for accidental cross-join |
| CTE re-evaluated | CTE referenced multiple times in same query | Materialize into a `CREATE TEMP TABLE` before the main query |
| Warehouse queue buildup | Too many concurrent queries for warehouse size | Scale up warehouse size or enable multi-cluster auto-scaling |
| High cloud service credits | Many tiny queries hitting metadata layer | Batch small queries; enable Snowflake result cache |
| DMF check failing on NULLs | Missing NOT NULL constraint or no data freshness check | Add NOT NULL constraint; add freshness threshold in DMF config |
| Runaway recursive CTE | Missing or wrong termination condition | Add `MAX_RECURSION` limit; verify anchor + recursive member logic |
| Job failing on retry | Transient Snowflake error not caught | Add retry logic with exponential backoff; log transient vs fatal errors |
| Expensive aggregation | No pre-aggregated summary table | Create a materialized summary table or scheduled aggregation job |

---

## 4. Multi-Plan Management

When multiple issues are detected in the same monitoring cycle:

1. Draft a separate plan file for each issue: `plan_<job_name>_<YYYYMMDD>.md`.
2. Post them to Teams one at a time — do not post all plans in one card.
3. Wait for approval on each plan independently before branching (see [APPROVAL.md](APPROVAL.md)).
4. Do not start coding plan B while waiting for approval on plan A.

---

## 5. Revising a Plan After Feedback

When the developer sends `REQUEST_CHANGES` via Teams:

1. Read the feedback comment in the Teams thread.
2. Address every comment explicitly — do not silently absorb feedback.
3. Update the plan document. Mark changed sections with `[REVISED]`.
4. Re-post to Teams using [ALERTS.md](ALERTS.md) §3 with `[FINAL]` prepended to the title.
5. Wait again. Do not proceed until `APPROVE` is received.
