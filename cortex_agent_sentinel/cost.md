# COST.md — Query Cost Analysis

Run this after [DETECT.md](DETECT.md) finds an issue, before posting to Teams.
Attach the cost tier and estimated USD cost to every alert and improvement plan.

---

## 1. Identify Expensive Queries (Last 7 Days)

```sql
SELECT
    query_id,
    query_text,
    user_name,
    warehouse_name,
    warehouse_size,
    start_time,
    end_time,
    DATEDIFF('second', start_time, end_time)             AS duration_seconds,
    ROUND(bytes_scanned / POWER(1024, 3), 2)             AS gb_scanned,
    rows_produced,
    credits_used_cloud_services,
    ROUND(
        (DATEDIFF('second', start_time, end_time) / 3600.0)
        * CASE warehouse_size
              WHEN 'X-Small' THEN 1
              WHEN 'Small'   THEN 2
              WHEN 'Medium'  THEN 4
              WHEN 'Large'   THEN 8
              WHEN 'X-Large' THEN 16
              WHEN '2X-Large' THEN 32
              ELSE 4
          END
        * 3.0,   -- $3 per credit — adjust to your Snowflake contract rate
    2) AS estimated_cost_usd
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND execution_status = 'SUCCESS'
ORDER BY estimated_cost_usd DESC
LIMIT 20;
```

---

## 2. Cost by Job (Aggregate Over 7 Days)

Use this to see which jobs are consistently expensive, not just one-off spikes:

```sql
SELECT
    j.job_name,
    q.warehouse_name,
    q.warehouse_size,
    COUNT(*)                                                   AS run_count,
    AVG(DATEDIFF('minute', q.start_time, q.end_time))         AS avg_duration_mins,
    MAX(DATEDIFF('minute', q.start_time, q.end_time))         AS max_duration_mins,
    ROUND(SUM(q.bytes_scanned) / POWER(1024, 3), 2)          AS total_gb_scanned,
    SUM(q.credits_used_cloud_services)                        AS total_cloud_service_credits
FROM snowflake.account_usage.query_history q
JOIN job_monitoring_db.monitoring.job_run_log j
    ON q.query_id = j.query_id
WHERE q.start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1, 2, 3
ORDER BY total_cloud_service_credits DESC;
```

---

## 3. Classify Each Query

Apply this classification in Python before posting to Teams or writing the plan:

```python
def classify_query(estimated_cost_usd: float, duration_mins: float) -> str:
    if estimated_cost_usd > 50 or duration_mins > 120:
        return "CRITICAL"
    elif estimated_cost_usd > 10 or duration_mins > 60:
        return "HIGH"
    elif estimated_cost_usd > 2 or duration_mins > 20:
        return "MEDIUM"
    else:
        return "LOW"
```

Include the tier in every Teams alert card and in the improvement plan header.

---

## 4. Cost Tier Reference

| Tier | Condition | Teams alert color | Action |
|---|---|---|---|
| CRITICAL | > $50 or > 120 min | `FF0000` 🔴 | Alert immediately + draft plan today |
| HIGH | > $10 or > 60 min | `FFA500` 🟡 | Alert + plan within same business day |
| MEDIUM | > $2 or > 20 min | `FF6600` 🟠 | Alert + plan in next cycle |
| LOW | below all thresholds | `1565C0` 🔵 | Log only; no immediate action |

---

## 5. Common Cost Patterns to Look For

| Pattern | Signal | What to report in plan |
|---|---|---|
| Full table scan | `gb_scanned` >> expected for the filter | Missing clustering key or date filter |
| Repeated runs of same query | `run_count` high, no caching | Result cache disabled or query non-deterministic |
| Large warehouse for small query | S or M query running on XL | Right-size warehouse or use dedicated small WH |
| Cloud service credits > compute | `credits_used_cloud_services` dominates | Too many small queries; batch them |
| Single runaway query | One `query_id` with very high cost | Likely missing join predicate or cartesian product |

Carry these findings into [PLAN.md](PLAN.md) as the root cause section.
