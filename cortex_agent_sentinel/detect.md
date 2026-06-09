# DETECT.md — Detecting Issues

Run all three checks every 5 minutes inside the Streamlit monitoring loop.
When any check returns rows, proceed immediately to [COST.md](COST.md), then [ALERTS.md](ALERTS.md).

---

## 1. Failed Jobs

```sql
SELECT
    job_id,
    job_name,
    status,
    error_message,
    start_time,
    end_time,
    retry_count,
    DATEDIFF('minute', start_time, end_time) AS duration_mins
FROM job_monitoring_db.monitoring.job_run_log
WHERE status NOT IN ('SUCCESS', 'RUNNING')
  AND start_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
ORDER BY start_time DESC;
```

A row here means a job has failed. Capture `job_id`, `job_name`, `error_message`, and `duration_mins` for the Teams alert.

---

## 2. Long-Running Queries (threshold: 60 minutes)

A query is long-running when:
- `start_time` is set
- `end_time` is NULL (still in progress)
- Elapsed time since `start_time` exceeds **60 minutes**

### Query used in the Streamlit app

```sql
SELECT
    query_id,
    job_name,
    query_text,
    user_name,
    warehouse_name,
    start_time,
    DATEDIFF('minute', start_time, CURRENT_TIMESTAMP()) AS running_since_mins,
    status
FROM job_monitoring_db.monitoring.job_run_log
WHERE end_time IS NULL
  AND start_time IS NOT NULL
  AND DATEDIFF('minute', start_time, CURRENT_TIMESTAMP()) > 60
  AND status = 'RUNNING'
ORDER BY running_since_mins DESC;
```

### Cross-reference with Snowflake native query history

Always corroborate app results against Snowflake's own history to catch orphaned queries:

```sql
SELECT
    query_id,
    query_text,
    user_name,
    warehouse_name,
    warehouse_size,
    start_time,
    DATEDIFF('minute', start_time, CURRENT_TIMESTAMP()) AS elapsed_mins,
    bytes_scanned,
    rows_produced,
    credits_used_cloud_services
FROM snowflake.account_usage.query_history
WHERE execution_status = 'RUNNING'
  AND DATEDIFF('minute', start_time, CURRENT_TIMESTAMP()) > 60
ORDER BY elapsed_mins DESC;
```

Capture `query_id`, `job_name`, `elapsed_mins`, `warehouse_name`, and a truncated `query_text` for the Teams alert.

---

## 3. DMF / Data Quality Failures

```sql
SELECT
    check_name,
    table_name,
    check_type,
    status,
    failed_count,
    checked_at
FROM job_monitoring_db.monitoring.dmf_results
WHERE status = 'FAILED'
  AND checked_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
ORDER BY checked_at DESC;
```

Capture `check_name`, `table_name`, `check_type`, and `failed_count` for the Teams alert.

---

## 4. Streamlit Polling Loop

Wire all three checks into the Streamlit app's background polling thread:

```python
import time
import snowflake.connector

def run_monitoring_loop(conn, interval_seconds: int = 300):
    """Poll every 5 minutes. Call from a background thread in the Streamlit app."""
    while True:
        failed_jobs    = query_failed_jobs(conn)
        long_running   = query_long_running(conn)
        dmf_failures   = query_dmf_failures(conn)

        for job in failed_jobs:
            handle_issue("FAILED_JOB", job, conn)

        for query in long_running:
            handle_issue("LONG_RUNNING", query, conn)

        for check in dmf_failures:
            handle_issue("DMF_FAILURE", check, conn)

        time.sleep(interval_seconds)
```

`handle_issue` should: compute cost (see [COST.md](COST.md)), then post a Teams alert (see [ALERTS.md](ALERTS.md)).

---

## 5. Diagnosis Checklist

Before drafting a plan for any detected issue, answer these questions internally:

- [ ] What is the query doing? (full scan, join, aggregation, DML, recursive CTE?)
- [ ] What warehouse size is it running on?
- [ ] Is there a clustering key on the target table?
- [ ] Are there filters that enable micro-partition pruning?
- [ ] Is there a cartesian join or a missing join predicate?
- [ ] Are CTEs being evaluated multiple times unnecessarily?
- [ ] Is result caching disabled or bypassed?
- [ ] Is the warehouse auto-suspended correctly? Is concurrency causing queue buildup?

Record answers before opening [PLAN.md](PLAN.md).
