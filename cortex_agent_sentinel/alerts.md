# ALERTS.md — Teams Alerting

Post a separate Teams card for each issue. Never batch unrelated issues into one card.
The webhook URL must be set as `TEAMS_WEBHOOK_URL` in the environment.

---

## 1. Base Alert Function

All alert types use this base. Call it with the right `theme_color` and `facts` per §2.

```python
import requests, json, os

def post_teams_card(
    title: str,
    subtitle: str,
    facts: list[dict],
    theme_color: str,
    action_buttons: list[dict] | None = None
):
    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": theme_color,
        "summary": title,
        "sections": [{
            "activityTitle": f"**{title}**",
            "activitySubtitle": subtitle,
            "facts": facts,
            "markdown": True
        }],
        "potentialAction": action_buttons or [{
            "@type": "OpenUri",
            "name": "Open Streamlit Dashboard",
            "targets": [{"os": "default", "uri": os.environ["STREAMLIT_APP_URL"]}]
        }]
    }
    requests.post(
        os.environ["TEAMS_WEBHOOK_URL"],
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"},
        timeout=10
    )
```

---

## 2. Alert Types

### Job Failure

```python
def alert_job_failure(job_name, job_id, error_message, start_time, warehouse, cost_tier):
    post_teams_card(
        title="🔴 Job Failure Detected",
        subtitle=f"Job: `{job_name}` | ID: `{job_id}`",
        theme_color="FF0000",
        facts=[
            {"name": "Status",     "value": "FAILED"},
            {"name": "Start Time", "value": str(start_time)},
            {"name": "Warehouse",  "value": warehouse},
            {"name": "Cost Tier",  "value": cost_tier},
            {"name": "Error",      "value": str(error_message)[:300]},
        ]
    )
```

### Long-Running Query

```python
def alert_long_running(job_name, query_id, elapsed_mins, warehouse, query_text, cost_tier, estimated_cost_usd):
    post_teams_card(
        title="🟡 Long-Running Query Alert",
        subtitle=f"Job: `{job_name}` | Query: `{query_id}`",
        theme_color="FFA500",
        facts=[
            {"name": "Elapsed Time",    "value": f"{elapsed_mins} minutes"},
            {"name": "Threshold",       "value": "60 minutes"},
            {"name": "Warehouse",       "value": warehouse},
            {"name": "Cost Tier",       "value": cost_tier},
            {"name": "Estimated Cost",  "value": f"${estimated_cost_usd}"},
            {"name": "Query (preview)", "value": str(query_text)[:300]},
        ]
    )
```

### DMF / Data Quality Failure

```python
def alert_dmf_failure(check_name, table_name, check_type, failed_count, checked_at):
    post_teams_card(
        title="🟠 Data Quality Check Failed",
        subtitle=f"Check: `{check_name}` on `{table_name}`",
        theme_color="FF6600",
        facts=[
            {"name": "Check Type",   "value": check_type},
            {"name": "Failed Count", "value": str(failed_count)},
            {"name": "Detected At",  "value": str(checked_at)},
        ]
    )
```

### Expensive Query Identified

```python
def alert_expensive_query(job_name, query_id, estimated_cost_usd, duration_mins, warehouse, cost_tier):
    post_teams_card(
        title="🔵 Expensive Query Identified",
        subtitle=f"Job: `{job_name}` | Query: `{query_id}`",
        theme_color="1565C0",
        facts=[
            {"name": "Estimated Cost", "value": f"${estimated_cost_usd}"},
            {"name": "Duration",       "value": f"{duration_mins} mins"},
            {"name": "Warehouse",      "value": warehouse},
            {"name": "Cost Tier",      "value": cost_tier},
        ]
    )
```

---

## 3. Posting an Improvement Plan for Approval

Post a separate plan card for each issue. Include Approve and Request Changes buttons.
Pass the `approval_endpoint` URL from your approval service (see [APPROVAL.md](APPROVAL.md)).

```python
def post_plan_for_approval(plan_text: str, job_name: str, query_id: str, approval_endpoint: str):
    post_teams_card(
        title="🟢 Improvement Plan — Action Required",
        subtitle=f"Job: `{job_name}` | Query: `{query_id}`",
        theme_color="00897B",
        facts=[
            {"name": "Status", "value": "Awaiting developer approval"},
        ],
        action_buttons=[
            {
                "@type": "HttpPOST",
                "name": "✅ Approve",
                "target": approval_endpoint,
                "body": json.dumps({"action": "APPROVE", "query_id": query_id})
            },
            {
                "@type": "HttpPOST",
                "name": "❌ Request Changes",
                "target": approval_endpoint,
                "body": json.dumps({"action": "REQUEST_CHANGES", "query_id": query_id})
            }
        ]
    )
    # Post plan text as a follow-up card (Teams cards have a 1500-char body limit)
    post_teams_card(
        title=f"📋 Plan Details — {job_name}",
        subtitle="Read below and use the buttons above to approve or request changes",
        theme_color="00897B",
        facts=[{"name": "Plan", "value": f"```\n{plan_text[:1400]}\n```"}]
    )
```

When the developer requests changes, revise the plan and re-post with `[FINAL]` in the title before requesting approval again.

---

## 4. Posting a PR Link After Merge

Call this immediately after `gh pr create` succeeds in [PR.md](PR.md).

```python
def post_pr_link(pr_url: str, job_name: str, branch_name: str, change_summary: str):
    post_teams_card(
        title="🔀 Pull Request Raised",
        subtitle=f"Job: `{job_name}` | Branch: `{branch_name}`",
        theme_color="1976D2",
        facts=[
            {"name": "Summary", "value": change_summary},
            {"name": "PR URL",  "value": pr_url},
        ],
        action_buttons=[{
            "@type": "OpenUri",
            "name": "Review PR",
            "targets": [{"os": "default", "uri": pr_url}]
        }]
    )
```

---

## 5. Alert Priority Reference

| Issue | theme_color | Emoji | Auto-escalate? |
|---|---|---|---|
| Job failure | `FF0000` | 🔴 | Yes — if prod environment |
| Long-running > 60 min | `FFA500` | 🟡 | No |
| DMF check failure | `FF6600` | 🟠 | No |
| Expensive query | `1565C0` | 🔵 | No |
| Plan ready for approval | `00897B` | 🟢 | No |
| PR raised | `1976D2` | 🔀 | No |
