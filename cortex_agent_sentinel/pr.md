# PR.md — Feature Branch, Code Fix & Pull Request

Only open this file after receiving `APPROVE` from [APPROVAL.md](APPROVAL.md).
Record the approver name and timestamp before creating a single file.

---

## 1. Create the Feature Branch

```bash
git checkout main
git pull origin main

# Branch naming:
#   fix/   → job failures and error fixes
#   perf/  → long-running query improvements
#   cost/  → cost optimisation

git checkout -b <prefix>/<job-name>-<YYYYMMDD>

# Examples:
#   fix/failed-job-etl-customer-20260608
#   perf/long-running-fact-sales-20260608
#   cost/expensive-query-dim-product-20260608
```

---

## 2. Debug Loop

Work through every step in order. Do not skip any. Do not move to the next step until the current one passes.

```
Step 1 — Reproduce
  Run the query in the Snowflake dev environment.
  Confirm: same failure or slowness observed as described in the plan.

Step 2 — Apply the minimum fix from the approved plan
  Touch only the files listed in the plan's "Files / Objects to Modify" section.
  Run EXPLAIN or open the Snowflake query profile.
  Confirm: measurable improvement visible (micro-partition pruning, lower scan, etc.)

Step 3 — Check git diff
  Every changed line must trace to the approved plan.
  Confirm: no adjacent code, comments, imports, or formatting was touched.

Step 4 — Run existing Streamlit tests and DMF assertions
  Confirm: all tests pass before and after your change.

Step 5 — Write a regression test if needed
  If no test existed for this failure path, write it first, then make it pass.
  Confirm: test is green with fix applied.
```

If any step fails unexpectedly, stop. Post a short update to the Teams thread before proceeding.
Do not silently work around a failure — surface it.

---

## 3. Surgical Change Rules

- Do not improve adjacent queries, comments, or formatting.
- Do not refactor unrelated Streamlit pages or helper functions.
- Match existing SQL style: casing, alias conventions, CTE naming.
- If you notice unrelated dead code, mention it in the PR description — never delete it silently.
- Remove only imports / variables / functions that **your changes** made unused.
- The test: every changed line in `git diff` should trace directly to the approved plan.

---

## 4. Commit Message Format

```
<type>(<scope>): <short summary>

- <what changed>
- Before: <X mins / $Y cost>  →  After: <X mins / $Y cost>
- No changes to DMF checks or Streamlit UI logic

Fixes: <JIRA ticket or issue number>
Approved by: @<developer-name> (Teams, <ISO 8601 timestamp>)
```

Types: `fix` for failures, `perf` for performance, `cost` for cost, `test` for test-only changes.

Example:

```
perf(fact-sales): add date filter to reduce full-table scan

- Added WHERE start_date >= DATEADD('day', -90, CURRENT_DATE) to job_monitor.sql
- Before: 95 min avg / $42 estimated  →  After: 18 min avg / $9 estimated
- No changes to DMF checks or Streamlit UI logic

Fixes: DE-1234
Approved by: @priya-sharma (Teams, 2026-06-08T14:32:00+05:30)
```

---

## 5. Raise the Pull Request

```bash
git add <only the files changed by your fix>
git commit -m "<commit message from §4>"
git push origin <branch-name>

gh pr create \
  --title "<type>: <job-name> — <short description>" \
  --body "$(cat pr_body.md)"
```

Write `pr_body.md` using this template before running the command above:

```markdown
## What
[One-line summary of the change]

## Why
[Link to Teams alert thread and improvement plan]

## How
[Specific lines changed and why — reference the approved plan]

## Evidence
| Metric | Before | After |
|---|---|---|
| Avg duration | X mins | Y mins |
| Estimated cost | $X | $Y |
| DMF checks | — | PASS |
| Streamlit tests | — | PASS |

## Approval
Approved by [Developer Name] on [date] via Teams.
Plan version: [FINAL / v1 / v2]

## Notes
[Any unrelated dead code noticed — do not fix here, just call it out]
```

---

## 6. Post PR Link to Teams

After `gh pr create` succeeds, call `post_pr_link` from [ALERTS.md](ALERTS.md) §4 immediately.

```python
post_pr_link(
    pr_url=pr_url,              # URL returned by gh pr create
    job_name=job_name,
    branch_name=branch_name,
    change_summary="<one-line summary matching the PR title>"
)
```

This closes the loop for the developer — they receive the PR link in the same Teams channel where the alert and plan were posted.

---

## 7. After the PR is Raised

- Do not merge the PR yourself unless you have explicit permission.
- Respond to review comments in the PR — do not go back to Teams for code-level discussions.
- If the reviewer requests changes, make them on the same branch and push again.
- When the PR is merged, confirm the fix in production by re-running the detection queries from [DETECT.md](DETECT.md).
- Post a final resolution message to the Teams channel:

```python
post_teams_card(
    title="✅ Issue Resolved",
    subtitle=f"Job: `{job_name}` | PR merged",
    theme_color="2E7D32",
    facts=[
        {"name": "PR",        "value": pr_url},
        {"name": "Merged At", "value": "<timestamp>"},
        {"name": "Verified",  "value": "Detection query returns 0 rows in production"},
    ]
)
```
