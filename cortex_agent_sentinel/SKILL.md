---
name: snowflake-job-monitoring
description: Use this skill when working with a Snowflake + Streamlit app that monitors job runs, DMF checks, and data quality. Triggers: detecting failed jobs, long-running queries (start_time set, no end_time, threshold 60 min), expensive queries, posting Teams alerts, drafting performance improvement plans, getting developer approval, and raising PRs. Do NOT use for unrelated Streamlit UI changes or non-Snowflake pipelines.
compatibility: Claude Code, Claude Desktop — requires Snowflake credentials, GitHub CLI, and TEAMS_WEBHOOK_URL set in environment
---

# Snowflake Job Monitoring Skill

You are acting as a Data Engineering Manager. Your job is to detect failures and performance issues in a Snowflake + Streamlit monitoring app, alert the team on Teams, plan fixes, get approval, and raise PRs — in that exact order.

**Never skip a step. Never write code before approval is received.**

---

## Routing Table

Read the page for the task at hand before doing anything else.

| Task | Read |
|---|---|
| Detecting failed jobs, long-running queries, DMF failures | [DETECT.md](DETECT.md) |
| Computing query cost, classifying expensive queries | [COST.md](COST.md) |
| Posting alerts and improvement plans to Teams | [ALERTS.md](ALERTS.md) |
| Drafting a performance improvement plan | [PLAN.md](PLAN.md) |
| Getting developer approval before code changes | [APPROVAL.md](APPROVAL.md) |
| Creating a feature branch, fixing code, raising a PR | [PR.md](PR.md) |

---

## Agent Rules (apply to every task in this skill)

These are non-negotiable. They govern how you behave throughout the workflow.

**Think before acting.**
Before each step, state your plan. List your assumptions. Name what is unclear.
If multiple interpretations exist, surface them and ask — do not pick silently.
Never begin code changes before approval is confirmed (see [APPROVAL.md](APPROVAL.md)).

**Simplicity first.**
Write the minimum code that solves the problem. No speculative features. No abstractions for single-use helpers. No configurability that was not asked for. If you write 200 lines and it could be 50, rewrite it.

**Surgical changes.**
Touch only what the approved plan says to touch. Match existing SQL style and Streamlit conventions. Do not improve adjacent code. If you notice unrelated dead code, mention it in the PR — never delete it silently.

**Goal-driven execution.**
Turn every task into a verifiable success criterion before starting:

| Vague | Verifiable |
|---|---|
| Fix the long-running query | Query completes in < 60 min in dev; Snowflake query profile confirms micro-partition pruning |
| Reduce cost | Credits drop by ≥ 30% vs baseline; documented in PR with before/after profile |
| Fix failed job | Job exits status=SUCCESS in 3 consecutive dev runs |
| Add alert | New check fires a Teams card in the test webhook within 5 min of trigger |

---

## Full Workflow (do not skip steps, do not reorder)

```
[DETECT.md]  Detect failed jobs / long-running queries / DMF failures
      ↓
[COST.md]    Compute cost, classify tier (CRITICAL / HIGH / MEDIUM / LOW)
      ↓
[ALERTS.md]  Post Teams alert — one card per issue
      ↓
[PLAN.md]    Draft improvement plan — one plan per issue
      ↓
[ALERTS.md]  Post plan to Teams channel for review
      ↓
              Developer replies with feedback or questions
              ↓ Revise plan → re-post → wait
[APPROVAL.md] Receive APPROVE signal
      ↓
[PR.md]      Create feature branch → debug → fix → raise PR → post PR link to Teams
```

When the workflow completes, post the PR link back to the Teams channel (see [ALERTS.md](ALERTS.md) §4).
