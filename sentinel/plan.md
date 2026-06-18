# Self-Healing Pipeline Agent — Implementation Roadmap

**Project:** ELT_STAGEDB.LOGGING  
**Owner:** Data Engineering Team  
**Workspace:** `USER$.PUBLIC."nandini_snowflake_code"`  
**Notebook:** `Cortext_agent_task.ipynb`  
**Date:** 2026-06-17  

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        SNOWFLAKE — ELT_STAGEDB.LOGGING                              │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  ┌─ TASK 1: TASK_DETECT_FAILURES (every 60 min) ──────────────────────────────┐    │
│  │  CALL SP_DETECT_AND_ALERT()                                                │    │
│  │  1. Query JOB_CONTROL_LOG + LOG_STEPS for failures/long-running            │    │
│  │  2. INSERT into AGENT_ALERTS                                               │    │
│  │  3. Call DATA_AGENT_RUN() → Agent reasons over failures                    │    │
│  │  4. Agent generates fix proposals → INSERT into AGENT_FIX_PROPOSALS        │    │
│  │  5. Call SP_SEND_TEAMS_ALERT → immediate alert: "X jobs failed"            │    │
│  └────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                     │
│  ┌─ TASK 2: TASK_POLL_APPROVALS (every 2 hrs) ────────────────────────────────┐    │
│  │  CALL SP_POLL_PENDING_PROPOSALS()                                          │    │
│  │  1. SELECT * FROM AGENT_FIX_PROPOSALS WHERE STATUS = 'PENDING'             │    │
│  │  2. Bundle all pending proposals into structured Teams message             │    │
│  │  3. Send batch summary with [Approve] [Reject] per proposal               │    │
│  └────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                     │
│       │ Human approves via Teams / Streamlit / SQL                                  │
│       │ UPDATE AGENT_FIX_PROPOSALS SET STATUS='APPROVED'                            │
│       ▼                                                                             │
│  ┌─ TASK 3: TASK_EXECUTE_FIXES (every 30 min) ────────────────────────────────┐    │
│  │  CALL SP_EXECUTE_APPROVED_FIXES()                                          │    │
│  │  1. SELECT approved proposals WHERE PR_URL IS NULL                         │    │
│  │  2. For each: Call DATA_AGENT_RUN() → Agent creates bug + PR              │    │
│  │     a. Agent calls SP_CREATE_AZURE_DEVOPS_BUG → Bug Work Item             │    │
│  │     b. Agent calls SP_GIT_CREATE_PR → Branch + Commit + PR                │    │
│  │  3. UPDATE proposal: STATUS='APPLIED', PR_URL, BUG_ID                     │    │
│  │  4. Send Teams notification: "PR #X created for Bug #Y"                   │    │
│  └────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                     │
│  ┌─ CORTEX AGENT: JOB_MONITOR_AGENT ─────────────────────────────────────────┐    │
│  │  Tools:                                                                    │    │
│  │    • QueryJobs (SP_QUERY_JOBS) — fetch failure data                       │    │
│  │    • SendAlert (SP_SEND_TEAMS_ALERT) — post to Teams                      │    │
│  │    • CreateBug (SP_CREATE_AZURE_DEVOPS_BUG) — Azure DevOps bug            │    │
│  │    • GitCreatePR (SP_GIT_CREATE_PR) — create branch + PR                  │    │
│  │    • code_execution — analyze errors, generate fix code                   │    │
│  └────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
         │                              │
         ▼                              ▼
┌─────────────────┐          ┌──────────────────────┐
│  Microsoft Teams │          │  Azure DevOps        │
│  (Webhook)       │          │  • Work Items (Bugs) │
│                  │          │  • Repos (Git/PR)    │
└─────────────────┘          └──────────────────────┘
```

---

## 2. Data Flow — How Data Gets Into AGENT_FIX_PROPOSALS

**Source tables (already exist):**
- `ELT_STAGEDB.LOGGING.JOB_CONTROL_LOG` — job execution records
- `ELT_STAGEDB.LOGGING.LOG_STEPS` — step-level logs with ERROR_MESSAGE
- `ELT_STAGEDB.LOGGING.DATA_AUDIT_LOG` — reconciliation gaps

**Two-Table Design (Drafts vs Approved):**

| Table | Purpose | Lifecycle |
|-------|---------|----------|
| `AGENT_SUGGESTIONS` | Working drafts — agent writes here, human can ask to revise | Overwritten on each revision. Deleted after approval/rejection. |
| `AGENT_FIX_PROPOSALS` | Final approved plans ONLY — triggers bug + PR creation | Immutable once inserted. Only written when human approves. |

**Flow:**
1. `TASK_DETECT_FAILURES` fires every 60 min
2. SP queries source tables → finds failures with error messages
3. SP calls `SNOWFLAKE.CORTEX.DATA_AGENT_RUN('ELT_STAGEDB.LOGGING.JOB_MONITOR_AGENT', ...)`
4. Agent reasons over the error data using `code_execution` tool
5. Agent calls its `QueryJobs` tool to get detailed error context
6. Agent generates a fix suggestion → **UPSERTS into `AGENT_SUGGESTIONS`** (not proposals)
7. Suggestion sits with `STATUS = 'DRAFT'` — sent to Teams for review
8. **If human asks to change the plan:** Agent regenerates → **UPDATES the same row** in `AGENT_SUGGESTIONS` (revision_count increments)
9. **If human approves:** Row is MOVED from `AGENT_SUGGESTIONS` → INSERT into `AGENT_FIX_PROPOSALS`, then DELETE from suggestions
10. **If human rejects:** Row is DELETED from `AGENT_SUGGESTIONS` (no trace in proposals)

**Key principle:** `AGENT_FIX_PROPOSALS` only ever contains approved, final plans that will be executed.

---

## 3. Table DDL

### 3.1 AGENT_ALERTS

```sql
CREATE TABLE IF NOT EXISTS ELT_STAGEDB.LOGGING.AGENT_ALERTS (
    ALERT_ID          NUMBER AUTOINCREMENT PRIMARY KEY,
    ALERT_TIME        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ALERT_TYPE        VARCHAR(50),      -- 'JOB_FAILURE', 'LONG_RUNNING', 'DATA_QUALITY'
    SEVERITY          VARCHAR(20),      -- 'CRITICAL', 'WARNING', 'INFO'
    JOB_NAMES         VARCHAR(4000),    -- comma-separated list of affected jobs
    FAILURE_COUNT     NUMBER,
    LONG_RUNNING_COUNT NUMBER,
    MESSAGE           VARCHAR(4000),
    TEAMS_SENT        BOOLEAN DEFAULT FALSE,
    AGENT_INVOKED     BOOLEAN DEFAULT FALSE,
    AGENT_RESPONSE    VARIANT,          -- full agent JSON response
    CREATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### 3.2 AGENT_SUGGESTIONS (Working Drafts — Revisions Happen Here)

```sql
CREATE TABLE IF NOT EXISTS ELT_STAGEDB.LOGGING.AGENT_SUGGESTIONS (
    SUGGESTION_ID     NUMBER AUTOINCREMENT PRIMARY KEY,
    ALERT_ID          NUMBER,           -- FK to AGENT_ALERTS
    CREATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    JOB_NAME          VARCHAR(500),
    EXECUTION_ID      VARCHAR(200),
    FAILURE_REASON    VARCHAR(4000),     -- error message from LOG_STEPS
    ROOT_CAUSE        VARCHAR(4000),     -- agent's root cause analysis
    PROPOSED_FIX      VARCHAR(4000),     -- human-readable fix description
    FIX_FILE_PATH     VARCHAR(1000),     -- path in repo to fix
    FIX_CODE          TEXT,              -- actual code fix generated by agent
    STATUS            VARCHAR(20) DEFAULT 'DRAFT',
                                        -- DRAFT | REVISION_REQUESTED | APPROVED | REJECTED
    REVISION_COUNT    NUMBER DEFAULT 0,  -- how many times human asked to revise
    REVISION_NOTES    VARCHAR(4000),     -- human's feedback for revision
    AGENT_THREAD_ID   NUMBER            -- for multi-turn agent conversations
);
```

### 3.3 AGENT_FIX_PROPOSALS (Approved Plans ONLY — Immutable)

```sql
CREATE TABLE IF NOT EXISTS ELT_STAGEDB.LOGGING.AGENT_FIX_PROPOSALS (
    PROPOSAL_ID       NUMBER AUTOINCREMENT PRIMARY KEY,
    SUGGESTION_ID     NUMBER,           -- FK to the original suggestion
    ALERT_ID          NUMBER,           -- FK to AGENT_ALERTS
    APPROVED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    APPROVED_BY       VARCHAR(200),
    JOB_NAME          VARCHAR(500),
    EXECUTION_ID      VARCHAR(200),
    FAILURE_REASON    VARCHAR(4000),
    ROOT_CAUSE        VARCHAR(4000),
    PROPOSED_FIX      VARCHAR(4000),     -- the FINAL approved fix description
    FIX_FILE_PATH     VARCHAR(1000),
    FIX_CODE          TEXT,              -- the FINAL approved code
    REVISION_COUNT    NUMBER,            -- how many revisions before approval
    STATUS            VARCHAR(20) DEFAULT 'APPROVED',
                                        -- APPROVED | APPLIED | FAILED
    BUG_ID            VARCHAR(100),      -- Azure DevOps Work Item ID
    BUG_URL           VARCHAR(1000),
    PR_ID             VARCHAR(100),      -- Pull Request ID
    PR_URL            VARCHAR(1000),
    APPLIED_AT        TIMESTAMP_NTZ
);
```

**Lifecycle:**
```
Agent drafts fix → AGENT_SUGGESTIONS (STATUS=DRAFT)
       │
       ├─ Human says "change X" → Agent UPDATES same row (REVISION_COUNT++, STATUS=DRAFT)
       ├─ Human says "change Y" → Agent UPDATES same row again
       │
       ├─ Human says "approve"  → INSERT into AGENT_FIX_PROPOSALS, DELETE from SUGGESTIONS
       │
       └─ Human says "reject"   → DELETE from AGENT_SUGGESTIONS (nothing saved)
```

---

## 4. Stored Procedures

### 4.1 SP_QUERY_JOBS — Agent Tool

**Purpose:** Returns job failure data as JSON for the agent to reason over.

```sql
CREATE OR REPLACE PROCEDURE ELT_STAGEDB.LOGGING.SP_QUERY_JOBS(
    QUERY_TYPE    VARCHAR,    -- 'FAILURES' | 'LONG_RUNNING' | 'DATA_QUALITY' | 'ALL'
    HOURS_BACK    NUMBER DEFAULT 1
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    result VARCHAR;
BEGIN
    CASE UPPER(QUERY_TYPE)
        WHEN 'FAILURES' THEN
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
                'execution_id', jc.EXECUTION_ID,
                'job_name', jc.JOB_NAME,
                'tool_source', jc.TOOL_SOURCE,
                'job_type', jc.JOB_TYPE,
                'start_time', jc.START_TIME::VARCHAR,
                'end_time', jc.END_TIME::VARCHAR,
                'duration_minutes', TIMEDIFF(MINUTE, jc.START_TIME, jc.END_TIME),
                'error_messages', (
                    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
                        'step_name', ls.STEP_NAME,
                        'entity_name', ls.ENTITY_NAME,
                        'error_message', ls.ERROR_MESSAGE,
                        'log_details', ls.LOG_DETAILS
                    ))
                    FROM ELT_STAGEDB.LOGGING.LOG_STEPS ls
                    WHERE ls.EXECUTION_ID = jc.EXECUTION_ID
                      AND ls.STATUS = 'FAILED'
                )
            ))::VARCHAR INTO :result
            FROM ELT_STAGEDB.LOGGING.JOB_CONTROL_LOG jc
            WHERE jc.STATUS IN ('Failed', 'FAILED')
              AND jc.START_TIME >= DATEADD(HOUR, -:HOURS_BACK, CURRENT_TIMESTAMP());

        WHEN 'LONG_RUNNING' THEN
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
                'execution_id', jc.EXECUTION_ID,
                'job_name', jc.JOB_NAME,
                'duration_minutes', TIMEDIFF(MINUTE, jc.START_TIME, CURRENT_TIMESTAMP()),
                'start_time', jc.START_TIME::VARCHAR
            ))::VARCHAR INTO :result
            FROM ELT_STAGEDB.LOGGING.JOB_CONTROL_LOG jc
            WHERE jc.STATUS NOT IN ('Failed', 'FAILED', 'SUCCESS', 'SUCCEEDED', 'Succeeded')
              AND TIMEDIFF(MINUTE, jc.START_TIME, CURRENT_TIMESTAMP()) > 60;

        WHEN 'DATA_QUALITY' THEN
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
                'audit_id', d.AUDIT_ID,
                'execution_id', d.EXECUTION_ID,
                'entity_name', d.ENTITY_NAME,
                'source_row_count', d.SOURCE_ROW_COUNT,
                'target_total_count', d.TARGET_TOTAL_COUNT,
                'reconciliation_gap', d.RECONCILIATION_GAP
            ))::VARCHAR INTO :result
            FROM ELT_STAGEDB.LOGGING.DATA_AUDIT_LOG d
            WHERE d.RECONCILIATION_GAP > 0
              AND d.AUDIT_TIMESTAMP >= DATEADD(HOUR, -:HOURS_BACK, CURRENT_TIMESTAMP());

        ELSE
            -- Return all types combined
            CALL ELT_STAGEDB.LOGGING.SP_QUERY_JOBS('FAILURES', :HOURS_BACK);
    END CASE;

    RETURN COALESCE(:result, '[]');
END;
$$;
```

---

### 4.2 SP_SEND_TEAMS_ALERT — Agent Tool

**Purpose:** Sends a message to Microsoft Teams via webhook.

```sql
CREATE OR REPLACE PROCEDURE ELT_STAGEDB.LOGGING.SP_SEND_TEAMS_ALERT(
    MESSAGE    VARCHAR,
    SEVERITY   VARCHAR DEFAULT 'INFO'    -- 'CRITICAL' | 'WARNING' | 'INFO'
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests', 'snowflake-snowpark-python')
HANDLER = 'send_alert'
EXTERNAL_ACCESS_INTEGRATIONS = (TEAMS_INTEGRATION)
SECRETS = ('teams_webhook' = DEV_STREAMLIT_APPS.PUBLIC.TEAMS_WEBHOOK_SECRET)
AS
$$
import _snowflake
import requests
import json

def send_alert(session, message, severity):
    webhook_url = _snowflake.get_generic_secret_string('teams_webhook')

    color_map = {'CRITICAL': 'FF0000', 'WARNING': 'FFA500', 'INFO': '0078D4'}
    color = color_map.get(severity.upper(), '0078D4')

    payload = {
        "@type": "MessageCard",
        "themeColor": color,
        "title": f"Pipeline Alert [{severity.upper()}]",
        "text": message,
        "sections": [{
            "activityTitle": "Self-Healing Pipeline Agent",
            "activitySubtitle": "ELT_STAGEDB.LOGGING"
        }]
    }

    response = requests.post(
        webhook_url,
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=10
    )

    return f"Status: {response.status_code}"
$$;
```

---

### 4.3 SP_DETECT_AND_ALERT — Task 1 Body

**Purpose:** Main detection loop. Queries failures, invokes agent, stores proposals.

```sql
CREATE OR REPLACE PROCEDURE ELT_STAGEDB.LOGGING.SP_DETECT_AND_ALERT()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    failure_count INTEGER;
    long_running_count INTEGER;
    failure_data VARCHAR;
    alert_msg VARCHAR;
    alert_id NUMBER;
    agent_response VARCHAR;
BEGIN
    -- 1. Count failures in last 60 minutes
    SELECT COUNT(*) INTO :failure_count
    FROM ELT_STAGEDB.LOGGING.JOB_CONTROL_LOG
    WHERE STATUS IN ('Failed', 'FAILED')
      AND START_TIME >= DATEADD(MINUTE, -60, CURRENT_TIMESTAMP());

    -- 2. Count long-running jobs (threshold: 60 minutes)
    SELECT COUNT(*) INTO :long_running_count
    FROM ELT_STAGEDB.LOGGING.JOB_CONTROL_LOG
    WHERE STATUS NOT IN ('Failed', 'FAILED', 'SUCCESS', 'SUCCEEDED', 'Succeeded')
      AND TIMEDIFF(MINUTE, START_TIME, CURRENT_TIMESTAMP()) > 60;

    -- 3. If issues found, proceed
    IF (:failure_count > 0 OR :long_running_count > 0) THEN

        -- 4. Get detailed failure data
        CALL ELT_STAGEDB.LOGGING.SP_QUERY_JOBS('FAILURES', 1);
        failure_data := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));

        -- 5. Log the alert
        alert_msg := :failure_count || ' failed jobs, ' || :long_running_count || ' long-running jobs detected';

        INSERT INTO ELT_STAGEDB.LOGGING.AGENT_ALERTS (
            ALERT_TYPE, SEVERITY, FAILURE_COUNT, LONG_RUNNING_COUNT, MESSAGE
        ) VALUES (
            'JOB_FAILURE',
            CASE WHEN :failure_count > 3 THEN 'CRITICAL' ELSE 'WARNING' END,
            :failure_count,
            :long_running_count,
            :alert_msg
        );

        alert_id := (SELECT MAX(ALERT_ID) FROM ELT_STAGEDB.LOGGING.AGENT_ALERTS);

        -- 6. Send immediate Teams alert
        CALL ELT_STAGEDB.LOGGING.SP_SEND_TEAMS_ALERT(:alert_msg,
            CASE WHEN :failure_count > 3 THEN 'CRITICAL' ELSE 'WARNING' END);

        -- 7. Invoke the agent to analyze failures and generate fix proposals
        agent_response := SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
            'ELT_STAGEDB.LOGGING.JOB_MONITOR_AGENT',
            $${
                "messages": [{
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": "Analyze these job failures and generate fix proposals for each. Return structured JSON with job_name, failure_reason, root_cause, proposed_fix, fix_file_path, and fix_code for each failure: $$ || :failure_data || $$"
                    }]
                }]
            }$$,
            TRUE
        );

        -- 8. Update alert with agent response
        UPDATE ELT_STAGEDB.LOGGING.AGENT_ALERTS
        SET AGENT_INVOKED = TRUE,
            AGENT_RESPONSE = TRY_PARSE_JSON(:agent_response),
            TEAMS_SENT = TRUE
        WHERE ALERT_ID = :alert_id;

        -- 9. Agent inserts drafts into AGENT_SUGGESTIONS (not AGENT_FIX_PROPOSALS)
        -- The agent is instructed to UPSERT into AGENT_SUGGESTIONS with STATUS='DRAFT'
        -- AGENT_FIX_PROPOSALS only gets populated when human approves

    END IF;

    RETURN COALESCE(:alert_msg, 'No issues detected');
END;
$$;
```

---

### 4.4 SP_POLL_PENDING_SUGGESTIONS — Task 2 Body

**Purpose:** Every 2 hours, collect all DRAFT suggestions and send batch to Teams for approval.

```sql
CREATE OR REPLACE PROCEDURE ELT_STAGEDB.LOGGING.SP_POLL_PENDING_SUGGESTIONS()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests', 'snowflake-snowpark-python')
HANDLER = 'poll_suggestions'
EXTERNAL_ACCESS_INTEGRATIONS = (TEAMS_INTEGRATION)
SECRETS = ('teams_webhook' = DEV_STREAMLIT_APPS.PUBLIC.TEAMS_WEBHOOK_SECRET)
AS
$$
import _snowflake
import requests
import json

def poll_suggestions(session):
    df = session.sql("""
        SELECT SUGGESTION_ID, JOB_NAME, FAILURE_REASON, ROOT_CAUSE, PROPOSED_FIX, 
               REVISION_COUNT, CREATED_AT, UPDATED_AT
        FROM ELT_STAGEDB.LOGGING.AGENT_SUGGESTIONS
        WHERE STATUS = 'DRAFT'
        ORDER BY UPDATED_AT DESC
    """).to_pandas()

    if df.empty:
        return "No pending suggestions"

    webhook_url = _snowflake.get_generic_secret_string('teams_webhook')

    # Build summary message
    sections = []
    for _, row in df.iterrows():
        revision_note = f" (Revised {row['REVISION_COUNT']}x)" if row['REVISION_COUNT'] > 0 else ""
        sections.append({
            "activityTitle": f"Suggestion #{row['SUGGESTION_ID']} — {row['JOB_NAME']}{revision_note}",
            "facts": [
                {"name": "Failure", "value": str(row['FAILURE_REASON'])[:200]},
                {"name": "Root Cause", "value": str(row['ROOT_CAUSE'])[:200]},
                {"name": "Proposed Fix", "value": str(row['PROPOSED_FIX'])[:200]},
                {"name": "Last Updated", "value": str(row['UPDATED_AT'])}
            ]
        })

    payload = {
        "@type": "MessageCard",
        "themeColor": "FFA500",
        "title": f"Pipeline Agent: {len(df)} Suggestions Awaiting Approval",
        "summary": f"{len(df)} suggestions need review",
        "sections": sections,
        "potentialAction": [{
            "@type": "OpenUri",
            "name": "Open Streamlit Dashboard",
            "targets": [{"os": "default", "uri": "https://app.snowflake.com"}]
        }]
    }

    response = requests.post(
        webhook_url,
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=10
    )

    return f"Sent {len(df)} suggestions to Teams. Status: {response.status_code}"
$$;
```

---

### 4.5 SP_EXECUTE_APPROVED_FIXES — Task 3 Body

**Purpose:** Pick up approved proposals, invoke agent to create bug and PR.

```sql
CREATE OR REPLACE PROCEDURE ELT_STAGEDB.LOGGING.SP_EXECUTE_APPROVED_FIXES()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    proposal_cursor CURSOR FOR
        SELECT PROPOSAL_ID, JOB_NAME, PROPOSED_FIX, FIX_FILE_PATH, FIX_CODE, ROOT_CAUSE
        FROM ELT_STAGEDB.LOGGING.AGENT_FIX_PROPOSALS
        WHERE STATUS = 'APPROVED'
          AND PR_URL IS NULL;
    v_proposal_id NUMBER;
    v_job_name VARCHAR;
    v_proposed_fix VARCHAR;
    v_fix_file_path VARCHAR;
    v_fix_code TEXT;
    v_root_cause VARCHAR;
    v_agent_response VARCHAR;
    v_processed INTEGER DEFAULT 0;
BEGIN
    OPEN proposal_cursor;

    FOR record IN proposal_cursor DO
        v_proposal_id := record.PROPOSAL_ID;
        v_job_name := record.JOB_NAME;
        v_proposed_fix := record.PROPOSED_FIX;
        v_fix_file_path := record.FIX_FILE_PATH;
        v_fix_code := record.FIX_CODE;
        v_root_cause := record.ROOT_CAUSE;

        -- Invoke agent to create bug + PR
        v_agent_response := SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
            'ELT_STAGEDB.LOGGING.JOB_MONITOR_AGENT',
            $${
                "messages": [{
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": "Proposal #$$ || v_proposal_id::VARCHAR || $$ is APPROVED. Execute the following: 1) Create an Azure DevOps Bug work item titled 'Pipeline Fix: $$ || v_job_name || $$' with description: '$$ || v_root_cause || $$'. 2) Create a git branch 'fix/proposal-$$ || v_proposal_id::VARCHAR || $$', commit the fix to file '$$ || v_fix_file_path || $$', and open a PR. The fix code is: $$ || v_fix_code || $$"
                    }]
                }]
            }$$,
            TRUE
        );

        -- Mark as applied (agent tools will also update, but this is a safety net)
        UPDATE ELT_STAGEDB.LOGGING.AGENT_FIX_PROPOSALS
        SET STATUS = 'APPLIED',
            APPLIED_AT = CURRENT_TIMESTAMP()
        WHERE PROPOSAL_ID = :v_proposal_id
          AND STATUS = 'APPROVED';

        v_processed := v_processed + 1;
    END FOR;

    CLOSE proposal_cursor;
    RETURN v_processed || ' proposals processed';
END;
$$;
```

---

### 4.6 SP_CREATE_AZURE_DEVOPS_BUG — Agent Tool

**Purpose:** Creates a Bug work item in Azure DevOps via REST API.

**Azure DevOps API:** `POST https://dev.azure.com/{org}/{project}/_apis/wit/workitems/$Bug?api-version=7.1`

```sql
CREATE OR REPLACE PROCEDURE ELT_STAGEDB.LOGGING.SP_CREATE_AZURE_DEVOPS_BUG(
    TITLE          VARCHAR,
    DESCRIPTION    VARCHAR,
    SEVERITY       VARCHAR DEFAULT '2 - High',     -- '1 - Critical' | '2 - High' | '3 - Medium'
    ASSIGNED_TO    VARCHAR DEFAULT NULL,
    AREA_PATH      VARCHAR DEFAULT NULL
)
RETURNS VARCHAR    -- Returns JSON: {"bug_id": "12345", "bug_url": "https://..."}
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests', 'snowflake-snowpark-python')
HANDLER = 'create_bug'
EXTERNAL_ACCESS_INTEGRATIONS = (AZURE_DEVOPS_INTEGRATION)
SECRETS = ('ado_pat' = ELT_STAGEDB.LOGGING.AZURE_DEVOPS_PAT_SECRET)
AS
$$
import _snowflake
import requests
import json
import base64

def create_bug(session, title, description, severity, assigned_to, area_path):
    pat = _snowflake.get_generic_secret_string('ado_pat')

    # Azure DevOps config — UPDATE THESE VALUES
    org = "YOUR_ORG"
    project = "YOUR_PROJECT"
    base_url = f"https://dev.azure.com/{org}/{project}/_apis/wit/workitems/$Bug"

    # PAT auth: base64 encode ":PAT"
    auth_string = base64.b64encode(f":{pat}".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth_string}",
        "Content-Type": "application/json-patch+json"
    }

    # Work item fields (JSON Patch format)
    body = [
        {"op": "add", "path": "/fields/System.Title", "value": title},
        {"op": "add", "path": "/fields/System.Description", "value": description},
        {"op": "add", "path": "/fields/Microsoft.VSTS.Common.Severity", "value": severity},
        {"op": "add", "path": "/fields/System.Tags", "value": "auto-generated;pipeline-agent"}
    ]

    if assigned_to:
        body.append({"op": "add", "path": "/fields/System.AssignedTo", "value": assigned_to})
    if area_path:
        body.append({"op": "add", "path": "/fields/System.AreaPath", "value": area_path})

    response = requests.post(
        f"{base_url}?api-version=7.1",
        headers=headers,
        data=json.dumps(body),
        timeout=30
    )

    if response.status_code in (200, 201):
        result = response.json()
        bug_id = str(result.get("id", ""))
        bug_url = result.get("_links", {}).get("html", {}).get("href", "")
        return json.dumps({"bug_id": bug_id, "bug_url": bug_url, "status": "created"})
    else:
        return json.dumps({"error": response.text, "status_code": response.status_code})
$$;
```

---

### 4.7 SP_GIT_CREATE_PR — Agent Tool

**Purpose:** Creates a branch, commits a file, and opens a Pull Request in Azure Repos.

**Azure Repos API:**
- Push: `POST https://dev.azure.com/{org}/{project}/_apis/git/repositories/{repo}/pushes?api-version=7.1`
- PR: `POST https://dev.azure.com/{org}/{project}/_apis/git/repositories/{repo}/pullrequests?api-version=7.1`

```sql
CREATE OR REPLACE PROCEDURE ELT_STAGEDB.LOGGING.SP_GIT_CREATE_PR(
    BRANCH_NAME    VARCHAR,        -- e.g., 'fix/proposal-42'
    FILE_PATH      VARCHAR,        -- e.g., 'pipelines/load_customers.sql'
    FILE_CONTENT   VARCHAR,        -- the fix code
    PR_TITLE       VARCHAR,        -- e.g., 'Pipeline Fix: load_customers timeout'
    PR_DESCRIPTION VARCHAR         -- markdown body
)
RETURNS VARCHAR    -- Returns JSON: {"pr_id": "123", "pr_url": "https://..."}
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests', 'snowflake-snowpark-python')
HANDLER = 'create_pr'
EXTERNAL_ACCESS_INTEGRATIONS = (AZURE_DEVOPS_INTEGRATION)
SECRETS = ('ado_pat' = ELT_STAGEDB.LOGGING.AZURE_DEVOPS_PAT_SECRET)
AS
$$
import _snowflake
import requests
import json
import base64

def create_pr(session, branch_name, file_path, file_content, pr_title, pr_description):
    pat = _snowflake.get_generic_secret_string('ado_pat')

    # Azure Repos config — UPDATE THESE VALUES
    org = "YOUR_ORG"
    project = "YOUR_PROJECT"
    repo = "YOUR_REPO"
    base_branch = "main"
    base_url = f"https://dev.azure.com/{org}/{project}/_apis/git/repositories/{repo}"

    auth_string = base64.b64encode(f":{pat}".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth_string}",
        "Content-Type": "application/json"
    }

    # Step 1: Get the latest commit SHA from main
    refs_url = f"{base_url}/refs?filter=heads/{base_branch}&api-version=7.1"
    refs_resp = requests.get(refs_url, headers=headers, timeout=15)
    refs_data = refs_resp.json()
    old_object_id = refs_data["value"][0]["objectId"]

    # Step 2: Create branch + commit file in one push
    content_b64 = base64.b64encode(file_content.encode()).decode()
    push_body = {
        "refUpdates": [{
            "name": f"refs/heads/{branch_name}",
            "oldObjectId": "0000000000000000000000000000000000000000"
        }],
        "commits": [{
            "comment": pr_title,
            "changes": [{
                "changeType": "add",
                "item": {"path": f"/{file_path}"},
                "newContent": {
                    "content": file_content,
                    "contentType": "rawtext"
                }
            }]
        }]
    }

    # Create branch from main's HEAD
    push_body["refUpdates"][0]["oldObjectId"] = old_object_id

    push_url = f"{base_url}/pushes?api-version=7.1"
    push_resp = requests.post(push_url, headers=headers, json=push_body, timeout=30)

    if push_resp.status_code not in (200, 201):
        return json.dumps({"error": f"Push failed: {push_resp.text}"})

    # Step 3: Create Pull Request
    pr_body = {
        "sourceRefName": f"refs/heads/{branch_name}",
        "targetRefName": f"refs/heads/{base_branch}",
        "title": pr_title,
        "description": pr_description,
        "labels": [{"name": "auto-generated"}, {"name": "pipeline-fix"}]
    }

    pr_url = f"{base_url}/pullrequests?api-version=7.1"
    pr_resp = requests.post(pr_url, headers=headers, json=pr_body, timeout=30)

    if pr_resp.status_code in (200, 201):
        pr_data = pr_resp.json()
        pr_id = str(pr_data.get("pullRequestId", ""))
        pr_web_url = pr_data.get("url", "").replace("_apis/git/repositories", "_git").split("/pullrequests")[0]
        pr_link = f"https://dev.azure.com/{org}/{project}/_git/{repo}/pullrequest/{pr_id}"
        return json.dumps({"pr_id": pr_id, "pr_url": pr_link, "status": "created"})
    else:
        return json.dumps({"error": f"PR failed: {pr_resp.text}"})
$$;
```

---

## 5. External Access Integration Setup

### 5.1 Network Rules

```sql
-- Azure DevOps API access
CREATE OR REPLACE NETWORK RULE ELT_STAGEDB.LOGGING.AZURE_DEVOPS_NETWORK_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('dev.azure.com', 'vssps.dev.azure.com');

-- Teams webhook (if not already available via TEAMS_INTEGRATION)
CREATE OR REPLACE NETWORK RULE ELT_STAGEDB.LOGGING.TEAMS_NETWORK_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('outlook.office.com', '*.webhook.office.com');
```

### 5.2 Secrets

```sql
-- Azure DevOps Personal Access Token
-- Scopes needed: Work Items (Read/Write), Code (Read/Write), Pull Requests (Read/Write)
CREATE OR REPLACE SECRET ELT_STAGEDB.LOGGING.AZURE_DEVOPS_PAT_SECRET
    TYPE = GENERIC_STRING
    SECRET_STRING = '<YOUR_AZURE_DEVOPS_PAT>';
```

### 5.3 External Access Integration

```sql
-- Requires ACCOUNTADMIN role
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION AZURE_DEVOPS_INTEGRATION
    ALLOWED_NETWORK_RULES = (ELT_STAGEDB.LOGGING.AZURE_DEVOPS_NETWORK_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = (ELT_STAGEDB.LOGGING.AZURE_DEVOPS_PAT_SECRET)
    ENABLED = TRUE;

-- Grant to the role that owns the SPs
GRANT USAGE ON INTEGRATION AZURE_DEVOPS_INTEGRATION TO ROLE SNF_DATA_GOVERNOR;
```

---

## 6. Cortex Agent Specification

```sql
CREATE OR REPLACE AGENT ELT_STAGEDB.LOGGING.JOB_MONITOR_AGENT
    COMMENT = 'Self-healing pipeline agent: detects failures, suggests fixes, creates bugs and PRs'
    FROM SPECIFICATION $$
models:
    orchestration: auto

orchestration:
    budget:
        seconds: 120
        tokens: 32000

instructions:
    orchestration: |
        You are a pipeline monitoring and self-healing agent for the ELT_STAGEDB data platform.
        Your workflow:
        1. When asked to analyze failures: Use QueryJobs to fetch error details. Use code_execution to analyze patterns.
        2. For each failure: Identify root cause, propose a specific code fix with exact file path and code.
        3. When asked to execute a fix: First create an Azure DevOps Bug, then create a PR with the fix code.
        4. Always report back with structured results.

        Rules:
        - Never apply fixes without explicit approval.
        - Always create a Bug BEFORE creating a PR (traceability).
        - Branch naming: fix/proposal-{proposal_id}
        - PR must reference the Bug ID in description.
    response: |
        Structure responses as:
        - ISSUE: What failed and when
        - ROOT CAUSE: Why (based on error logs)
        - FIX: Specific code change
        - STATUS: Current state

tools:
    - tool_spec:
        type: generic
        name: QueryJobs
        description: "Query job execution logs for failures, long-running jobs, or data quality issues. Returns JSON array of issues with error messages."
        input_schema:
            type: object
            properties:
                query_type:
                    type: string
                    description: "FAILURES | LONG_RUNNING | DATA_QUALITY | ALL"
                hours_back:
                    type: number
                    description: "How many hours to look back. Default 1."
            required:
                - query_type

    - tool_spec:
        type: generic
        name: SendAlert
        description: "Send an alert message to the Microsoft Teams channel."
        input_schema:
            type: object
            properties:
                message:
                    type: string
                    description: "The alert message content"
                severity:
                    type: string
                    description: "CRITICAL | WARNING | INFO"
            required:
                - message
                - severity

    - tool_spec:
        type: generic
        name: CreateBug
        description: "Create a Bug work item in Azure DevOps for tracking a pipeline failure fix."
        input_schema:
            type: object
            properties:
                title:
                    type: string
                    description: "Bug title, e.g. 'Pipeline Fix: job_name - error description'"
                description:
                    type: string
                    description: "Detailed description including root cause and proposed fix"
                severity:
                    type: string
                    description: "1 - Critical | 2 - High | 3 - Medium"
            required:
                - title
                - description

    - tool_spec:
        type: generic
        name: GitCreatePR
        description: "Create a git branch, commit a code fix, and open a Pull Request in Azure Repos."
        input_schema:
            type: object
            properties:
                branch_name:
                    type: string
                    description: "Branch name, e.g. fix/proposal-42"
                file_path:
                    type: string
                    description: "Path to the file to create/update in the repo"
                file_content:
                    type: string
                    description: "The complete file content with the fix applied"
                pr_title:
                    type: string
                    description: "Pull request title"
                pr_description:
                    type: string
                    description: "Pull request body in markdown. Must include Bug ID reference."
            required:
                - branch_name
                - file_path
                - file_content
                - pr_title
                - pr_description

    - tool_spec:
        type: code_execution
        name: code_execution
        description: "Execute Python code to analyze error patterns, parse logs, and generate fix code."

tool_resources:
    QueryJobs:
        type: function
        identifier: ELT_STAGEDB.LOGGING.SP_QUERY_JOBS
        execution_environment:
            type: warehouse
            warehouse: REPORTING_WH

    SendAlert:
        type: function
        identifier: ELT_STAGEDB.LOGGING.SP_SEND_TEAMS_ALERT
        execution_environment:
            type: warehouse
            warehouse: REPORTING_WH

    CreateBug:
        type: function
        identifier: ELT_STAGEDB.LOGGING.SP_CREATE_AZURE_DEVOPS_BUG
        execution_environment:
            type: warehouse
            warehouse: REPORTING_WH

    GitCreatePR:
        type: function
        identifier: ELT_STAGEDB.LOGGING.SP_GIT_CREATE_PR
        execution_environment:
            type: warehouse
            warehouse: REPORTING_WH

    code_execution:
        artifact_repositories:
            - SNOWFLAKE.SNOWPARK.PYPI_SHARED_REPOSITORY
$$;
```

---

## 7. Snowflake Tasks

### 7.1 Task 1: Detection (every 60 minutes)

```sql
CREATE OR REPLACE TASK ELT_STAGEDB.LOGGING.TASK_DETECT_FAILURES
    WAREHOUSE = REPORTING_WH
    SCHEDULE = '60 MINUTE'
    COMMENT = 'Detects pipeline failures and invokes agent for analysis'
AS
    CALL ELT_STAGEDB.LOGGING.SP_DETECT_AND_ALERT();
```

### 7.2 Task 2: Approval Polling (every 2 hours)

```sql
CREATE OR REPLACE TASK ELT_STAGEDB.LOGGING.TASK_POLL_APPROVALS
    WAREHOUSE = REPORTING_WH
    SCHEDULE = '120 MINUTE'
    COMMENT = 'Polls pending fix proposals and sends batch to Teams for approval'
AS
    CALL ELT_STAGEDB.LOGGING.SP_POLL_PENDING_PROPOSALS();
```

### 7.3 Task 3: Execute Approved Fixes (every 30 minutes)

```sql
CREATE OR REPLACE TASK ELT_STAGEDB.LOGGING.TASK_EXECUTE_FIXES
    WAREHOUSE = REPORTING_WH
    SCHEDULE = '30 MINUTE'
    COMMENT = 'Picks up approved proposals and creates bugs + PRs'
AS
    CALL ELT_STAGEDB.LOGGING.SP_EXECUTE_APPROVED_FIXES();
```

### Resume All Tasks

```sql
ALTER TASK ELT_STAGEDB.LOGGING.TASK_DETECT_FAILURES RESUME;
ALTER TASK ELT_STAGEDB.LOGGING.TASK_POLL_APPROVALS RESUME;
ALTER TASK ELT_STAGEDB.LOGGING.TASK_EXECUTE_FIXES RESUME;
```

---

## 8. Implementation Order & Dependencies

| Phase | Step | Task | Object to Create | Depends On | Owner | Est. Hours |
|-------|------|------|------------------|------------|-------|------------|
| 1 | 1.1 | Create AGENT_ALERTS table | `ELT_STAGEDB.LOGGING.AGENT_ALERTS` | Schema exists | DE | 0.5 |
| 1 | 1.2 | Create AGENT_FIX_PROPOSALS table | `ELT_STAGEDB.LOGGING.AGENT_FIX_PROPOSALS` | Schema exists | DE | 0.5 |
| 2 | 2.1 | Create SP_QUERY_JOBS | `ELT_STAGEDB.LOGGING.SP_QUERY_JOBS` | Tables exist | DE | 2 |
| 2 | 2.2 | Test SP_QUERY_JOBS manually | — | 2.1 | DE | 0.5 |
| 3 | 3.1 | Create SP_SEND_TEAMS_ALERT | `ELT_STAGEDB.LOGGING.SP_SEND_TEAMS_ALERT` | TEAMS_INTEGRATION exists | DE | 1 |
| 3 | 3.2 | Test Teams alert manually | — | 3.1 | DE | 0.5 |
| 4 | 4.1 | Create network rule for Azure DevOps | `AZURE_DEVOPS_NETWORK_RULE` | — | DBA/Admin | 0.5 |
| 4 | 4.2 | Create Azure DevOps PAT secret | `AZURE_DEVOPS_PAT_SECRET` | — | DBA/Admin | 0.5 |
| 4 | 4.3 | Create External Access Integration | `AZURE_DEVOPS_INTEGRATION` | 4.1, 4.2 | ACCOUNTADMIN | 0.5 |
| 4 | 4.4 | Grant integration to role | — | 4.3 | ACCOUNTADMIN | 0.25 |
| 5 | 5.1 | Create SP_CREATE_AZURE_DEVOPS_BUG | `ELT_STAGEDB.LOGGING.SP_CREATE_AZURE_DEVOPS_BUG` | 4.3 | DE | 2 |
| 5 | 5.2 | Test bug creation manually | — | 5.1 | DE | 0.5 |
| 5 | 5.3 | Create SP_GIT_CREATE_PR | `ELT_STAGEDB.LOGGING.SP_GIT_CREATE_PR` | 4.3 | DE | 3 |
| 5 | 5.4 | Test PR creation manually | — | 5.3 | DE | 0.5 |
| 6 | 6.1 | Create SP_DETECT_AND_ALERT | `ELT_STAGEDB.LOGGING.SP_DETECT_AND_ALERT` | 2.1, 3.1 | DE | 2 |
| 6 | 6.2 | Create SP_POLL_PENDING_PROPOSALS | `ELT_STAGEDB.LOGGING.SP_POLL_PENDING_PROPOSALS` | 3.1 | DE | 1.5 |
| 6 | 6.3 | Create SP_EXECUTE_APPROVED_FIXES | `ELT_STAGEDB.LOGGING.SP_EXECUTE_APPROVED_FIXES` | 5.1, 5.3 | DE | 2 |
| 7 | 7.1 | Grant CORTEX_USER / CORTEX_AGENT_USER roles | — | — | ACCOUNTADMIN | 0.25 |
| 7 | 7.2 | Create JOB_MONITOR_AGENT | `ELT_STAGEDB.LOGGING.JOB_MONITOR_AGENT` | All SPs | DE | 2 |
| 7 | 7.3 | Test agent in CoWork | — | 7.2 | DE | 2 |
| 8 | 8.1 | Create TASK_DETECT_FAILURES | Task | 6.1, 7.2 | DE | 0.5 |
| 8 | 8.2 | Create TASK_POLL_APPROVALS | Task | 6.2 | DE | 0.5 |
| 8 | 8.3 | Create TASK_EXECUTE_FIXES | Task | 6.3, 7.2 | DE | 0.5 |
| 8 | 8.4 | Resume all tasks | — | 8.1–8.3 | DE | 0.25 |
| 9 | 9.1 | End-to-end test | — | All | DE | 4 |

**Total estimated: ~27 hours (~3.5 dev days)**

---

## 9. Testing Matrix

| # | Test Scenario | Trigger | Expected Result | Verify |
|---|---------------|---------|-----------------|--------|
| T1 | SP_QUERY_JOBS returns failures | `CALL SP_QUERY_JOBS('FAILURES', 24)` | JSON array of recent failures | Compare with Streamlit dashboard |
| T2 | SP_SEND_TEAMS_ALERT delivers | `CALL SP_SEND_TEAMS_ALERT('Test', 'INFO')` | Message appears in Teams | Visual check |
| T3 | SP_CREATE_AZURE_DEVOPS_BUG works | `CALL SP_CREATE_AZURE_DEVOPS_BUG('Test Bug', 'Test desc', '3 - Medium')` | Bug appears in Azure DevOps | Check work items |
| T4 | SP_GIT_CREATE_PR works | `CALL SP_GIT_CREATE_PR('test/branch', 'test.txt', 'hello', 'Test PR', 'body')` | PR appears in Azure Repos | Check PRs |
| T5 | Agent responds to "what failed?" | Ask in CoWork | Agent uses QueryJobs, returns analysis | Check tool_use events |
| T6 | Detection task fires | Wait 60 min or `EXECUTE TASK` | Alert in AGENT_ALERTS + Teams message | Query table |
| T7 | Proposal appears in table | After T6 | Row in AGENT_FIX_PROPOSALS with status PENDING | Query table |
| T8 | Approval poll bundles proposals | Wait 2 hrs or `EXECUTE TASK` | Batch Teams message with all pending | Visual check |
| T9 | Manual approval triggers fix | `UPDATE ... SET STATUS='APPROVED'` | Next Task 3 run creates bug + PR | Check ADO + Repos |
| T10 | Full loop end-to-end | Simulate failure → wait → approve | Bug + PR created, proposal APPLIED | Full audit |

---

## 10. Git Version Control Strategy

```
Repository: azure-devops-org/pipeline-agent
├── /ddl/
│   ├── tables.sql                 -- AGENT_ALERTS, AGENT_FIX_PROPOSALS
│   ├── network_rules.sql          -- Network rules + EAI
│   └── secrets.sql                -- Secret DDL (no actual values!)
├── /stored_procedures/
│   ├── sp_query_jobs.sql
│   ├── sp_send_teams_alert.sql
│   ├── sp_detect_and_alert.sql
│   ├── sp_poll_pending_proposals.sql
│   ├── sp_execute_approved_fixes.sql
│   ├── sp_create_azure_devops_bug.sql
│   └── sp_git_create_pr.sql
├── /agent/
│   └── job_monitor_agent.sql      -- CREATE AGENT DDL
├── /tasks/
│   ├── task_detect_failures.sql
│   ├── task_poll_approvals.sql
│   └── task_execute_fixes.sql
├── /tests/
│   ├── test_sp_query_jobs.sql
│   ├── test_sp_teams_alert.sql
│   └── test_end_to_end.sql
└── README.md
```

**Branching strategy:**
- `main` — production code
- `develop` — integration branch
- `feature/phase-X` — per-phase development
- Tags: `v0.1-tables`, `v0.2-detection`, `v0.3-alerting`, `v0.4-devops`, `v0.5-agent`, `v1.0-complete`

---

## 11. Key Decisions Required

| # | Decision | Who Decides | Options | Impact |
|---|----------|-------------|---------|--------|
| 1 | Azure DevOps Organization & Project name | Team Lead | — | Needed for all API calls |
| 2 | Azure DevOps PAT — who creates, what scope? | DevOps/Security | `Work Items R/W` + `Code R/W` + `PR R/W` | Needed for EAI setup |
| 3 | Git repo name for pipeline code | Team Lead | Existing repo or new? | Determines file paths in PRs |
| 4 | ACCOUNTADMIN availability | DBA | Schedule time slot | Blocker for Phase 4 |
| 5 | Long-running threshold | Business | 60 min default — OK? | Affects detection sensitivity |
| 6 | How users approve | Team | Teams adaptive card / Streamlit page / direct SQL | Affects SP_POLL_PENDING logic |
| 7 | Who can approve? | Manager | Any team member / specific roles only? | May need RBAC on UPDATE |
| 8 | PR reviewers / auto-assign | Team Lead | Specific people or team? | Affects PR body |
| 9 | Base branch for PRs | DevOps | `main` or `develop`? | Affects SP_GIT_CREATE_PR |
| 10 | Bug area path in Azure DevOps | PM | Which area? | Affects SP_CREATE_AZURE_DEVOPS_BUG |

---

## 12. Risk & Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| ACCOUNTADMIN unavailable for EAI | Blocks Phase 4–5 | Request early; work on Phase 1–3 in parallel |
| Agent generates incorrect fix code | Bad PR merged | Human approval gate + PR review required |
| Azure DevOps PAT expires | Bug/PR creation fails | Set long expiry + alert on 401 errors |
| Teams webhook rate limits | Alerts not delivered | Batch messages; use retry logic |
| DATA_AGENT_RUN timeout (15 min max) | Agent can't complete | Keep prompts focused; split complex analysis |
| Agent hallucinates file paths | PR to wrong location | Validate file_path against known repo structure |

---

## 13. Quick Start (Phase 1 — Do This First)

```sql
-- Run in ELT_STAGEDB.LOGGING schema
-- Step 1: Create tables
-- Step 2: Create SP_QUERY_JOBS
-- Step 3: Test: CALL SP_QUERY_JOBS('FAILURES', 24);
-- Step 4: Create SP_SEND_TEAMS_ALERT (uses existing TEAMS_INTEGRATION)
-- Step 5: Test: CALL SP_SEND_TEAMS_ALERT('Hello from Agent', 'INFO');
```

Once these work, you have the foundation to build everything else on top.
