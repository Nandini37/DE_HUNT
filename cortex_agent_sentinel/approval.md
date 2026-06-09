# APPROVAL.md — Developer Approval Workflow

This step is mandatory. Do not open [PR.md](PR.md) until you have received an `APPROVE` signal.

---

## 1. Approval States

| State | What it means | What you do |
|---|---|---|
| `PENDING` | Plan posted, no response yet | Wait. Do not write code. Do not create branches. |
| `REQUEST_CHANGES` | Developer has questions or wants revisions | Revise plan (see [PLAN.md](PLAN.md) §5), re-post, wait again |
| `APPROVE` | Developer has confirmed the plan | Record approver name + timestamp, proceed to [PR.md](PR.md) |
| `REJECT` | Developer has rejected the approach | Close the plan. Notify manager. Do not implement. |

**The only transition that unblocks coding is `APPROVE`.**
Any other state means you stay in this step.

---

## 2. How Approval is Captured

Approval comes in from the Teams card action buttons posted in [ALERTS.md](ALERTS.md) §3.
The card sends a POST to your `APPROVAL_ENDPOINT` with a JSON body:

```json
{ "action": "APPROVE", "query_id": "<query_id>" }
```

or

```json
{ "action": "REQUEST_CHANGES", "query_id": "<query_id>" }
```

Your approval endpoint handler:

```python
from flask import Flask, request, jsonify

app = Flask(__name__)
approval_store: dict[str, str] = {}  # query_id → state

@app.post("/approval")
def handle_approval():
    body = request.get_json()
    query_id = body.get("query_id")
    action   = body.get("action")   # "APPROVE" | "REQUEST_CHANGES" | "REJECT"
    if query_id and action:
        approval_store[query_id] = action
        return jsonify({"status": "recorded", "query_id": query_id, "action": action})
    return jsonify({"error": "missing fields"}), 400

def get_approval_state(query_id: str) -> str:
    return approval_store.get(query_id, "PENDING")
```

Poll `get_approval_state(query_id)` every 60 seconds. Do not proceed until it returns `"APPROVE"`.

---

## 3. What to Record When Approved

Before proceeding to [PR.md](PR.md), record the following:

```python
approval_record = {
    "query_id":       query_id,
    "approver":       "<developer Teams display name>",
    "approved_at":    "<ISO 8601 timestamp>",
    "plan_version":   "FINAL",   # or "v1", "v2" etc if revised
}
```

Include `approver` and `approved_at` in:
- The git commit message
- The PR description
- The Teams PR notification card (see [ALERTS.md](ALERTS.md) §4)

---

## 4. Handling REQUEST_CHANGES

When the state is `REQUEST_CHANGES`:

1. Read the feedback from the Teams thread carefully.
2. Go back to [PLAN.md](PLAN.md) §5 and revise the plan.
3. Re-post the revised plan via [ALERTS.md](ALERTS.md) §3 with `[FINAL]` in the title.
4. Reset the approval state for this `query_id` to `PENDING`.
5. Wait again.

Do not argue with the feedback in the Teams channel. Address it directly in the revised plan.
If something in the feedback is technically incorrect, surface it clearly with evidence — do not ignore it.

---

## 5. Handling REJECT

When the state is `REJECT`:

1. Post a brief acknowledgment to the Teams thread.
2. Close the plan document. Do not delete it — archive it with a `[REJECTED]` prefix.
3. Do not implement any code changes.
4. Notify the Data Engineering Manager that the plan was rejected and the issue remains open.
5. Log the rejection in the monitoring app for audit purposes.
