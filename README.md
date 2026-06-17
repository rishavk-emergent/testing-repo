# testing-repo — staging for analytics-dags

Edit and test DAGs here. When satisfied, push each file to the **flat** `dags/`
folder in `emergentbase/analytics-dags` (subfolders below are local-only for
readability — they do NOT exist in the deploy target).

## Deploy mapping (local → analytics-dags)

| Local file (here)                          | Deploy path (analytics-dags)              |
|--------------------------------------------|-------------------------------------------|
| `l3/real_l3_open_pending_dag.py`           | `dags/real_l3_open_pending_dag.py`        |
| `l3/l3_needs_review_dag.py`                | `dags/l3_needs_review_dag.py`             |
| `l3/real_l3_hygiene_dag.py`                | `dags/real_l3_hygiene_dag.py`             |
| `cs_reports/cs_report_daily_dag.py`        | `dags/cs_report_daily_dag.py`             |
| `cs_reports/cs_report_weekly_dag.py`       | `dags/cs_report_weekly_dag.py`            |
| `prod_sos/prod_sos_dag.py`                 | `dags/prod_sos_dag.py`                    |

## Status of the staged files
- `prod_sos/prod_sos_dag.py` is a DUMMY placeholder (paused, no real logic yet).
- All five are standalone single `.py` files with `is_paused_upon_creation=False`.
- `real_l3_open_pending_dag.py` has `from __future__ import annotations` (Py3.8-safe).
- `cs_reports/*` render with **Pillow only** (no matplotlib); the DejaVu font is
  embedded as base64 inside each file (no external files; Pillow >= 8.0).
- `real_l3_hygiene_dag.py` writes a dedup state table `support.real_l3_hygiene_pinged`
  and uses Slack `users.lookupByEmail` (bot scope `users:read.email`, already added).

## Workflow
1. Make changes in the file under `l3/` or `cs_reports/`.
2. Test (render / fire to test channel `C0B4J9RBWDC`).
3. Push the changed file(s) to `dags/<name>.py` on a branch in analytics-dags, open PR.
