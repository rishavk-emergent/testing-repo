"""
CS Customer Success Report v2 — WEEKLY (image report).
Queries BigQuery for the weekly payload (last 5 completed Mon-Sun weeks), builds 3
HTML sections, commits to render/latest-weekly/. A GitHub Action screenshots + posts.
Schedule: 0 10 * * 1 (Asia/Kolkata -> Mon 10 AM IST). Triggers: NONE.
Composer env: CS_REPORT_GH_TOKEN, CS_REPORT_GH_REPO, CS_REPORT_GH_BRANCH.
"""
from datetime import timedelta
import json, logging, os
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.slack.bigquery_client import get_bigquery_client

logger = logging.getLogger(__name__)
GH_REPO   = os.getenv('CS_REPORT_GH_REPO', 'rishavk-emergent/daily-report')
GH_BRANCH = os.getenv('CS_REPORT_GH_BRANCH', 'main')
GH_TOKEN  = os.getenv('CS_REPORT_GH_TOKEN', '')
RENDER_DIR = os.getenv('CS_REPORT_RENDER_DIR', 'render/latest-weekly')

QUERY = r'''-- Customer Success Report v2 — single-row payload (newest-first comma series, 30d).
-- §1 volume/automation, §2 TAT phases (trinity_ticket_tat), §3 CSAT+Reopen w/ counts.
-- Bucketed by first-eval day. Deltas are computed in the renderer (today vs yesterday).
DECLARE last_week_start DATE DEFAULT DATE_SUB(DATE_TRUNC(CURRENT_DATE('Asia/Kolkata'), WEEK(MONDAY)), INTERVAL 1 WEEK);
DECLARE start_week DATE DEFAULT GREATEST(DATE_SUB(last_week_start, INTERVAL 9 WEEK), DATE_TRUNC(DATE('2026-05-19'), WEEK(MONDAY)));
DECLARE win_start DATE DEFAULT start_week;
DECLARE win_end   DATE DEFAULT DATE_ADD(last_week_start, INTERVAL 6 DAY);

WITH
cpst_latest AS (
  SELECT c.id AS ticket_id, c.number,
    COALESCE(NULLIF(c.support_level,'N/A'),'untagged') AS tier,
    c.closed_at AS closed_at_epoch, c.created_at AS created_at_epoch
  FROM `emergent-default.analytics.closed_pending_support_tickets` c
  QUALIFY ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY c.sync_timestamp DESC)=1
),
first_eval_per_ticket AS (
  SELECT a.job_id AS ticket_id, DATE(MIN(a.started_at),'Asia/Kolkata') AS first_eval_day
  FROM `emergent-default.overwatch_bq.v_ab_auto_send_audit` a
  WHERE a.started_at IS NOT NULL AND a.conversation_id IS NOT NULL GROUP BY a.job_id
),
tat_latest AS (SELECT ticket_number, reopen_flag FROM `emergent-default.analytics.support_tickets_tat` QUALIFY ROW_NUMBER() OVER (PARTITION BY ticket_number ORDER BY timestamp DESC)=1),
csat_trinity AS (
  SELECT vt.atlas_id AS ticket_id, CASE WHEN cs.rating='GOOD' THEN 1 WHEN cs.rating='BAD' THEN 0 END AS csat_score
  FROM `emergent-default.trinity_database.v_csat_surveys` cs
  JOIN (SELECT _id,atlas_id FROM `emergent-default.trinity_database.v_tickets` WHERE atlas_id IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY source_timestamp DESC)=1) vt ON vt._id=cs.ticket_id
  WHERE cs.rating IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY vt.atlas_id ORDER BY cs.rated_at DESC)=1
),
conv_email AS (SELECT job_id AS ticket_id, ANY_VALUE(customer_email) AS customer_email FROM `emergent-default.overwatch_bq.v_oracle_triggers` WHERE job_id IS NOT NULL GROUP BY job_id),
spam_pairs AS (SELECT ce.customer_email, fe.first_eval_day FROM conv_email ce INNER JOIN first_eval_per_ticket fe USING(ticket_id) WHERE ce.customer_email IS NOT NULL GROUP BY ce.customer_email, fe.first_eval_day HAVING COUNT(DISTINCT ce.ticket_id)>10),
spam_tickets AS (SELECT DISTINCT ce.ticket_id FROM conv_email ce INNER JOIN first_eval_per_ticket fe USING(ticket_id) INNER JOIN spam_pairs sp ON sp.customer_email=ce.customer_email AND sp.first_eval_day=fe.first_eval_day),
audit_per_ticket AS (
  SELECT a.job_id AS ticket_id, COUNTIF(LOWER(IFNULL(JSON_VALUE(a.checks,'$.escalation_required'),''))='true') AS escalation_count
  FROM `emergent-default.overwatch_bq.v_ab_auto_send_audit` a INNER JOIN first_eval_per_ticket fe ON fe.ticket_id=a.job_id WHERE a.conversation_id IS NOT NULL GROUP BY a.job_id
),
-- Trinity TAT phase split, mapped to atlas ticket id
ticket_tat AS (
  SELECT vt.atlas_id AS ticket_id, t.time1 AS ow_t, t.time3 AS hufrt_t, (t.time1 + t.time2 + t.time3) AS frt_t
  FROM (SELECT ticket_id, time1, time2, time3 FROM `emergent-default.support.trinity_ticket_tat` QUALIFY ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY sync_timestamp DESC)=1) t
  JOIN (SELECT _id, atlas_id FROM `emergent-default.trinity_database.v_tickets` WHERE atlas_id IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY source_timestamp DESC)=1) vt ON vt._id = t.ticket_id
),
closures AS (
  SELECT fe.ticket_id, COALESCE(cl.tier,'untagged') AS tier, DATE_TRUNC(fe.first_eval_day, WEEK(MONDAY)) AS day,
    (IFNULL(ap.escalation_count,0)=0) AS ow_solved,
    ct.csat_score, IFNULL(tt.reopen_flag,0)=1 AS is_reopened,
    tat.ow_t, IF(IFNULL(ap.escalation_count,0)=0, NULL, tat.hufrt_t) AS hufrt_t, tat.frt_t
  FROM first_eval_per_ticket fe
  LEFT JOIN cpst_latest cl ON cl.ticket_id=fe.ticket_id
  LEFT JOIN tat_latest tt ON tt.ticket_number=cl.number
  LEFT JOIN csat_trinity ct ON ct.ticket_id=fe.ticket_id
  LEFT JOIN audit_per_ticket ap ON ap.ticket_id=fe.ticket_id
  LEFT JOIN ticket_tat tat ON tat.ticket_id=fe.ticket_id
  WHERE fe.first_eval_day BETWEEN win_start AND win_end AND fe.ticket_id NOT IN (SELECT ticket_id FROM spam_tickets)
),
week_scaffold AS (SELECT w AS day FROM UNNEST(GENERATE_DATE_ARRAY(start_week, last_week_start, INTERVAL 1 WEEK)) AS w),
m AS (
  SELECT s.day AS day,
    CONCAT(FORMAT_DATE('%d/%m', s.day),'-',FORMAT_DATE('%d/%m', DATE_ADD(s.day, INTERVAL 6 DAY))) AS period_label,
    FORMAT_DATE('%Y-%m-%d', s.day) AS period_start,
    -- §1
    COUNTIF(c.ticket_id IS NOT NULL) AS closed,
    COUNTIF(c.ow_solved) AS overwatch_total,
    COUNTIF(NOT c.ow_solved AND c.ticket_id IS NOT NULL) AS human_total,
    ROUND(100.0*COUNTIF(c.ow_solved)/NULLIF(COUNTIF(c.ticket_id IS NOT NULL),0),1) AS pct_overwatch,
    ROUND(100.0*COUNTIF(NOT c.ow_solved AND c.ticket_id IS NOT NULL)/NULLIF(COUNTIF(c.ticket_id IS NOT NULL),0),1) AS pct_human,
    COUNTIF(c.tier='L1') AS total_L1, COUNTIF(c.ow_solved AND c.tier='L1') AS overwatch_L1, COUNTIF(NOT c.ow_solved AND c.tier='L1') AS human_L1,
    COUNTIF(c.tier='L2') AS total_L2, COUNTIF(c.ow_solved AND c.tier='L2') AS overwatch_L2, COUNTIF(NOT c.ow_solved AND c.tier='L2') AS human_L2,
    -- §2 created->OW (time1), all tickets
    ROUND(APPROX_QUANTILES(c.ow_t,100)[OFFSET(50)],1) AS ow_p50, ROUND(APPROX_QUANTILES(c.ow_t,100)[OFFSET(75)],1) AS ow_p75, ROUND(APPROX_QUANTILES(c.ow_t,100)[OFFSET(90)],1) AS ow_p90,
    ROUND(APPROX_QUANTILES(IF(c.tier='L1',c.ow_t,NULL),100)[OFFSET(50)],1) AS ow_p50_L1, ROUND(APPROX_QUANTILES(IF(c.tier='L1',c.ow_t,NULL),100)[OFFSET(75)],1) AS ow_p75_L1, ROUND(APPROX_QUANTILES(IF(c.tier='L1',c.ow_t,NULL),100)[OFFSET(90)],1) AS ow_p90_L1,
    ROUND(APPROX_QUANTILES(IF(c.tier='L2',c.ow_t,NULL),100)[OFFSET(50)],1) AS ow_p50_L2, ROUND(APPROX_QUANTILES(IF(c.tier='L2',c.ow_t,NULL),100)[OFFSET(75)],1) AS ow_p75_L2, ROUND(APPROX_QUANTILES(IF(c.tier='L2',c.ow_t,NULL),100)[OFFSET(90)],1) AS ow_p90_L2,
    -- §2 esc->first human resolution (time3), human tickets only
    ROUND(APPROX_QUANTILES(c.hufrt_t,100)[OFFSET(50)],1) AS hufrt_p50, ROUND(APPROX_QUANTILES(c.hufrt_t,100)[OFFSET(75)],1) AS hufrt_p75, ROUND(APPROX_QUANTILES(c.hufrt_t,100)[OFFSET(90)],1) AS hufrt_p90,
    ROUND(APPROX_QUANTILES(IF(c.tier='L1',c.hufrt_t,NULL),100)[OFFSET(50)],1) AS hufrt_p50_L1, ROUND(APPROX_QUANTILES(IF(c.tier='L1',c.hufrt_t,NULL),100)[OFFSET(75)],1) AS hufrt_p75_L1, ROUND(APPROX_QUANTILES(IF(c.tier='L1',c.hufrt_t,NULL),100)[OFFSET(90)],1) AS hufrt_p90_L1,
    ROUND(APPROX_QUANTILES(IF(c.tier='L2',c.hufrt_t,NULL),100)[OFFSET(50)],1) AS hufrt_p50_L2, ROUND(APPROX_QUANTILES(IF(c.tier='L2',c.hufrt_t,NULL),100)[OFFSET(75)],1) AS hufrt_p75_L2, ROUND(APPROX_QUANTILES(IF(c.tier='L2',c.hufrt_t,NULL),100)[OFFSET(90)],1) AS hufrt_p90_L2,
    -- §2 created->first resolution (time1+2+3), all tickets
    ROUND(APPROX_QUANTILES(c.frt_t,100)[OFFSET(50)],1) AS frt_p50, ROUND(APPROX_QUANTILES(c.frt_t,100)[OFFSET(75)],1) AS frt_p75, ROUND(APPROX_QUANTILES(c.frt_t,100)[OFFSET(90)],1) AS frt_p90,
    ROUND(APPROX_QUANTILES(IF(c.tier='L1',c.frt_t,NULL),100)[OFFSET(50)],1) AS frt_p50_L1, ROUND(APPROX_QUANTILES(IF(c.tier='L1',c.frt_t,NULL),100)[OFFSET(75)],1) AS frt_p75_L1, ROUND(APPROX_QUANTILES(IF(c.tier='L1',c.frt_t,NULL),100)[OFFSET(90)],1) AS frt_p90_L1,
    ROUND(APPROX_QUANTILES(IF(c.tier='L2',c.frt_t,NULL),100)[OFFSET(50)],1) AS frt_p50_L2, ROUND(APPROX_QUANTILES(IF(c.tier='L2',c.frt_t,NULL),100)[OFFSET(75)],1) AS frt_p75_L2, ROUND(APPROX_QUANTILES(IF(c.tier='L2',c.frt_t,NULL),100)[OFFSET(90)],1) AS frt_p90_L2,
    -- §3 CSAT % + response count
    ROUND(100.0*COUNTIF(c.csat_score=1)/NULLIF(COUNTIF(c.csat_score IS NOT NULL),0),1) AS csat_pos, COUNTIF(c.csat_score IS NOT NULL) AS csat_n,
    ROUND(100.0*COUNTIF(c.ow_solved AND c.csat_score=1)/NULLIF(COUNTIF(c.ow_solved AND c.csat_score IS NOT NULL),0),1) AS csat_pos_ow, COUNTIF(c.ow_solved AND c.csat_score IS NOT NULL) AS csat_n_ow,
    ROUND(100.0*COUNTIF(NOT c.ow_solved AND c.ticket_id IS NOT NULL AND c.csat_score=1)/NULLIF(COUNTIF(NOT c.ow_solved AND c.ticket_id IS NOT NULL AND c.csat_score IS NOT NULL),0),1) AS csat_pos_hu, COUNTIF(NOT c.ow_solved AND c.ticket_id IS NOT NULL AND c.csat_score IS NOT NULL) AS csat_n_hu,
    ROUND(100.0*COUNTIF(NOT c.ow_solved AND c.tier='L1' AND c.csat_score=1)/NULLIF(COUNTIF(NOT c.ow_solved AND c.tier='L1' AND c.csat_score IS NOT NULL),0),1) AS csat_pos_hu_L1, COUNTIF(NOT c.ow_solved AND c.tier='L1' AND c.csat_score IS NOT NULL) AS csat_n_hu_L1,
    ROUND(100.0*COUNTIF(NOT c.ow_solved AND c.tier='L2' AND c.csat_score=1)/NULLIF(COUNTIF(NOT c.ow_solved AND c.tier='L2' AND c.csat_score IS NOT NULL),0),1) AS csat_pos_hu_L2, COUNTIF(NOT c.ow_solved AND c.tier='L2' AND c.csat_score IS NOT NULL) AS csat_n_hu_L2,
    -- §3 Reopen % + count
    ROUND(100.0*COUNTIF(c.is_reopened)/NULLIF(COUNTIF(c.ticket_id IS NOT NULL),0),1) AS reopen_rate, COUNTIF(c.is_reopened) AS reopen_n,
    ROUND(100.0*COUNTIF(c.is_reopened AND c.tier='L1')/NULLIF(COUNTIF(c.tier='L1'),0),1) AS reopen_rate_L1, COUNTIF(c.is_reopened AND c.tier='L1') AS reopen_n_L1,
    ROUND(100.0*COUNTIF(c.is_reopened AND c.tier='L2')/NULLIF(COUNTIF(c.tier='L2'),0),1) AS reopen_rate_L2, COUNTIF(c.is_reopened AND c.tier='L2') AS reopen_n_L2
  FROM week_scaffold s
  LEFT JOIN closures c ON c.day = s.day
  GROUP BY s.day
)
SELECT TO_JSON_STRING(STRUCT(
  STRING_AGG(period_label, ',' ORDER BY day DESC) AS period_label,
  STRING_AGG(period_start, ',' ORDER BY day DESC) AS period_start,
  STRING_AGG(CAST(closed AS STRING), ',' ORDER BY day DESC) AS closed,
  STRING_AGG(CAST(overwatch_total AS STRING), ',' ORDER BY day DESC) AS overwatch_total,
  STRING_AGG(CAST(human_total AS STRING), ',' ORDER BY day DESC) AS human_total,
  STRING_AGG(IFNULL(CAST(pct_overwatch AS STRING),''), ',' ORDER BY day DESC) AS pct_overwatch,
  STRING_AGG(IFNULL(CAST(pct_human AS STRING),''), ',' ORDER BY day DESC) AS pct_human,
  STRING_AGG(CAST(total_L1 AS STRING), ',' ORDER BY day DESC) AS total_L1,
  STRING_AGG(CAST(overwatch_L1 AS STRING), ',' ORDER BY day DESC) AS overwatch_L1,
  STRING_AGG(CAST(human_L1 AS STRING), ',' ORDER BY day DESC) AS human_L1,
  STRING_AGG(CAST(total_L2 AS STRING), ',' ORDER BY day DESC) AS total_L2,
  STRING_AGG(CAST(overwatch_L2 AS STRING), ',' ORDER BY day DESC) AS overwatch_L2,
  STRING_AGG(CAST(human_L2 AS STRING), ',' ORDER BY day DESC) AS human_L2,
  STRING_AGG(IFNULL(CAST(ow_p50 AS STRING),''), ',' ORDER BY day DESC) AS ow_p50,
  STRING_AGG(IFNULL(CAST(ow_p75 AS STRING),''), ',' ORDER BY day DESC) AS ow_p75,
  STRING_AGG(IFNULL(CAST(ow_p90 AS STRING),''), ',' ORDER BY day DESC) AS ow_p90,
  STRING_AGG(IFNULL(CAST(ow_p50_L1 AS STRING),''), ',' ORDER BY day DESC) AS ow_p50_L1,
  STRING_AGG(IFNULL(CAST(ow_p75_L1 AS STRING),''), ',' ORDER BY day DESC) AS ow_p75_L1,
  STRING_AGG(IFNULL(CAST(ow_p90_L1 AS STRING),''), ',' ORDER BY day DESC) AS ow_p90_L1,
  STRING_AGG(IFNULL(CAST(ow_p50_L2 AS STRING),''), ',' ORDER BY day DESC) AS ow_p50_L2,
  STRING_AGG(IFNULL(CAST(ow_p75_L2 AS STRING),''), ',' ORDER BY day DESC) AS ow_p75_L2,
  STRING_AGG(IFNULL(CAST(ow_p90_L2 AS STRING),''), ',' ORDER BY day DESC) AS ow_p90_L2,
  STRING_AGG(IFNULL(CAST(hufrt_p50 AS STRING),''), ',' ORDER BY day DESC) AS hufrt_p50,
  STRING_AGG(IFNULL(CAST(hufrt_p75 AS STRING),''), ',' ORDER BY day DESC) AS hufrt_p75,
  STRING_AGG(IFNULL(CAST(hufrt_p90 AS STRING),''), ',' ORDER BY day DESC) AS hufrt_p90,
  STRING_AGG(IFNULL(CAST(hufrt_p50_L1 AS STRING),''), ',' ORDER BY day DESC) AS hufrt_p50_L1,
  STRING_AGG(IFNULL(CAST(hufrt_p75_L1 AS STRING),''), ',' ORDER BY day DESC) AS hufrt_p75_L1,
  STRING_AGG(IFNULL(CAST(hufrt_p90_L1 AS STRING),''), ',' ORDER BY day DESC) AS hufrt_p90_L1,
  STRING_AGG(IFNULL(CAST(hufrt_p50_L2 AS STRING),''), ',' ORDER BY day DESC) AS hufrt_p50_L2,
  STRING_AGG(IFNULL(CAST(hufrt_p75_L2 AS STRING),''), ',' ORDER BY day DESC) AS hufrt_p75_L2,
  STRING_AGG(IFNULL(CAST(hufrt_p90_L2 AS STRING),''), ',' ORDER BY day DESC) AS hufrt_p90_L2,
  STRING_AGG(IFNULL(CAST(frt_p50 AS STRING),''), ',' ORDER BY day DESC) AS frt_p50,
  STRING_AGG(IFNULL(CAST(frt_p75 AS STRING),''), ',' ORDER BY day DESC) AS frt_p75,
  STRING_AGG(IFNULL(CAST(frt_p90 AS STRING),''), ',' ORDER BY day DESC) AS frt_p90,
  STRING_AGG(IFNULL(CAST(frt_p50_L1 AS STRING),''), ',' ORDER BY day DESC) AS frt_p50_L1,
  STRING_AGG(IFNULL(CAST(frt_p75_L1 AS STRING),''), ',' ORDER BY day DESC) AS frt_p75_L1,
  STRING_AGG(IFNULL(CAST(frt_p90_L1 AS STRING),''), ',' ORDER BY day DESC) AS frt_p90_L1,
  STRING_AGG(IFNULL(CAST(frt_p50_L2 AS STRING),''), ',' ORDER BY day DESC) AS frt_p50_L2,
  STRING_AGG(IFNULL(CAST(frt_p75_L2 AS STRING),''), ',' ORDER BY day DESC) AS frt_p75_L2,
  STRING_AGG(IFNULL(CAST(frt_p90_L2 AS STRING),''), ',' ORDER BY day DESC) AS frt_p90_L2,
  STRING_AGG(IFNULL(CAST(csat_pos AS STRING),''), ',' ORDER BY day DESC) AS csat_pos,
  STRING_AGG(CAST(csat_n AS STRING), ',' ORDER BY day DESC) AS csat_n,
  STRING_AGG(IFNULL(CAST(csat_pos_ow AS STRING),''), ',' ORDER BY day DESC) AS csat_pos_ow,
  STRING_AGG(CAST(csat_n_ow AS STRING), ',' ORDER BY day DESC) AS csat_n_ow,
  STRING_AGG(IFNULL(CAST(csat_pos_hu AS STRING),''), ',' ORDER BY day DESC) AS csat_pos_hu,
  STRING_AGG(CAST(csat_n_hu AS STRING), ',' ORDER BY day DESC) AS csat_n_hu,
  STRING_AGG(IFNULL(CAST(csat_pos_hu_L1 AS STRING),''), ',' ORDER BY day DESC) AS csat_pos_hu_L1,
  STRING_AGG(CAST(csat_n_hu_L1 AS STRING), ',' ORDER BY day DESC) AS csat_n_hu_L1,
  STRING_AGG(IFNULL(CAST(csat_pos_hu_L2 AS STRING),''), ',' ORDER BY day DESC) AS csat_pos_hu_L2,
  STRING_AGG(CAST(csat_n_hu_L2 AS STRING), ',' ORDER BY day DESC) AS csat_n_hu_L2,
  STRING_AGG(IFNULL(CAST(reopen_rate AS STRING),''), ',' ORDER BY day DESC) AS reopen_rate,
  STRING_AGG(CAST(reopen_n AS STRING), ',' ORDER BY day DESC) AS reopen_n,
  STRING_AGG(IFNULL(CAST(reopen_rate_L1 AS STRING),''), ',' ORDER BY day DESC) AS reopen_rate_L1,
  STRING_AGG(CAST(reopen_n_L1 AS STRING), ',' ORDER BY day DESC) AS reopen_n_L1,
  STRING_AGG(IFNULL(CAST(reopen_rate_L2 AS STRING),''), ',' ORDER BY day DESC) AS reopen_rate_L2,
  STRING_AGG(CAST(reopen_n_L2 AS STRING), ',' ORDER BY day DESC) AS reopen_n_L2
)) AS payload
FROM m;
'''
V2_BUILD = r'''import json, urllib.request, base64

input_data.update(json.loads(input_data['payload']))

def split_all(key):
    raw = input_data.get(key, '') or ''
    return [p.strip() for p in raw.split(',')]

def to_num(s):
    if s is None or s == '': return None
    try: return float(s)
    except: return None

def nums(key):
    return [to_num(v) for v in split_all(key)]

all_labels = split_all('period_label')
all_starts = split_all('period_start')

def y(key):
    a = nums(key); return a[0] if a else None
def yprev(key):
    a = nums(key); return a[1] if len(a) > 1 else None

def fmt_int(n):
    if n is None: return '–'
    try: return '{:,}'.format(int(round(float(n))))
    except: return '–'
def fmt_pct(n):
    if n is None: return '–'
    try: return '{:.0f}%'.format(float(n))
    except: return '–'
def fmt_tat(n):
    if n is None: return '–'
    try:
        m = float(n)
        if m < 60: return '{}m'.format(int(round(m)))
        h = m/60.0
        if h < 24: return '{:.1f}h'.format(h)
        return '{:.1f}d'.format(h/24.0)
    except: return '–'

def pct_of(numkey, denkey, idx=0):
    a = nums(numkey); b = nums(denkey)
    if idx < len(a) and idx < len(b) and a[idx] is not None and b[idx] not in (None, 0):
        return 100.0 * a[idx] / b[idx]
    return None

def share_series(numkey, denkey, n=5):
    a = nums(numkey); b = nums(denkey)
    out = []
    for i in range(min(n, len(a), len(b))):
        out.append(100.0*a[i]/b[i] if (a[i] is not None and b[i] not in (None,0)) else None)
    return list(reversed(out))

def series7(key, n=5):
    return list(reversed(nums(key)[:n]))

chart_labels = [ (s.split('-')[2]+'/'+s.split('-')[1]) if len(s.split('-'))==3 else s for s in reversed(all_starts[:5]) ]

def _lerp(a, b, t):
    return tuple(int(round(a[i] + (b[i]-a[i])*t)) for i in range(3))
RED=(248,81,73); AMBER=(210,153,34); GREEN=(63,185,80); GREY=(139,148,158)
def _hex(c): return '#%02x%02x%02x' % c

def delta_calc(today, prev, direction):
    if today is None or prev is None or prev == 0:
        return ('#30363d', '<span class="c-delta" style="color:#6e7681">&middot; no prev</span>')
    diff = today - prev
    pctc = diff / abs(prev) * 100.0
    arrow = '&#9650;' if diff > 0 else ('&#9660;' if diff < 0 else '&middot;')
    if direction == 'neutral':
        col = _hex(GREY)
    else:
        good = pctc if direction == 'up_good' else -pctc
        t = max(-1.0, min(1.0, good / 15.0))
        col = _hex(_lerp(GREY, GREEN, t)) if t >= 0 else _hex(_lerp(GREY, RED, -t))
    sign = '+' if diff > 0 else ''
    return (col, '<span class="c-delta" style="color:' + col + '">' + arrow + ' ' + sign + '{:.0f}%'.format(pctc) + ' vs prev wk</span>')

def card(label, value_str, sub_str, today, prev, direction):
    col, dh = delta_calc(today, prev, direction)
    return ('<div class="card" style="border-top-color:' + col + '"><div class="c-label">' + label + '</div>'
            '<div class="c-val">' + value_str + '</div>'
            '<div class="c-sub">' + sub_str + '</div>' + dh + '</div>')

def tat_card(label, p50, p75, p90, direction='down_good'):
    col, dh = delta_calc(p75[0], p75[1], direction)
    cell = lambda nm, v: '<div class="t"><div class="t-l">' + nm + '</div><div class="t-v">' + fmt_tat(v) + '</div></div>'
    return ('<div class="card tat" style="border-top-color:' + col + '"><div class="c-label">' + label + '</div>'
            '<div class="tat-row">' + cell('p50', p50[0]) + cell('p75', p75[0]) + cell('p90', p90[0]) + '</div>'
            + dh + '</div>')

CHART_JS = r"""<script>
var labels=__LABELS__;var charts=__CHARTS__;
function fmtY(v,u){if(u==='min'){if(v<60)return Math.round(v)+'m';if(v<1440)return Math.round(v/60)+'h';return Math.round(v/1440)+'d';}return Math.round(v)+(u||'');}
function renderChart(id,cfg){var svg=document.getElementById(id);if(!svg)return;
 var W=1000,H=300,padL=84,padR=104,padT=30,padB=72,cw=W-padL-padR,ch=H-padT-padB;
 svg.setAttribute('viewBox','0 0 '+W+' '+H);svg.setAttribute('preserveAspectRatio','xMidYMid meet');
 var all=[];cfg.series.forEach(function(s){s.data.forEach(function(v){if(v!==null&&v!==undefined)all.push(v);});});
 if(all.length===0)return;
 var dMin=(cfg.yMin!==undefined)?cfg.yMin:Math.min.apply(null,all);
 var dMax=(cfg.yMax!==undefined)?cfg.yMax:Math.max.apply(null,all)*1.18;var rng=(dMax-dMin)||1,h='';
 for(var i=0;i<=4;i++){var gy=padT+ch*i/4,yv=dMax-rng*i/4;
  h+='<line x1="'+padL+'" y1="'+gy+'" x2="'+(W-padR)+'" y2="'+gy+'" stroke="#30363d" stroke-width="1.2" stroke-dasharray="4,4"/>';
  h+='<text x="'+(padL-12)+'" y="'+(gy+7)+'" text-anchor="end" fill="#8b949e" font-family="JetBrains Mono" font-size="20">'+fmtY(yv,cfg.unit)+'</text>';}
 var n=labels.length;
 labels.forEach(function(l,i){var x=padL+cw*i/(n-1);h+='<text x="'+x+'" y="'+(H-36)+'" text-anchor="middle" fill="#8b949e" font-family="JetBrains Mono" font-size="19">'+l+'</text>';});
 cfg.series.forEach(function(s){
  var pts=s.data.map(function(v,i){if(v===null||v===undefined)return null;return{x:padL+cw*i/(n-1),y:padT+ch*(1-(v-dMin)/rng),v:v};});
  var d='';pts.forEach(function(p,i){if(!p)return;d+=(d===''||!pts[i-1]?'M ':' L ')+p.x+' '+p.y;});
  h+='<path d="'+d+'" fill="none" stroke="'+s.color+'" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"/>';
  pts.forEach(function(p,i){if(!p)return;var last=i===pts.length-1;h+='<circle cx="'+p.x+'" cy="'+p.y+'" r="'+(last?6:4)+'" fill="'+s.color+'" stroke="#0d1117" stroke-width="2"/>';if(last){h+='<text x="'+(p.x+12)+'" y="'+(p.y+7)+'" text-anchor="start" fill="'+s.color+'" font-family="JetBrains Mono" font-size="22" font-weight="700">'+fmtY(p.v,cfg.unit)+'</text>';}});
 });
 var lx=padL,ly=H-6;cfg.series.forEach(function(s){if(!s.label)return;h+='<rect x="'+lx+'" y="'+(ly-15)+'" width="16" height="16" rx="3" fill="'+s.color+'"/>';h+='<text x="'+(lx+23)+'" y="'+ly+'" fill="#c9d1d9" font-family="JetBrains Mono" font-size="19">'+s.label+'</text>';lx+=40+s.label.length*12;});
 svg.innerHTML=h;}
(function(){function go(){Object.keys(charts).forEach(function(id){renderChart(id,charts[id]);});}if(document.readyState!=='loading'){go();}else{document.addEventListener('DOMContentLoaded',go);}})();
</script>"""

charts_data = {}
def add_chart(cid, series, unit='', yMin=None, yMax=None):
    cfg = {'series': series, 'unit': unit}
    if yMin is not None: cfg['yMin'] = yMin
    if yMax is not None: cfg['yMax'] = yMax
    charts_data[cid] = cfg

BLUE='#58a6ff'; PINK='#f778ba'; GREENC='#3fb950'; AMBERC='#d29922'; PURPLE='#bc8cff'

def chart_block(cid, caption, unit_label):
    return ('<div class="chart-area"><div class="chart-caption"><span>' + caption + '</span><span>' + unit_label + '</span></div>'
            '<svg class="chart-svg" id="' + cid + '"></svg></div>')

display_date = all_starts[0] if all_starts else ''
try:
    from datetime import date
    p = (all_starts[0] if all_starts else '').split('-')
    display_date = (all_labels[0] if all_labels else '').replace('-',' \u2013 ')
except: pass
newest_label = all_labels[0] if all_labels else ''

FONTS = ('<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
'<link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,400;9..144,600;9..144,700;9..144,900&family=JetBrains+Mono:wght@400;500;600;700&family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">')

STYLE = ('<style>'
':root{--bg:#0d1117;--bg-card:#161b22;--bg-card-elevated:#1c2128;--border:#30363d;--border-strong:#484f58;--text:#e6edf3;--text-muted:#8b949e;--text-dim:#6e7681;--accent:#f7c948;--green:#3fb950;--amber:#d29922;--red:#f85149}'
'*{margin:0;padding:0;box-sizing:border-box}'
'body{background:var(--bg);color:var(--text);font-family:"Inter",sans-serif;padding:32px;max-width:1180px;margin:0 auto;-webkit-font-smoothing:antialiased}'
'.masthead{display:flex;align-items:flex-end;justify-content:space-between;padding-bottom:18px;border-bottom:2px solid var(--text);margin-bottom:22px}'
'.masthead-title{font-family:"Fraunces",serif;font-weight:900;font-size:32px;letter-spacing:-0.02em;line-height:1}'
'.masthead-sub{font-family:"JetBrains Mono",monospace;font-size:11px;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.12em;margin-top:7px}'
'.masthead-meta .date{font-family:"Fraunces",serif;font-size:20px;font-weight:600;text-align:right}'
'.row-label{font-family:"JetBrains Mono",monospace;font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:0.15em;margin:16px 0 8px 2px}'
'.cards{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}'
'.card{background:var(--bg-card);border:1px solid var(--border);border-top:3px solid var(--accent);padding:14px 16px;min-height:104px}'
'.card .c-label{font-family:"JetBrains Mono",monospace;font-size:10px;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.1em;margin-bottom:7px}'
'.card .c-val{font-family:"Fraunces",serif;font-weight:700;font-size:30px;line-height:1;letter-spacing:-0.02em}'
'.card .c-sub{font-family:"JetBrains Mono",monospace;font-size:11px;color:var(--text-muted);margin:5px 0 7px}'
'.card .c-delta{font-family:"JetBrains Mono",monospace;font-size:11px;font-weight:600;display:block}'
'.card.tat .tat-row{display:flex;gap:16px;margin:2px 0 8px}'
'.card.tat .t-l{font-family:"JetBrains Mono",monospace;font-size:9px;color:var(--text-dim);text-transform:uppercase}'
'.card.tat .t-v{font-family:"Fraunces",serif;font-weight:700;font-size:21px;line-height:1.1}'
'.card.tat .tat-row .t:nth-child(2) .t-v{color:var(--accent)}'
'.chart-area{background:var(--bg-card-elevated);border:1px solid var(--border);border-radius:4px;padding:16px;margin-top:12px}'
'.chart-caption{font-family:"JetBrains Mono",monospace;font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:0.1em;margin-bottom:8px;display:flex;justify-content:space-between}'
'.chart-svg{width:100%;aspect-ratio:1000/300;height:auto;display:block}'
'.grid3{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}'
'</style>')

def masthead(sub):
    return ('<div class="masthead"><div><div class="masthead-title">WEEKLY CS REPORT</div>'
            '<div class="masthead-sub">' + sub + '</div></div>'
            '<div><div class="date">' + display_date + '</div></div></div>')

def doc(inner, chart_ids):
    js = ''
    if chart_ids:
        subset = {k: charts_data[k] for k in chart_ids if k in charts_data}
        js = CHART_JS.replace('__LABELS__', json.dumps(chart_labels)).replace('__CHARTS__', json.dumps(subset))
    return '<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">' + FONTS + STYLE + '</head><body>' + inner + js + '</body></html>'

def vol_rows():
    rows = ''
    rows += '<div class="row-label">&mdash; total</div><div class="cards">'
    rows += card('Total Tickets Closed', fmt_int(y('closed')), 'closed (by first-eval week)', y('closed'), yprev('closed'), 'neutral')
    rows += card('Overwatch', fmt_pct(y('pct_overwatch')), fmt_int(y('overwatch_total')) + ' tickets', y('pct_overwatch'), yprev('pct_overwatch'), 'up_good')
    rows += card('Human', fmt_pct(y('pct_human')), fmt_int(y('human_total')) + ' tickets', y('pct_human'), yprev('pct_human'), 'down_good')
    rows += '</div>'
    for t in ['L1','L2']:
        rows += '<div class="row-label">&mdash; ' + t.lower() + '</div><div class="cards">'
        rows += card(t + ' Tickets Closed', fmt_int(y('total_'+t)), 'closed', y('total_'+t), yprev('total_'+t), 'neutral')
        owp = pct_of('overwatch_'+t, 'total_'+t, 0); owp1 = pct_of('overwatch_'+t, 'total_'+t, 1)
        hup = pct_of('human_'+t, 'total_'+t, 0); hup1 = pct_of('human_'+t, 'total_'+t, 1)
        rows += card(t + ' Overwatch', fmt_pct(owp), fmt_int(y('overwatch_'+t)) + ' tickets', owp, owp1, 'up_good')
        rows += card(t + ' Human', fmt_pct(hup), fmt_int(y('human_'+t)) + ' tickets', hup, hup1, 'down_good')
        rows += '</div>'
    return rows

add_chart('v-ov', [{'data': series7('pct_overwatch'), 'color': BLUE, 'label':'OW%'},
                   {'data': series7('pct_human'), 'color': PINK, 'label':'Human%'}], unit='%', yMin=0, yMax=100)
add_chart('v-l1', [{'data': share_series('overwatch_L1','total_L1'), 'color': BLUE, 'label':'OW%'},
                   {'data': share_series('human_L1','total_L1'), 'color': PINK, 'label':'Human%'}], unit='%', yMin=0, yMax=100)
add_chart('v-l2', [{'data': share_series('overwatch_L2','total_L2'), 'color': BLUE, 'label':'OW%'},
                   {'data': share_series('human_L2','total_L2'), 'color': PINK, 'label':'Human%'}], unit='%', yMin=0, yMax=100)

html_r1 = doc(
    masthead('Volume &amp; Automation') + vol_rows()
    + '<div class="row-label">&mdash; OW% vs Human% &middot; last 5 weeks</div>'
    + '<div class="grid3">' + chart_block('v-ov','Overall','%') + chart_block('v-l1','L1','%') + chart_block('v-l2','L2','%') + '</div>',
    ['v-ov','v-l1','v-l2'])

def tat_rows():
    rows = ''
    defs = [('Overall',''), ('L1','_L1'), ('L2','_L2')]
    cols = [('Created &rarr; OW','ow'), ('Esc &rarr; Human FRT','hufrt'), ('Created &rarr; First Resolution','frt')]
    for tname, sfx in defs:
        rows += '<div class="row-label">&mdash; ' + tname.lower() + '</div><div class="cards">'
        for cname, ph in cols:
            rows += tat_card(tname + ' &middot; ' + cname,
                             (y(ph+'_p50'+sfx), yprev(ph+'_p50'+sfx)),
                             (y(ph+'_p75'+sfx), yprev(ph+'_p75'+sfx)),
                             (y(ph+'_p90'+sfx), yprev(ph+'_p90'+sfx)))
        rows += '</div>'
    return rows

add_chart('t-ow', [{'data': series7('ow_p75'), 'color': BLUE}], unit='min', yMin=0)
add_chart('t-hu', [{'data': series7('hufrt_p75'), 'color': AMBERC}], unit='min', yMin=0)
add_chart('t-frt', [{'data': series7('frt_p75'), 'color': PINK}], unit='min', yMin=0)

html_r2 = doc(
    masthead('Resolution TAT &middot; p50 / p75 / p90') + tat_rows()
    + '<div class="row-label">&mdash; p75 trend &middot; last 5 weeks</div>'
    + '<div class="grid3">' + chart_block('t-ow','Created &rarr; OW','min') + chart_block('t-hu','Esc &rarr; Human FRT','min') + chart_block('t-frt','Created &rarr; First Resolution','min') + '</div>',
    ['t-ow','t-hu','t-frt'])

def _latest_idx(pct_key):
    arr = nums(pct_key)
    for i, v in enumerate(arr):
        if v is not None:
            return i
    return None

def csat_card(label, pct_key, n_key):
    arr = nums(pct_key); na = nums(n_key)
    i = _latest_idx(pct_key)
    if i is None:
        return card(label, '–', '0 responses', None, None, 'up_good')
    nval = na[i] if i < len(na) else None
    prev = None
    for j in range(i + 1, len(arr)):
        if arr[j] is not None:
            prev = arr[j]; break
    asof = all_labels[i] if i < len(all_labels) else ''
    sub = fmt_int(nval) + ' responses' + (' &middot; as of ' + asof if i > 0 else '')
    return card(label, fmt_pct(arr[i]), sub, arr[i], prev, 'up_good')

def csat_reopen_rows():
    rows = ''
    rows += '<div class="row-label">&mdash; csat % positive</div><div class="cards">'
    rows += csat_card('CSAT Overall', 'csat_pos', 'csat_n')
    rows += csat_card('OW CSAT', 'csat_pos_ow', 'csat_n_ow')
    rows += csat_card('Human CSAT', 'csat_pos_hu', 'csat_n_hu')
    rows += '</div>'
    rows += '<div class="row-label">&mdash; human csat by tier</div><div class="cards">'
    rows += csat_card('Human CSAT', 'csat_pos_hu', 'csat_n_hu')
    rows += csat_card('L1 Human CSAT', 'csat_pos_hu_L1', 'csat_n_hu_L1')
    rows += csat_card('L2 Human CSAT', 'csat_pos_hu_L2', 'csat_n_hu_L2')
    rows += '</div>'
    rows += '<div class="row-label">&mdash; reopen rate</div><div class="cards">'
    rows += card('Reopen Overall', fmt_pct(y('reopen_rate')), fmt_int(y('reopen_n')) + ' tickets', y('reopen_rate'), yprev('reopen_rate'), 'down_good')
    rows += card('L1 Reopen', fmt_pct(y('reopen_rate_L1')), fmt_int(y('reopen_n_L1')) + ' tickets', y('reopen_rate_L1'), yprev('reopen_rate_L1'), 'down_good')
    rows += card('L2 Reopen', fmt_pct(y('reopen_rate_L2')), fmt_int(y('reopen_n_L2')) + ' tickets', y('reopen_rate_L2'), yprev('reopen_rate_L2'), 'down_good')
    rows += '</div>'
    return rows

add_chart('c-csat', [{'data': series7('csat_pos'), 'color': GREENC, 'label':'Overall'},
                     {'data': series7('csat_pos_ow'), 'color': BLUE, 'label':'OW'},
                     {'data': series7('csat_pos_hu'), 'color': PINK, 'label':'Human'}], unit='%', yMin=0, yMax=100)
add_chart('c-reopen', [{'data': series7('reopen_rate'), 'color': AMBERC, 'label':'Overall'},
                       {'data': series7('reopen_rate_L1'), 'color': BLUE, 'label':'L1'},
                       {'data': series7('reopen_rate_L2'), 'color': PURPLE, 'label':'L2'}], unit='%', yMin=0)

html_r3 = doc(
    masthead('CSAT &amp; Reopen') + csat_reopen_rows()
    + '<div class="row-label">&mdash; trends &middot; last 5 weeks</div>'
    + '<div class="grid3" style="grid-template-columns:repeat(2,1fr)">' + chart_block('c-csat','CSAT % positive','%') + chart_block('c-reopen','Reopen rate','%') + '</div>',
    ['c-csat','c-reopen'])

slack_text = (':bar_chart: *WEEKLY CUSTOMER SUCCESS REPORT — ' + newest_label + '*\n'
              + 'Total *' + fmt_int(y('closed')) + '* tickets closed · Overwatch *' + fmt_pct(y('pct_overwatch'))
              + '* / Human *' + fmt_pct(y('pct_human')) + '* · CSAT *' + fmt_pct(y('csat_pos'))
              + '* · Reopen *' + fmt_pct(y('reopen_rate')) + '*')

GH_TOKEN  = (input_data.get('gh_token', '') or '').strip()
GH_REPO   = (input_data.get('gh_repo', '') or '').strip()
GH_BRANCH = (input_data.get('gh_branch', '') or 'main').strip()
API = 'https://api.github.com/repos/' + GH_REPO
HDR = {'Authorization': 'token ' + GH_TOKEN, 'Accept': 'application/vnd.github+json', 'User-Agent': 'polaris-daily-zap', 'Content-Type': 'application/json'}
def gh(method, path, body=None):
    data = json.dumps(body).encode('utf-8') if body is not None else None
    return json.loads(urllib.request.urlopen(urllib.request.Request(API+path, data=data, headers=HDR, method=method), timeout=60).read().decode('utf-8'))

_render_dir = (input_data.get('render_dir', 'render/latest') or 'render/latest').strip()
files = {
    _render_dir + '/r1.html': html_r1,
    _render_dir + '/r2.html': html_r2,
    _render_dir + '/r3.html': html_r3,
    _render_dir + '/caption.txt': slack_text,
}
committed = ''
errors = []
try:
    base_sha = gh('GET', '/git/ref/heads/' + GH_BRANCH)['object']['sha']
    base_tree = gh('GET', '/git/commits/' + base_sha)['tree']['sha']
    tree = []
    for p, content in files.items():
        blob = gh('POST', '/git/blobs', {'content': base64.b64encode(content.encode('utf-8')).decode(), 'encoding': 'base64'})
        tree.append({'path': p, 'mode': '100644', 'type': 'blob', 'sha': blob['sha']})
    nt = gh('POST', '/git/trees', {'base_tree': base_tree, 'tree': tree})
    nc = gh('POST', '/git/commits', {'message': 'cs report ' + newest_label, 'tree': nt['sha'], 'parents': [base_sha]})
    gh('PATCH', '/git/refs/heads/' + GH_BRANCH, {'sha': nc['sha']})
    committed = nc.get('html_url', nc.get('sha', ''))
except Exception as e:
    errors.append(str(e))

output = {'committed': committed, 'errors': '; '.join(errors), 'slack_text': slack_text}
'''

def run_cs_report(**context):
    client = get_bigquery_client()
    rows = list(client.query(QUERY).result())
    if not rows:
        raise Exception("query returned no payload row")
    r0 = rows[0]
    payload = r0['payload'] if isinstance(r0, dict) else r0.payload
    g = {'input_data': {'payload': payload, 'gh_token': GH_TOKEN, 'gh_repo': GH_REPO,
                        'gh_branch': GH_BRANCH, 'render_dir': RENDER_DIR}}
    exec(V2_BUILD, g)
    out = g.get('output', {}) or {}
    if out.get('errors'):
        raise Exception("commit failed: " + out['errors'])
    logger.info("CS report v2 weekly committed -> %s", out.get('committed', ''))

default_args = {
    'owner': 'cs_team', 'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 1, 1, tz='Asia/Kolkata'),
    'email_on_failure': False, 'email_on_retry': False,
    'retries': 2, 'retry_delay': timedelta(minutes=3),
}
dag = DAG('cs_report_v2_weekly', default_args=default_args,
    description='Weekly CS Success Report (last 5 weeks) -> commit to render-weekly -> GitHub Action posts images',
    schedule_interval='0 10 * * 1', catchup=False,
    tags=['slack','analytics','cs_metrics','reporting','images','weekly'])
PythonOperator(task_id='build_and_commit_cs_report_weekly', python_callable=run_cs_report, dag=dag)
