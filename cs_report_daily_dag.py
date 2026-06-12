"""
CS Customer Success Report - DAILY (pure-Python image report).
Standalone DAG: queries BigQuery, renders 3 report sections as PNGs with matplotlib,
and posts them to Slack directly via files_upload_v2. No HTML, no headless Chrome,
no external git repo. Schedule: 0 11 * * * (11 AM IST). Triggers: NONE.
Slack: posts via utils.slack.slack_config.SLACK_BOT_TOKEN_ALERTS (shared bot, already
provisioned in Composer). Channel via CS_REPORT_SLACK_CHANNEL env (default cs-associates).
PyPI deps: matplotlib, Pillow (Pillow present; matplotlib must be added to the Composer env).
"""
from datetime import timedelta
import logging, os
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.slack.bigquery_client import get_bigquery_client
from utils.slack.slack_config import SLACK_BOT_TOKEN_ALERTS as SLACK_TOKEN

logger = logging.getLogger(__name__)
MODE = 'daily'
SLACK_CHANNEL = os.getenv('CS_REPORT_SLACK_CHANNEL', 'C0B075CBPS7')

QUERY = r'''-- Customer Success Report v2 — single-row payload (newest-first comma series, 30d).
-- §1 volume/automation, §2 TAT phases (trinity_ticket_tat), §3 CSAT+Reopen w/ counts.
-- Bucketed by first-eval day. Deltas are computed in the renderer (today vs yesterday).
DECLARE end_day   DATE DEFAULT DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL 1 DAY);
DECLARE start_day DATE DEFAULT GREATEST(DATE_SUB(end_day, INTERVAL 29 DAY), DATE('2026-05-19'));

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
  SELECT vt.atlas_id AS ticket_id, t.time1 AS ow_t, t.time3 AS hufrt_t, (t.time1 + t.time3) AS frt_t
  FROM (SELECT ticket_id, time1, time2, time3 FROM `emergent-default.support.trinity_ticket_tat` QUALIFY ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY sync_timestamp DESC)=1) t
  JOIN (SELECT _id, atlas_id FROM `emergent-default.trinity_database.v_tickets` WHERE atlas_id IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY source_timestamp DESC)=1) vt ON vt._id = t.ticket_id
),
closures AS (
  SELECT fe.ticket_id, COALESCE(cl.tier,'untagged') AS tier, fe.first_eval_day AS day,
    (IFNULL(ap.escalation_count,0)=0) AS ow_solved,
    ct.csat_score, IFNULL(tt.reopen_flag,0)=1 AS is_reopened,
    tat.ow_t, IF(IFNULL(ap.escalation_count,0)=0, NULL, tat.hufrt_t) AS hufrt_t, IF(IFNULL(ap.escalation_count,0)=0, NULL, tat.frt_t) AS frt_t
  FROM first_eval_per_ticket fe
  LEFT JOIN cpst_latest cl ON cl.ticket_id=fe.ticket_id
  LEFT JOIN tat_latest tt ON tt.ticket_number=cl.number
  LEFT JOIN csat_trinity ct ON ct.ticket_id=fe.ticket_id
  LEFT JOIN audit_per_ticket ap ON ap.ticket_id=fe.ticket_id
  LEFT JOIN ticket_tat tat ON tat.ticket_id=fe.ticket_id
  WHERE fe.first_eval_day BETWEEN start_day AND end_day AND fe.ticket_id NOT IN (SELECT ticket_id FROM spam_tickets)
),
day_scaffold AS (SELECT d AS day FROM UNNEST(GENERATE_DATE_ARRAY(start_day, end_day)) AS d),
m AS (
  SELECT s.day AS day,
    FORMAT_DATE('%d/%m', s.day) AS period_label,
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
    -- §2 created->human FRT (time1+3), escalated tickets only
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
  FROM day_scaffold s
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
FROM m;'''

# ===== CS Success Report renderer (pure Python: matplotlib -> PNG -> Slack) =====
# Shared, self-contained block embedded verbatim into both DAG files.
# Public entry points: render_report(payload, mode) -> [(title, png_bytes)]
#                      slack_upload_v2(token, channel, images, comment)
import io, json, urllib.request, urllib.parse
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle, FancyBboxPatch
from matplotlib.lines import Line2D

BG='#0d1117'; CARD='#161b22'; BORDER='#30363d'; INK='#e6edf3'; SUB='#8b949e'
RED=(0.973,0.318,0.286); GREEN=(0.247,0.725,0.314); GREY=(0.545,0.580,0.620)
BLUE='#58a6ff'; PINK='#f778ba'; GREENC='#3fb950'; AMBERC='#d29922'; PURPLE='#bc8cff'

def _split(d,k): return [p.strip() for p in (d.get(k,'') or '').split(',')]
def _to_num(s):
    if s is None or s=='' : return None
    try: return float(s)
    except: return None
def _nums(d,k): return [_to_num(v) for v in _split(d,k)]
def _y(d,k):
    a=_nums(d,k); return a[0] if a else None
def _yprev(d,k):
    a=_nums(d,k); return a[1] if len(a)>1 else None
def _fmt_int(n):
    if n is None: return '–'
    try: return '{:,}'.format(int(round(float(n))))
    except: return '–'
def _fmt_pct(n):
    if n is None: return '–'
    try: return '{:.0f}%'.format(float(n))
    except: return '–'
def _fmt_tat(n):
    if n is None: return '–'
    m=float(n)
    if m<60: return '%dm'%round(m)
    h=m/60.0
    if h<24: return '%.1fh'%h
    return '%.1fd'%(h/24.0)
def _pct_of(d,nk,dk,i=0):
    a=_nums(d,nk); b=_nums(d,dk)
    if i<len(a) and i<len(b) and a[i] is not None and b[i] not in (None,0): return 100.0*a[i]/b[i]
    return None
def _lerp(a,b,t): return tuple(a[i]+(b[i]-a[i])*t for i in range(3))
def _delta(today,prev,direction):
    if today is None or prev in (None,0): return GREY,'· no prev'
    diff=today-prev; pctc=diff/abs(prev)*100.0
    arrow='▲' if diff>0 else ('▼' if diff<0 else '·')
    if direction=='neutral': col=GREY
    else:
        good=pctc if direction=='up_good' else -pctc
        t=max(-1.0,min(1.0,good/15.0))
        col=_lerp(GREY,GREEN,t) if t>=0 else _lerp(GREY,RED,-t)
    sign='+' if diff>0 else ''
    return col,'%s %s%.0f%% vs prev'%(arrow,sign,pctc)
def _series(d,key,n): return list(reversed(_nums(d,key)[:n]))
def _series_gated(d,key,gate_key,thr,n):
    vals=_nums(d,key)[:n]; gates=_nums(d,gate_key)[:n]; out=[]
    for i in range(len(vals)):
        g=gates[i] if i<len(gates) else None
        out.append(vals[i] if (g is not None and g>=thr) else None)
    return list(reversed(out))

def _card(ax,label,value,sub,dcol,dtext):
    ax.set_xlim(0,1); ax.set_ylim(0,1); ax.axis('off')
    ax.add_patch(FancyBboxPatch((0.02,0.05),0.96,0.90,boxstyle="round,pad=0.008,rounding_size=0.025",lw=0.9,edgecolor=BORDER,facecolor=CARD,mutation_aspect=0.5))
    ax.add_patch(Rectangle((0.03,0.945),0.94,0.018,color=dcol))   # thin accent line
    ax.text(0.08,0.77,label.upper(),color=SUB,fontsize=8,family='monospace',va='center')
    ax.text(0.08,0.50,value,color=INK,fontsize=19,fontweight='bold',family='serif',va='center')
    ax.text(0.08,0.26,sub,color=SUB,fontsize=8.5,va='center')
    ax.text(0.08,0.12,dtext,color=dcol,fontsize=8.5,family='monospace',va='center')
def _tat_card(ax,d,label,ph,sfx):
    ax.set_xlim(0,1); ax.set_ylim(0,1); ax.axis('off')
    col,dt=_delta(_y(d,ph+'_p75'+sfx),_yprev(d,ph+'_p75'+sfx),'down_good')
    ax.add_patch(FancyBboxPatch((0.02,0.05),0.96,0.90,boxstyle="round,pad=0.008,rounding_size=0.025",lw=0.9,edgecolor=BORDER,facecolor=CARD,mutation_aspect=0.5))
    ax.add_patch(Rectangle((0.03,0.945),0.94,0.018,color=col))
    ax.text(0.08,0.78,label.upper(),color=SUB,fontsize=8,family='monospace',va='center')
    for i,(nm,key) in enumerate([('p50','_p50'),('p75','_p75'),('p90','_p90')]):
        x=0.10+i*0.30
        ax.text(x,0.54,nm,color=SUB,fontsize=7.5,family='monospace',va='center')
        ax.text(x,0.36,_fmt_tat(_y(d,ph+key+sfx)),color=INK,fontsize=13,fontweight='bold',family='serif',va='center')
    ax.text(0.08,0.13,dt,color=col,fontsize=8,family='monospace',va='center')
def _chart(ax,labels,series_list,unit='',ymin=0,ymax=None,title=None):
    ax.set_facecolor(CARD)
    for sp in ax.spines.values(): sp.set_color(BORDER); sp.set_linewidth(0.8)
    ax.tick_params(colors=SUB,labelsize=7,length=0)
    allv=[v for s in series_list for v in s['data'] if v is not None]
    if allv:
        lo=ymin if ymin is not None else min(allv)*0.9
        hi=ymax if ymax is not None else max(allv)*1.4
        if hi<=lo: hi=lo+1
        ax.set_ylim(lo,hi)
        if ymax==100: ax.set_yticks([0,25,50,75,100])
    n=len(labels); ax.set_xlim(-0.3,n-1+0.85)
    for s in series_list:
        xv=[i for i,v in enumerate(s['data']) if v is not None]
        yv=[v for v in s['data'] if v is not None]
        ax.plot(xv,yv,marker='o',ms=2.6,lw=1.5,color=s['color'],label=s.get('label'),solid_capstyle='round')
        if xv:
            ax.annotate(('%.0f'%yv[-1])+unit,(xv[-1],yv[-1]),color=s['color'],fontsize=7,fontweight='bold',xytext=(4,0),textcoords='offset points',va='center')
    ax.set_xticks(range(n)); ax.set_xticklabels(labels)
    ax.grid(True,axis='y',color=BORDER,ls='-',lw=0.5,alpha=0.35); ax.set_axisbelow(True)
    if title:
        ax.text(0.0,1.08,title,transform=ax.transAxes,color=SUB,fontsize=7.5,family='monospace',va='bottom',ha='left')
        ax.text(1.0,1.08,unit if unit else '%',transform=ax.transAxes,color=SUB,fontsize=7.5,family='monospace',va='bottom',ha='right')
    if any(s.get('label') for s in series_list):
        lg=ax.legend(loc='upper left',fontsize=6,frameon=False,ncol=len(series_list),handlelength=1.0,columnspacing=0.9,handletextpad=0.4,borderaxespad=0.2)
        for txt in lg.get_texts(): txt.set_color(SUB)

def _fig():
    f=plt.figure(figsize=(11,7),dpi=150); f.patch.set_facecolor(BG); return f
def _masthead(fig,title,sub,period):
    fig.text(0.04,0.965,title,color=INK,fontsize=22,fontweight='bold',family='serif',va='center')
    fig.text(0.04,0.93,sub,color=SUB,fontsize=11,family='monospace',va='center')
    fig.text(0.96,0.95,period,color=SUB,fontsize=12,family='monospace',va='center',ha='right')
    fig.add_artist(Line2D([0.04,0.96],[0.905,0.905],color=INK,lw=1.2))
def _cards(fig):
    ax=[]
    for rb in (0.715,0.520,0.325):     # rows raised; gives charts more height below
        for c in range(3): ax.append(fig.add_axes([0.04+c*0.322,rb,0.30,0.165]))
    return ax
def _charts(fig,nc):
    # charts align to the card columns above them
    out=[]
    if nc==3:
        xs=[0.04,0.362,0.684]; w=0.30
    else:
        xs=[0.04,0.522]; w=0.462
    for c in range(nc): out.append(fig.add_axes([xs[c],0.065,w,0.185]))
    return out
def _png(fig):
    buf=io.BytesIO(); fig.savefig(buf,format='png',facecolor=BG); plt.close(fig); return buf.getvalue()

def render_report(payload, mode):
    d=json.loads(payload) if isinstance(payload,str) else payload
    weekly = (mode=='weekly')
    n = 5 if weekly else 7
    title = 'WEEKLY CS REPORT' if weekly else 'CUSTOMER SUCCESS REPORT'
    period = _split(d,'period_label')[0]
    span = 'LAST 5 WEEKS' if weekly else 'LAST 7 DAYS'
    starts = list(reversed(_split(d,'period_start')[:n]))
    labels = [ (s.split('-')[2]+'/'+s.split('-')[1]) if len(s.split('-'))==3 else s for s in starts ]
    tiers=[('Total',''),('L1','_L1'),('L2','_L2')]
    def tat_series(key):
        return _series_gated(d,key,'tat_n',1500,n) if weekly else _series(d,key,n)
    def csat_series(key,nk):
        return _series_gated(d,key,nk,20,n) if weekly else _series(d,key,n)
    out=[]

    # ---- §1 Volume & Automation ----
    fig=_fig(); _masthead(fig,title,'VOLUME & AUTOMATION',period); ax=_cards(fig)
    for r,(tn,sfx) in enumerate(tiers):
        totk='closed' if not sfx else 'total'+sfx
        owk='overwatch_total' if not sfx else 'overwatch'+sfx
        huk='human_total' if not sfx else 'human'+sfx
        owp=_pct_of(d,owk,totk,0); owp1=_pct_of(d,owk,totk,1)
        hup=_pct_of(d,huk,totk,0); hup1=_pct_of(d,huk,totk,1)
        c,t=_delta(_y(d,totk),_yprev(d,totk),'neutral'); _card(ax[r*3+0],tn+' Tickets',_fmt_int(_y(d,totk)),'handled',c,t)
        c,t=_delta(owp,owp1,'up_good');   _card(ax[r*3+1],tn+' Overwatch',_fmt_pct(owp),_fmt_int(_y(d,owk))+' tickets',c,t)
        c,t=_delta(hup,hup1,'down_good'); _card(ax[r*3+2],tn+' Human',_fmt_pct(hup),_fmt_int(_y(d,huk))+' tickets',c,t)
    cax=_charts(fig,3)
    _chart(cax[0],labels,[{'data':_series(d,'pct_overwatch',n),'color':BLUE,'label':'OW%'},{'data':_series(d,'pct_human',n),'color':PINK,'label':'Human%'}],'%',0,100,title='OVERALL')
    for i,sfx,nm in [(1,'_L1','L1'),(2,'_L2','L2')]:
        ow=[100.0*a/b if (a and b) else None for a,b in zip(_nums(d,'overwatch'+sfx)[:n],_nums(d,'total'+sfx)[:n])][::-1]
        hu=[100.0*a/b if (a and b) else None for a,b in zip(_nums(d,'human'+sfx)[:n],_nums(d,'total'+sfx)[:n])][::-1]
        _chart(cax[i],labels,[{'data':ow,'color':BLUE,'label':'OW%'},{'data':hu,'color':PINK,'label':'Human%'}],'%',0,100,title=nm)
    fig.text(0.04,0.30,'— OW% vs HUMAN% · '+span,color=SUB,fontsize=9,family='monospace')
    out.append(('Volume & Automation',_png(fig)))

    # ---- §2 Resolution TAT ----
    fig=_fig(); _masthead(fig,title,'RESOLUTION TAT · P50 / P75 / P90',period); ax=_cards(fig)
    cols=[('Created→OW','ow'),('Esc→Human FRT','hufrt'),('Created→Human FRT','frt')]
    for r,(tn,sfx) in enumerate(tiers):
        for cc,(cn,ph) in enumerate(cols): _tat_card(ax[r*3+cc],d,tn+' · '+cn,ph,sfx)
    cax=_charts(fig,3)
    _chart(cax[0],labels,[{'data':tat_series('ow_p50'),'color':BLUE}],'m',0,title='CREATED → OW')
    _chart(cax[1],labels,[{'data':tat_series('hufrt_p50'),'color':AMBERC}],'m',0,title='ESC → HUMAN FRT')
    _chart(cax[2],labels,[{'data':tat_series('frt_p50'),'color':PINK}],'m',0,title='CREATED → HUMAN FRT')
    fig.text(0.04,0.30,'— P50 TREND · '+span,color=SUB,fontsize=9,family='monospace')
    out.append(('Resolution TAT',_png(fig)))

    # ---- §3 CSAT & Reopen ----
    fig=_fig(); _masthead(fig,title,'CSAT & REOPEN',period); ax=_cards(fig)
    def csat_latest(pk,nk):
        arr=_nums(d,pk); na=_nums(d,nk)
        for i,v in enumerate(arr):
            if v is not None:
                prev=next((arr[j] for j in range(i+1,len(arr)) if arr[j] is not None),None)
                return v,prev,(na[i] if i<len(na) else None)
        return None,None,None
    def csat_window(pk,nk,start,days):
        # pool GOOD/BAD counts across a window (don't average daily %): pct = sum(pos)/sum(n)
        pos=_nums(d,pk); na=_nums(d,nk); P=0.0; N=0
        for i in range(start, min(start+days, len(na))):
            nv=na[i] if i<len(na) else None
            pv=pos[i] if i<len(pos) else None
            if nv and pv is not None: P+=pv/100.0*nv; N+=nv
        return (100.0*P/N if N>0 else None), N
    cdefs=[('CSAT Overall','csat_pos','csat_n'),('OW CSAT','csat_pos_ow','csat_n_ow'),('Human CSAT','csat_pos_hu','csat_n_hu'),
           ('Human CSAT','csat_pos_hu','csat_n_hu'),('L1 Human CSAT','csat_pos_hu_L1','csat_n_hu_L1'),('L2 Human CSAT','csat_pos_hu_L2','csat_n_hu_L2')]
    for i,(lab,pk,nk) in enumerate(cdefs):
        if weekly:
            v,prev,nv=csat_latest(pk,nk); sub=_fmt_int(nv)+' responses'
        else:
            # daily: single-day CSAT is too thin -> pool the last 7 days, delta vs prior 7
            v,nv=csat_window(pk,nk,0,7); prev,_=csat_window(pk,nk,7,7); sub=_fmt_int(nv)+' responses · last 7 days'
        c,t=_delta(v,prev,'up_good')
        _card(ax[i],lab,_fmt_pct(v),sub,c,t)
    def reopen_window(nk,dk,start,days):
        # pool reopened count / total handled across a window: rate = sum(reopened)/sum(total)
        num=_nums(d,nk); den=_nums(d,dk); N=0; D=0
        for i in range(start, min(start+days, len(num))):
            nv=num[i] if i<len(num) else None; dv=den[i] if i<len(den) else None
            if nv is not None and dv: N+=nv; D+=dv
        return (100.0*N/D if D>0 else None), N
    reo=[('Reopen Overall','reopen_rate','reopen_n','closed'),('L1 Reopen','reopen_rate_L1','reopen_n_L1','total_L1'),('L2 Reopen','reopen_rate_L2','reopen_n_L2','total_L2')]
    for i,(lab,rk,nk,dk) in enumerate(reo):
        if weekly:
            val=_y(d,rk); prev=_yprev(d,rk); nv=_y(d,nk); sub=_fmt_int(nv)+' tickets'
        else:
            val,nv=reopen_window(nk,dk,0,7); prev,_=reopen_window(nk,dk,7,7); sub=_fmt_int(nv)+' reopened · last 7 days'
        c,t=_delta(val,prev,'down_good')
        _card(ax[6+i],lab,_fmt_pct(val),sub,c,t)
    cax=_charts(fig,2)
    _chart(cax[0],labels,[{'data':csat_series('csat_pos','csat_n'),'color':GREENC,'label':'Overall'},{'data':csat_series('csat_pos_ow','csat_n_ow'),'color':BLUE,'label':'OW'},{'data':csat_series('csat_pos_hu','csat_n_hu'),'color':PINK,'label':'Human'}],'%',0,100,title='CSAT % POSITIVE')
    _chart(cax[1],labels,[{'data':_series(d,'reopen_rate',n),'color':AMBERC,'label':'Overall'},{'data':_series(d,'reopen_rate_L1',n),'color':BLUE,'label':'L1'},{'data':_series(d,'reopen_rate_L2',n),'color':PURPLE,'label':'L2'}],'%',0,title='REOPEN RATE')
    fig.text(0.04,0.30,'— TRENDS · '+span,color=SUB,fontsize=9,family='monospace')
    out.append(('CSAT & Reopen',_png(fig)))
    return out

def report_caption(payload, mode):
    d=json.loads(payload) if isinstance(payload,str) else payload
    head='WEEKLY CUSTOMER SUCCESS REPORT' if mode=='weekly' else 'CUSTOMER SUCCESS REPORT'
    return (':bar_chart: *%s — %s*\n' % (head,_split(d,'period_label')[0])
            + 'Total *%s* tickets handled · Overwatch *%s* / Human *%s* · CSAT *%s* · Reopen *%s*'
              % (_fmt_int(_y(d,'closed')),_fmt_pct(_y(d,'pct_overwatch')),_fmt_pct(_y(d,'pct_human')),_fmt_pct(_y(d,'csat_pos')),_fmt_pct(_y(d,'reopen_rate'))))

def slack_upload_v2(token, channel, images, comment):
    # images: list of (title, png_bytes). 3-step files_upload_v2 via stdlib urllib.
    def _api(method, fields, files=None):
        url='https://slack.com/api/'+method
        if files is None:
            data=urllib.parse.urlencode(fields).encode()
            req=urllib.request.Request(url,data=data,headers={'Authorization':'Bearer '+token})
            return json.loads(urllib.request.urlopen(req,timeout=60).read().decode())
    file_ids=[]
    for title,blob in images:
        meta=_api('files.getUploadURLExternal',{'filename':title+'.png','length':len(blob)})
        if not meta.get('ok'): raise Exception('getUploadURL: '+str(meta.get('error')))
        req=urllib.request.Request(meta['upload_url'],data=blob,headers={'Content-Type':'application/octet-stream'})
        urllib.request.urlopen(req,timeout=60).read()
        file_ids.append({'id':meta['file_id'],'title':title})
    data=urllib.parse.urlencode({'channel_id':channel,'initial_comment':comment,'files':json.dumps(file_ids)}).encode()
    req=urllib.request.Request('https://slack.com/api/files.completeUploadExternal',data=data,headers={'Authorization':'Bearer '+token})
    res=json.loads(urllib.request.urlopen(req,timeout=60).read().decode())
    if not res.get('ok'): raise Exception('completeUpload: '+str(res.get('error')))
    return res


def run_cs_report(**context):
    client = get_bigquery_client()
    rows = list(client.query(QUERY).result())
    if not rows:
        raise Exception('query returned no payload row')
    r0 = rows[0]
    payload = r0['payload'] if isinstance(r0, dict) else r0.payload
    images = render_report(payload, MODE)
    caption = report_caption(payload, MODE)
    slack_upload_v2(SLACK_TOKEN, SLACK_CHANNEL, images, caption)
    logger.info('CS report %s posted to %s', MODE, SLACK_CHANNEL)

default_args = {
    'owner': 'cs_team', 'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 1, 1, tz='Asia/Kolkata'),
    'email_on_failure': False, 'email_on_retry': False,
    'retries': 2, 'retry_delay': timedelta(minutes=3),
}
dag = DAG('cs_report_daily', default_args=default_args,
    description='Daily CS Success Report (3 image sections) rendered in Python and posted to Slack',
    schedule_interval='0 11 * * *', catchup=False,
    tags=['slack','analytics','cs_metrics','reporting','images'])
PythonOperator(task_id='build_and_post_cs_report_daily', python_callable=run_cs_report, dag=dag)
