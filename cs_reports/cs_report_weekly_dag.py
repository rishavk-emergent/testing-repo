"""
CS Customer Success Report - WEEKLY (pure-Python image report).
Standalone DAG: queries BigQuery (Trinity v_overwatch_runs), last 5 completed weeks,
renders 3 report sections as PNGs with Pillow, posts via files_upload_v2. No HTML/Chrome/git.
Schedule: 0 10 * * 1 (Mon 10 AM IST).
Slack: posts via utils.slack.slack_config.SLACK_BOT_TOKEN_ALERTS (shared bot, provisioned
in Composer). Channel via CS_REPORT_SLACK_CHANNEL env (default cs-associates).
PyPI deps: Pillow only (already in Composer). Fonts (DejaVu TTFs) bundled under ./fonts.
"""
from datetime import timedelta
import logging, os
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.slack.bigquery_client import get_bigquery_client
from utils.slack.slack_config import SLACK_BOT_TOKEN_ALERTS as SLACK_TOKEN

logger = logging.getLogger(__name__)
MODE = 'weekly'
SLACK_CHANNEL = os.getenv('CS_REPORT_SLACK_CHANNEL', 'C0B075CBPS7')

QUERY = r'''-- Customer Success Report — WEEKLY payload (Trinity-sourced §1: v_overwatch_runs).
-- Volume/automation/tier from v_overwatch_runs (ow_escalation_required / ow_support_level, MAX).
-- TAT/CSAT/reopen via v_tickets.atlas_id. Last 5 completed weeks (floor 2026-05-04); older
-- weeks are sparse until Trinity history backfills (charts gate on tat_n).
DECLARE last_week_start DATE DEFAULT DATE_SUB(DATE_TRUNC(CURRENT_DATE('Asia/Kolkata'), WEEK(MONDAY)), INTERVAL 1 WEEK);
DECLARE start_week DATE DEFAULT GREATEST(DATE_SUB(last_week_start, INTERVAL 9 WEEK), DATE_TRUNC(DATE('2026-05-04'), WEEK(MONDAY)));
WITH
runs AS (
  SELECT ticket_id AS cid,
    LOGICAL_OR(COALESCE(SAFE_CAST(ow_escalation_required AS BOOL),FALSE)) AS esc,
    MAX(ow_support_level) AS tier,
    DATE(MIN(created_at),'Asia/Kolkata') AS day,
    ANY_VALUE(request_payload_customer_email) AS email
  FROM `emergent-default.trinity_database.v_overwatch_runs`
  WHERE ticket_id IS NOT NULL AND created_at IS NOT NULL GROUP BY ticket_id
),
spam AS (SELECT email, day FROM runs WHERE email IS NOT NULL AND email!='' GROUP BY email, day HAVING COUNT(DISTINCT cid)>10),
vt AS (SELECT _id, atlas_id FROM `emergent-default.trinity_database.v_tickets` WHERE atlas_id IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY source_timestamp DESC)=1),
cpst AS (SELECT id, number FROM `emergent-default.analytics.closed_pending_support_tickets` QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY sync_timestamp DESC)=1),
reop AS (SELECT ticket_number, reopen_flag FROM `emergent-default.analytics.support_tickets_tat` QUALIFY ROW_NUMBER() OVER (PARTITION BY ticket_number ORDER BY timestamp DESC)=1),
csat AS (
  SELECT vt2.atlas_id AS ticket_id, CASE WHEN cs.rating='GOOD' THEN 1 WHEN cs.rating='BAD' THEN 0 END AS s
  FROM `emergent-default.trinity_database.v_csat_surveys` cs
  JOIN (SELECT _id,atlas_id FROM `emergent-default.trinity_database.v_tickets` WHERE atlas_id IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY source_timestamp DESC)=1) vt2 ON vt2._id=cs.ticket_id
  WHERE cs.rating IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY vt2.atlas_id ORDER BY cs.rated_at DESC)=1
),
tat AS (
  SELECT vt3.atlas_id AS ticket_id, t.time1 AS ow_t, t.time3 AS hufrt_raw, (t.time1+t.time3) AS frt_raw
  FROM (SELECT ticket_id,time1,time3 FROM `emergent-default.support.trinity_ticket_tat` QUALIFY ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY sync_timestamp DESC)=1) t
  JOIN (SELECT _id,atlas_id FROM `emergent-default.trinity_database.v_tickets` WHERE atlas_id IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY source_timestamp DESC)=1) vt3 ON vt3._id=t.ticket_id
),
c AS (
  SELECT DATE_TRUNC(r.day, WEEK(MONDAY)) AS wk, COALESCE(r.tier,'untagged') AS tier, (NOT r.esc) AS ow_solved,
    csat.s AS csat_score, IFNULL(reop.reopen_flag,0)=1 AS is_reopened,
    tat.ow_t AS ow_t, IF(r.esc, tat.hufrt_raw, NULL) AS hufrt_t, IF(r.esc, tat.frt_raw, NULL) AS frt_t
  FROM runs r
  LEFT JOIN spam sp ON sp.email=r.email AND sp.day=r.day
  LEFT JOIN vt ON vt._id=r.cid
  LEFT JOIN cpst ON cpst.id=vt.atlas_id
  LEFT JOIN reop ON reop.ticket_number=cpst.number
  LEFT JOIN csat ON csat.ticket_id=vt.atlas_id
  LEFT JOIN tat  ON tat.ticket_id=vt.atlas_id
  WHERE r.day BETWEEN start_week AND DATE_ADD(last_week_start, INTERVAL 6 DAY) AND sp.email IS NULL
),
m AS (
  SELECT wk AS day,
    CONCAT(FORMAT_DATE('%d/%m', wk),'-',FORMAT_DATE('%d/%m', DATE_ADD(wk, INTERVAL 6 DAY))) AS period_label,
    FORMAT_DATE('%Y-%m-%d', wk) AS period_start,
    COUNT(*) AS closed, COUNTIF(ow_solved) AS overwatch_total, COUNTIF(NOT ow_solved) AS human_total,
    ROUND(100.0*COUNTIF(ow_solved)/NULLIF(COUNT(*),0),1) AS pct_overwatch,
    ROUND(100.0*COUNTIF(NOT ow_solved)/NULLIF(COUNT(*),0),1) AS pct_human,
    COUNTIF(tier='L1') AS total_L1, COUNTIF(ow_solved AND tier='L1') AS overwatch_L1, COUNTIF(NOT ow_solved AND tier='L1') AS human_L1,
    COUNTIF(tier='L2') AS total_L2, COUNTIF(ow_solved AND tier='L2') AS overwatch_L2, COUNTIF(NOT ow_solved AND tier='L2') AS human_L2,
    COUNTIF(ow_t IS NOT NULL) AS tat_n,
    ROUND(APPROX_QUANTILES(ow_t,100)[OFFSET(50)],1) AS ow_p50, ROUND(APPROX_QUANTILES(ow_t,100)[OFFSET(75)],1) AS ow_p75, ROUND(APPROX_QUANTILES(ow_t,100)[OFFSET(90)],1) AS ow_p90,
    ROUND(APPROX_QUANTILES(IF(tier='L1',ow_t,NULL),100)[OFFSET(50)],1) AS ow_p50_L1, ROUND(APPROX_QUANTILES(IF(tier='L1',ow_t,NULL),100)[OFFSET(75)],1) AS ow_p75_L1, ROUND(APPROX_QUANTILES(IF(tier='L1',ow_t,NULL),100)[OFFSET(90)],1) AS ow_p90_L1,
    ROUND(APPROX_QUANTILES(IF(tier='L2',ow_t,NULL),100)[OFFSET(50)],1) AS ow_p50_L2, ROUND(APPROX_QUANTILES(IF(tier='L2',ow_t,NULL),100)[OFFSET(75)],1) AS ow_p75_L2, ROUND(APPROX_QUANTILES(IF(tier='L2',ow_t,NULL),100)[OFFSET(90)],1) AS ow_p90_L2,
    ROUND(APPROX_QUANTILES(hufrt_t,100)[OFFSET(50)],1) AS hufrt_p50, ROUND(APPROX_QUANTILES(hufrt_t,100)[OFFSET(75)],1) AS hufrt_p75, ROUND(APPROX_QUANTILES(hufrt_t,100)[OFFSET(90)],1) AS hufrt_p90,
    ROUND(APPROX_QUANTILES(IF(tier='L1',hufrt_t,NULL),100)[OFFSET(50)],1) AS hufrt_p50_L1, ROUND(APPROX_QUANTILES(IF(tier='L1',hufrt_t,NULL),100)[OFFSET(75)],1) AS hufrt_p75_L1, ROUND(APPROX_QUANTILES(IF(tier='L1',hufrt_t,NULL),100)[OFFSET(90)],1) AS hufrt_p90_L1,
    ROUND(APPROX_QUANTILES(IF(tier='L2',hufrt_t,NULL),100)[OFFSET(50)],1) AS hufrt_p50_L2, ROUND(APPROX_QUANTILES(IF(tier='L2',hufrt_t,NULL),100)[OFFSET(75)],1) AS hufrt_p75_L2, ROUND(APPROX_QUANTILES(IF(tier='L2',hufrt_t,NULL),100)[OFFSET(90)],1) AS hufrt_p90_L2,
    ROUND(APPROX_QUANTILES(frt_t,100)[OFFSET(50)],1) AS frt_p50, ROUND(APPROX_QUANTILES(frt_t,100)[OFFSET(75)],1) AS frt_p75, ROUND(APPROX_QUANTILES(frt_t,100)[OFFSET(90)],1) AS frt_p90,
    ROUND(APPROX_QUANTILES(IF(tier='L1',frt_t,NULL),100)[OFFSET(50)],1) AS frt_p50_L1, ROUND(APPROX_QUANTILES(IF(tier='L1',frt_t,NULL),100)[OFFSET(75)],1) AS frt_p75_L1, ROUND(APPROX_QUANTILES(IF(tier='L1',frt_t,NULL),100)[OFFSET(90)],1) AS frt_p90_L1,
    ROUND(APPROX_QUANTILES(IF(tier='L2',frt_t,NULL),100)[OFFSET(50)],1) AS frt_p50_L2, ROUND(APPROX_QUANTILES(IF(tier='L2',frt_t,NULL),100)[OFFSET(75)],1) AS frt_p75_L2, ROUND(APPROX_QUANTILES(IF(tier='L2',frt_t,NULL),100)[OFFSET(90)],1) AS frt_p90_L2,
    ROUND(100.0*COUNTIF(csat_score=1)/NULLIF(COUNTIF(csat_score IS NOT NULL),0),1) AS csat_pos, COUNTIF(csat_score IS NOT NULL) AS csat_n,
    ROUND(100.0*COUNTIF(ow_solved AND csat_score=1)/NULLIF(COUNTIF(ow_solved AND csat_score IS NOT NULL),0),1) AS csat_pos_ow, COUNTIF(ow_solved AND csat_score IS NOT NULL) AS csat_n_ow,
    ROUND(100.0*COUNTIF(NOT ow_solved AND csat_score=1)/NULLIF(COUNTIF(NOT ow_solved AND csat_score IS NOT NULL),0),1) AS csat_pos_hu, COUNTIF(NOT ow_solved AND csat_score IS NOT NULL) AS csat_n_hu,
    ROUND(100.0*COUNTIF(NOT ow_solved AND tier='L1' AND csat_score=1)/NULLIF(COUNTIF(NOT ow_solved AND tier='L1' AND csat_score IS NOT NULL),0),1) AS csat_pos_hu_L1, COUNTIF(NOT ow_solved AND tier='L1' AND csat_score IS NOT NULL) AS csat_n_hu_L1,
    ROUND(100.0*COUNTIF(NOT ow_solved AND tier='L2' AND csat_score=1)/NULLIF(COUNTIF(NOT ow_solved AND tier='L2' AND csat_score IS NOT NULL),0),1) AS csat_pos_hu_L2, COUNTIF(NOT ow_solved AND tier='L2' AND csat_score IS NOT NULL) AS csat_n_hu_L2,
    ROUND(100.0*COUNTIF(is_reopened)/NULLIF(COUNT(*),0),1) AS reopen_rate, COUNTIF(is_reopened) AS reopen_n,
    ROUND(100.0*COUNTIF(is_reopened AND tier='L1')/NULLIF(COUNTIF(tier='L1'),0),1) AS reopen_rate_L1, COUNTIF(is_reopened AND tier='L1') AS reopen_n_L1,
    ROUND(100.0*COUNTIF(is_reopened AND tier='L2')/NULLIF(COUNTIF(tier='L2'),0),1) AS reopen_rate_L2, COUNTIF(is_reopened AND tier='L2') AS reopen_n_L2,
    ROUND(100.0*COUNTIF(is_reopened AND ow_solved)/NULLIF(COUNTIF(ow_solved),0),1) AS reopen_rate_ow, COUNTIF(is_reopened AND ow_solved) AS reopen_n_ow,
    ROUND(100.0*COUNTIF(is_reopened AND NOT ow_solved)/NULLIF(COUNTIF(NOT ow_solved),0),1) AS reopen_rate_hu, COUNTIF(is_reopened AND NOT ow_solved) AS reopen_n_hu,
    ROUND(100.0*COUNTIF(is_reopened AND NOT ow_solved AND tier='L1')/NULLIF(COUNTIF(NOT ow_solved AND tier='L1'),0),1) AS reopen_rate_hu_L1, COUNTIF(is_reopened AND NOT ow_solved AND tier='L1') AS reopen_n_hu_L1,
    ROUND(100.0*COUNTIF(is_reopened AND NOT ow_solved AND tier='L2')/NULLIF(COUNTIF(NOT ow_solved AND tier='L2'),0),1) AS reopen_rate_hu_L2, COUNTIF(is_reopened AND NOT ow_solved AND tier='L2') AS reopen_n_hu_L2
  FROM c GROUP BY wk
)
SELECT TO_JSON_STRING(STRUCT(
  STRING_AGG(period_label,',' ORDER BY day DESC) AS period_label,
  STRING_AGG(period_start,',' ORDER BY day DESC) AS period_start,
  STRING_AGG(CAST(closed AS STRING),',' ORDER BY day DESC) AS closed,
  STRING_AGG(CAST(overwatch_total AS STRING),',' ORDER BY day DESC) AS overwatch_total,
  STRING_AGG(CAST(human_total AS STRING),',' ORDER BY day DESC) AS human_total,
  STRING_AGG(IFNULL(CAST(pct_overwatch AS STRING),''),',' ORDER BY day DESC) AS pct_overwatch,
  STRING_AGG(IFNULL(CAST(pct_human AS STRING),''),',' ORDER BY day DESC) AS pct_human,
  STRING_AGG(CAST(total_L1 AS STRING),',' ORDER BY day DESC) AS total_L1,
  STRING_AGG(CAST(overwatch_L1 AS STRING),',' ORDER BY day DESC) AS overwatch_L1,
  STRING_AGG(CAST(human_L1 AS STRING),',' ORDER BY day DESC) AS human_L1,
  STRING_AGG(CAST(total_L2 AS STRING),',' ORDER BY day DESC) AS total_L2,
  STRING_AGG(CAST(overwatch_L2 AS STRING),',' ORDER BY day DESC) AS overwatch_L2,
  STRING_AGG(CAST(human_L2 AS STRING),',' ORDER BY day DESC) AS human_L2,
  STRING_AGG(CAST(tat_n AS STRING),',' ORDER BY day DESC) AS tat_n,
  STRING_AGG(IFNULL(CAST(ow_p50 AS STRING),''),',' ORDER BY day DESC) AS ow_p50,
  STRING_AGG(IFNULL(CAST(ow_p75 AS STRING),''),',' ORDER BY day DESC) AS ow_p75,
  STRING_AGG(IFNULL(CAST(ow_p90 AS STRING),''),',' ORDER BY day DESC) AS ow_p90,
  STRING_AGG(IFNULL(CAST(ow_p50_L1 AS STRING),''),',' ORDER BY day DESC) AS ow_p50_L1,
  STRING_AGG(IFNULL(CAST(ow_p75_L1 AS STRING),''),',' ORDER BY day DESC) AS ow_p75_L1,
  STRING_AGG(IFNULL(CAST(ow_p90_L1 AS STRING),''),',' ORDER BY day DESC) AS ow_p90_L1,
  STRING_AGG(IFNULL(CAST(ow_p50_L2 AS STRING),''),',' ORDER BY day DESC) AS ow_p50_L2,
  STRING_AGG(IFNULL(CAST(ow_p75_L2 AS STRING),''),',' ORDER BY day DESC) AS ow_p75_L2,
  STRING_AGG(IFNULL(CAST(ow_p90_L2 AS STRING),''),',' ORDER BY day DESC) AS ow_p90_L2,
  STRING_AGG(IFNULL(CAST(hufrt_p50 AS STRING),''),',' ORDER BY day DESC) AS hufrt_p50,
  STRING_AGG(IFNULL(CAST(hufrt_p75 AS STRING),''),',' ORDER BY day DESC) AS hufrt_p75,
  STRING_AGG(IFNULL(CAST(hufrt_p90 AS STRING),''),',' ORDER BY day DESC) AS hufrt_p90,
  STRING_AGG(IFNULL(CAST(hufrt_p50_L1 AS STRING),''),',' ORDER BY day DESC) AS hufrt_p50_L1,
  STRING_AGG(IFNULL(CAST(hufrt_p75_L1 AS STRING),''),',' ORDER BY day DESC) AS hufrt_p75_L1,
  STRING_AGG(IFNULL(CAST(hufrt_p90_L1 AS STRING),''),',' ORDER BY day DESC) AS hufrt_p90_L1,
  STRING_AGG(IFNULL(CAST(hufrt_p50_L2 AS STRING),''),',' ORDER BY day DESC) AS hufrt_p50_L2,
  STRING_AGG(IFNULL(CAST(hufrt_p75_L2 AS STRING),''),',' ORDER BY day DESC) AS hufrt_p75_L2,
  STRING_AGG(IFNULL(CAST(hufrt_p90_L2 AS STRING),''),',' ORDER BY day DESC) AS hufrt_p90_L2,
  STRING_AGG(IFNULL(CAST(frt_p50 AS STRING),''),',' ORDER BY day DESC) AS frt_p50,
  STRING_AGG(IFNULL(CAST(frt_p75 AS STRING),''),',' ORDER BY day DESC) AS frt_p75,
  STRING_AGG(IFNULL(CAST(frt_p90 AS STRING),''),',' ORDER BY day DESC) AS frt_p90,
  STRING_AGG(IFNULL(CAST(frt_p50_L1 AS STRING),''),',' ORDER BY day DESC) AS frt_p50_L1,
  STRING_AGG(IFNULL(CAST(frt_p75_L1 AS STRING),''),',' ORDER BY day DESC) AS frt_p75_L1,
  STRING_AGG(IFNULL(CAST(frt_p90_L1 AS STRING),''),',' ORDER BY day DESC) AS frt_p90_L1,
  STRING_AGG(IFNULL(CAST(frt_p50_L2 AS STRING),''),',' ORDER BY day DESC) AS frt_p50_L2,
  STRING_AGG(IFNULL(CAST(frt_p75_L2 AS STRING),''),',' ORDER BY day DESC) AS frt_p75_L2,
  STRING_AGG(IFNULL(CAST(frt_p90_L2 AS STRING),''),',' ORDER BY day DESC) AS frt_p90_L2,
  STRING_AGG(IFNULL(CAST(csat_pos AS STRING),''),',' ORDER BY day DESC) AS csat_pos,
  STRING_AGG(CAST(csat_n AS STRING),',' ORDER BY day DESC) AS csat_n,
  STRING_AGG(IFNULL(CAST(csat_pos_ow AS STRING),''),',' ORDER BY day DESC) AS csat_pos_ow,
  STRING_AGG(CAST(csat_n_ow AS STRING),',' ORDER BY day DESC) AS csat_n_ow,
  STRING_AGG(IFNULL(CAST(csat_pos_hu AS STRING),''),',' ORDER BY day DESC) AS csat_pos_hu,
  STRING_AGG(CAST(csat_n_hu AS STRING),',' ORDER BY day DESC) AS csat_n_hu,
  STRING_AGG(IFNULL(CAST(csat_pos_hu_L1 AS STRING),''),',' ORDER BY day DESC) AS csat_pos_hu_L1,
  STRING_AGG(CAST(csat_n_hu_L1 AS STRING),',' ORDER BY day DESC) AS csat_n_hu_L1,
  STRING_AGG(IFNULL(CAST(csat_pos_hu_L2 AS STRING),''),',' ORDER BY day DESC) AS csat_pos_hu_L2,
  STRING_AGG(CAST(csat_n_hu_L2 AS STRING),',' ORDER BY day DESC) AS csat_n_hu_L2,
  STRING_AGG(IFNULL(CAST(reopen_rate AS STRING),''),',' ORDER BY day DESC) AS reopen_rate,
  STRING_AGG(CAST(reopen_n AS STRING),',' ORDER BY day DESC) AS reopen_n,
  STRING_AGG(IFNULL(CAST(reopen_rate_L1 AS STRING),''),',' ORDER BY day DESC) AS reopen_rate_L1,
  STRING_AGG(CAST(reopen_n_L1 AS STRING),',' ORDER BY day DESC) AS reopen_n_L1,
  STRING_AGG(IFNULL(CAST(reopen_rate_L2 AS STRING),''),',' ORDER BY day DESC) AS reopen_rate_L2,
  STRING_AGG(CAST(reopen_n_L2 AS STRING),',' ORDER BY day DESC) AS reopen_n_L2,
  STRING_AGG(IFNULL(CAST(reopen_rate_ow AS STRING),''),',' ORDER BY day DESC) AS reopen_rate_ow,
  STRING_AGG(CAST(reopen_n_ow AS STRING),',' ORDER BY day DESC) AS reopen_n_ow,
  STRING_AGG(IFNULL(CAST(reopen_rate_hu AS STRING),''),',' ORDER BY day DESC) AS reopen_rate_hu,
  STRING_AGG(CAST(reopen_n_hu AS STRING),',' ORDER BY day DESC) AS reopen_n_hu,
  STRING_AGG(IFNULL(CAST(reopen_rate_hu_L1 AS STRING),''),',' ORDER BY day DESC) AS reopen_rate_hu_L1,
  STRING_AGG(CAST(reopen_n_hu_L1 AS STRING),',' ORDER BY day DESC) AS reopen_n_hu_L1,
  STRING_AGG(IFNULL(CAST(reopen_rate_hu_L2 AS STRING),''),',' ORDER BY day DESC) AS reopen_rate_hu_L2,
  STRING_AGG(CAST(reopen_n_hu_L2 AS STRING),',' ORDER BY day DESC) AS reopen_n_hu_L2
)) AS payload FROM m;'''

# ===== CS Success Report renderer (pure Python: Pillow -> PNG -> Slack) =====
# Shared, self-contained block embedded verbatim into both DAG files.
# Public entry points: render_report(payload, mode) -> [(title, png_bytes)]
#                      slack_upload_v2(token, channel, images, comment)
import io, json, os, urllib.request, urllib.parse
from PIL import Image, ImageDraw, ImageFont

# Pure-Pillow renderer (no matplotlib). Fonts: DejaVu TTFs bundled under ./fonts so
# the only runtime dependency is Pillow (already present in Composer). System and
# PIL-default fallbacks are tried if the bundled files are ever missing.
SS = 2                       # supersample: render at SSx, LANCZOS-downscale in _png (anti-aliasing)
BASE_W, BASE_H = 1650, 1050
W, H = BASE_W * SS, BASE_H * SS
PT = (150 / 72.0) * SS       # matplotlib-point (at 150 dpi) -> pixels, keeps prior sizing

_HERE = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else '.'
_FONT_CANDS = {
    'serif_bold': ['DejaVuSerif-Bold.ttf', '/usr/share/fonts/truetype/dejavu/DejaVuSerif-Bold.ttf', '/System/Library/Fonts/Supplemental/Georgia Bold.ttf'],
    'mono':       ['DejaVuSansMono.ttf', '/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf', '/System/Library/Fonts/Menlo.ttc'],
    'sans':       ['DejaVuSans.ttf', '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf', '/System/Library/Fonts/Helvetica.ttc'],
    'sans_bold':  ['DejaVuSans-Bold.ttf', '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf', '/System/Library/Fonts/Supplemental/Arial Bold.ttf'],
}
_font_cache = {}
def _font(family, pt):
    px = max(8, int(round(pt * PT)))
    key = (family, px)
    if key in _font_cache: return _font_cache[key]
    for cand in _FONT_CANDS.get(family, []):
        path = cand if os.path.isabs(cand) else os.path.join(_HERE, 'fonts', cand)
        try:
            f = ImageFont.truetype(path, px); _font_cache[key] = f; return f
        except Exception:
            continue
    f = ImageFont.load_default(); _font_cache[key] = f; return f

def _rgb(c):
    if isinstance(c, str): return c
    return tuple(int(round(max(0.0, min(1.0, v)) * 255)) for v in c[:3])

class _Canvas:
    def __init__(self):
        self.img = Image.new('RGB', (W, H), BG)
        self.d = ImageDraw.Draw(self.img)
    def text(self, xf, yf, s, color=None, fontsize=9, family='sans', fontweight=None, va='baseline', ha='left'):
        fam = 'serif_bold' if (family == 'serif' and fontweight == 'bold') else ('mono' if family in ('monospace', 'mono') else ('sans_bold' if fontweight == 'bold' else 'sans'))
        anchor = ('r' if ha == 'right' else ('m' if ha == 'center' else 'l')) + ('m' if va == 'center' else 's')
        self.d.text((xf * W, (1 - yf) * H), s, font=_font(fam, fontsize), fill=_rgb(color if color is not None else SUB), anchor=anchor)

class _Cell:
    __slots__ = ('d', 'x', 'y', 'w', 'h')
    def __init__(self, d, x, y, w, h): self.d = d; self.x = x; self.y = y; self.w = w; self.h = h

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

def _card(cell,label,value,sub,dcol,dtext):
    d=cell.d; x,y,w,h=cell.x,cell.y,cell.w,cell.h
    d.rounded_rectangle([x,y,x+w,y+h],radius=12*SS,fill=CARD,outline=BORDER,width=SS)
    dc=_rgb(dcol)
    d.rectangle([x+10*SS,y+8*SS,x+w-10*SS,y+13*SS],fill=dc)              # thin accent line
    tx=x+0.06*w
    d.text((tx,y+0.23*h),label.upper(),font=_font('mono',8),fill=_rgb(SUB),anchor='lm')
    d.text((tx,y+0.50*h),value,font=_font('serif_bold',19),fill=_rgb(INK),anchor='lm')
    d.text((tx,y+0.74*h),sub,font=_font('sans',8.5),fill=_rgb(SUB),anchor='lm')
    d.text((tx,y+0.88*h),dtext,font=_font('mono',8.5),fill=dc,anchor='lm')
def _tat_card(cell,dd,label,ph,sfx):
    d=cell.d; x,y,w,h=cell.x,cell.y,cell.w,cell.h
    col,dt=_delta(_y(dd,ph+'_p75'+sfx),_yprev(dd,ph+'_p75'+sfx),'down_good')
    cc=_rgb(col)
    d.rounded_rectangle([x,y,x+w,y+h],radius=12*SS,fill=CARD,outline=BORDER,width=SS)
    d.rectangle([x+10*SS,y+8*SS,x+w-10*SS,y+13*SS],fill=cc)
    tx=x+0.06*w
    d.text((tx,y+0.22*h),label.upper(),font=_font('mono',8),fill=_rgb(SUB),anchor='lm')
    for i,(nm,key) in enumerate([('p50','_p50'),('p75','_p75'),('p90','_p90')]):
        px=x+(0.10+i*0.30)*w
        d.text((px,y+0.46*h),nm,font=_font('mono',7.5),fill=_rgb(SUB),anchor='lm')
        d.text((px,y+0.64*h),_fmt_tat(_y(dd,ph+key+sfx)),font=_font('serif_bold',13),fill=_rgb(INK),anchor='lm')
    d.text((tx,y+0.87*h),dt,font=_font('mono',8),fill=cc,anchor='lm')
def _chart(cell,labels,series_list,unit='',ymin=0,ymax=None,title=None):
    d=cell.d; x,y,w,h=cell.x,cell.y,cell.w,cell.h
    d.rectangle([x,y,x+w,y+h],fill=CARD,outline=BORDER,width=SS)
    px0=x+38*SS; px1=x+w-34*SS; py0=y+14*SS; py1=y+h-20*SS
    allv=[v for s in series_list for v in s['data'] if v is not None]
    if allv:
        lo=ymin if ymin is not None else min(allv)*0.9
        hi=ymax if ymax is not None else max(allv)*1.4
        if hi<=lo: hi=lo+1
    else:
        lo,hi=0,1
    def Y(v): return py1-(v-lo)/(hi-lo)*(py1-py0)
    n=len(labels)
    def X(i): return px0+(px1-px0)*((i/(n-1)) if n>1 else 0.5)
    ticks=[0,25,50,75,100] if ymax==100 else [lo+(hi-lo)*t for t in (0,0.25,0.5,0.75,1.0)]
    ftk=_font('mono',7)
    for tv in ticks:
        if tv<lo-1e-9 or tv>hi+1e-9: continue
        yy=Y(tv)
        d.line([(px0,yy),(px1,yy)],fill=_rgb(BORDER),width=SS)
        d.text((px0-5*SS,yy),'%d'%round(tv),font=ftk,fill=_rgb(SUB),anchor='rm')
    for s in series_list:
        col=_rgb(s['color']); pts=[(X(i),Y(v)) for i,v in enumerate(s['data']) if v is not None]
        if len(pts)>=2: d.line(pts,fill=col,width=2*SS,joint='curve')
        for p in pts: d.ellipse([p[0]-3*SS,p[1]-3*SS,p[0]+3*SS,p[1]+3*SS],fill=col)
        if pts:
            lastv=[v for v in s['data'] if v is not None][-1]
            d.text((pts[-1][0]+5*SS,pts[-1][1]),('%.0f'%lastv)+unit,font=_font('sans_bold',7),fill=col,anchor='lm')
    fxl=_font('mono',7)
    for i,lab in enumerate(labels):
        d.text((X(i),py1+9*SS),lab,font=fxl,fill=_rgb(SUB),anchor='mm')
    if title:
        d.text((px0,y-4*SS),title,font=_font('mono',7.5),fill=_rgb(SUB),anchor='ls')
        d.text((px1,y-4*SS),unit if unit else '%',font=_font('mono',7.5),fill=_rgb(SUB),anchor='rs')
    if any(s.get('label') for s in series_list):
        lx=px0+2*SS; ly=py0+7*SS; fl=_font('sans',6.5)
        for s in series_list:
            if not s.get('label'): continue
            col=_rgb(s['color'])
            d.line([(lx,ly),(lx+12*SS,ly)],fill=col,width=2*SS)
            d.ellipse([lx+4*SS,ly-2*SS,lx+8*SS,ly+2*SS],fill=col)
            d.text((lx+16*SS,ly),s['label'],font=fl,fill=_rgb(SUB),anchor='lm')
            lx+=16*SS+int(fl.getlength(s['label']))+14*SS

def _fig(): return _Canvas()
def _masthead(fig,title,sub,period):
    fig.text(0.04,0.965,title,color=INK,fontsize=22,family='serif',fontweight='bold',va='center')
    fig.text(0.04,0.93,sub,color=SUB,fontsize=11,family='monospace',va='center')
    fig.text(0.96,0.95,period,color=SUB,fontsize=12,family='monospace',va='center',ha='right')
    yy=(1-0.905)*H
    fig.d.line([(0.04*W,yy),(0.96*W,yy)],fill=_rgb(INK),width=2*SS)
def _cards(fig, rbs=(0.715,0.520,0.325), h=0.165, ncols=3):
    cells=[]
    x0,pitch,w = (0.04,0.322,0.30) if ncols==3 else (0.04,0.24,0.222)
    for rb in rbs:
        top=(1-(rb+h))*H; hpx=h*H
        for c in range(ncols):
            cells.append(_Cell(fig.d,(x0+c*pitch)*W,top,w*W,hpx))
    return cells
def _charts(fig,nc,bottom=0.065,h=0.185):
    out=[]; top=(1-(bottom+h))*H; hpx=h*H
    xs,w = ([0.04,0.362,0.684],0.30) if nc==3 else ([0.04,0.522],0.462)
    for c in range(nc): out.append(_Cell(fig.d,xs[c]*W,top,w*W,hpx))
    return out
def _png(fig):
    buf=io.BytesIO()
    fig.img.resize((BASE_W, BASE_H), Image.LANCZOS).save(buf, format='PNG')  # downscale = anti-alias
    return buf.getvalue()

def render_report(payload, mode):
    d=json.loads(payload) if isinstance(payload,str) else payload
    weekly = (mode=='weekly')
    n = 5 if weekly else 7
    title = 'WEEKLY CS REPORT' if weekly else 'CUSTOMER SUCCESS REPORT'
    period = _split(d,'period_label')[0]
    span = 'LAST 5 WEEKS' if weekly else 'LAST 7 DAYS'
    if weekly:
        # weekly: label each point with the week-END date only (e.g. 07/06). The end
        # date is already in the past, so it isn't mistaken for "week start -> today".
        labels = [ (p.split('-')[1] if '-' in p else p) for p in reversed(_split(d,'period_label')[:n]) ]
    else:
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
    fig=_fig(); _masthead(fig,title,'CSAT & REOPEN',period); ax=_cards(fig, ncols=4)
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
    def reopen_window(nk,dk,start,days):
        # pool reopened count / total handled across a window: rate = sum(reopened)/sum(total)
        num=_nums(d,nk); den=_nums(d,dk); N=0; D=0
        for i in range(start, min(start+days, len(num))):
            nv=num[i] if i<len(num) else None; dv=den[i] if i<len(den) else None
            if nv is not None and dv: N+=nv; D+=dv
        return (100.0*N/D if D>0 else None), N
    def _csat_card(axn,lab,pk,nk):
        if weekly:
            v,prev,nv=csat_latest(pk,nk); sub=_fmt_int(nv)+' responses'
        else:  # daily: single-day CSAT too thin -> pool last 7 days
            v,nv=csat_window(pk,nk,0,7); prev,_=csat_window(pk,nk,7,7); sub=_fmt_int(nv)+' responses · last 7 days'
        cc,tt=_delta(v,prev,'up_good'); _card(axn,lab,_fmt_pct(v),sub,cc,tt)
    def _reopen_card(axn,lab,rk,nk,dk):
        if weekly:
            val=_y(d,rk); prev=_yprev(d,rk); nv=_y(d,nk); sub=_fmt_int(nv)+' reopened'
        else:
            val,nv=reopen_window(nk,dk,0,7); prev,_=reopen_window(nk,dk,7,7); sub=_fmt_int(nv)+' reopened · last 7 days'
        cc,tt=_delta(val,prev,'down_good'); _card(axn,lab,_fmt_pct(val),sub,cc,tt)
    # 3 rows x 4 columns — each column is a metric group (transposed)
    col_csat   =[('CSAT Overall','csat_pos','csat_n'),('OW CSAT','csat_pos_ow','csat_n_ow'),('Human CSAT','csat_pos_hu','csat_n_hu')]
    col_csat_t =[('Human CSAT','csat_pos_hu','csat_n_hu'),('L1 Human CSAT','csat_pos_hu_L1','csat_n_hu_L1'),('L2 Human CSAT','csat_pos_hu_L2','csat_n_hu_L2')]
    col_reo    =[('Reopen Overall','reopen_rate','reopen_n','closed'),('OW Reopen','reopen_rate_ow','reopen_n_ow','overwatch_total'),('Human Reopen','reopen_rate_hu','reopen_n_hu','human_total')]
    col_reo_t  =[('Human Reopen','reopen_rate_hu','reopen_n_hu','human_total'),('L1 Human Reopen','reopen_rate_hu_L1','reopen_n_hu_L1','human_L1'),('L2 Human Reopen','reopen_rate_hu_L2','reopen_n_hu_L2','human_L2')]
    for r in range(3):
        _csat_card(ax[r*4+0], *col_csat[r])
        _csat_card(ax[r*4+1], *col_csat_t[r])
        _reopen_card(ax[r*4+2], *col_reo[r])
        _reopen_card(ax[r*4+3], *col_reo_t[r])
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
dag = DAG('cs_report_weekly', default_args=default_args,
    description='Weekly CS Success Report (Trinity-sourced) rendered in Python and posted to Slack',
    schedule_interval='0 10 * * 1', catchup=False,
    is_paused_upon_creation=False,  # deploy active so it fires on the next schedule without a manual unpause
    tags=['slack','analytics','cs_metrics','reporting','images'])
PythonOperator(task_id='build_and_post_cs_report_weekly', python_callable=run_cs_report, dag=dag)
