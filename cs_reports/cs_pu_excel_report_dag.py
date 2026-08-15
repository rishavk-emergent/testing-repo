"""
CS Power-User Success Report — EXCEL to Slack (weekly, IST)  [replace-in-place, dependency-free .xlsx]

Every Monday builds ONE .xlsx (5 WoW-trend tabs: ALL power users + cohorts A/B/C/D) for the LTV
power-user cohorts and keeps a single copy in a Slack channel: it deletes last week's file+message, uploads the
fresh one, and (once the bot has pins:write) pins the new one / unpins the old. The "which file to
delete" is never guessed — the previous file_id + message ts are stored in an Airflow Variable.

WHERE THE LOGIC LIVES — all numbers + all config come from Redash; the DAG only fetches/renders/posts:
  * CONFIG  #44390  — channel_id / trigger_hour / weekly_day / series_query_id /
                      file_title / do_pin. Edit here, no code push.
  * DATA    series_query_id   (#44374)               — 8-week WoW series, all metrics, TAT p75.

.xlsx is written with a tiny stdlib OOXML writer (zipfile + XML) — no openpyxl/pandas needed, so it
runs on Composer regardless of installed packages (same "no external deps" spirit as the PNG DAGs).

Pinning: needs Slack scope `pins:write` on the "Daily Report on prod-traj-error" bot. Until granted,
pins.add returns missing_scope and is swallowed (file still posts+replaces); the day the scope lands,
set do_pin=TRUE in #44390 and it auto-pins with no code change.

Schedule: ticks hourly ('0 * * * *' IST); fires only at config trigger_hour on weekly_day.
CS_PU_EXCEL_FORCE_RUN=1 bypasses the gate; CS_PU_EXCEL_SLACK_CHANNEL overrides the channel (tests).
Ships paused.
"""
from datetime import timedelta
import io, os, json, time, zipfile, datetime, logging, urllib.request, urllib.parse
from xml.sax.saxutils import escape

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from utils.slack import RedashClient
from utils.slack.slack_config import REDASH_API_KEY, REDASH_BASE_URL, SLACK_BOT_TOKEN_ALERTS as SLACK_TOKEN

logger = logging.getLogger(__name__)

# ==================== CONFIG ====================
CONFIG_QUERY_ID = 44390
ENV_CHANNEL = os.getenv('CS_PU_EXCEL_SLACK_CHANNEL')       # test override; prod uses config row
FORCE_RUN = os.getenv('CS_PU_EXCEL_FORCE_RUN') == '1'      # bypass the trigger gate
STATE_VAR = 'PU_EXCEL_STATE'                               # Airflow Variable: {channel,file_id,ts}

# ==================== STDLIB .xlsx WRITER (no deps) ====================
STYLE = {'def': 0, 'bold': 1, 'title': 2, 'section': 3, 'header': 4, 'tier': 5,
         'num': 6, 'pct': 7, 'int': 8, 'boldint': 9, 'dgood': 10, 'dbad': 11, 'dneut': 12, 'textr': 13}
_STYLES_XML = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
    '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
    '<numFmts count="4"><numFmt numFmtId="164" formatCode="#,##0.0"/><numFmt numFmtId="165" formatCode="0.0%"/>'
    '<numFmt numFmtId="166" formatCode="#,##0"/><numFmt numFmtId="167" formatCode="+0%;\\-0%"/></numFmts>'
    '<fonts count="6">'
    '<font><sz val="11"/><name val="Calibri"/></font>'
    '<font><b/><sz val="11"/><name val="Calibri"/></font>'
    '<font><b/><sz val="11"/><color rgb="FFFFFFFF"/><name val="Calibri"/></font>'
    '<font><b/><sz val="11"/><color rgb="FF1A7F47"/><name val="Calibri"/></font>'
    '<font><b/><sz val="11"/><color rgb="FFC23934"/><name val="Calibri"/></font>'
    '<font><sz val="11"/><color rgb="FF6B7684"/><name val="Calibri"/></font></fonts>'
    '<fills count="5"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill>'
    '<fill><patternFill patternType="solid"><fgColor rgb="FFDCE6F1"/></patternFill></fill>'
    '<fill><patternFill patternType="solid"><fgColor rgb="FF2E5A88"/></patternFill></fill>'
    '<fill><patternFill patternType="solid"><fgColor rgb="FFEAF0F7"/></patternFill></fill></fills>'
    '<borders count="1"><border/></borders>'
    '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
    '<cellXfs count="14">'
    '<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>'
    '<xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/>'
    '<xf numFmtId="0" fontId="2" fillId="3" borderId="0" xfId="0" applyFont="1" applyFill="1"><alignment vertical="center"/></xf>'
    '<xf numFmtId="0" fontId="2" fillId="3" borderId="0" xfId="0" applyFont="1" applyFill="1"/>'
    '<xf numFmtId="0" fontId="1" fillId="2" borderId="0" xfId="0" applyFont="1" applyFill="1"><alignment horizontal="right" wrapText="1"/></xf>'
    '<xf numFmtId="0" fontId="1" fillId="4" borderId="0" xfId="0" applyFont="1" applyFill="1"/>'
    '<xf numFmtId="164" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"><alignment horizontal="right"/></xf>'
    '<xf numFmtId="165" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"><alignment horizontal="right"/></xf>'
    '<xf numFmtId="166" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"><alignment horizontal="right"/></xf>'
    '<xf numFmtId="166" fontId="1" fillId="0" borderId="0" xfId="0" applyNumberFormat="1" applyFont="1"><alignment horizontal="right"/></xf>'
    '<xf numFmtId="167" fontId="3" fillId="0" borderId="0" xfId="0" applyNumberFormat="1" applyFont="1"><alignment horizontal="right"/></xf>'
    '<xf numFmtId="167" fontId="4" fillId="0" borderId="0" xfId="0" applyNumberFormat="1" applyFont="1"><alignment horizontal="right"/></xf>'
    '<xf numFmtId="167" fontId="5" fillId="0" borderId="0" xfId="0" applyNumberFormat="1" applyFont="1"><alignment horizontal="right"/></xf>'
    '<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"><alignment horizontal="right"/></xf>'
    '</cellXfs><cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles></styleSheet>')


def _colref(c):
    s = ''
    while c > 0:
        c, r = divmod(c - 1, 26); s = chr(65 + r) + s
    return s


class Sheet:
    def __init__(self, name):
        self.name = name; self.rows = {}; self.merges = []; self.widths = {}; self.rowopts = {}

    def rowopt(self, r, level=0, hidden=False, collapsed=False):
        self.rowopts[r] = (level, hidden, collapsed)

    def cell(self, r, c, value, style='def'):
        self.rows.setdefault(r, {})[c] = (value, style)

    def merge(self, r, c1, c2):
        self.merges.append('%s%d:%s%d' % (_colref(c1), r, _colref(c2), r))

    def width(self, c, w):
        self.widths[c] = w

    def xml(self):
        cols = ''
        if self.widths:
            cols = '<cols>' + ''.join('<col min="%d" max="%d" width="%g" customWidth="1"/>' % (c, c, w)
                                      for c, w in sorted(self.widths.items())) + '</cols>'
        body = []
        for r in sorted(self.rows):
            cells = []
            for c in sorted(self.rows[r]):
                v, st = self.rows[r][c]; s = STYLE[st]; ref = '%s%d' % (_colref(c), r)
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    cells.append('<c r="%s" s="%d"><v>%r</v></c>' % (ref, s, v))
                else:
                    cells.append('<c r="%s" s="%d" t="inlineStr"><is><t xml:space="preserve">%s</t></is></c>'
                                 % (ref, s, escape('' if v is None else str(v))))
            lv, hd, cl = self.rowopts.get(r, (0, False, False))
            attr = (' outlineLevel="%d"' % lv if lv else '') + (' hidden="1"' if hd else '') + (' collapsed="1"' if cl else '')
            body.append('<row r="%d"%s>%s</row>' % (r, attr, ''.join(cells)))
        mg = ('<mergeCells count="%d">%s</mergeCells>' % (len(self.merges),
              ''.join('<mergeCell ref="%s"/>' % m for m in self.merges))) if self.merges else ''
        outlined = any(o[0] for o in self.rowopts.values())
        pr = '<sheetPr><outlinePr summaryBelow="0"/></sheetPr>' if outlined else ''
        fmt = '<sheetFormatPr defaultRowHeight="15" outlineLevelRow="1"/>' if outlined else ''
        return ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                + pr + fmt + cols + '<sheetData>' + ''.join(body) + '</sheetData>' + mg + '</worksheet>')


def write_xlsx(sheets, path):
    ct = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
          '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
          '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
          '<Default Extension="xml" ContentType="application/xml"/>'
          '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
          '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
          + ''.join('<Override PartName="/xl/worksheets/sheet%d.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>' % (i + 1) for i in range(len(sheets)))
          + '</Types>')
    rels = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>')
    wb = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
          '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
          'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>'
          + ''.join('<sheet name="%s" sheetId="%d" r:id="rId%d"/>' % (escape(s.name), i + 1, i + 1) for i, s in enumerate(sheets))
          + '</sheets></workbook>')
    wb_rels = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
               '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
               + ''.join('<Relationship Id="rId%d" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet%d.xml"/>' % (i + 1, i + 1) for i in range(len(sheets)))
               + '<Relationship Id="rId%d" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>' % (len(sheets) + 1)
               + '</Relationships>')
    with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED) as z:
        z.writestr('[Content_Types].xml', ct)
        z.writestr('_rels/.rels', rels)
        z.writestr('xl/workbook.xml', wb)
        z.writestr('xl/_rels/workbook.xml.rels', wb_rels)
        z.writestr('xl/styles.xml', _STYLES_XML)
        for i, s in enumerate(sheets):
            z.writestr('xl/worksheets/sheet%d.xml' % (i + 1), s.xml())
    return path

# ==================== REPORT BUILDER ====================
TIERS = [('TOTAL', 'TOTAL (L1+L2+Expo)'), ('L1', 'L1'), ('L2', 'L2 (excl. Expo)'), ('Expo', 'Expo (L2 · mobile)')]
# one tab per cohort: (cohort key, tab name <=31 chars, title-line sub)
COH_TABS = [('ALL', 'Power Users (LTV≥$300)', 'POWER USERS  ·  LTV ≥ $300'), ('A', 'Cohort A (LTV≥$10k)', 'COHORT A  ·  LTV ≥ $10k'),
            ('B', 'Cohort B (LTV $5k-$10k)', 'COHORT B  ·  LTV $5k – $10k'), ('C', 'Cohort C (LTV $1k-$5k)', 'COHORT C  ·  LTV $1k – $5k'),
            ('D', 'Cohort D (LTV $300-$1k)', 'COHORT D  ·  LTV $300 – $1k')]
# always-visible headline rows, then the collapsed (click-to-expand) detail rows
VISIBLE = [('Incoming', 'incoming', 'int'), ('p75 Created→Human frt', 'frt_p75', 'hm'),
           ('CSAT % Human', 'csat_pos_hu', 'pct'), ('Reopen Rate %', 'reopen_rate', 'pct')]
DETAIL = [('Closed', 'closed', 'int'), ('p75 Created→OW', 'ow_p75', 'hm'),
          ('p75 Esc→Human FRT', 'hufrt_p75', 'hm'), ('Responses (n)', 'csat_n_hu', 'int'), ('Reopens (n)', 'reopen_n', 'int')]


def _n(v):
    return None if v in (None, '') else float(v)


def _hm(v):
    if v is None:
        return '–'
    m = int(round(v)); h, mm = divmod(m, 60)
    return ('%dh %dm' % (h, mm)) if h else ('%dm' % mm)


def _dpct(cur, prev):
    cur, prev = _n(cur), _n(prev)
    if cur is None or prev in (None, 0):
        return None
    return (cur - prev) / prev


def _put(ws, r, c, v, fmt):
    if v is None:
        ws.cell(r, c, '–', 'textr'); return
    if fmt == 'int':
        ws.cell(r, c, int(round(v)), 'int')
    elif fmt == 'pct':
        ws.cell(r, c, round(v / 100.0, 4), 'pct')
    elif fmt == 'hm':
        ws.cell(r, c, _hm(v), 'textr')
    else:
        ws.cell(r, c, round(v, 1), 'num')


def _delta(ws, r, c, cur, prev, direction):
    pc = _dpct(cur, prev)
    if pc is None:
        return
    if direction == 'neutral' or round(pc * 100) == 0:
        st = 'dneut'
    else:
        good = (direction == 'low' and pc < 0) or (direction == 'high' and pc > 0)
        st = 'dgood' if good else 'dbad'
    ws.cell(r, c, round(pc, 3), st)


def _week_range(wk):
    s = datetime.date.fromisoformat(wk); e = s + datetime.timedelta(days=6)
    return '%s - %s' % (s.strftime('%d/%m'), e.strftime('%d/%m'))


def build_report(series_rows, path):
    """One tab per cohort; each tab has the 4 tier blocks as a WoW trend (latest week left).
    Headline rows always show; the 5 detail rows are collapsed behind an Excel row-group (click + to expand)."""
    weeks = sorted({r['wk'] for r in series_rows}, reverse=True)
    wlabel = {wk: _week_range(wk) for wk in weeks}
    idx = {(r['cohort'], r['tier'], r['wk']): r for r in series_rows}
    tcol = 1 + len(weeks)
    sheets = []
    for cid, tab, title in COH_TABS:
        ws = Sheet(tab)
        ws.cell(1, 1, 'POWER-USER SUCCESS REPORT — %s  ·  WoW (latest left) · TAT p75 · click + on the left to expand detail rows' % title, 'title')
        ws.merge(1, 1, tcol); ws.width(1, 24)
        for i in range(2, tcol + 1):
            ws.width(i, 13)
        rr = 3
        for tid, tlabel in TIERS:
            ws.cell(rr, 1, tlabel, 'section'); ws.merge(rr, 1, tcol); rr += 1
            ws.cell(rr, 1, 'Metric \\ Week', 'header')
            for j, wk in enumerate(weeks, 2):
                ws.cell(rr, j, wlabel[wk], 'header')
            rr += 1
            for k, (mlabel, mkey, mfmt) in enumerate(VISIBLE):
                ws.cell(rr, 1, mlabel, 'bold')
                for j, wk in enumerate(weeks, 2):
                    _put(ws, rr, j, _n(idx.get((cid, tid, wk), {}).get(mkey)), mfmt)
                if k == len(VISIBLE) - 1:
                    ws.rowopt(rr, collapsed=True)   # summary row sits above the collapsed detail group
                rr += 1
            for mlabel, mkey, mfmt in DETAIL:
                ws.cell(rr, 1, mlabel, 'bold')
                for j, wk in enumerate(weeks, 2):
                    _put(ws, rr, j, _n(idx.get((cid, tid, wk), {}).get(mkey)), mfmt)
                ws.rowopt(rr, level=1, hidden=True)   # collapsed by default
                rr += 1
            rr += 1
        sheets.append(ws)
    return write_xlsx(sheets, path)

# ==================== SLACK (stdlib) ====================
def _sl(method, data, get=False):
    url = 'https://slack.com/api/' + method
    if get:
        url += '?' + urllib.parse.urlencode(data)
        req = urllib.request.Request(url, headers={'Authorization': 'Bearer ' + SLACK_TOKEN})
    else:
        req = urllib.request.Request(url, data=urllib.parse.urlencode(data).encode(),
                                     headers={'Authorization': 'Bearer ' + SLACK_TOKEN})
    return json.loads(urllib.request.urlopen(req, timeout=60).read().decode())


def _ts_for_file(channel, file_id):
    for _ in range(6):
        h = _sl('conversations.history', {'channel': channel, 'limit': 10}, get=True)
        for m in h.get('messages', []):
            if any(f.get('id') == file_id for f in m.get('files', [])):
                return m.get('ts')
        time.sleep(1)
    return None


def _upload(channel, path, title, comment):
    ln = os.path.getsize(path)
    g = _sl('files.getUploadURLExternal', {'filename': title + '.xlsx', 'length': ln})
    if not g.get('ok'):
        raise Exception('getUploadURL: ' + str(g.get('error')))
    urllib.request.urlopen(urllib.request.Request(g['upload_url'], data=open(path, 'rb').read(),
                           headers={'Content-Type': 'application/octet-stream'}), timeout=60).read()
    c = _sl('files.completeUploadExternal', {'channel_id': channel, 'initial_comment': comment,
            'files': json.dumps([{'id': g['file_id'], 'title': title}])})
    if not c.get('ok'):
        raise Exception('completeUpload: ' + str(c.get('error')))
    ts = None
    for f in c.get('files', []):
        for scope in ('public', 'private'):
            for arr in (f.get('shares', {}).get(scope, {}) or {}).values():
                for m in arr:
                    ts = m.get('ts')
    return g['file_id'], ts or _ts_for_file(channel, g['file_id'])


def _try(method, data):
    try:
        r = _sl(method, data)
        if not r.get('ok'):
            logger.info('[pu_excel] %s -> %s (non-fatal)', method, r.get('error'))
        return r.get('ok')
    except Exception as e:
        logger.info('[pu_excel] %s raised %s (non-fatal)', method, e)
        return False

# ==================== TASK ====================
def run_pu_excel(**context):
    redash = RedashClient(api_key=REDASH_API_KEY, base_url=REDASH_BASE_URL)
    cfg = (redash.fetch_query_results(query_id=CONFIG_QUERY_ID, max_retries=3) or [{}])[0]
    channel = ENV_CHANNEL or cfg.get('channel_id')
    now = pendulum.now('Asia/Kolkata')
    if not FORCE_RUN:
        try:
            th = int(cfg.get('trigger_hour', 8))
        except Exception:
            th = 8
        if now.hour != th:
            logger.info('CS PU Excel: hour %d != trigger_hour %d -> skip', now.hour, th); return
        if now.format('dddd').lower() != str(cfg.get('weekly_day', 'monday')).strip().lower():
            logger.info('CS PU Excel: %s != weekly_day %s -> skip', now.format('dddd'), cfg.get('weekly_day')); return
    series = redash.fetch_query_results(query_id=int(cfg['series_query_id']), max_retries=3) or []
    if not series:
        raise Exception('empty series rows from query %s' % cfg.get('series_query_id'))
    title = cfg.get('file_title', 'Power-User Success Report (weekly)')
    do_pin = str(cfg.get('do_pin', True)).lower() not in ('false', '0', 'no', '')
    path = '/tmp/pu_excel_report.xlsx'
    build_report(series, path)

    # ---- replace-in-place using our own state (Airflow Variable) ----
    prev = None
    try:
        prev = json.loads(Variable.get(STATE_VAR))
    except Exception:
        prev = None
    if prev:
        pch = prev.get('channel') or channel   # clean up in the channel it was posted to (may differ if channel changed)
        if do_pin and prev.get('ts'):
            _try('pins.remove', {'channel': pch, 'timestamp': prev['ts']})
        if prev.get('ts'):
            _try('chat.delete', {'channel': pch, 'ts': prev['ts']})
        if prev.get('file_id'):
            _try('files.delete', {'file': prev['file_id']})

    period = now.subtract(days=now.weekday() + 1).start_of('week')  # last full week's Monday
    comment = ':bar_chart: *Power-User Success Report (weekly)* — week of %s. Always the latest here.' % period.format('DD/MM/YYYY')
    file_id, ts = _upload(channel, path, title, comment)
    pinned = _try('pins.add', {'channel': channel, 'timestamp': ts}) if (do_pin and ts) else False
    Variable.set(STATE_VAR, json.dumps({'channel': channel, 'file_id': file_id, 'ts': ts}))
    logger.info('CS PU Excel: posted file=%s ts=%s pinned=%s channel=%s', file_id, ts, pinned, channel)

# ==================== DAG ====================
_default_args = {'owner': 'cs_team', 'depends_on_past': False,
                 'start_date': pendulum.datetime(2025, 1, 1, tz='Asia/Kolkata'),
                 'email_on_failure': False, 'email_on_retry': False, 'retries': 1, 'retry_delay': timedelta(minutes=2)}
dag = DAG('cs_pu_excel_report_weekly', default_args=_default_args,
          description='Weekly Power-User Success Report as a replace-in-place Excel pinned in Slack (LTV cohorts)',
          schedule_interval='0 * * * *', catchup=False, is_paused_upon_creation=True,
          tags=['slack', 'cs_reports', 'cs_team', 'power_users', 'excel'])
PythonOperator(task_id='build_and_post_pu_excel', python_callable=run_pu_excel, dag=dag)
