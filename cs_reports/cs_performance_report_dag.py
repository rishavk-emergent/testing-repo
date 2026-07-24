"""
CS Weekly Performance Report - per-agent email (weekly, IST)  [v2 matrix layout]

Each rostered L1 / L2 agent gets a personal weekly email: (1) Team (their tier), (2) Shift
(their roster group), (3) You, each as a metrics x week matrix (latest week leftmost, small
good/bad arrow on the latest column only); a reopen-snapshot bucket x week matrix + an attached
per-agent reopen dump (.xlsx); and (4) AI notes written by an LLM from the numbers.

WHERE THE LOGIC LIVES - all numbers come from Redash; the DAG only slices, renders, and sends.
  * CORE_QUERY_ID   41791  - Team/Shift/Agent x week wide payload (closed-week, assignee-credited)
  * REOPEN_QUERY_ID 41792  - per reopen-event rows (last-closer attributed) -> buckets + xlsx + issue feed
  * CONFIG_QUERY_ID 41839  - from/reply-to, cc_l1/cc_l2, n_weeks, llm_*, gmail_app_password, ai_rubric
Config-in-Redash: edit the config row to change address / CC / model / app password with no code push.

DELIVERY: Gmail SMTP (smtp.gmail.com:587, app password from config). Each agent gets their own
report; CC by tier (L1 -> cc_l1, L2 Full Stack/Expo -> cc_l2). CS_PERF_DRY_RUN=1 writes HTML+xlsx
to disk instead of sending; CS_PERF_TEST_RECIPIENT routes all to one address (CC suppressed).

Schedule: Monday 07:00 Asia/Kolkata, after the prior Mon-Sun week has closed. Paused on creation.
"""

from datetime import timedelta, date
import logging, os, io, json, re, urllib.request

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.slack import RedashClient
from utils.slack.slack_config import REDASH_API_KEY, REDASH_BASE_URL

logger = logging.getLogger(__name__)

CORE_QUERY_ID     = 41791
REOPEN_QUERY_ID   = 41792
CONFIG_QUERY_ID   = 41839
BASELINES_QUERY_ID= 41854
HOURLY_QUERY_ID   = 38693   # per-agent human closes by (day, hour) IST, last 10 days
HOUR_BURST_BAR    = {'L1': 10, 'L2 Full Stack': 8, 'L2 Expo': 8}   # closes/hr outlier bar (~tier p99)
DRY_RUN         = os.getenv('CS_PERF_DRY_RUN') == '1'
DRY_RUN_DIR     = os.getenv('CS_PERF_DRY_RUN_DIR', '/tmp/cs_perf_out')

# palette / ink
INK='#0f172a'; MUT='#64748b'; FAINT='#94a3b8'; LINE='#eef1f5'; ROW='#f4f6f9'
HL='#eef5fd'; HLB='#d7e6fb'; UP='#15803d'; DOWN='#b91c1c'; GREY='#b3b8c0'
BUCKET_ORDER=['incorrect','incomplete','new_issue','clarification','noise']
BUCKET_NICE={'incorrect':'Incorrect','incomplete':'Incomplete','new_issue':'New issue','clarification':'Clarification','noise':'Noise'}
AVOID_BUCKETS={'incorrect','incomplete'}

# ==================== FETCH ====================
def _redash():
    return RedashClient(api_key=REDASH_API_KEY, base_url=REDASH_BASE_URL)

def fetch_all():
    r=_redash()
    cfg=r.fetch_query_results(query_id=CONFIG_QUERY_ID, max_retries=3)[0]
    core=r.fetch_query_results(query_id=CORE_QUERY_ID, max_retries=3)
    reopen=r.fetch_query_results(query_id=REOPEN_QUERY_ID, max_retries=3)
    baselines=r.fetch_query_results(query_id=BASELINES_QUERY_ID, max_retries=3)
    hourly=r.fetch_query_results(query_id=HOURLY_QUERY_ID, max_retries=3)
    return cfg, core, reopen, baselines, hourly

def baseline_summary(baselines):
    """Turn the [CS Perf] baselines rows into a compact, live summary for the LLM."""
    allrow=next((r for r in baselines if r['tag']=='__ALL__'), None)
    if not allrow or not allrow['n']:
        return {'avoidable_pct_of_all_reopens':None,'tag_bucket_leaders':[]}
    N=allrow['n']; base_av=allrow['avoidable_n']/N
    base_inc=allrow['incorrect_n']/N; base_incmp=allrow['incomplete_n']/N
    tags=[r for r in baselines if r['tag']!='__ALL__' and r['n']]
    def rec(r, lean):
        n=r['n']
        return {'tag':r['tag'],'n':n,'incorrect_pct':round(100*r['incorrect_n']/n,0),
                'incomplete_pct':round(100*r['incomplete_n']/n,0),
                'avoidable_lift':round((r['avoidable_n']/n)/base_av,2) if base_av else None,'leans':lean}
    inc_lead=sorted(tags,key=lambda r:(r['incorrect_n']/r['n'])/base_inc if base_inc else 0,reverse=True)[:8]
    incmp_lead=sorted(tags,key=lambda r:(r['incomplete_n']/r['n'])/base_incmp if base_incmp else 0,reverse=True)[:8]
    return {'avoidable_pct_of_all_reopens':round(100*base_av,1),
            'bucket_mix_pct':{'incorrect':round(100*base_inc,1),'incomplete':round(100*base_incmp,1)},
            'tag_bucket_leaders':[rec(r,'incorrect') for r in inc_lead]+[rec(r,'incomplete') for r in incmp_lead]}

# ==================== WEEK HELPERS ====================
def _week_label(week_start):
    d = week_start if isinstance(week_start, date) else pendulum.parse(str(week_start)).date()
    end = d + timedelta(days=6)
    return '%02d/%02d-%02d/%02d' % (d.day, d.month, end.day, end.month)

def _weeks_meta(core):
    """Return week_idx list oldest->newest and idx->label map from the core rows."""
    seen={}
    for r in core:
        seen[int(r['week_idx'])] = r['week_start']
    idxs=sorted(seen)                      # e.g. [1,2,3,4] (1=latest)
    old_to_new=list(reversed(idxs))        # [4,3,2,1]
    labels={i:_week_label(seen[i]) for i in idxs}
    return old_to_new, labels

# ==================== RENDER (v2 matrix) ====================
def _arrow(cur, prev, direction):
    if prev is None or cur is None: return ''
    d=cur-prev
    if abs(d)<1e-9: return f'<span style="font-size:9px;color:{GREY};margin-left:4px;">&#9644;</span>'
    up=d>0
    if direction=='neutral': col=GREY
    else:
        good=(d<0) if direction=='lower' else (d>0); col=UP if good else DOWN
    return f'<span style="font-size:9px;color:{col};margin-left:4px;">{"&#9650;" if up else "&#9660;"}</span>'

def _fmt_int(v):  return '&ndash;' if v is None else f'{int(round(v)):,}'
def _fmt_m(v):
    if v is None: return '&ndash;'
    t=int(round(v)); h,m=divmod(t,60)
    return (f'{h} hr {m} min' if h and m else (f'{h} hr' if h else f'{m} min'))
def _fmt_pct(v):  return '&ndash;' if v is None else f'{v:g}%'
def _fmt_plain(v):return '&ndash;' if v is None else f'{v:g}'

def _hdr(weeks):
    ths=[f'<th style="padding:7px 8px;border-bottom:2px solid {LINE};text-align:left;font-size:10px;letter-spacing:.4px;text-transform:uppercase;color:{FAINT};font-weight:600;">Metric</th>']
    for i,w in enumerate(weeks):
        newest=i==0; bg=f'background:{HL};' if newest else ''
        lbl=f'<div style="font-size:9px;color:{UP};font-weight:700;">LATEST</div>' if newest else ''
        ths.append(f'<th style="{bg}padding:7px 8px;border-bottom:2px solid {HLB if newest else LINE};text-align:right;font-size:10.5px;color:{MUT if newest else FAINT};font-weight:{"700" if newest else "600"};white-space:nowrap;">{lbl}{w}</th>')
    return f'<tr>{"".join(ths)}</tr>'

def _row(nw, label, primary, direction, pfmt=_fmt_plain, bracket=None, bfmt=_fmt_pct, cmp=None):
    """primary/bracket/cmp are oldest->newest lists (len nw). Displayed latest-left; arrow on latest only."""
    P=list(reversed(primary)); C=list(reversed(cmp if cmp is not None else primary))
    B=list(reversed(bracket)) if bracket is not None else None
    tds=[f'<td style="padding:6px 8px;border-bottom:1px solid {ROW};color:#334155;font-weight:500;letter-spacing:.1px;">{label}</td>']
    for i in range(nw):
        newest=i==0; bg=f'background:{HL};' if newest else ''
        prev=C[i+1] if i+1<nw else None
        br=f' <span style="font-size:10px;color:{FAINT};">({bfmt(B[i])})</span>' if (B is not None and B[i] is not None) else ''
        st=f'{bg}padding:6px 8px;border-bottom:1px solid {ROW};text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums;'
        st+=(f'font-weight:700;color:{INK};font-size:12.5px;' if newest else 'color:#475569;')
        ind=_arrow(C[i],prev,direction) if newest else ''
        tds.append(f'<td style="{st}">{pfmt(P[i])}{br}{ind}</td>')
    return f'<tr>{"".join(tds)}</tr>'

def _sep(nw): return f'<tr><td colspan="{nw+1}" style="padding:3px 0;"></td></tr>'
def _tbl(nw, weeks, rows): return f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;font-size:12px;">{_hdr(weeks)}{"".join(rows)}</table>'
def _sec(num,label,cap): return (f'<div style="font-size:11px;font-weight:700;letter-spacing:1.2px;text-transform:uppercase;color:{MUT};">{num} &middot; {label}</div><div style="font-size:12px;color:{FAINT};margin:2px 0 12px;">{cap}</div>')

def _series(by_idx, order, col):
    """by_idx: {week_idx: row}. order: oldest->newest week_idx list. Returns list of col values (or None)."""
    out=[]
    for i in order:
        r=by_idx.get(i)
        out.append(None if r is None or r.get(col) is None else r[col])
    return out

# ==================== ASSEMBLE (per agent) ====================
def assemble(core, reopen, hourly=None):
    order, labels = _weeks_meta(core)
    weeks_disp=[labels[i] for i in reversed(order)]  # latest-left
    nw=len(order)
    # latest week's 7 calendar dates (Mon..Sun) for the hourly grid
    lws=max(pendulum.parse(str(r['week_start'])).date() for r in core)
    week_dates=[(lws+timedelta(days=i)).isoformat() for i in range(7)]
    hourly_by={}
    for h in (hourly or []):
        hourly_by.setdefault(h['agent_email'], []).append(h)
    # index rows
    agents={}; shifts={}; teams={}
    for r in core:
        s=r['section']; wi=int(r['week_idx'])
        if s=='agent': agents.setdefault(r['agent_email'], {})[wi]=r
        elif s=='shift': shifts.setdefault((r['tier'],r['shift']), {})[wi]=r
        elif s=='team': teams.setdefault(r['tier'], {})[wi]=r
    reo_by_agent={}
    for r in reopen:
        reo_by_agent.setdefault(r['agent_email'], []).append(r)

    payloads=[]
    for email, aidx in agents.items():
        any_row=next(iter(aidx.values()))
        tier=any_row['tier']; shift=any_row['shift']; name=any_row['agent_name']
        tidx=teams.get(tier, {}); sidx=shifts.get((tier,shift), {})
        S=lambda idx,col: _series(idx, order, col)
        payloads.append(dict(
            email=email, name=name, tier=tier, shift=shift, nw=nw, weeks=weeks_disp,
            team={'total':S(tidx,'total'),'ow_n':S(tidx,'ow_n'),'pct_ow':S(tidx,'pct_ow'),'human_n':S(tidx,'human_n'),
                  'ow_p50':S(tidx,'ow_p50'),'hufrt_p50':S(tidx,'hufrt_p50'),'frt_p50':S(tidx,'frt_p50'),
                  'csat_n_ow':S(tidx,'csat_n_ow'),'csat_pos_ow':S(tidx,'csat_pos_ow'),'csat_n_hu':S(tidx,'csat_n_hu'),'csat_pos_hu':S(tidx,'csat_pos_hu'),
                  'reopen_n_ow':S(tidx,'reopen_n_ow'),'reopen_rate_ow':S(tidx,'reopen_rate_ow'),'reopen_n_hu':S(tidx,'reopen_n_hu'),'reopen_rate_hu':S(tidx,'reopen_rate_hu')},
            shift_m={'human_n':S(sidx,'human_n'),'hufrt_p50':S(sidx,'hufrt_p50'),'frt_p50':S(sidx,'frt_p50'),
                     'csat_n_hu':S(sidx,'csat_n_hu'),'csat_pos_hu':S(sidx,'csat_pos_hu'),'reopen_n_hu':S(sidx,'reopen_n_hu'),'reopen_rate_hu':S(sidx,'reopen_rate_hu')},
            you={'human_n':S(aidx,'human_n'),'hufrt_p50':S(aidx,'hufrt_p50'),'frt_p50':S(aidx,'frt_p50'),
                 'csat_n_hu':S(aidx,'csat_n_hu'),'csat_pos_hu':S(aidx,'csat_pos_hu'),'reopen_n_hu':S(aidx,'reopen_n_hu'),'reopen_rate_hu':S(aidx,'reopen_rate_hu')},
            buckets=_bucket_matrix(reo_by_agent.get(email, []), order),
            reopen_events=reo_by_agent.get(email, []),
            week_dates=week_dates, hourly=hourly_by.get(email, []),
        ))
    return payloads, order

def _bucket_matrix(events, order):
    """{bucket: [counts oldest->newest]} + avoidable/notfault/total series."""
    counts={b:{i:0 for i in order} for b in BUCKET_ORDER}
    for e in events:
        b=e.get('bucket'); wi=int(e['week_idx'])
        if b in counts and wi in counts[b]: counts[b][wi]+=1
    ser=lambda d:[d[i] for i in order]
    out={b:ser(counts[b]) for b in BUCKET_ORDER}
    out['avoidable']=[sum(out[b][k] for b in AVOID_BUCKETS) for k in range(len(order))]
    out['notfault']=[sum(out[b][k] for b in BUCKET_ORDER if b not in AVOID_BUCKETS) for k in range(len(order))]
    out['total']=[out['avoidable'][k]+out['notfault'][k] for k in range(len(order))]
    return out

# ==================== HTML EMAIL (per agent) ====================
def build_html(p, ai):
    nw=p['nw']; W=p['weeks']; T=p['team']; S=p['shift_m']; Y=p['you']; B=p['buckets']
    team=_tbl(nw,W,[
        _row(nw,'Total tickets',T['total'],'neutral',_fmt_int),
        _row(nw,'Overwatch tickets',T['ow_n'],'neutral',_fmt_int,bracket=T['pct_ow']),
        _row(nw,'Human tickets',T['human_n'],'neutral',_fmt_int),
        _sep(nw),
        _row(nw,'Created&rarr;OW (med)',T['ow_p50'],'lower',_fmt_m),
        _row(nw,'Escalated&rarr;human FRT',T['hufrt_p50'],'lower',_fmt_m),
        _row(nw,'Created&rarr;human FRT',T['frt_p50'],'lower',_fmt_m),
        _sep(nw),
        _row(nw,'OW CSAT % pos. (total resp.)',T['csat_pos_ow'],'higher',_fmt_pct,bracket=T['csat_n_ow'],bfmt=_fmt_int,cmp=T['csat_pos_ow']),
        _row(nw,'Human CSAT % pos. (total resp.)',T['csat_pos_hu'],'higher',_fmt_pct,bracket=T['csat_n_hu'],bfmt=_fmt_int,cmp=T['csat_pos_hu']),
        _row(nw,'OW reopen',T['reopen_n_ow'],'lower',_fmt_int,bracket=T['reopen_rate_ow'],cmp=T['reopen_rate_ow']),
        _row(nw,'Human reopen',T['reopen_n_hu'],'lower',_fmt_int,bracket=T['reopen_rate_hu'],cmp=T['reopen_rate_hu']),
    ])
    def human_tbl(D):
        return _tbl(nw,W,[
            _row(nw,'Human closes',D['human_n'],'neutral',_fmt_int),
            _row(nw,'Escalated&rarr;human FRT',D['hufrt_p50'],'lower',_fmt_m),
            _row(nw,'Created&rarr;human FRT',D['frt_p50'],'lower',_fmt_m),
            _row(nw,'Human CSAT % pos. (total resp.)',D['csat_pos_hu'],'higher',_fmt_pct,bracket=D['csat_n_hu'],bfmt=_fmt_int,cmp=D['csat_pos_hu']),
            _row(nw,'Human reopen',D['reopen_n_hu'],'lower',_fmt_int,bracket=D['reopen_rate_hu'],cmp=D['reopen_rate_hu']),
        ])
    buckets=_tbl(nw,W,[
        _row(nw,'&#128308; Incorrect',B['incorrect'],'lower',_fmt_plain),
        _row(nw,'&#128992; Incomplete',B['incomplete'],'lower',_fmt_plain),
        _row(nw,'&nbsp;&nbsp;<b>Avoidable</b>',B['avoidable'],'lower',_fmt_plain),
        _sep(nw),
        _row(nw,'New issue',B['new_issue'],'neutral',_fmt_plain),
        _row(nw,'Clarification',B['clarification'],'neutral',_fmt_plain),
        _row(nw,'Noise',B['noise'],'neutral',_fmt_plain),
        _row(nw,'&nbsp;&nbsp;Not your fault',B['notfault'],'neutral',_fmt_plain),
        _sep(nw),
        _row(nw,'<b>Total reopen events</b>',B['total'],'lower',_fmt_plain),
    ])
    ab=lambda color,t,body:f'<div style="margin-bottom:14px;"><div style="font-size:12px;font-weight:700;color:{INK};margin-bottom:5px;"><span style="display:inline-block;width:7px;height:7px;border-radius:50%;background:{color};margin-right:7px;"></span>{t}</div>{body}</div>'
    ul=lambda items:'<ul style="margin:0;padding-left:18px;">'+''.join(f'<li style="font-size:12.5px;color:#334155;line-height:1.6;margin-bottom:3px;">{i}</li>' for i in items)+'</ul>'
    para=lambda t:f'<p style="font-size:12.5px;color:#334155;line-height:1.6;margin:0;">{t}</p>'
    first=p['name'].split()[0].lower()
    dump_name=f"reopen_dump_{first}.pdf"; hourly_name=f"hourly_closes_{first}.pdf"
    snote=_shift_note(p)
    return f"""<!doctype html><html><body style="margin:0;padding:24px 0;background:#eef1f5;font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;">
<table role="presentation" align="center" width="640" cellpadding="0" cellspacing="0" style="width:640px;max-width:640px;margin:0 auto;background:#ffffff;border-radius:14px;overflow:hidden;">
  <tr><td style="background:#1e293b;padding:24px 26px;">
    <div style="font-size:19px;font-weight:650;color:#ffffff;">Your Weekly Report</div>
    <div style="font-size:13px;color:#c7d2e0;margin-top:6px;line-height:1.5;">This is our understanding of your work this week, meant to help, not to grade. If any number looks off or you'd like something changed, just <b style="color:#fff;">reply to this email</b>.</div>
    <div style="margin-top:14px;font-size:12.5px;color:#e2e8f0;">
      <span style="background:rgba(255,255,255,.16);border-radius:6px;padding:2px 9px;margin-right:6px;">{p['tier']}</span>
      <span style="background:rgba(255,255,255,.16);border-radius:6px;padding:2px 9px;margin-right:8px;">Shift {p['shift']} IST</span>
      <b style="color:#fff;">{p['name']}</b></div>
  </td></tr>
  <tr><td style="padding:20px 26px;">{_sec('1','Team Performance',f"Your tier &middot; <b>{p['tier']}</b> &middot; closed-week &middot; assignee-credited &middot; arrow vs prior week")}{team}</td></tr>
  <tr><td style="padding:20px 26px;border-top:1px solid {LINE};">{_sec('2','Shift Performance',f"Your shift &middot; <b>{p['tier']} &middot; {p['shift']}</b> &middot; roster-aggregate &middot; human-only")}{human_tbl(S)}</td></tr>
  <tr><td style="padding:20px 26px;border-top:1px solid {LINE};">{_sec('3','Your Performance',f"{p['name']} &middot; human-only")}{human_tbl(Y)}
    <div style="margin-top:18px;">{_sec('','Reopen snapshots','per reopen event, credited to you as last-closer (+1 each time) &middot; bucket &times; week')}{buckets}</div>
    <div style="margin-top:12px;font-size:11.5px;color:{MUT};background:#f8fafc;border:1px dashed #dbe2ea;border-radius:8px;padding:9px 12px;">&#128206; <b>Attached:</b> <i>{dump_name}</i> (every reopen by bucket + ticket link) &middot; <i>{hourly_name}</i> (your hour &times; day closures for the week).</div>
  </td></tr>
  <tr><td style="padding:20px 26px;border-top:1px solid {LINE};background:#f8fafc;">{_sec('4','AI Notes','Auto-generated from the tables above plus issue-type breakdown &middot; a read on the trend, not a verdict.')}
    {ab('#2a78d6','The trend', para(ai['trend']))}
    {ab('#15803d','What went well', ul(ai['strengths']))}
    {ab('#b91c1c','Where to tighten', ul(ai['weaknesses']))}
    {ab('#eda100','Suggested next steps', ul(ai['actions']))}
    {ab('#64748b','Shift activity', ul(snote)) if snote else ''}
  </td></tr>
  <tr><td style="padding:18px 26px 26px;border-top:1px solid {LINE};text-align:center;font-size:12.5px;color:{MUT};line-height:1.6;">
    Just reply to this email with any questions, suggestions, or anything else.</td></tr>
</table></body></html>"""

# ==================== XLSX (per-agent reopen dump, latest week) ====================
def _pil_font(sz, bold=False):
    from PIL import ImageFont
    paths=(['/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf','/System/Library/Fonts/Supplemental/Arial Bold.ttf','/Library/Fonts/Arial Bold.ttf'] if bold
           else ['/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf','/System/Library/Fonts/Supplemental/Arial.ttf','/Library/Fonts/Arial.ttf'])
    for pth in paths:
        try: return ImageFont.truetype(pth, sz)
        except Exception: pass
    try: return ImageFont.load_default(sz)
    except Exception: return ImageFont.load_default()

def _grid_pdf(title, subtitle, headers, rows, widths, aligns, bold_rows=None, sep_rows=None):
    """Render a simple table to a single-page PDF via Pillow (Composer-safe; no extra deps).
    rows: list of cell-lists. bold_rows/sep_rows: sets of row indices."""
    from PIL import Image, ImageDraw
    s=2; INK=(15,23,42); MUT=(100,116,139); TXT=(51,65,85); GRID=(226,230,236); HEADBG=(244,246,249)
    fn=_pil_font(11*s); fnb=_pil_font(11*s,True); ft=_pil_font(15*s,True); fsub=_pil_font(10*s)
    bold_rows=bold_rows or set(); sep_rows=sep_rows or set()
    colx=[6];
    for w in widths: colx.append(colx[-1]+w*s)
    W=colx[-1]+6; rh=int(24*s)
    ytop=int(12*s + (18*s if subtitle else 6*s) + 8*s)
    H=ytop + rh*(len(rows)+1) + 16*s
    img=Image.new('RGB',(W,H),'white'); d=ImageDraw.Draw(img)
    d.text((6,6*s), title, font=ft, fill=INK)
    if subtitle: d.text((6,6*s+17*s), subtitle, font=fsub, fill=MUT)
    def row(y, cells, fonts, fills, bg=None):
        if bg: d.rectangle([colx[0],y,colx[-1],y+rh], fill=bg)
        for i,c in enumerate(cells):
            f=fonts[i]; tw=d.textlength(str(c), font=f); fw=colx[i+1]-colx[i]; al=aligns[i]
            tx = colx[i]+6*s if al=='l' else (colx[i]+fw-6*s-tw if al=='r' else colx[i]+(fw-tw)/2)
            d.text((tx, y+6*s), str(c), font=f, fill=fills[i])
        d.line([colx[0],y+rh,colx[-1],y+rh], fill=GRID)
    row(ytop, headers, [fnb]*len(headers), [MUT]*len(headers), bg=HEADBG); y=ytop+rh
    for ri,r in enumerate(rows):
        b = ri in bold_rows
        row(y, r, [fnb if b else fn]*len(r), [INK if b else TXT]*len(r), bg=(HEADBG if ri in sep_rows else None)); y+=rh
    buf=io.BytesIO(); img.save(buf, format='PDF', resolution=120.0); return buf.getvalue()

def build_reopen_pdf(p):
    """Reopen dump as a PDF (Pillow image; content readable, links shown as ticket #s - not clickable)."""
    from collections import defaultdict
    events=[e for e in p['reopen_events'] if int(e['week_idx'])==1]
    by=defaultdict(list)
    for e in events: by[e['bucket']].append(e)
    rows=[]; bold=set(); sep=set()
    for b in BUCKET_ORDER:
        items=by.get(b,[]); tag='avoidable' if b in AVOID_BUCKETS else 'not your fault'
        sep.add(len(rows)); bold.add(len(rows))
        rows.append([f'{BUCKET_NICE[b]}  ({len(items)}) - {tag}','',''])
        for it in items:
            rows.append([f"#{it['ticket_number']}", it.get('reopen_ts',''), (it.get('trinity_tags') or '')[:42]])
    sub=f"{p['name']} - {p['tier']} {p['shift']} - latest week - {len(events)} reopen events - open by ticket # in Trinity"
    return _grid_pdf('Reopen dump', sub, ['Ticket #','Reopened (IST)','Tags'], rows, [90,150,240], ['l','l','l'], bold_rows=bold, sep_rows=sep)

# ==================== HOURLY GRID (xlsx) + SHIFT ACTIVITY ====================
def _shift_bounds(shift):
    a,b=shift.split('-'); return int(a.split(':')[0]), int(b.split(':')[0])

def _shift_hours(shift):
    """'21:00-06:00' -> [21,22,23,0,1,2,3,4,5] (end exclusive); handles cross-midnight."""
    try:
        s,e=_shift_bounds(shift)
    except Exception:
        return list(range(24))
    if e==s: return list(range(24))
    return list(range(s,e)) if e>s else list(range(s,24))+list(range(0,e))

def _hourly_grid(p):
    days=set(p.get('week_dates') or [])
    g={}
    for h in p.get('hourly') or []:
        d=str(h['day_ist'])
        if d in days: g[(d,int(h['hour_ist']))]=int(h['ticket_count'] or 0)
    return g

def build_hourly_pdf(p):
    """Hourly hour x day closure grid as a PDF (Pillow; no shading; rostered shift hours bold)."""
    g=_hourly_grid(p); dates=p.get('week_dates') or []; sh=set(_shift_hours(p['shift']))
    labs=[pendulum.parse(d).format('DD/MM ddd') for d in dates]
    headers=['Hour (IST)']+labs+['Total']
    rows=[]; bold=set(); coltot=[0]*len(dates); grand=0
    for h in range(24):
        rt=0; cells=[f'{h:02d}:00 - {h:02d}:59']
        for j,d in enumerate(dates):
            v=g.get((d,h),0); rt+=v; coltot[j]+=v; cells.append(str(v) if v else '')
        cells.append(str(rt) if rt else ''); grand+=rt
        if h in sh: bold.add(len(rows))
        rows.append(cells)
    bold.add(len(rows))
    rows.append(['Total']+[str(t) for t in coltot]+[str(grand)])
    widths=[90]+[52]*len(dates)+[46]; aligns=['l']+['c']*len(dates)+['c']
    sub=(f"{p['name']} - {p['tier']} {p['shift']} - week "
         f"{pendulum.parse(dates[0]).format('DD/MM') if dates else ''}-{pendulum.parse(dates[-1]).format('DD/MM') if dates else ''} - bold = rostered shift hours")
    return _grid_pdf('Hourly human closes', sub, headers, rows, widths, aligns, bold_rows=bold)

def shift_activity(p):
    """Per shift-INSTANCE downtime + burst signals (names the day, handles cross-midnight, and
    excludes unworked/day-off shifts). Rubric adds the caveats."""
    g=_hourly_grid(p); dates=p.get('week_dates') or []; sh=_shift_hours(p['shift']); shset=set(sh)
    try: s_hr,e_hr=_shift_bounds(p['shift']); cross=(e_hr<=s_hr)
    except Exception: cross=False
    fmt=lambda d: pendulum.parse(d).format('ddd DD/MM')
    # a shift instance = its hours mapped to the actual calendar date they fall on
    worst=None; off=[]
    for i,d in enumerate(dates):
        seq=[]
        for h in sh:
            if (not cross) or (h>=s_hr): seq.append((h,d))              # hours on the start date
            else:
                nd=dates[i+1] if i+1<len(dates) else None               # early-morning -> next date
                if nd: seq.append((h,nd))
        if sum(g.get((dd,hh),0) for hh,dd in seq)==0:                   # unworked shift (likely day off)
            off.append(d); continue
        run=0; start=None
        for hh,dd in seq:
            if g.get((dd,hh),0)==0:
                if run==0: start=hh
                run+=1
                if worst is None or run>worst[0]: worst=(run,d,start,hh)
            else: run=0
    quiet=None
    if worst and worst[0]>=2:
        run,d,st,en=worst
        quiet={'day':fmt(d),'from':'%02d:00'%st,'to':'%02d:59'%en,'consecutive_hours':run}
    busiest=None
    if g:
        (bd,bh),bn=max(g.items(), key=lambda kv: kv[1]); bar=HOUR_BURST_BAR.get(p['tier'],10)
        busiest={'day':fmt(bd),'hour':'%02d:00'%bh,'closes':bn,'tier_norm_per_hr':bar,'exceeds_norm':bn>=bar}
    return {'shift':p['shift'],'shift_length_hrs':len(sh),
            'closes_in_shift':sum(v for (d,h),v in g.items() if h in shset),'closes_total':sum(g.values()),
            'likely_off_or_unworked_shifts':[fmt(d) for d in off],
            'quiet_stretch_in_shift':quiet,'busiest_hour':busiest}

def _shift_note(p):
    """Deterministic, code-owned shift-activity lines for section 4 (never LLM-generated, so the
    day + numbers are always exact and the caveats are always present)."""
    sa=shift_activity(p); out=[]; tot=sa['closes_total']; ins=sa['closes_in_shift']
    q=sa.get('quiet_stretch_in_shift'); b=sa.get('busiest_hour')
    if tot and ins < 0.30*tot:
        out.append(f"Most of your closes landed <b>outside</b> your rostered <b>{sa['shift']}</b> window this week ({ins} of {tot} in-shift) &mdash; worth checking your shift mapping, not a performance issue.")
    elif q:
        out.append(f"No closures were logged on <b>{q['day']}</b> between <b>{q['from']} and {q['to']}</b> ({q['consecutive_hours']}h) during your shift &mdash; could be a tough ticket, a break, or simply low volume. Just flagging, not a mark against you (and your day off is excluded).")
    if b and b.get('exceeds_norm'):
        out.append(f"Heads-up: <b>{b['closes']} closes in a single hour</b> ({b['day']} {b['hour']}) vs the ~{b['tier_norm_per_hr']}/hr {p['tier']} norm &mdash; worth a quick look (often a legitimate batch of duplicates/spam).")
    return out

# ==================== SECTION 4 - LLM (OpenAI-compatible REST) ====================
def _agent_top_tags(p):
    """This agent's latest-week avoidable reopens, by tag, split incorrect vs incomplete."""
    from collections import defaultdict
    d=defaultdict(lambda:{'incorrect':0,'incomplete':0})
    for e in p['reopen_events']:
        if int(e['week_idx'])==1 and e.get('bucket') in AVOID_BUCKETS and e.get('trinity_tags'):
            for t in str(e['trinity_tags']).split(', '):
                if t: d[t][e['bucket']]+=1
    out=[{'tag':t,'incorrect':v['incorrect'],'incomplete':v['incomplete'],'total':v['incorrect']+v['incomplete']}
         for t,v in d.items()]
    return sorted(out,key=lambda x:x['total'],reverse=True)[:8]

def _load_rubric(cfg):
    """Rubric source of truth. If config has a rubric_doc_url, fetch that Google Doc as plain text
    and use it when it's readable AND non-empty; otherwise fall back to the config ai_rubric column."""
    url=(cfg.get('rubric_doc_url') or '').strip()
    if url:
        m=re.search(r'/document/d/([A-Za-z0-9_-]+)', url)
        txt_url=('https://docs.google.com/document/d/%s/export?format=txt' % m.group(1)) if m else url
        try:
            body=urllib.request.urlopen(urllib.request.Request(txt_url, headers={'User-Agent':'cs-perf-dag'}), timeout=30).read().decode('utf-8','replace')
            head=body.lstrip()[:400].lower()
            readable = body.strip() and '<html' not in head and '<!doctype' not in head and 'accounts.google.com' not in body[:3000]
            if readable:
                return body.strip()
            logger.warning('rubric doc empty/unreadable (%s) - using config ai_rubric', txt_url)
        except Exception as e:
            logger.warning('rubric doc fetch failed (%s): %s - using config ai_rubric', txt_url, e)
    return (cfg.get('ai_rubric') or '').strip()

def ai_notes(p, cfg, baseline):
    def metric(series, unit=''):
        """Return a clearly-labeled {latest, prior, series_oldest_to_newest} block. series is oldest->newest."""
        latest=series[-1] if series else None
        prior=series[-2] if len(series)>1 else None
        return {'latest':(None if latest is None else f'{latest:g}{unit}'),
                'prior_week':(None if prior is None else f'{prior:g}{unit}'),
                'weekly_oldest_to_newest':[None if v is None else f'{v:g}{unit}' for v in series]}
    y=p['you']; s=p['shift_m']
    ctx={'agent':p['name'],'tier':p['tier'],'shift':p['shift'],'weeks_oldest_to_newest':list(p['weeks'][::-1]),
      'you':{
        'human_closes':metric(y['human_n']),
        'created_to_human_FRT_minutes':metric(y['frt_p50'],'m'),
        'escalated_to_human_FRT_minutes':metric(y['hufrt_p50'],'m'),
        'CSAT_percent_positive':metric(y['csat_pos_hu'],'%'),
        'CSAT_num_responses':metric(y['csat_n_hu']),
        'reopen_rate_percent':metric(y['reopen_rate_hu'],'%'),
        'reopen_count':metric(y['reopen_n_hu'])},
      'shift_median_for_context':{
        'created_to_human_FRT_minutes':metric(s['frt_p50'],'m'),
        'CSAT_percent_positive':metric(s['csat_pos_hu'],'%'),
        'reopen_rate_percent':metric(s['reopen_rate_hu'],'%')},
      'reopen_quality_latest_week':{
        'avoidable_total':p['buckets']['avoidable'][-1],
        'incorrect':p['buckets']['incorrect'][-1],'incomplete':p['buckets']['incomplete'][-1],
        'not_your_fault_total':p['buckets']['notfault'][-1]},
      'AGENT_TOP_TAGS':_agent_top_tags(p),
      'POPULATION_BASELINE':baseline,
      'TAG_BUCKET_LEADERS':baseline.get('tag_bucket_leaders', [])}
    # The analysis framework (the "skill") lives in the config query column ai_rubric - editable in
    # Redash, no code push. Fall back to a minimal instruction if the column is missing.
    rubric=_load_rubric(cfg) or (
      "Write supportive weekly AI notes. Use only the numbers in DATA, quoting them exactly. "
      "Return STRICT JSON: trend (string), strengths (2), weaknesses (2), actions (2).")
    prompt=rubric+"\n\nDATA:\n"+json.dumps(ctx, default=str)
    try:
        body=json.dumps({'model':cfg['llm_model'],'max_tokens':600,'temperature':0.4,
            'messages':[{'role':'user','content':prompt}]}).encode()
        req=urllib.request.Request(cfg['llm_proxy_url'].rstrip('/')+'/chat/completions', data=body,
            headers={'Authorization':'Bearer '+cfg['llm_proxy_api_key'],'Content-Type':'application/json'})
        resp=json.loads(urllib.request.urlopen(req, timeout=60).read())
        txt=resp['choices'][0]['message']['content'].strip()
        if txt.startswith('```'): txt=txt.strip('`').split('\n',1)[1] if '\n' in txt else txt.strip('`')
        j=json.loads(txt)
        return {'trend':j['trend'],'strengths':j['strengths'][:3],'weaknesses':j['weaknesses'][:3],'actions':j['actions'][:3]}
    except Exception as e:
        logger.warning('AI notes fell back for %s: %s', p['email'], e)
        return _ai_fallback(p)

def _ai_fallback(p):
    y=p['you']; frt=y['frt_p50']; reop=y['reopen_rate_hu']
    def dlt(a):
        if len(a)<2 or a[-1] is None or a[-2] is None: return 'held steady'
        return 'improved' if a[-1]<a[-2] else ('rose' if a[-1]>a[-2] else 'held steady')
    return {
      'trend': f"This week you closed {y['human_n'][-1]} tickets; FRT {dlt(frt)} and reopen rate {dlt(reop)} versus last week.",
      'strengths':['Consistent weekly volume.','Metrics tracked and shared for transparency.'],
      'weaknesses':['Review the avoidable reopen buckets (incorrect / incomplete) in the attached dump.'],
      'actions':['Confirm the fix and restate the customer request before closing.','Check the reopen dump to spot recurring issue types.'],
    }

# ==================== DELIVERY ====================
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT   = 587

def _gmail_app_password(cfg):
    """16-char Gmail App Password for from_email. From the config query (gmail_app_password),
    editable in Redash like the LLM key; env override for local tests. Spaces are stripped."""
    pw = os.getenv('CS_PERF_GMAIL_APP_PASSWORD') or (cfg.get('gmail_app_password') or '')
    return pw.replace(' ', '')

def _plain_fallback():
    return ("Your weekly performance report is in the HTML body of this email, with your reopen "
            "dump attached as an .xlsx. Questions, suggestions, or anything else - just reply.")

def cc_for_tier(tier, cfg):
    """CC list by tier, from the config query. L1 -> cc_l1; L2 Full Stack/Expo -> cc_l2."""
    key = 'cc_l1' if tier == 'L1' else 'cc_l2'
    return [e.strip() for e in (cfg.get(key) or '').split(',') if e.strip()]

def send_email(to, subject, html, attachments, cfg, cc=None):
    """Send one agent's report as HTML + .xlsx attachments via Gmail SMTP (app password).
    attachments = list of (filename, bytes). DRY_RUN writes HTML+attachments to disk instead."""
    cc = cc or []; attachments = attachments or []
    if DRY_RUN:
        os.makedirs(DRY_RUN_DIR, exist_ok=True)
        base=os.path.join(DRY_RUN_DIR, to.split('@')[0])
        open(base+'.html','w').write(html)
        for fn,b in attachments: open(base+'_'+fn,'wb').write(b)
        logger.info('[DRY_RUN] wrote %s.html + %d attachment(s); cc=%s', base, len(attachments), cc)
        return
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.application import MIMEApplication
    sender = cfg.get('from_email'); reply_to = cfg.get('reply_to') or sender
    if not sender:
        raise RuntimeError('from_email missing in config query')
    msg = MIMEMultipart('mixed')
    msg['Subject'] = subject; msg['From'] = 'CS Weekly Report <%s>' % sender
    msg['To'] = to; msg['Reply-To'] = reply_to
    if cc:
        msg['Cc'] = ', '.join(cc)
    alt = MIMEMultipart('alternative')
    alt.attach(MIMEText(_plain_fallback(), 'plain'))
    alt.attach(MIMEText(html, 'html'))
    msg.attach(alt)
    import mimetypes
    from email.mime.base import MIMEBase
    from email import encoders
    for fn, b in attachments:
        ctype = mimetypes.guess_type(fn)[0] or 'application/octet-stream'
        mt, st = ctype.split('/', 1)
        part = MIMEBase(mt, st); part.set_payload(b); encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment', filename=fn)
        msg.attach(part)
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=60) as s:
        s.starttls(); s.login(sender, _gmail_app_password(cfg))
        s.sendmail(sender, [to] + cc, msg.as_string())   # envelope incl. CC recipients
    logger.info('sent report to %s (cc=%s, %d attachments)', to, cc, len(attachments))

# ==================== MAIN TASK ====================
def run_perf_report(**context):
    cfg, core, reopen, baselines, hourly = fetch_all()
    payloads, _ = assemble(core, reopen, hourly)
    base = baseline_summary(baselines)
    logger.info('assembled %d agent reports (%d core, %d reopen rows); baseline avoidable=%s%%',
                len(payloads), len(core), len(reopen), base.get('avoidable_pct_of_all_reopens'))
    subject_prefix = cfg.get('subject_prefix') or 'Your Weekly Report'
    test_to = os.getenv('CS_PERF_TEST_RECIPIENT')             # pilot: route ALL emails here
    limit   = int(os.getenv('CS_PERF_LIMIT', '0') or 0)       # pilot: only first N agents (0 = all)
    if limit: payloads = payloads[:limit]
    if test_to: logger.warning('PILOT: routing all %d emails to %s', len(payloads), test_to)
    sent=0; failed=[]
    for p in payloads:
        try:
            ai = ai_notes(p, cfg, base)                       # LLM (has its own fallback)
            html = build_html(p, ai)
            first = p['name'].split()[0].lower().replace('/','-') or 'agent'
            attachments = [(f"reopen_dump_{first}.pdf", build_reopen_pdf(p)),
                           (f"hourly_closes_{first}.pdf", build_hourly_pdf(p))]
            to = test_to or p['email']
            subj = ('[TEST %s] ' % p['name'] + subject_prefix) if test_to else subject_prefix
            cc = [] if test_to else cc_for_tier(p['tier'], cfg)   # no CC during pilot/test sends
            send_email(to, subj, html, attachments, cfg, cc=cc)
            sent+=1
        except Exception as e:                                # one bad agent must not sink the run
            logger.exception('perf report FAILED for %s: %s', p.get('email'), e)
            failed.append(p.get('email'))
    logger.info('CS WEEKLY PERFORMANCE REPORT: %s for %d/%d agents (%d failed: %s)',
                'DRY_RUN wrote' if DRY_RUN else 'sent', sent, len(payloads), len(failed), failed)
    if failed and not DRY_RUN:
        logger.warning('agents skipped due to errors: %s', failed)

# ==================== DAG ====================
default_args = {
    'owner': 'cs_team', 'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 1, 1, tz='Asia/Kolkata'),
    'email_on_failure': False, 'email_on_retry': False,
    'retries': 1, 'retry_delay': timedelta(minutes=3),
}
dag = DAG(
    'cs_performance_report_weekly',
    default_args=default_args,
    description='Per-agent weekly performance email (v2 matrix); numbers in Redash 41791/41792, config 41839',
    schedule_interval='0 7 * * 1',    # Monday 07:00 IST
    catchup=False, is_paused_upon_creation=True,
    tags=['email', 'trinity', 'performance', 'cs_reports', 'cs_team'],
)
PythonOperator(task_id='run_perf_report', python_callable=run_perf_report, dag=dag)
