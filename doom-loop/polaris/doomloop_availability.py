"""
Doom-Loop availability store - Polaris Mongo source of truth for "who can I tag".

Drop-in Polaris backend route: copy to /app/backend/routes/doomloop_availability.py
and register in server.py:
    from routes.doomloop_availability import router as doomloop_av_router
    app.include_router(doomloop_av_router, prefix="/api")

Collection `doomloop_availability`, one doc per roster person:
  _id          = slack_id (fallback: name)
  name         = display name
  role         = 'csm' | 'tech'
  slack_id     = Slack user id (enforcement / real pings)
  order        = pickup order within the role (lower = earlier)
  status       = 'available' | 'busy' | 'out'
  current_call = thread_ts of the call they're on, else None
  updated_at   = epoch seconds

The poller/engine use the helpers below: av_next_available(role) to pick who to
tag, av_claim() when someone joins (-> busy), av_free() when the call ends
(-> available). Seed from the roster via POST /doomloop/availability/seed.
"""
import time

from fastapi import APIRouter, Request, HTTPException
from shared import db, logger, require_admin

router = APIRouter()
COLL = "doomloop_availability"
STATUSES = {"available", "busy", "out"}
ROLES = {"csm", "tech"}


def _norm(d):
    return {
        "slack_id": d.get("slack_id") or d.get("_id"),
        "name": d.get("name"),
        "role": d.get("role"),
        "order": d.get("order", 0),
        "status": d.get("status", "available"),
        "current_call": d.get("current_call"),
        "updated_at": d.get("updated_at"),
    }


# ─────────────── helpers used by the poller/engine ───────────────

async def av_list(role=None):
    q = {"role": role} if role else {}
    docs = await db[COLL].find(q).to_list(500)
    docs.sort(key=lambda d: (d.get("role", ""), d.get("order", 0)))
    return [_norm(d) for d in docs]


async def av_next_available(role):
    """First available (not busy/out) person in the role, by pickup order."""
    docs = await db[COLL].find({"role": role, "status": "available"}).to_list(500)
    if not docs:
        return None
    docs.sort(key=lambda d: d.get("order", 0))
    return _norm(docs[0])


async def av_all_unavailable(role):
    """True when no one in the role is available (all busy/out) -> trigger fallback."""
    n = await db[COLL].count_documents({"role": role, "status": "available"})
    return n == 0


async def av_set(key, status, current_call=None):
    if status not in STATUSES:
        raise ValueError("bad status")
    upd = {"status": status, "updated_at": time.time()}
    upd["current_call"] = current_call if status == "busy" else None
    res = await db[COLL].update_one({"_id": key}, {"$set": upd})
    return res.matched_count > 0


async def av_claim(key, call_id):
    return await av_set(key, "busy", call_id)


async def av_free(key):
    return await av_set(key, "available")


async def av_upsert(person):
    key = person.get("slack_id") or person.get("name")
    if not key:
        raise ValueError("person needs slack_id or name")
    role = person.get("role", "csm")
    if role not in ROLES:
        raise ValueError("bad role")
    doc = {"name": person.get("name"), "role": role,
           "slack_id": person.get("slack_id"), "order": int(person.get("order", 0)),
           "status": person.get("status", "available"), "updated_at": time.time()}
    doc.setdefault("current_call", None)
    await db[COLL].update_one({"_id": key}, {"$set": doc,
                              "$setOnInsert": {"current_call": None}}, upsert=True)
    return key


# ─────────────── admin endpoints ───────────────

@router.get("/doomloop/availability")
async def list_availability(request: Request, role: str = None):
    await require_admin(request)
    return {"people": await av_list(role)}


@router.post("/doomloop/availability/seed")
async def seed_availability(request: Request):
    """Bulk upsert the roster: body {"people": [{name, role, slack_id, order, status?}]}.
    Existing people keep their live status unless status is explicitly provided."""
    await require_admin(request)
    body = await request.json()
    people = body.get("people") or []
    keys = []
    for p in people:
        existing = None
        key = p.get("slack_id") or p.get("name")
        if key:
            existing = await db[COLL].find_one({"_id": key})
        if existing and "status" not in p:
            p = {**p, "status": existing.get("status", "available")}
        keys.append(await av_upsert(p))
    logger.info("[doomloop-av] seeded %d people", len(keys))
    return {"ok": True, "count": len(keys), "keys": keys}


@router.put("/doomloop/availability/status")
async def set_status(request: Request):
    """Body {"key"|"slack_id": ..., "status": available|busy|out, "current_call"?}."""
    await require_admin(request)
    body = await request.json()
    key = body.get("key") or body.get("slack_id")
    status = body.get("status")
    if not key or status not in STATUSES:
        raise HTTPException(400, "key and valid status required")
    ok = await av_set(key, status, body.get("current_call"))
    if not ok:
        raise HTTPException(404, "person not found")
    return {"ok": True, "key": key, "status": status}


@router.get("/doomloop/availability/next")
async def next_available(request: Request, role: str = "csm"):
    await require_admin(request)
    if role not in ROLES:
        raise HTTPException(400, "bad role")
    return {"person": await av_next_available(role),
            "all_unavailable": await av_all_unavailable(role)}
