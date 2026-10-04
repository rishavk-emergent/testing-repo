# Apply: Doom-Loop availability store (Polaris)

Staged — run these once the envcore gateway reconnects.

1. `create_file` → `/app/backend/routes/doomloop_availability.py` (contents = this folder's `doomloop_availability.py`).
2. Edit `/app/backend/server.py`:
   - import: `from routes.doomloop_availability import router as doomloop_av_router`
   - include: `app.include_router(doomloop_av_router, prefix="/api")`  (next to the doomloop_router include)
3. `supervisorctl restart backend`
4. Seed from the roster: `POST /api/doomloop/availability/seed` with `{"people":[{name,role,slack_id,order}]}` (csm + tech).
5. Verify: `GET /api/doomloop/availability` lists everyone `available`; `GET /api/doomloop/availability/next?role=csm` returns the first; flip one to busy via `PUT /api/doomloop/availability/status` and confirm `next` skips them.

Endpoints: GET /doomloop/availability[?role], POST /seed, PUT /status, GET /next?role.
Helpers for the poller: av_next_available(role), av_all_unavailable(role), av_claim(key,call), av_free(key), av_set(key,status,current_call), av_upsert(person).
