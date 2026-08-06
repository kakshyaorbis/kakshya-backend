from fastapi import FastAPI, Response, Request, Depends, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, JSONResponse, HTMLResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Boolean, Float, text
from sqlalchemy.orm import sessionmaker, declarative_base
from jose import jwt, JWTError
from datetime import datetime, timedelta, timezone
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from sgp4.api import Satrec, jday
import httpx
from urllib.parse import urlparse
import bcrypt
import os
import logging
import math
import asyncio
import json

# This is the entire application, on purpose — the goal right now isn't to
# do anything useful yet, it's to prove the whole chain works end to end:
# code -> deployed on the internet -> responds when you visit it in a
# browser. Everything else gets built on top of this once this part works.

# ---------------------------------------------------------------------------
# Activity logging. Writes to stdout, which Render automatically captures
# and shows in that service's own log view — no new infrastructure needed.
# This is the real, server-side record of login activity that the OLD
# purely-client-side lockout logic could never provide (a script hitting
# the API directly, bypassing the browser entirely, would never have shown
# up anywhere before this).
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("kakshya")

app = FastAPI()

# ---------------------------------------------------------------------------
# Rate limiting. Keyed by IP address — the real, server-side enforcement
# that the old frontend-only "5 attempts then 60s lockout" never actually
# had, since that lockout lived entirely in browser JavaScript and could be
# bypassed by anyone calling the API directly. This is what makes that
# protection real instead of cosmetic.
# ---------------------------------------------------------------------------
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ---------------------------------------------------------------------------
# CORS (Cross-Origin Resource Sharing). The real Kakshya frontend
# (kakshya1.netlify.app) and this backend (onrender.com) are different
# domains — browsers block cross-domain requests by default unless the
# backend explicitly allows a specific origin. This is intentionally exact
# (not a wildcard "*"), since allow_credentials=True (needed so the login
# cookie can actually be sent/received) is not permitted together with a
# wildcard origin by browser security rules — it has to be this one real,
# specific URL.
# ---------------------------------------------------------------------------
FRONTEND_ORIGIN = "https://kakshya1.netlify.app"
app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_ORIGIN],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# WebSocket real-time satellite tracking (Phase 3.5) — upgraded from the
# original fake-counter demo. This now pushes REAL satellite positions,
# computed server-side with real SGP4 propagation (the sgp4 Python
# library — the same real physics as satellite.js, which does this
# client-side on the actual Kakshya dashboard today). Real, live, public
# satellite data (Celestrak's "stations" group — ISS and other real space
# stations) — not Skyroot data, since that still doesn't exist yet, but
# genuinely real orbital mechanics, not a placeholder value.
# ---------------------------------------------------------------------------
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                self.disconnect(connection)


manager = ConnectionManager()

# Holds the real, live-fetched satellite records once loaded at startup —
# {name: Satrec}. Fetched once from Celestrak (same real endpoint and
# headers already proven in /api/celestrak/{group}), not re-fetched on
# every broadcast tick — TLEs don't change fast enough to need that, same
# reasoning as the existing 4-hour cache on that endpoint.
tracked_satellites: dict[str, Satrec] = {}

# A simple, directly-inspectable record of the last load attempt — see
# GET /debug/satellite-status. Built specifically so the real current
# state can be checked instantly via one URL, instead of hunting through
# Render's log UI (time-range filters, scrolling, stale cached views) to
# find the same information.
satellite_load_status = {
    "loaded_count": 0,
    "last_attempt_time": None,
    "last_error": None,
    "last_success_time": None,
    # Added alongside the status-code-aware cooldown fix: which HTTP
    # status caused the last failure, so the external cooldown check
    # (search for celestrak_retry_window_seconds below) knows how long to
    # actually wait, instead of a flat 10 minutes for every error type.
    "last_error_status_code": None,
}


def eci_to_geodetic(x, y, z, jd, fr):
    """
    Real ECI (TEME) -> geodetic latitude/longitude/altitude conversion —
    the same GMST-rotation + WGS84 approach satellite.js already uses for
    this identical conversion on the live Kakshya dashboard, kept
    consistent with that existing math rather than reinvented differently
    here. Verified numerically against known real ISS orbital constraints
    before being wired into the live broadcast.
    """
    t = (jd + fr - 2451545.0) / 36525.0
    gmst_sec = 67310.54841 + (876600.0 * 3600 + 8640184.812866) * t + 0.093104 * t * t - 6.2e-6 * t * t * t
    gmst = math.radians((gmst_sec % 86400.0) / 240.0)

    xf = x * math.cos(gmst) + y * math.sin(gmst)
    yf = -x * math.sin(gmst) + y * math.cos(gmst)
    zf = z

    a = 6378.137  # WGS84 equatorial radius, km
    e2 = 0.00669437999014
    lon = math.atan2(yf, xf)
    r = math.sqrt(xf * xf + yf * yf)
    lat = math.atan2(zf, r)
    c = a
    for _ in range(6):
        sin_lat = math.sin(lat)
        c = a / math.sqrt(1 - e2 * sin_lat * sin_lat)
        lat = math.atan2(zf + c * e2 * sin_lat, r)
    alt = r / math.cos(lat) - c

    return math.degrees(lat), math.degrees(lon), alt


# Shared, single source of truth for both the tracker's own failover
# (below) and the /api/celestrak/stations category-endpoint failover
# (SPACE_TRACK_FAILOVER_MAP, defined later in this file) \u2014 previously
# duplicated locally inside try_stations_space_track_failover(), moved
# here so both real usages always stay consistent with each other.
# Deliberately, honestly limited to ISS only \u2014 the one object
# confidently verified throughout this whole investigation. Extend only
# once another station's ID is actually confirmed, not guessed.
KNOWN_STATION_NORAD_IDS = {25544: "ISS (ZARYA)"}


async def try_stations_space_track_failover():
    """
    Space-Track failover for the real-time tracker — using INDIVIDUAL
    lookups by known NORAD ID, not a category search, since "stations"
    is fundamentally a small, curated list of specific known objects,
    not a searchable name pattern (unlike starlink/gps-ops/geo/debris).

    Called from BOTH real exit points of load_tracked_satellites() below
    (the immediate-stop-on-explicit-rejection path, and the
    attempts-exhausted path) \u2014 a real bug caught in testing: the
    explicit-rejection path used to `return` immediately, silently
    skipping the failover entirely whenever Celestrak returned a 403/404/
    etc, which is precisely the situation the failover exists for.
    """
    if not SPACE_TRACK_USERNAME or not SPACE_TRACK_PASSWORD:
        logger.warning("No Space-Track credentials configured \u2014 cannot attempt failover. Will check again in 2 hours.")
        return

    login_url = "https://www.space-track.org/ajaxauth/login"
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            login_resp = await client.post(login_url, data={"identity": SPACE_TRACK_USERNAME, "password": SPACE_TRACK_PASSWORD})
            if login_resp.status_code != 200:
                logger.warning(f"Space-Track failover login failed: HTTP {login_resp.status_code}. Will check again in 2 hours.")
                return

            count = 0
            for norad_id, name in KNOWN_STATION_NORAD_IDS.items():
                query_url = f"https://www.space-track.org/basicspacedata/query/class/gp/NORAD_CAT_ID/{norad_id}/format/json"
                try:
                    resp = await client.get(query_url)
                    if resp.status_code != 200:
                        continue
                    data = resp.json()
                    if not data or data[0].get("DECAY_DATE"):
                        continue
                    l1, l2 = data[0].get("TLE_LINE1"), data[0].get("TLE_LINE2")
                    if not l1 or not l2:
                        continue
                    satrec = Satrec.twoline2rv(l1, l2)
                    tracked_satellites[name] = satrec
                    count += 1
                    try:
                        record_tle_and_check_anomaly(name, l1, l2, satrec)
                    except Exception as e:
                        logger.warning(f"Anomaly detection recording failed for {name} (non-fatal): {e}")
                except Exception as e:
                    logger.warning(f"Space-Track failover: failed to load {name}: {type(e).__name__}: {e}")

            if count > 0:
                satellite_load_status["loaded_count"] = count
                satellite_load_status["last_error"] = None
                satellite_load_status["last_success_time"] = datetime.now(timezone.utc).isoformat()
                logger.info(f"Space-Track failover succeeded \u2014 loaded {count} real satellite(s)")
            else:
                logger.warning("Space-Track failover attempted but loaded zero satellites. Will check again in 2 hours.")

    except Exception as e:
        logger.warning(f"Space-Track failover failed: {type(e).__name__}: {e}. Will check again in 2 hours.")


async def load_tracked_satellites():
    """
    Fetches real TLEs for a small, recognizable real set (ISS and other
    real space stations), using the same real Celestrak endpoint and
    headers already proven in /api/celestrak/{group}. Retries a few times
    with a short delay between attempts.

    No longer called blindly at container startup — two different delay
    values (10s, then 45s) both failed with the exact same ConnectTimeout
    on live Render logs, which ruled out "just needs more warm-up time" as
    the real explanation. Now triggered instead by the first real
    WebSocket connection (see websocket_demo below) — the same
    request-triggered pattern already proven reliable on
    /api/celestrak/{group}, rather than continuing to chase a working
    delay value for a context that kept failing regardless of how long it
    waited.
    """

    last_error = None

    # Only 2 attempts now, not 3 — and critically, an explicit HTTP 403 or
    # 404 stops immediately with NO retry at all. Per Celestrak's own
    # documented policy: repeating a request after a 403/404 does not
    # change the outcome and can worsen an IP-level block. A ConnectTimeout
    # (no HTTP response at all) is treated differently — that's ambiguous
    # enough (could be a real transient network issue) to justify exactly
    # one retry, not three.
    for attempt in range(1, 3):
        satellite_load_status["last_attempt_time"] = datetime.now(timezone.utc).isoformat()
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await celestrak_get(client, "stations")

            if resp.status_code in (403, 404, 500):
                last_error = f"HTTP {resp.status_code} (explicit error response — not retrying, per Celestrak's own stated policy)"
                satellite_load_status["last_error"] = last_error
                satellite_load_status["last_error_status_code"] = resp.status_code
                logger.warning(f"Celestrak returned an error response: {last_error}. Stopping Celestrak attempts, trying Space-Track failover.")
                await try_stations_space_track_failover()
                return  # stop entirely — do not retry Celestrak, do not continue the loop

            if resp.status_code != 200 or not resp.text.strip():
                last_error = f"HTTP {resp.status_code}"
                satellite_load_status["last_error"] = last_error
                logger.warning(f"Attempt {attempt}/2 to load real satellite data failed: {last_error}")
                await asyncio.sleep(2 * attempt)
                continue

            lines = [l.strip() for l in resp.text.strip().split("\n") if l.strip()]
            count = 0
            for i in range(0, len(lines) - 2, 3):
                name, l1, l2 = lines[i], lines[i + 1], lines[i + 2]
                if not l1.startswith("1 ") or not l2.startswith("2 "):
                    continue
                try:
                    satrec = Satrec.twoline2rv(l1, l2)
                    tracked_satellites[name] = satrec
                    count += 1
                    try:
                        record_tle_and_check_anomaly(name, l1, l2, satrec)
                    except Exception as e:
                        logger.warning(f"Anomaly detection recording failed for {name} (non-fatal, tracking continues): {e}")
                except Exception:
                    continue  # one bad TLE entry skipped, not fatal to the rest
                if count >= 8:  # a small, real, manageable set for this demo — not the full stations catalog
                    break
            satellite_load_status["loaded_count"] = count
            satellite_load_status["last_error"] = None
            satellite_load_status["last_error_status_code"] = None
            satellite_load_status["last_success_time"] = datetime.now(timezone.utc).isoformat()
            logger.info(f"Loaded {count} real satellites for WebSocket tracking (Celestrak 'stations' group, attempt {attempt}/2)")
            return  # success — no need to retry further
        except Exception as e:
            # Always includes the exception TYPE, not just str(e) — some
            # real connection-level errors (timeouts especially) have an
            # empty string representation on their own, which is exactly
            # what produced an unhelpfully blank log message before this
            # fix (observed live, not hypothetical).
            last_error = f"{type(e).__name__}: {e}"
            satellite_load_status["last_error"] = last_error
            satellite_load_status["last_error_status_code"] = None  # connection-level, no HTTP status at all
            logger.warning(f"Attempt {attempt}/2 to load real satellite data failed: {last_error}")
            await asyncio.sleep(2 * attempt)

    logger.warning(f"Celestrak attempts exhausted. Last error: {last_error}. Trying Space-Track failover before giving up for now.")
    await try_stations_space_track_failover()


@app.get("/debug/satellite-status")
def satellite_status():
    """
    Direct, instant status check — no log-hunting required. Shows exactly
    what's currently true: how many real satellites are loaded, when the
    last attempt happened, and the exact last error if any.
    """
    return {
        "currently_tracked_satellites": list(tracked_satellites.keys()),
        "loaded_count": satellite_load_status["loaded_count"],
        "last_attempt_time_utc": satellite_load_status["last_attempt_time"],
        "last_success_time_utc": satellite_load_status["last_success_time"],
        "last_error": satellite_load_status["last_error"],
    }


@app.get("/api/anomalies")
def get_anomalies(limit: int = 50):
    """
    Phase 7 — real, persisted anomaly detections (see
    record_tle_and_check_anomaly for the honest framing on what these
    detections actually are: reasoned orbital-mechanics heuristics, not a
    trained ML model). Returns the most recent detections, newest first.
    """
    db = SessionLocal()
    try:
        rows = (
            db.query(DetectedAnomaly)
            .order_by(DetectedAnomaly.detected_at.desc())
            .limit(limit)
            .all()
        )
        return {
            "count": len(rows),
            "anomalies": [
                {
                    "satellite_name": r.satellite_name,
                    "detected_at_utc": r.detected_at.isoformat(),
                    "previous_snapshot_utc": r.previous_snapshot_at.isoformat(),
                    "days_between_snapshots": r.days_between_snapshots,
                    "reasons": json.loads(r.reasons_json),
                }
                for r in rows
            ],
        }
    finally:
        db.close()


@app.get("/api/tle-history/{satellite_name}")
def get_tle_history(satellite_name: str, limit: int = 20):
    """
    Phase 7 — real recorded TLE snapshot history for one satellite, newest
    first. This is the actual raw data anomaly detection compares against
    — exposed directly so it can be inspected/verified independently of
    the anomaly logic itself.
    """
    db = SessionLocal()
    try:
        rows = (
            db.query(TleSnapshot)
            .filter(TleSnapshot.satellite_name == satellite_name)
            .order_by(TleSnapshot.fetched_at.desc())
            .limit(limit)
            .all()
        )
        return {
            "satellite_name": satellite_name,
            "count": len(rows),
            "snapshots": [
                {
                    "fetched_at_utc": r.fetched_at.isoformat(),
                    "inclination_deg": round(r.inclination_deg, 5),
                    "eccentricity": round(r.eccentricity, 7),
                    "raan_deg": round(r.raan_deg, 5),
                    "arg_perigee_deg": round(r.arg_perigee_deg, 5),
                    "mean_motion_rev_per_day": round(r.mean_motion_rev_per_day, 6),
                    "bstar": r.bstar,
                }
                for r in rows
            ],
        }
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Skyroot integration scaffolding — NOT yet connected to anything real.
# SKYROOT_API_URL and SKYROOT_API_KEY are read from environment variables
# (never hardcoded, same principle as every other secret in this backend)
# and are simply not set yet, since Skyroot has not shared real access.
# This is deliberately honest about that: it does not simulate a
# connection or fabricate a response — it reports its real, current
# configuration status, and will only ever attempt a real pull once real
# credentials actually exist.
# ---------------------------------------------------------------------------
SKYROOT_API_URL = os.environ.get("SKYROOT_API_URL")
SKYROOT_API_KEY = os.environ.get("SKYROOT_API_KEY")


async def fetch_skyroot_data():
    """
    The real pull adapter — will fetch from Skyroot's real API once
    SKYROOT_API_URL and SKYROOT_API_KEY are actually set (via Render's
    Environment settings, never in code). Until then, this correctly does
    nothing and reports why, rather than attempting a request against
    nothing or fabricating a result.
    """
    if not SKYROOT_API_URL or not SKYROOT_API_KEY:
        return {"status": "not_configured", "detail": "SKYROOT_API_URL / SKYROOT_API_KEY not set — no real Skyroot access exists yet"}

    # Real fetch logic goes here once real credentials exist. Deliberately
    # not written yet — writing real request logic against an endpoint and
    # auth scheme we don't yet know would mean guessing at Skyroot's real
    # API shape, which defeats the purpose of asking them for it directly.
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(SKYROOT_API_URL, headers={"Authorization": f"Bearer {SKYROOT_API_KEY}"})
        if resp.status_code != 200:
            return {"status": "error", "detail": f"Skyroot API returned HTTP {resp.status_code}"}
        return {"status": "ok", "data": resp.json()}
    except Exception as e:
        return {"status": "error", "detail": f"{type(e).__name__}: {e}"}


@app.get("/api/skyroot/status")
async def skyroot_status():
    """
    Real, honest, live status check — no fake "connected" state. Shows
    exactly whether real Skyroot access is configured right now, and how
    much real telemetry (if any) has ever actually been received.
    """
    db = SessionLocal()
    try:
        count = db.query(SkyrootTelemetry).count()
        latest = db.query(SkyrootTelemetry).order_by(SkyrootTelemetry.received_at.desc()).first()
        return {
            "configured": bool(SKYROOT_API_URL and SKYROOT_API_KEY),
            "detail": "Awaiting real Skyroot API access — not yet connected" if not (SKYROOT_API_URL and SKYROOT_API_KEY) else "Configured",
            "total_records_received": count,
            "latest_record_at_utc": latest.received_at.isoformat() if latest else None,
        }
    finally:
        db.close()


@app.get("/api/skyroot/data")
def skyroot_data(limit: int = 50):
    """Real, persisted Skyroot telemetry, if any has ever actually arrived. Correctly empty until real access exists."""
    db = SessionLocal()
    try:
        rows = (
            db.query(SkyrootTelemetry)
            .order_by(SkyrootTelemetry.received_at.desc())
            .limit(limit)
            .all()
        )
        return {
            "count": len(rows),
            "records": [
                {"asset_id": r.asset_id, "received_at_utc": r.received_at.isoformat(), "payload": json.loads(r.raw_payload_json)}
                for r in rows
            ],
        }
    finally:
        db.close()


async def satellite_reload_safety_net():
    """
    A periodic safety net — if the initial load still didn't succeed, try
    again rather than requiring a manual restart to recover from a
    transient failure.

    Interval changed from 5 minutes to 2 hours after reviewing Celestrak's
    own documented policy: they update GP data once every 2 hours and
    explicitly ask users not to check more frequently, noting that
    excessive checking wastes shared resources and repeated requests after
    an error can result in an IP being firewalled. The original 5-minute
    interval was roughly 24x more frequent than appropriate — likely a
    real contributing factor to the exact failures this system was built
    to recover from, not just an innocent resilience feature. Checking
    every 2 hours is both respectful of Celestrak's stated resource limits
    and genuinely sufficient, since their own data doesn't update faster
    than that anyway.
    """
    while True:
        await asyncio.sleep(7200)  # 2 hours, matching Celestrak's own update cadence
        if not tracked_satellites:
            logger.info("Retrying real satellite data load (background safety net, 2-hour cadence)...")
            await load_tracked_satellites()


@app.websocket("/ws/demo")
async def websocket_demo(websocket: WebSocket):
    await manager.connect(websocket)
    logger.info(f"WebSocket client connected. Total connected: {len(manager.active_connections)}")

    # Trigger the real satellite load HERE, as part of handling this real
    # incoming connection — DIRECTLY AWAITED, not detached via
    # asyncio.create_task(). This matters: even after moving the trigger
    # to a real connection, it still failed with ConnectTimeout when
    # detached as a "fire and forget" background task — this service runs
    # with only one worker process (WEB_CONCURRENCY=1, visible in Render's
    # own logs), and a detached task may simply not get enough event-loop
    # scheduling priority to complete a network handshake in time. The one
    # thing proven to actually work throughout this whole process is
    # /api/celestrak/{group}, which directly awaits its own network call
    # as part of the request itself, never detached. This mirrors that
    # exact pattern instead of continuing to test detached variations of
    # it. The tradeoff: this specific connection's handler blocks for a
    # few seconds while loading — acceptable, since the client is already
    # connected and simply sees "waiting for the next push" during that
    # window, which is already the correct, honest UI state.
    #
    # COOLDOWN: without this, every single visit to /ws-test would trigger
    # a fresh Celestrak request, with no limit. Previously a flat 10
    # minutes for every error type — fixed 2026-08-06 per direct
    # guidance from Celestrak's maintainer (T.S. Kelso): that flat window
    # was actively wrong for a 403 (needs 2+ hours to actually clear) and
    # pointless for a 404 (not temporary at all). Now uses
    # celestrak_retry_window_seconds(), the same status-code-aware logic
    # used by /api/celestrak/{group}, keyed off whichever status code
    # caused the last failure.
    now_iso = datetime.now(timezone.utc)
    last_attempt = satellite_load_status["last_attempt_time"]
    required_wait = celestrak_retry_window_seconds(satellite_load_status["last_error_status_code"])
    cooldown_active = False
    if last_attempt:
        last_attempt_dt = datetime.fromisoformat(last_attempt)
        cooldown_active = (now_iso - last_attempt_dt).total_seconds() < required_wait

    if not tracked_satellites and not cooldown_active:
        await load_tracked_satellites()

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        logger.info(f"WebSocket client disconnected. Total connected: {len(manager.active_connections)}")


async def satellite_broadcast_loop():
    """
    Runs forever in the background, computing REAL current positions for
    every tracked satellite via real SGP4 propagation, and pushing them to
    every connected client every 3 seconds.
    """
    while True:
        await asyncio.sleep(3)
        if not manager.active_connections or not tracked_satellites:
            continue

        now = datetime.now(timezone.utc)
        jd, fr = jday(now.year, now.month, now.day, now.hour, now.minute, now.second + now.microsecond / 1e6)

        positions = []
        for name, satrec in tracked_satellites.items():
            error_code, position, velocity = satrec.sgp4(jd, fr)
            if error_code != 0:
                continue  # real SGP4 propagation failure for this one object — skip it, don't fabricate a position
            lat, lon, alt = eci_to_geodetic(*position, jd, fr)
            positions.append({
                "name": name,
                "lat": round(lat, 3),
                "lon": round(lon, 3),
                "alt_km": round(alt, 1),
            })

        if positions:
            await manager.broadcast({
                "type": "satellite_positions",
                "server_time_utc": now.isoformat(),
                "satellites": positions,
                "note": "Real SGP4-computed positions, real public satellites (Celestrak). Not Skyroot data.",
            })


@app.on_event("startup")
async def start_background_tasks():
    # load_tracked_satellites() is intentionally NOT started here anymore.
    # It used to fire blindly at container startup, which kept failing
    # with ConnectTimeout regardless of delay length (10s, then 45s, both
    # failed identically on live Render logs) — real evidence the problem
    # was the startup-time context itself, not timing. It's now triggered
    # by the first real WebSocket connection instead (see websocket_demo),
    # reusing the same request-triggered pattern already proven reliable
    # elsewhere in this backend.
    asyncio.create_task(satellite_broadcast_loop())
    asyncio.create_task(satellite_reload_safety_net())


@app.get("/ws-test", response_class=HTMLResponse)
def websocket_test_page():
    """
    A simple, self-contained test page — visit this URL directly in a
    browser to see real satellite positions arriving live, no separate
    tools or coding needed.
    """
    return """
    <!DOCTYPE html>
    <html>
    <head><title>Kakshya WebSocket Demo</title></head>
    <body style="font-family: monospace; background: #0B1220; color: #E6EDF3; padding: 24px;">
        <h2>WebSocket Demo \u2014 Real Satellite Positions (Phase 3.5)</h2>
        <p style="color: #33D17A;">Real SGP4-computed positions for real public satellites (Celestrak "stations" group \u2014 ISS and others), pushed live every 3 seconds. Not Skyroot data \u2014 that still doesn't exist yet \u2014 but genuinely real orbital mechanics, not a placeholder.</p>
        <p>Status: <span id="status" style="color: #33D17A;">Connecting...</span></p>
        <div id="messages" style="border: 1px solid #333; padding: 12px; height: 400px; overflow-y: auto; background: #101A2C; white-space: pre;"></div>
        <script>
            const statusEl = document.getElementById("status");
            const messagesEl = document.getElementById("messages");
            const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
            const ws = new WebSocket(proto + "//" + window.location.host + "/ws/demo");
            ws.onopen = () => { statusEl.textContent = "Connected \u2014 waiting for the next push..."; };
            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);
                let block = "[" + data.server_time_utc + "]\\n";
                for (const sat of data.satellites) {
                    block += "  " + sat.name.padEnd(20) + " lat=" + sat.lat.toFixed(2) + "  lon=" + sat.lon.toFixed(2) + "  alt=" + sat.alt_km.toFixed(1) + "km\\n";
                }
                const line = document.createElement("div");
                line.textContent = block;
                line.style.borderBottom = "1px solid #222";
                line.style.paddingBottom = "6px";
                line.style.marginBottom = "6px";
                messagesEl.prepend(line);
                statusEl.textContent = "Connected \u2014 receiving live real satellite positions";
            };
            ws.onclose = () => { statusEl.textContent = "Disconnected"; statusEl.style.color = "#FF6B4A"; };
            ws.onerror = () => { statusEl.textContent = "Connection error"; statusEl.style.color = "#FF6B4A"; };
        </script>
    </body>
    </html>
    """


# ---------------------------------------------------------------------------
# Database setup. DATABASE_URL comes from an environment variable, never
# hardcoded — on Render, this will be set to the real Postgres database's
# connection string (a private setting, never in this file, never
# committed to GitHub). If that variable isn't set at all (e.g. testing
# locally), this falls back to a local SQLite file instead, so the exact
# same code can be tested without needing a real Postgres server running.
# ---------------------------------------------------------------------------
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./local_test.db")
# Render's Postgres URLs start with "postgres://", but SQLAlchemy needs
# "postgresql://" — this rewrites it automatically so the same DATABASE_URL
# Render gives you works without manual editing.
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True, nullable=False)
    hashed_password = Column(String, nullable=False)


class CelestrakCacheEntry(Base):
    """
    A REAL, persistent cache — stored in the database, not just in memory.
    This matters specifically because Render's free tier spins the service
    down after ~15 minutes of inactivity and restarts it fresh on the next
    request; an in-memory-only cache would be wiped on every single one of
    those restarts, meaning the very first request after any idle period
    would always trigger a fresh Celestrak hit regardless of how recently
    real data was actually fetched — directly contrary to Celestrak's own
    documented guidance to persist data with a timestamp and only
    re-fetch when it's genuinely stale, no matter how many times the
    calling process itself has restarted in the meantime.
    """
    __tablename__ = "celestrak_cache"
    group_name = Column(String, primary_key=True)
    data = Column(Text)
    cached_at = Column(DateTime(timezone=True))
    is_error = Column(Boolean)
    # Added after a real bug found via the user's own testing: is_error
    # alone can't distinguish "real Celestrak succeeded" from "the
    # Space-Track failover succeeded instead" \u2014 both were being recorded
    # identically as is_error=False, since both genuinely did return valid
    # data. This field is what the health check actually needs to report
    # accurately. Nullable for backward compatibility with rows written
    # before this field existed.
    source = Column(String, nullable=True)
    # Added 2026-08-06 after real, specific guidance from Celestrak's
    # maintainer (T.S. Kelso): a flat retry cooldown for every error type
    # is wrong and actively harmful for a 403 — repeating a request during
    # an active block prevents it from ever clearing. This stores WHICH
    # status code caused the failure so the retry window can be computed
    # per-code (see celestrak_retry_window_seconds()) instead of a single
    # constant for every error. Nullable: rows written before this field
    # existed, or connection-level failures with no HTTP status at all.
    error_status_code = Column(Integer, nullable=True)


class SpaceTrackCacheEntry(Base):
    """
    Persistent, database-backed cache for real Space-Track category
    queries — same reasoning and pattern as CelestrakCacheEntry above:
    survives free-tier restarts, avoids repeated real requests to a
    system already understood to be closely monitored.
    """
    __tablename__ = "space_track_cache"
    category_name = Column(String, primary_key=True)
    data = Column(Text)
    cached_at = Column(DateTime(timezone=True))
    is_error = Column(Boolean)


class TleSnapshot(Base):
    """
    Phase 7 — anomaly detection. Every time a real TLE is successfully
    fetched for a tracked object, its real orbital elements are recorded
    here with a timestamp. Comparing a new snapshot against the most
    recent PRIOR one for the same object is what makes maneuver detection
    possible — this is the actual history that comparison needs; nothing
    upstream of this currently stores it.
    """
    __tablename__ = "tle_snapshots"
    id = Column(Integer, primary_key=True)
    satellite_name = Column(String, nullable=False, index=True)
    tle_line1 = Column(String)
    tle_line2 = Column(String)
    inclination_deg = Column(Float)
    eccentricity = Column(Float)
    raan_deg = Column(Float)
    arg_perigee_deg = Column(Float)
    mean_motion_rev_per_day = Column(Float)
    bstar = Column(Float)
    fetched_at = Column(DateTime(timezone=True))


class DetectedAnomaly(Base):
    """A real, persisted record of each detected anomaly, so past results are queryable via GET /api/anomalies, not just reported once and lost."""
    __tablename__ = "detected_anomalies"
    id = Column(Integer, primary_key=True)
    satellite_name = Column(String, index=True)
    detected_at = Column(DateTime(timezone=True))
    previous_snapshot_at = Column(DateTime(timezone=True))
    days_between_snapshots = Column(Float)
    reasons_json = Column(Text)


class SkyrootTelemetry(Base):
    """
    Scaffolding for real Skyroot integration — NOT yet connected to
    anything real. Deliberately generic/flexible (raw JSON payload, not a
    fixed schema) since Skyroot's actual data format, update frequency,
    and authentication mechanism are not yet known — that's precisely what
    the project's Skyroot one-pager is asking them for. This table exists
    so the adapter and dashboard panel below have something real to write
    to and read from once that real data actually starts arriving; it is
    empty and will stay empty until then.
    """
    __tablename__ = "skyroot_telemetry"
    id = Column(Integer, primary_key=True)
    asset_id = Column(String, index=True)
    raw_payload_json = Column(Text)
    received_at = Column(DateTime(timezone=True))


def record_tle_and_check_anomaly(name: str, l1: str, l2: str, satrec) -> dict | None:
    """
    Records a real TLE snapshot for this object, and — if a prior snapshot
    exists — compares real orbital elements to flag likely maneuvers.

    IMPORTANT, honest framing: the thresholds below are reasoned,
    domain-informed heuristics based on real orbital mechanics (e.g., a
    satellite's inclination is normally extremely stable outside a
    deliberate plane-change burn; mean motion changes gradually and
    predictably due to drag, so a large sudden jump is a real signal of a
    station-keeping or altitude-change maneuver). This is NOT a trained
    statistical/ML model — there's no historical population of real
    maneuver events available yet to fit one properly, and building one
    without that data would repeat the exact "looks scientific but isn't
    grounded" problem flagged earlier in this project. This is genuine
    statistical outlier detection using real, defensible fixed rules, not
    a black box.
    """
    db = SessionLocal()
    try:
        now = datetime.now(timezone.utc)
        inclination_deg = math.degrees(satrec.inclo)
        eccentricity = satrec.ecco
        raan_deg = math.degrees(satrec.nodeo)
        arg_perigee_deg = math.degrees(satrec.argpo)
        mean_motion_rev_per_day = satrec.no_kozai * 1440 / (2 * math.pi)
        bstar = satrec.bstar

        previous = (
            db.query(TleSnapshot)
            .filter(TleSnapshot.satellite_name == name)
            .order_by(TleSnapshot.fetched_at.desc())
            .first()
        )

        anomaly_result = None
        if previous:
            # Defensive, explicit timezone handling — the same class of bug
            # (SQLite not preserving timezone info) caused a real crash
            # earlier in this project; handled the same defensive way here
            # rather than trusting a compact one-liner.
            previous_fetched_at = previous.fetched_at
            if previous_fetched_at.tzinfo is None:
                previous_fetched_at = previous_fetched_at.replace(tzinfo=timezone.utc)
            days_elapsed = (now - previous_fetched_at).total_seconds() / 86400

            if days_elapsed > 0.01:  # avoid divide-by-near-zero for near-simultaneous snapshots
                incl_rate = abs(inclination_deg - previous.inclination_deg) / days_elapsed
                mm_rate = abs(mean_motion_rev_per_day - previous.mean_motion_rev_per_day) / days_elapsed
                ecc_rate = abs(eccentricity - previous.eccentricity) / days_elapsed

                reasons = []
                if incl_rate > 0.05:
                    reasons.append(f"inclination changed {incl_rate:.4f} deg/day \u2014 unusual, inclination is normally very stable outside a deliberate plane-change maneuver")
                if mm_rate > 0.005:
                    reasons.append(f"mean motion changed {mm_rate:.5f} rev/day \u2014 unusual, much faster than typical gradual drag-driven change, consistent with a station-keeping or altitude-change burn")
                if ecc_rate > 0.001:
                    reasons.append(f"eccentricity changed {ecc_rate:.5f}/day \u2014 unusual")

                if reasons:
                    anomaly_result = {
                        "satellite_name": name,
                        "detected_at_utc": now.isoformat(),
                        "previous_snapshot_utc": previous.fetched_at.isoformat(),
                        "days_between_snapshots": round(days_elapsed, 3),
                        "reasons": reasons,
                    }
                    db.add(DetectedAnomaly(
                        satellite_name=name, detected_at=now,
                        previous_snapshot_at=previous.fetched_at,
                        days_between_snapshots=days_elapsed,
                        reasons_json=json.dumps(reasons),
                    ))

        db.add(TleSnapshot(
            satellite_name=name, tle_line1=l1, tle_line2=l2,
            inclination_deg=inclination_deg, eccentricity=eccentricity,
            raan_deg=raan_deg, arg_perigee_deg=arg_perigee_deg,
            mean_motion_rev_per_day=mean_motion_rev_per_day, bstar=bstar,
            fetched_at=now,
        ))
        db.commit()
        return anomaly_result
    finally:
        db.close()


Base.metadata.create_all(bind=engine)  # creates the users table if it doesn't already exist — safe to run every time the app starts

# Safe, defensive migration: create_all() above only creates NEW tables —
# it does NOT add new columns to a table that already exists on the real,
# live database. The `source` column was added to CelestrakCacheEntry
# after that table already existed in production, so without this, the
# real deployed database would be missing it entirely, and every query
# referencing it would fail at runtime. This checks whether the column
# already exists, and only adds it if genuinely missing — safe to run on
# every startup, whether the column is already there or not.
try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT source FROM celestrak_cache LIMIT 1"))
except Exception:
    try:
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE celestrak_cache ADD COLUMN source VARCHAR"))
            conn.commit()
        logger.info("Migration: added missing 'source' column to celestrak_cache table")
    except Exception as e:
        logger.warning(f"Migration attempt for celestrak_cache.source failed (non-fatal, may already exist): {e}")

# Same pattern, same reason: error_status_code was added to
# CelestrakCacheEntry after the table already existed in production.
try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT error_status_code FROM celestrak_cache LIMIT 1"))
except Exception:
    try:
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE celestrak_cache ADD COLUMN error_status_code INTEGER"))
            conn.commit()
        logger.info("Migration: added missing 'error_status_code' column to celestrak_cache table")
    except Exception as e:
        logger.warning(f"Migration attempt for celestrak_cache.error_status_code failed (non-fatal, may already exist): {e}")


class RegisterRequest(BaseModel):
    username: str
    password: str


class LoginRequest(BaseModel):
    username: str
    password: str


# ---------------------------------------------------------------------------
# Session tokens (JWT). SECRET_KEY comes from an environment variable, same
# principle as DATABASE_URL — never hardcoded, never committed to GitHub.
# The fallback value below is ONLY for local testing; on Render, a real
# random secret must be set, or every session becomes forgeable by anyone
# who's read this file (and this file is public, on GitHub).
# ---------------------------------------------------------------------------
SECRET_KEY = os.environ.get("SECRET_KEY", "local-testing-only-not-secure")
ALGORITHM = "HS256"
TOKEN_EXPIRE_HOURS = 24
COOKIE_NAME = "kakshya_session"


def create_session_token(username: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(hours=TOKEN_EXPIRE_HOURS)
    return jwt.encode({"sub": username, "exp": expire}, SECRET_KEY, algorithm=ALGORITHM)


def get_current_user(request: Request) -> str:
    """
    A dependency — any endpoint that includes this in its signature
    automatically requires a valid session. Reads the token from the
    HttpOnly cookie (never from anything client-side JavaScript could read
    or tamper with), verifies its signature and expiry, and returns the
    logged-in username. Raises a real 401 if the cookie is missing,
    expired, or has been tampered with.
    """
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        raise HTTPException(status_code=401, detail="Not logged in.")
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload["sub"]
    except JWTError:
        raise HTTPException(status_code=401, detail="Session invalid or expired.")


# Same real NOAA endpoints and User-Agent header already proven working in
# the existing Node.js version (solar-wind.js) — not re-guessed, mirrored
# exactly from what was already confirmed live.
PLASMA_URL = "https://services.swpc.noaa.gov/json/rtsw/rtsw_wind_1m.json"
MAG_URL = "https://services.swpc.noaa.gov/json/rtsw/rtsw_mag_1m.json"
FETCH_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; KakshyaDashboard/1.0; +https://kakshaya.netlify.app)",
    "Accept": "application/json",
}

# Same whitelist as the existing Node.js celestrak-group.js — deliberately
# not an open proxy for arbitrary Celestrak queries.
ALLOWED_GROUPS = {
    "stations", "starlink", "weather", "gps-ops", "geo", "resource", "science",
    "cosmos-2251-debris", "iridium-33-debris", "fengyun-1c-debris",
}
CELESTRAK_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; KakshyaDashboard/1.0; +https://kakshya1.netlify.app)",
    "Accept": "text/plain",
}

# Real base URL Celestrak requests are built from. Kept as a mutable
# module-level value (not a constant) specifically so celestrak_get()
# below can update it automatically if Celestrak ever issues a redirect —
# see the 2026-08-06 correspondence with T.S. Kelso, who pointed out that
# repeatedly hitting an old URL after receiving an HTTP 301 is exactly
# the kind of behavior that gets an IP firewalled.
CELESTRAK_BASE_URL = "https://celestrak.org/NORAD/elements/gp.php"


def celestrak_retry_window_seconds(status_code):
    """
    How long to wait before trying Celestrak again, given the status code
    of the last failure. Not a single flat number — per real, specific
    guidance from Celestrak's maintainer (T.S. Kelso, 2026-08-06):

      - HTTP 403: a temporary block, but it does NOT clear unless queries
        stop for a minimum of 2 hours. Retrying sooner guarantees it never
        clears, and repeated offenses risk being sent to the firewall.
      - HTTP 404: not a temporary problem at all — the group/URL itself is
        wrong. No amount of waiting fixes it, so this gets a long window
        and should be flagged for manual review rather than silently
        retried forever on a timer.
      - HTTP 301: normally followed automatically by celestrak_get() below
        before this is ever reached. If one still lands here (e.g. a
        redirect loop we declined to follow), treat it with the same
        caution as a 404 — it means the automatic handling didn't resolve
        cleanly and a human should look at it.
      - HTTP 500: a genuine server-side hiccup on Celestrak's end. Kelso
        confirmed a short wait is reasonable here.
      - Anything else (including connection-level errors with no HTTP
        response at all — timeouts, DNS failures): ambiguous, so a
        cautious middle-ground default.
    """
    if status_code == 403:
        return 7200  # 2 hours minimum, per Kelso
    if status_code in (404, 301):
        return 86400  # not temporary — long window, needs manual review
    if status_code == 500:
        return 600  # 10 minutes is fine per Kelso
    return 3600  # unknown / connection-level error — cautious default


async def celestrak_get(client: httpx.AsyncClient, group: str):
    """
    The one place that actually calls Celestrak, for both real usage
    paths (the /api/celestrak/{group} endpoint and the WebSocket demo's
    load_tracked_satellites()). Two real fixes live here, both directly
    from Kelso's 2026-08-06 feedback:

      1. follow_redirects=True — previously every Celestrak call in this
         file lacked this, so an HTTP 301 came back as a dead-end error
         instead of being followed like a normal client would.
      2. If a redirect DID happen, the new base URL is captured from
         resp.history and CELESTRAK_BASE_URL is updated for all future
         requests — so we stop asking for the old URL entirely, rather
         than paying for a redirect on every single request forever.
    """
    global CELESTRAK_BASE_URL
    url = f"{CELESTRAK_BASE_URL}?GROUP={group}&FORMAT=TLE"
    resp = await client.get(url, headers=CELESTRAK_HEADERS, follow_redirects=True)

    if resp.history:
        first_redirect = resp.history[0]
        location = first_redirect.headers.get("location", "")
        target = location if location.startswith("http") else str(resp.url)
        parsed = urlparse(target)
        new_base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if new_base and new_base != CELESTRAK_BASE_URL:
            logger.warning(
                f"Celestrak issued HTTP {first_redirect.status_code} for group \"{group}\": "
                f"updating base URL from {CELESTRAK_BASE_URL} to {new_base} for all future requests."
            )
            CELESTRAK_BASE_URL = new_base

    return resp

# Cache is now database-backed (see CelestrakCacheEntry model above) so it
# survives free-tier restarts, not just in-memory within one process run.


@app.get("/")
def read_root():
    return {"status": "alive", "message": "Kakshya backend is running"}


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.post("/api/register")
@limiter.limit("10/hour")
def register(request: Request, req: RegisterRequest):
    """
    Creates a real user account. The password itself is never stored —
    only a one-way bcrypt hash of it, so even someone with direct database
    access can't recover the actual password. Rate-limited to 10 attempts
    per hour per IP address, to prevent mass automated account creation.
    """
    db = SessionLocal()
    try:
        existing = db.query(User).filter(User.username == req.username).first()
        if existing:
            logger.warning(f"Registration attempt for already-taken username: {req.username}")
            return JSONResponse(status_code=400, content={"error": "That username is already taken."})

        hashed = bcrypt.hashpw(req.password.encode("utf-8"), bcrypt.gensalt())
        new_user = User(username=req.username, hashed_password=hashed.decode("utf-8"))
        db.add(new_user)
        db.commit()
        logger.info(f"New account registered: {req.username}")
        return {"status": "ok", "message": f"Account created for {req.username}"}
    finally:
        db.close()


@app.post("/api/login")
@limiter.limit("5/minute")
def login(request: Request, req: LoginRequest, response: Response):
    """
    Checks real credentials against the database, and issues a real signed
    session token as an HttpOnly cookie on success. Rate-limited to 5
    attempts per minute per IP — the real, server-side version of the
    protection the old frontend-only lockout only ever pretended to
    provide. HttpOnly means client-side JavaScript can never read or
    tamper with the session cookie, which is what makes this a real
    session rather than just a client-side "remember I logged in" flag.
    """
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.username == req.username).first()
        if not user:
            logger.warning(f"Failed login attempt (unknown user): {req.username}")
            return JSONResponse(status_code=401, content={"error": "Incorrect username or password."})

        correct = bcrypt.checkpw(req.password.encode("utf-8"), user.hashed_password.encode("utf-8"))
        if not correct:
            logger.warning(f"Failed login attempt (wrong password): {req.username}")
            return JSONResponse(status_code=401, content={"error": "Incorrect username or password."})

        token = create_session_token(user.username)
        response.set_cookie(
            key=COOKIE_NAME,
            value=token,
            httponly=True,
            secure=True,       # required alongside samesite="none" — browsers reject SameSite=None cookies without Secure
            samesite="none",   # required for the cookie to be sent back on cross-origin requests (frontend and backend are different domains) — "lax" (the more common default) does not reliably work for this
            max_age=TOKEN_EXPIRE_HOURS * 3600,
        )
        logger.info(f"Successful login: {req.username}")
        return {"status": "ok", "message": f"Welcome, {req.username} — credentials verified, session started."}
    finally:
        db.close()


@app.post("/api/logout")
def logout(response: Response):
    """Clears the session cookie — the real, complete way to log out."""
    response.delete_cookie(COOKIE_NAME)
    return {"status": "ok", "message": "Logged out."}


@app.get("/api/me")
def get_me(current_user: str = Depends(get_current_user)):
    """
    A real protected endpoint — this is the proof that sessions actually
    work. Only responds successfully if a valid, unexpired session cookie
    was sent; otherwise the get_current_user dependency raises a 401
    before this function's own code ever runs.
    """
    return {"status": "ok", "logged_in_as": current_user}


@app.get("/api/space-weather")
async def space_weather():
    """
    Real-time solar wind data, fetched server-side from NOAA — same data
    the existing Node.js proxy already serves, now from this new backend
    instead. Real attempt, honest failure: if NOAA is unreachable or
    returns an error, this reports the specific real error rather than
    hiding it behind a generic message or fabricating a fallback reading.
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            plasma_resp = await client.get(PLASMA_URL, headers=FETCH_HEADERS)
            mag_resp = await client.get(MAG_URL, headers=FETCH_HEADERS)

        if plasma_resp.status_code != 200:
            return {"error": f"NOAA plasma endpoint returned HTTP {plasma_resp.status_code}"}
        if mag_resp.status_code != 200:
            return {"error": f"NOAA mag endpoint returned HTTP {mag_resp.status_code}"}

        return {"plasma": plasma_resp.json(), "mag": mag_resp.json()}

    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Space-Track failover for Celestrak categories — ONLY for categories
# genuinely proven (via real, staged testing) to have a comprehensive,
# correct Space-Track equivalent. Deliberately does NOT include weather,
# resource, or science (tested but found incomplete — dominated by
# debris/defunct objects under simple name matching) or stations
# (fundamentally needs a curated ID list, not a category query).
# ---------------------------------------------------------------------------
SPACE_TRACK_FAILOVER_MAP = {
    "starlink": {"type": "name", "value": "STARLINK"},
    "gps-ops": {"type": "name", "value": "NAVSTAR"},
    "cosmos-2251-debris": {"type": "name", "value": "COSMOS 2251"},
    "iridium-33-debris": {"type": "name", "value": "IRIDIUM 33"},
    "fengyun-1c-debris": {"type": "name", "value": "FENGYUN 1C"},
    "geo": {"type": "period_range", "value": (1430, 1445)},
    # Added after direct, real testing confirmed OBJECT_TYPE=PAYLOAD
    # cleanly removes the debris ("DEB") entries that dominated the
    # earlier unfiltered "weather" result \u2014 verified zero debris
    # entries in the filtered result, all real, current NOAA satellites.
    "weather": {"type": "name_payload_only", "value": "NOAA"},
    # Added after direct testing: OBJECT_TYPE=PAYLOAD alone left old
    # 1970s-era LANDSAT satellites in the result (still technically
    # payloads, just long defunct). A LAUNCH_DATE range excludes them \u2014
    # verified result was LANDSAT 8 (2013) and LANDSAT 9 (2021) only,
    # both real, current, operational satellites.
    "resource": {"type": "name_payload_launchdate", "value": "LANDSAT", "launch_range": ("2005-01-01", "2030-01-01")},
    # Deliberately narrow, and honestly incomplete \u2014 only two real,
    # individually-verified science satellites (HST, SWIFT). A third
    # candidate (CHANDRA) was tested and found to incorrectly match
    # "CHANDRAYAAN" (India's lunar missions) as a substring \u2014 a real bug,
    # not just noise \u2014 so it was dropped rather than guessed around.
    # HST, SWIFT verified earlier. TESS added after direct testing: a
    # single, clean, unambiguous match (NORAD 43435) with no substring
    # collision risk (unlike CHANDRA, which incorrectly matched
    # CHANDRAYAAN and was deliberately excluded).
    "science": {"type": "multi_name_payload", "value": ["HST", "SWIFT", "TESS"]},
    # Reuses the same shared KNOWN_STATION_NORAD_IDS the tracker's own
    # failover already uses (individual lookups, not a name search) \u2014
    # honestly limited to ISS only, same real reason as there.
    "stations": {"type": "norad_id_list"},
}


def space_track_json_to_tle_text(objects: list) -> str:
    """
    Converts Space-Track's JSON GP records into Celestrak-style plain-text
    TLE format (3 lines per object: name, line 1, line 2) — so the
    failover is genuinely transparent to anything consuming this endpoint,
    which has always expected Celestrak's plain-text shape, not JSON.
    """
    lines = []
    for obj in objects:
        name = obj.get("TLE_LINE0", obj.get("OBJECT_NAME", "")).lstrip("0 ").strip()
        l1 = obj.get("TLE_LINE1")
        l2 = obj.get("TLE_LINE2")
        if name and l1 and l2:
            lines.append(name)
            lines.append(l1)
            lines.append(l2)
    return "\n".join(lines)


async def try_space_track_failover(group: str) -> tuple[bool, str]:
    """
    Attempts the Space-Track failover for one Celestrak group. Returns
    (success, data_or_error_message). Only ever called for groups in
    SPACE_TRACK_FAILOVER_MAP \u2014 real, proven equivalents, not a guess.
    """
    if group not in SPACE_TRACK_FAILOVER_MAP or not SPACE_TRACK_USERNAME or not SPACE_TRACK_PASSWORD:
        return False, "No Space-Track failover available for this group, or credentials not configured"

    mapping = SPACE_TRACK_FAILOVER_MAP[group]
    login_url = "https://www.space-track.org/ajaxauth/login"

    try:
        async with httpx.AsyncClient(timeout=25.0) as client:
            login_resp = await client.post(login_url, data={"identity": SPACE_TRACK_USERNAME, "password": SPACE_TRACK_PASSWORD})
            if login_resp.status_code != 200:
                return False, f"Space-Track login failed: HTTP {login_resp.status_code}"

            if mapping["type"] == "multi_name_payload":
                # Multiple separate, small queries (one per confidently-real
                # name) merged together \u2014 chosen over guessing at an
                # unverified multi-value query syntax, reusing the exact
                # reliable single-name pattern already proven working
                # elsewhere. Used for "science", deliberately narrow.
                all_active = []
                for name in mapping["value"]:
                    q_url = f"https://www.space-track.org/basicspacedata/query/class/gp/OBJECT_NAME/~~{name}/OBJECT_TYPE/PAYLOAD/orderby/EPOCH%20desc/limit/10/format/json"
                    resp = await client.get(q_url)
                    if resp.status_code == 200:
                        data = resp.json()
                        if isinstance(data, list):
                            all_active.extend([obj for obj in data if not obj.get("DECAY_DATE")])
                    await asyncio.sleep(1.0)
                active = all_active
            elif mapping["type"] == "norad_id_list":
                # Individual lookups by known NORAD ID, not a name search
                # \u2014 "stations" is a small curated list, not a searchable
                # pattern. Reuses the exact same shared
                # KNOWN_STATION_NORAD_IDS the real-time tracker's own
                # failover already uses, so both stay consistent.
                all_active = []
                for norad_id in KNOWN_STATION_NORAD_IDS:
                    q_url = f"https://www.space-track.org/basicspacedata/query/class/gp/NORAD_CAT_ID/{norad_id}/format/json"
                    resp = await client.get(q_url)
                    if resp.status_code == 200:
                        data = resp.json()
                        if isinstance(data, list):
                            all_active.extend([obj for obj in data if not obj.get("DECAY_DATE")])
                    await asyncio.sleep(1.0)
                active = all_active
            else:
                if mapping["type"] == "name":
                    query_url = f"https://www.space-track.org/basicspacedata/query/class/gp/OBJECT_NAME/~~{mapping['value']}/orderby/EPOCH%20desc/limit/{SPACE_TRACK_RESULT_CAP}/format/json"
                elif mapping["type"] == "name_payload_only":
                    # Adds OBJECT_TYPE/PAYLOAD to exclude debris entries
                    # that a plain name search picks up \u2014 verified via
                    # real testing to cleanly remove "DEB" entries for
                    # weather/NOAA specifically.
                    query_url = f"https://www.space-track.org/basicspacedata/query/class/gp/OBJECT_NAME/~~{mapping['value']}/OBJECT_TYPE/PAYLOAD/orderby/EPOCH%20desc/limit/{SPACE_TRACK_RESULT_CAP}/format/json"
                elif mapping["type"] == "name_payload_launchdate":
                    # Adds a LAUNCH_DATE range on top of the payload filter,
                    # to exclude old-but-still-a-payload objects the
                    # payload filter alone can't catch \u2014 verified via real
                    # testing for "resource"/LANDSAT specifically.
                    low, high = mapping["launch_range"]
                    query_url = f"https://www.space-track.org/basicspacedata/query/class/gp/OBJECT_NAME/~~{mapping['value']}/OBJECT_TYPE/PAYLOAD/LAUNCH_DATE/{low}--{high}/orderby/EPOCH%20desc/limit/{SPACE_TRACK_RESULT_CAP}/format/json"
                else:  # period_range
                    low, high = mapping["value"]
                    query_url = f"https://www.space-track.org/basicspacedata/query/class/gp/PERIOD/{low}--{high}/orderby/EPOCH%20desc/limit/{SPACE_TRACK_RESULT_CAP}/format/json"

                data_resp = await client.get(query_url)
                if data_resp.status_code != 200:
                    return False, f"Space-Track query failed: HTTP {data_resp.status_code}"
                raw_data = data_resp.json()
                active = [obj for obj in raw_data if not obj.get("DECAY_DATE")] if isinstance(raw_data, list) else []

            if not active:
                return False, "Space-Track returned no active objects for this group"

            tle_text = space_track_json_to_tle_text(active)
            return True, tle_text

    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


@app.get("/api/celestrak/{group}")
async def celestrak_group(group: str):
    """
    Real satellite category data, fetched server-side from Celestrak — same
    whitelist and identifying header as before, now with a REAL, PERSISTENT
    database-backed cache (not just in-memory, and not just a client-side
    header) so repeated calls from any source — browser, curl, a monitoring
    script, multiple different users, or this same service restarting after
    a free-tier idle spin-down — only ever result in Celestrak actually
    being contacted once per real cache window, never more.

    NEW: if Celestrak fails, automatically falls back to Space-Track for
    the categories genuinely proven to have a comprehensive, correct
    equivalent (see SPACE_TRACK_FAILOVER_MAP below). Categories without a
    proven equivalent yet (weather, resource, science, stations) get the
    honest Celestrak error instead of a silently incomplete substitute.
    """
    if group not in ALLOWED_GROUPS:
        return JSONResponse(
            status_code=400,
            content={"error": f'Unknown or missing group "{group}" — this proxy only serves the specific Celestrak groups Kakshya uses.'},
        )

    now = datetime.now(timezone.utc)
    db = SessionLocal()
    try:
        cached = db.query(CelestrakCacheEntry).filter(CelestrakCacheEntry.group_name == group).first()
        if cached:
            # Defensive fix for a real bug caught in testing: SQLite (used
            # for local testing) doesn't preserve timezone info the way
            # PostgreSQL does, so a value read back from the database can
            # come back timezone-naive even though it was stored as
            # timezone-aware. Explicitly assume UTC if naive, rather than
            # letting the subtraction below crash.
            cached_at = cached.cached_at
            if cached_at.tzinfo is None:
                cached_at = cached_at.replace(tzinfo=timezone.utc)
            age_seconds = (now - cached_at).total_seconds()
            # Successes cached for 4 hours (unchanged intent from before).
            # Failures: previously a flat 1 hour for every error type —
            # fixed 2026-08-06 per direct guidance from Celestrak's
            # maintainer (T.S. Kelso). A 403 needs a minimum of 2 hours to
            # actually clear; retrying sooner guarantees it never does. A
            # 404 isn't temporary at all, so it gets a long window and is
            # flagged for manual review below rather than retried on a
            # timer forever.
            cache_window = 14400 if not cached.is_error else celestrak_retry_window_seconds(cached.error_status_code)
            if age_seconds < cache_window:
                if cached.is_error:
                    needs_manual_review = cached.error_status_code in (404, 301)
                    return JSONResponse(status_code=502, content={
                        "error": cached.data,
                        "note": "cached failure — not re-attempted yet, to avoid repeatedly hitting Celestrak",
                        "cached_at_utc": cached_at.isoformat(),
                        "cache_age_minutes": round(age_seconds / 60, 1),
                        "will_retry_in_minutes": round((cache_window - age_seconds) / 60, 1),
                        "needs_manual_review": needs_manual_review,
                        "manual_review_reason": f"HTTP {cached.error_status_code} is not a temporary problem — the group/URL itself likely needs fixing, not just time." if needs_manual_review else None,
                    })
                return PlainTextResponse(content=cached.data, headers={"Cache-Control": "public, max-age=14400", "X-Cache": "hit", "X-Source": cached.source or "unknown"})

        def save_cache(data: str, is_error: bool, source: str, status_code: int = None):
            existing = db.query(CelestrakCacheEntry).filter(CelestrakCacheEntry.group_name == group).first()
            if existing:
                existing.data = data
                existing.cached_at = now
                existing.is_error = is_error
                existing.source = source
                existing.error_status_code = status_code
            else:
                db.add(CelestrakCacheEntry(group_name=group, data=data, cached_at=now, is_error=is_error, source=source, error_status_code=status_code))
            db.commit()

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await celestrak_get(client, group)

            celestrak_error = None
            error_status_code = None
            if resp.status_code != 200:
                celestrak_error = f'Celestrak returned HTTP {resp.status_code} for group "{group}"'
                error_status_code = resp.status_code
            elif not resp.text or not resp.text.strip():
                celestrak_error = f'Celestrak returned an empty response for group "{group}" — the group name may no longer be valid'

            if celestrak_error is None:
                save_cache(resp.text, False, "celestrak")
                return PlainTextResponse(
                    content=resp.text,
                    headers={"Cache-Control": "public, max-age=14400", "X-Cache": "miss", "X-Source": "celestrak"},
                )

        except Exception as e:
            celestrak_error = str(e)
            error_status_code = None  # connection-level, no HTTP status at all

        # Celestrak failed (whichever way) — try the Space-Track failover
        # before giving up, but ONLY for groups with a real, proven
        # equivalent (see SPACE_TRACK_FAILOVER_MAP).
        failover_ok, failover_data = await try_space_track_failover(group)
        if failover_ok:
            save_cache(failover_data, False, "space-track-failover")
            return PlainTextResponse(
                content=failover_data,
                headers={"Cache-Control": "public, max-age=14400", "X-Cache": "miss", "X-Source": "space-track-failover"},
            )

        # Both Celestrak and any available failover failed — the honest
        # original Celestrak error, not a fabricated or silently-empty
        # substitute.
        save_cache(celestrak_error, True, "none", error_status_code)
        return JSONResponse(status_code=502, content={
            "error": celestrak_error,
            "failover_attempted": group in SPACE_TRACK_FAILOVER_MAP,
            "failover_result": failover_data if group in SPACE_TRACK_FAILOVER_MAP else None,
            "needs_manual_review": error_status_code in (404, 301),
        })
    finally:
        db.close()


# ---------------------------------------------------------------------------
# DIAGNOSTIC ONLY — added to determine whether the ongoing ConnectTimeout
# issue is specific to Celestrak or a broader Render-outbound-networking
# problem, by testing a genuinely different external TLE provider (N2YO).
# Deliberately isolated: does not touch, replace, or share any code with
# the real Celestrak proxy above. Safe to remove entirely without
# affecting anything else — see BACKEND_BUILD_LOG.txt for the exact
# checkpoint this was added on top of.
#
# Honest note on the N2YO API shape below: reconstructed from general
# knowledge of their documented REST API, not independently verified live
# (n2yo.com isn't reachable from the development/testing environment this
# was built in). Real behavior will only be confirmed once this is
# actually deployed and tested against a real N2YO API key.
# ---------------------------------------------------------------------------
N2YO_API_KEY = os.environ.get("N2YO_API_KEY")
ISS_NORAD_ID = 25544  # a well-known, stable object — same one used throughout this project's own testing


@app.get("/api/debug/n2yo-test")
async def n2yo_diagnostic_test():
    """
    Isolated diagnostic: attempts one real fetch from N2YO for the ISS TLE.
    Reports a clear, honest result either way — this is explicitly a
    same-vs-different-provider test, not a new production feature.
    """
    if not N2YO_API_KEY:
        return {
            "status": "not_configured",
            "detail": "N2YO_API_KEY not set \u2014 add it in Render's Environment settings to run this test",
        }

    url = f"https://api.n2yo.com/rest/v1/satellite/tle/{ISS_NORAD_ID}"
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, params={"apiKey": N2YO_API_KEY})

        if resp.status_code != 200:
            return {
                "status": "error",
                "detail": f"N2YO returned HTTP {resp.status_code}",
                "diagnostic_conclusion": "N2YO also failing \u2014 points toward a broader Render-networking issue, not Celestrak specifically",
            }

        data = resp.json()
        return {
            "status": "success",
            "detail": "N2YO reachable and returned real data",
            "diagnostic_conclusion": "N2YO works while Celestrak doesn't \u2014 confirms the issue is specific to Celestrak, not Render's networking",
            "sample_data": data,
        }

    except Exception as e:
        return {
            "status": "error",
            "detail": f"{type(e).__name__}: {e}",
            "diagnostic_conclusion": "N2YO also failing (connection-level) \u2014 points toward a broader Render-networking issue, not Celestrak specifically",
        }


@app.get("/api/debug/n2yo-grouping-test")
async def n2yo_grouping_diagnostic_test():
    """
    DIAGNOSTIC ONLY \u2014 tests N2YO's category/grouping capability, to see
    what it actually returns before deciding whether it can replace
    Celestrak's GROUP= bulk-category browsing (used by TRACK CATEGORY on
    the main dashboard).

    Honest, explicit uncertainty: N2YO's "above" endpoint is understood
    (not independently verified live) to return satellites of a given
    category currently overhead a SPECIFIC location within a search
    radius \u2014 a fundamentally different query shape than Celestrak's
    GROUP=, which returns every satellite in a category globally,
    regardless of location. This test exists specifically to confirm or
    correct that understanding with a real result, not to assume it.
    """
    if not N2YO_API_KEY:
        return {"status": "not_configured", "detail": "N2YO_API_KEY not set"}

    # A real, arbitrary Indian location (New Delhi) and a generous 70-degree
    # search radius \u2014 category 0 is believed to mean "all categories" in
    # N2YO's scheme, but this is exactly the kind of detail this test is
    # meant to confirm for real, not assume.
    lat, lng, alt = 28.6139, 77.2090, 0
    radius_deg = 70
    category_id = 0
    url = f"https://api.n2yo.com/rest/v1/satellite/above/{lat}/{lng}/{alt}/{radius_deg}/{category_id}"

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, params={"apiKey": N2YO_API_KEY})

        if resp.status_code != 200:
            return {"status": "error", "detail": f"N2YO returned HTTP {resp.status_code}"}

        data = resp.json()
        sat_count = len(data.get("above", []))
        return {
            "status": "success",
            "detail": f"N2YO 'above' endpoint returned {sat_count} object(s) currently over New Delhi (within {radius_deg}\u00b0)",
            "honest_interpretation": "If this looks location-scoped (a modest count tied to one point on Earth) rather than a full global category list, it confirms this is NOT a drop-in replacement for Celestrak's GROUP= bulk category browsing \u2014 it answers a different question (what's overhead right now) than TRACK CATEGORY currently does (show me the whole category, anywhere).",
            "sample_data": data,
        }

    except Exception as e:
        return {"status": "error", "detail": f"{type(e).__name__}: {e}"}


# Cached result for the category sweep below \u2014 this is a genuinely
# expensive diagnostic (many real requests to N2YO), so it should only
# ever actually run once and be reused after that, not re-triggered on
# every visit. Same principle applied throughout this whole project:
# never hammer an external provider more than genuinely necessary.
_category_sweep_cache = {"result": None}


@app.get("/api/debug/n2yo-category-sweep")
async def n2yo_category_sweep():
    """
    DIAGNOSTIC ONLY \u2014 tests a real range of N2YO category IDs (0\u2013100)
    against a real location, reporting back real satellite names for
    each, so category IDs can be identified from genuine evidence rather
    than an asserted lookup table nobody has verified.

    Deliberately conservative: a 0.5s delay between each of the 101 real
    requests (this alone takes ~50+ seconds to complete), and the result
    is cached after the first successful run so this expensive sweep
    never needs to repeat itself on a second visit.
    """
    if not N2YO_API_KEY:
        return {"status": "not_configured", "detail": "N2YO_API_KEY not set"}

    if _category_sweep_cache["result"] is not None:
        return {"status": "success", "note": "cached from the first real sweep \u2014 not re-run", **_category_sweep_cache["result"]}

    lat, lng, alt, radius_deg = 28.6139, 77.2090, 0, 70
    results = {}

    async with httpx.AsyncClient(timeout=15.0) as client:
        for category_id in range(0, 101):
            url = f"https://api.n2yo.com/rest/v1/satellite/above/{lat}/{lng}/{alt}/{radius_deg}/{category_id}"
            try:
                resp = await client.get(url, params={"apiKey": N2YO_API_KEY})
                if resp.status_code == 200:
                    data = resp.json()
                    above = data.get("above", [])
                    results[str(category_id)] = {
                        "count": len(above),
                        "sample_names": [s.get("satname") for s in above[:3]],
                    }
                else:
                    results[str(category_id)] = {"error": f"HTTP {resp.status_code}"}
            except Exception as e:
                results[str(category_id)] = {"error": f"{type(e).__name__}: {e}"}

            await asyncio.sleep(0.5)  # respectful pacing \u2014 never hammer, same lesson as Celestrak

    final = {
        "detail": "Real results for category IDs 0\u2013100, from a live sweep against New Delhi \u2014 use the sample_names to identify which ID (if any) maps to something meaningful like 'Starlink' or 'weather'",
        "results_by_category_id": results,
    }
    _category_sweep_cache["result"] = final
    return {"status": "success", **final}


@app.get("/api/debug/n2yo-starlink-max-radius-test")
async def n2yo_starlink_max_radius_test():
    """
    DIAGNOSTIC ONLY \u2014 tests whether N2YO's /above endpoint (category 52,
    confirmed to be Starlink via n2yo.com's own website) can approximate a
    full, location-independent category list by using the maximum radius
    the API allows (90 degrees \u2014 the whole visible sky from one point).

    Purpose: N2YO's website shows a full, unscoped Starlink list, but that
    may be a separate website feature, not something exposed the same way
    via their public REST API (which our backend must actually call). This
    test provides real evidence either way, by comparing the real count
    returned against Starlink's real, known constellation size (several
    thousand active satellites as of this project's own knowledge).
    """
    if not N2YO_API_KEY:
        return {"status": "not_configured", "detail": "N2YO_API_KEY not set"}

    lat, lng, alt = 28.6139, 77.2090, 0
    max_radius_deg = 90
    starlink_category_id = 52
    url = f"https://api.n2yo.com/rest/v1/satellite/above/{lat}/{lng}/{alt}/{max_radius_deg}/{starlink_category_id}"

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.get(url, params={"apiKey": N2YO_API_KEY})

        if resp.status_code != 200:
            return {"status": "error", "detail": f"N2YO returned HTTP {resp.status_code}"}

        data = resp.json()
        above = data.get("above", [])
        count = len(above)

        return {
            "status": "success",
            "count_returned": count,
            "honest_interpretation": (
                "If this count is in the low thousands (close to Starlink's real total constellation size), "
                "the /above endpoint at max radius genuinely approximates a full category query \u2014 usable as a "
                "TRACK CATEGORY replacement. If it's a much smaller number (hundreds or less), this confirms "
                "the API remains fundamentally location-limited even at maximum radius, and the full unscoped "
                "list seen on n2yo.com's own website is likely a separate feature not available this way via the API."
            ),
            "sample_names": [s.get("satname") for s in above[:5]],
        }

    except Exception as e:
        return {"status": "error", "detail": f"{type(e).__name__}: {e}"}


SPACE_TRACK_USERNAME = os.environ.get("SPACE_TRACK_USERNAME")
SPACE_TRACK_PASSWORD = os.environ.get("SPACE_TRACK_PASSWORD")


@app.get("/api/debug/space-track-test")
async def space_track_diagnostic_test():
    """
    DIAGNOSTIC ONLY \u2014 tests real Space-Track.org access: a real session
    login, followed by ONE small, targeted data request (ISS only, the
    same well-known object used throughout this project's own testing).

    Deliberately minimal, on purpose: Space-Track is understood (per
    Celestrak's own documentation, referencing a real past Space-Track
    outage that caused cascading over-requesting from other users) to be
    a more heavily monitored, stricter system than either Celestrak or
    N2YO. This test makes exactly two real requests total \u2014 one login,
    one data query \u2014 not a sweep, out of genuine caution learned directly
    from this project's own Celestrak experience.

    Honest note: the exact API request shape below is reconstructed from
    general knowledge of Space-Track's documented API, not independently
    verified live (space-track.org isn't reachable from the development
    environment this was built in). Real behavior will only be confirmed
    once this actually runs against real, logged-in credentials.
    """
    if not SPACE_TRACK_USERNAME or not SPACE_TRACK_PASSWORD:
        return {
            "status": "not_configured",
            "detail": "SPACE_TRACK_USERNAME / SPACE_TRACK_PASSWORD not set \u2014 add both in Render's Environment settings to run this test",
        }

    login_url = "https://www.space-track.org/ajaxauth/login"
    query_url = "https://www.space-track.org/basicspacedata/query/class/gp/NORAD_CAT_ID/25544/format/json"

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            login_resp = await client.post(
                login_url,
                data={"identity": SPACE_TRACK_USERNAME, "password": SPACE_TRACK_PASSWORD},
            )
            if login_resp.status_code != 200:
                return {
                    "status": "error",
                    "detail": f"Login failed: HTTP {login_resp.status_code}",
                    "stage": "login",
                }

            # The same authenticated client (carrying the real session
            # cookie from login) makes the one real data request.
            data_resp = await client.get(query_url)
            if data_resp.status_code != 200:
                return {
                    "status": "error",
                    "detail": f"Login succeeded, but the data query failed: HTTP {data_resp.status_code}",
                    "stage": "query",
                }

            data = data_resp.json()
            return {
                "status": "success",
                "detail": "Real login and real data query both succeeded",
                "sample_data": data,
            }

    except Exception as e:
        return {"status": "error", "detail": f"{type(e).__name__}: {e}"}


@app.get("/api/debug/space-track-bulk-test")
async def space_track_bulk_diagnostic_test():
    """
    DIAGNOSTIC ONLY \u2014 tests whether Space-Track's query API can do a
    Celestrak-GROUP=-style bulk category query (e.g., "all Starlink
    satellites") rather than only single-object lookups.

    Deliberately minimal, same reasoning as the first Space-Track test:
    exactly two real requests (one login, one query), and the query
    itself is capped to a small result limit \u2014 this proves the mechanism
    works without ever pulling a large, uncapped result on a first test
    against a system already understood to be closely monitored.

    Honest note: the exact query syntax below (OBJECT_NAME with a '~~'
    contains-match, and a /limit/ clause) is reconstructed from general
    knowledge of Space-Track's documented query language, not
    independently verified live. This test exists specifically to
    confirm or correct that understanding with a real result.
    """
    if not SPACE_TRACK_USERNAME or not SPACE_TRACK_PASSWORD:
        return {"status": "not_configured", "detail": "SPACE_TRACK_USERNAME / SPACE_TRACK_PASSWORD not set"}

    login_url = "https://www.space-track.org/ajaxauth/login"
    # Deliberately capped to 5 results \u2014 this is a mechanism test, not a
    # real data pull. OBJECT_NAME/~~STARLINK is a "contains" match, per
    # Space-Track's documented query language (reconstructed, not
    # independently verified \u2014 see docstring above).
    query_url = "https://www.space-track.org/basicspacedata/query/class/gp/OBJECT_NAME/~~STARLINK/orderby/EPOCH%20desc/limit/5/format/json"

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            login_resp = await client.post(login_url, data={"identity": SPACE_TRACK_USERNAME, "password": SPACE_TRACK_PASSWORD})
            if login_resp.status_code != 200:
                return {"status": "error", "detail": f"Login failed: HTTP {login_resp.status_code}", "stage": "login"}

            data_resp = await client.get(query_url)
            if data_resp.status_code != 200:
                return {
                    "status": "error",
                    "detail": f"Login succeeded, but the bulk query failed: HTTP {data_resp.status_code}",
                    "stage": "bulk_query",
                    "raw_response_text": data_resp.text[:500],
                }

            data = data_resp.json()
            names = [obj.get("OBJECT_NAME") for obj in data] if isinstance(data, list) else None

            return {
                "status": "success",
                "detail": f"Bulk name-pattern query succeeded \u2014 returned {len(data) if isinstance(data, list) else '?'} object(s), capped at 5",
                "honest_interpretation": "If these are real Starlink satellites, this confirms Space-Track CAN do category-style bulk queries \u2014 a genuine potential replacement for Celestrak's GROUP= browsing, pending real rate-limit research before removing the cap.",
                "returned_names": names,
            }

    except Exception as e:
        return {"status": "error", "detail": f"{type(e).__name__}: {e}"}


@app.get("/api/debug/space-track-scale-test")
async def space_track_scale_diagnostic_test():
    """
    DIAGNOSTIC ONLY \u2014 tests progressively larger result limits (50, then
    200, then 500) against Space-Track's real bulk query, ONE login
    followed by up to 3 real queries, with a 2-second pause between each.

    Critically: stops immediately the moment any tier shows trouble (a
    non-200 response, an unexpectedly small result, or an error) rather
    than blindly continuing to the next, larger tier. This is a direct,
    deliberate application of the lesson learned from the Celestrak
    episode \u2014 escalate carefully, watch for the first sign of a problem,
    don't assume generous limits on an unproven, closely-monitored system.
    """
    if not SPACE_TRACK_USERNAME or not SPACE_TRACK_PASSWORD:
        return {"status": "not_configured", "detail": "SPACE_TRACK_USERNAME / SPACE_TRACK_PASSWORD not set"}

    login_url = "https://www.space-track.org/ajaxauth/login"
    tiers = [50, 200, 500]
    results = []

    try:
        async with httpx.AsyncClient(timeout=25.0) as client:
            login_resp = await client.post(login_url, data={"identity": SPACE_TRACK_USERNAME, "password": SPACE_TRACK_PASSWORD})
            if login_resp.status_code != 200:
                return {"status": "error", "detail": f"Login failed: HTTP {login_resp.status_code}", "stage": "login"}

            for limit in tiers:
                query_url = f"https://www.space-track.org/basicspacedata/query/class/gp/OBJECT_NAME/~~STARLINK/orderby/EPOCH%20desc/limit/{limit}/format/json"
                try:
                    data_resp = await client.get(query_url)
                    if data_resp.status_code != 200:
                        results.append({"requested_limit": limit, "status": "error", "detail": f"HTTP {data_resp.status_code}"})
                        break  # stop immediately \u2014 do not continue to a larger tier after trouble

                    data = data_resp.json()
                    returned_count = len(data) if isinstance(data, list) else 0
                    results.append({"requested_limit": limit, "status": "success", "returned_count": returned_count})

                    if returned_count < limit and returned_count > 0:
                        # Fewer results than requested is fine IF it's because
                        # we've genuinely run out of matching Starlink objects
                        # (a real, expected stopping point) \u2014 not necessarily
                        # a problem. Noted, not treated as an error.
                        results[-1]["note"] = "Returned fewer than requested \u2014 likely means this is the real total available for this name pattern, not a sign of trouble"

                except Exception as e:
                    results.append({"requested_limit": limit, "status": "error", "detail": f"{type(e).__name__}: {e}"})
                    break  # stop immediately on any exception too

                await asyncio.sleep(2.0)  # deliberate, generous pause between escalating tiers

        all_succeeded = all(r["status"] == "success" for r in results)
        return {
            "status": "success" if all_succeeded else "partial",
            "detail": "Results for each tested limit, in order \u2014 stopped early if any tier showed trouble" if not all_succeeded else "All tested tiers succeeded cleanly",
            "tier_results": results,
        }

    except Exception as e:
        return {"status": "error", "detail": f"{type(e).__name__}: {e}"}


# ---------------------------------------------------------------------------
# REAL endpoint (not a diagnostic) — a genuine, production-quality
# alternative to Celestrak's /api/celestrak/{group} for whichever
# categories have actually been tested and proven to work via Space-Track.
#
# Deliberately, honestly scoped: only "starlink" is included right now,
# because that's the only category actually verified end-to-end in this
# investigation. Adding another category means testing it first (the same
# name-pattern-match approach, verified via a real request) — not guessing
# at a mapping and adding it untested. This dict is intentionally small
# and will grow only as real evidence justifies it.
# ---------------------------------------------------------------------------
SPACE_TRACK_CATEGORIES = {
    "starlink": "STARLINK",
}

# Matches the existing 300-object cap already used elsewhere in this
# application's own TRACK CATEGORY feature (a deliberate, pre-existing
# performance choice, not something new introduced here) — chosen for
# consistency, well within the 500-result tier already proven clean.
SPACE_TRACK_RESULT_CAP = 300


@app.get("/api/space-track/category/{category}")
async def space_track_category(category: str, refresh: bool = False):
    """
    Real satellite category data from Space-Track — an alternative to
    /api/celestrak/{group}, for categories that have actually been tested
    and proven to work (see SPACE_TRACK_CATEGORIES above). Same
    responsible patterns as the rest of this backend: persistent
    server-side caching, honest error handling, no fabricated fallback
    data.

    Accepts an optional ?refresh=true to deliberately bypass the cache and
    force a genuinely fresh query — a real, reusable capability (e.g. for
    verifying a real code change took effect), not just a one-off
    workaround. Normal caching still applies to whatever this fresh
    request returns.
    """
    if category not in SPACE_TRACK_CATEGORIES:
        return JSONResponse(
            status_code=400,
            content={"error": f'Unknown or not-yet-verified category "{category}" \u2014 currently supported: {list(SPACE_TRACK_CATEGORIES.keys())}'},
        )
    if not SPACE_TRACK_USERNAME or not SPACE_TRACK_PASSWORD:
        return JSONResponse(status_code=503, content={"error": "Space-Track credentials not configured on this server"})

    now = datetime.now(timezone.utc)
    db = SessionLocal()
    try:
        cached = None if refresh else db.query(SpaceTrackCacheEntry).filter(SpaceTrackCacheEntry.category_name == category).first()
        if cached:
            cached_at = cached.cached_at
            if cached_at.tzinfo is None:
                cached_at = cached_at.replace(tzinfo=timezone.utc)
            age_seconds = (now - cached_at).total_seconds()
            # Same 4h-success / 1h-failure windows as the Celestrak cache,
            # for consistency across both real data sources.
            cache_window = 14400 if not cached.is_error else 3600
            if age_seconds < cache_window:
                if cached.is_error:
                    return JSONResponse(status_code=502, content={"error": cached.data, "note": "cached failure \u2014 not re-attempted yet"})
                return JSONResponse(content=json.loads(cached.data), headers={"X-Cache": "hit", "X-Source": "space-track"})

        def save_cache(data_str: str, is_error: bool):
            existing = db.query(SpaceTrackCacheEntry).filter(SpaceTrackCacheEntry.category_name == category).first()
            if existing:
                existing.data = data_str
                existing.cached_at = now
                existing.is_error = is_error
            else:
                db.add(SpaceTrackCacheEntry(category_name=category, data=data_str, cached_at=now, is_error=is_error))
            db.commit()

        name_pattern = SPACE_TRACK_CATEGORIES[category]
        login_url = "https://www.space-track.org/ajaxauth/login"
        query_url = f"https://www.space-track.org/basicspacedata/query/class/gp/OBJECT_NAME/~~{name_pattern}/orderby/EPOCH%20desc/limit/{SPACE_TRACK_RESULT_CAP}/format/json"

        try:
            async with httpx.AsyncClient(timeout=25.0) as client:
                login_resp = await client.post(login_url, data={"identity": SPACE_TRACK_USERNAME, "password": SPACE_TRACK_PASSWORD})
                if login_resp.status_code != 200:
                    error_msg = f"Space-Track login failed: HTTP {login_resp.status_code}"
                    save_cache(error_msg, True)
                    return JSONResponse(status_code=502, content={"error": error_msg})

                data_resp = await client.get(query_url)
                if data_resp.status_code != 200:
                    error_msg = f"Space-Track query failed: HTTP {data_resp.status_code}"
                    save_cache(error_msg, True)
                    return JSONResponse(status_code=502, content={"error": error_msg})

                raw_data = data_resp.json()
                # Real correctness fix, found via the user's own live test:
                # Space-Track's OBJECT_NAME search returns ALL historical
                # launches matching the name, including satellites that
                # have already decayed and no longer exist in orbit
                # (unlike Celestrak's GROUP=, which only ever returns
                # currently-tracked live objects). Filtered here in Python
                # rather than via a guessed query-syntax null-filter, to
                # avoid relying on unverified Space-Track query syntax.
                data = [obj for obj in raw_data if not obj.get("DECAY_DATE")] if isinstance(raw_data, list) else raw_data
                data_str = json.dumps(data)
                save_cache(data_str, False)
                return JSONResponse(content=data, headers={
                    "X-Cache": "miss",
                    "X-Source": "space-track",
                    "X-Filtered-Decayed-Count": str(len(raw_data) - len(data)) if isinstance(raw_data, list) else "0",
                })

        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}"
            save_cache(error_msg, True)
            return JSONResponse(status_code=502, content={"error": error_msg})
    finally:
        db.close()


@app.get("/api/debug/space-track-all-categories-test")
async def space_track_all_categories_diagnostic():
    """
    DIAGNOSTIC ONLY \u2014 tests Space-Track equivalents for every remaining
    Celestrak category (all of ALLOWED_GROUPS except "starlink", already
    proven). Honest about confidence per category, since these are NOT
    all the same kind of query:

    - Name-pattern categories (like Starlink was): gps-ops (~~NAVSTAR),
      the three named debris groups (~~COSMOS 2251, ~~IRIDIUM 33,
      ~~FENGYUN 1C). Reasonable confidence, same proven technique.
    - weather / resource / science: attempted with a best-guess name
      pattern, but LOW confidence \u2014 Celestrak's own definition of these
      categories spans many different program names, so a single pattern
      match is unlikely to be a complete or clean equivalent. Included
      to see real evidence either way, not because the guess is trusted.
    - geo: a genuinely different query type \u2014 filtered by real orbital
      period (near 1436 minutes, geostationary) rather than by name at
      all, since GEO isn't a naming convention.
    - stations: intentionally NOT included here \u2014 it's fundamentally a
      small, curated list of specific known objects (like the ISS test),
      not a searchable pattern or range. Handled separately, not by a
      sweep like this.

    One login, then one small, capped (5 results each), 2-second-paced
    query per category \u2014 same conservative pattern as every other
    Space-Track test in this project.
    """
    if not SPACE_TRACK_USERNAME or not SPACE_TRACK_PASSWORD:
        return {"status": "not_configured", "detail": "SPACE_TRACK_USERNAME / SPACE_TRACK_PASSWORD not set"}

    login_url = "https://www.space-track.org/ajaxauth/login"

    # (celestrak_category, query_type, query_value, confidence)
    tests = [
        ("gps-ops", "name", "NAVSTAR", "moderate-high \u2014 confirmed real name pattern seen in earlier N2YO testing"),
        ("cosmos-2251-debris", "name", "COSMOS 2251", "moderate \u2014 plausible based on Celestrak's own group name"),
        ("iridium-33-debris", "name", "IRIDIUM 33", "moderate \u2014 plausible based on Celestrak's own group name"),
        ("fengyun-1c-debris", "name", "FENGYUN 1C", "moderate \u2014 plausible based on Celestrak's own group name"),
        ("weather", "name", "NOAA", "LOW \u2014 weather satellites span many programs (NOAA, GOES, METEOSAT, INSAT...), a single pattern won't be complete"),
        ("resource", "name", "LANDSAT", "LOW \u2014 genuinely uncertain what Celestrak's 'resource' category fully contains"),
        ("science", "name", "HST", "LOW \u2014 genuinely uncertain, science satellites have no single naming convention"),
        ("geo", "period_range", (1430, 1445), "moderate \u2014 geostationary orbit has a real, physics-defined period (~1436 min), not a naming pattern"),
    ]

    results = {}
    try:
        async with httpx.AsyncClient(timeout=25.0) as client:
            login_resp = await client.post(login_url, data={"identity": SPACE_TRACK_USERNAME, "password": SPACE_TRACK_PASSWORD})
            if login_resp.status_code != 200:
                return {"status": "error", "detail": f"Login failed: HTTP {login_resp.status_code}"}

            for celestrak_name, query_type, value, confidence in tests:
                if query_type == "name":
                    query_url = f"https://www.space-track.org/basicspacedata/query/class/gp/OBJECT_NAME/~~{value}/orderby/EPOCH%20desc/limit/5/format/json"
                else:  # period_range
                    low, high = value
                    query_url = f"https://www.space-track.org/basicspacedata/query/class/gp/PERIOD/{low}--{high}/orderby/EPOCH%20desc/limit/5/format/json"

                try:
                    resp = await client.get(query_url)
                    if resp.status_code == 200:
                        data = resp.json()
                        active = [obj for obj in data if not obj.get("DECAY_DATE")] if isinstance(data, list) else []
                        results[celestrak_name] = {
                            "status": "success",
                            "confidence": confidence,
                            "active_count_returned": len(active),
                            "sample_names_with_epoch": [f"{o.get('OBJECT_NAME')} (epoch: {o.get('EPOCH')})" for o in active[:5]],
                        }
                    else:
                        results[celestrak_name] = {"status": "error", "detail": f"HTTP {resp.status_code}", "confidence": confidence}
                except Exception as e:
                    results[celestrak_name] = {"status": "error", "detail": f"{type(e).__name__}: {e}", "confidence": confidence}

                await asyncio.sleep(2.0)

        return {
            "status": "success",
            "detail": "Real results per category \u2014 review sample_names against what each Celestrak category is actually supposed to contain before trusting any of these as a real replacement",
            "results_by_category": results,
        }

    except Exception as e:
        return {"status": "error", "detail": f"{type(e).__name__}: {e}"}


@app.get("/api/debug/space-track-payload-filter-test")
async def space_track_payload_filter_diagnostic():
    """
    DIAGNOSTIC ONLY \u2014 re-tests weather, resource, and science with an
    added OBJECT_TYPE/PAYLOAD filter, to see honestly how much this
    actually helps versus the earlier unfiltered attempt.

    Expected, stated honestly in advance: this should meaningfully help
    "weather" (excludes the "DEB" debris entries that dominated the
    earlier result). It will NOT fully fix "resource" (old-but-still-a-
    payload satellites like LANDSAT 1 from 1972 will still appear \u2014
    OBJECT_TYPE=PAYLOAD doesn't know or care how old something is). It
    will NOT address "science" at all (that problem is the search being
    too narrow, a single name, not debris contamination).
    """
    if not SPACE_TRACK_USERNAME or not SPACE_TRACK_PASSWORD:
        return {"status": "not_configured", "detail": "SPACE_TRACK_USERNAME / SPACE_TRACK_PASSWORD not set"}

    login_url = "https://www.space-track.org/ajaxauth/login"
    tests = [
        ("weather", "NOAA"),
        ("resource", "LANDSAT"),
        ("science", "HST"),
    ]
    results = {}

    try:
        async with httpx.AsyncClient(timeout=25.0) as client:
            login_resp = await client.post(login_url, data={"identity": SPACE_TRACK_USERNAME, "password": SPACE_TRACK_PASSWORD})
            if login_resp.status_code != 200:
                return {"status": "error", "detail": f"Login failed: HTTP {login_resp.status_code}"}

            for celestrak_name, name_pattern in tests:
                query_url = f"https://www.space-track.org/basicspacedata/query/class/gp/OBJECT_NAME/~~{name_pattern}/OBJECT_TYPE/PAYLOAD/orderby/EPOCH%20desc/limit/8/format/json"
                try:
                    resp = await client.get(query_url)
                    if resp.status_code == 200:
                        data = resp.json()
                        active = [obj for obj in data if not obj.get("DECAY_DATE")] if isinstance(data, list) else []
                        results[celestrak_name] = {
                            "status": "success",
                            "active_count_returned": len(active),
                            "sample_names_with_epoch": [f"{o.get('OBJECT_NAME')} (epoch: {o.get('EPOCH')})" for o in active[:8]],
                        }
                    else:
                        results[celestrak_name] = {"status": "error", "detail": f"HTTP {resp.status_code}"}
                except Exception as e:
                    results[celestrak_name] = {"status": "error", "detail": f"{type(e).__name__}: {e}"}

                await asyncio.sleep(2.0)

        return {
            "status": "success",
            "detail": "Compare against the earlier space-track-all-categories-test result for the same three categories \u2014 look for whether 'DEB' entries are actually gone now",
            "results_by_category": results,
        }

    except Exception as e:
        return {"status": "error", "detail": f"{type(e).__name__}: {e}"}


@app.get("/api/debug/space-track-resource-science-fix-test")
async def space_track_resource_science_fix_diagnostic():
    """
    DIAGNOSTIC ONLY \u2014 tests real, targeted fixes for "resource" and
    "science", the two categories not yet added to the proven failover map.

    resource: adds a LAUNCH_DATE range (last ~20 years) alongside the
    existing OBJECT_TYPE=PAYLOAD filter, to exclude old-but-still-payload
    satellites like 1970s LANDSAT 1\u20133 that the payload filter alone
    couldn't catch. Honest caveat: an arbitrary cutoff, not a perfect
    "is this operational" signal \u2014 a real, still-active satellite older
    than the cutoff would be wrongly excluded, though none are expected
    for LANDSAT specifically given the program's real history.

    science: tests two ADDITIONAL real, well-known science satellites
    (CHANDRA, SWIFT) as separate queries, run alongside the existing HST
    result, to see whether combining several confidently-real names
    meaningfully broadens coverage. Deliberately NOT guessing at an
    exhaustive list of everything Celestrak's "science" group might
    contain \u2014 only names with real, high confidence are tested.
    """
    if not SPACE_TRACK_USERNAME or not SPACE_TRACK_PASSWORD:
        return {"status": "not_configured", "detail": "SPACE_TRACK_USERNAME / SPACE_TRACK_PASSWORD not set"}

    login_url = "https://www.space-track.org/ajaxauth/login"
    results = {}

    try:
        async with httpx.AsyncClient(timeout=25.0) as client:
            login_resp = await client.post(login_url, data={"identity": SPACE_TRACK_USERNAME, "password": SPACE_TRACK_PASSWORD})
            if login_resp.status_code != 200:
                return {"status": "error", "detail": f"Login failed: HTTP {login_resp.status_code}"}

            # --- resource: payload filter + launch-date range ---
            resource_url = "https://www.space-track.org/basicspacedata/query/class/gp/OBJECT_NAME/~~LANDSAT/OBJECT_TYPE/PAYLOAD/LAUNCH_DATE/2005-01-01--2030-01-01/orderby/EPOCH%20desc/limit/8/format/json"
            try:
                resp = await client.get(resource_url)
                if resp.status_code == 200:
                    data = resp.json()
                    active = [obj for obj in data if not obj.get("DECAY_DATE")] if isinstance(data, list) else []
                    results["resource"] = {
                        "status": "success",
                        "active_count_returned": len(active),
                        "sample_names_with_launch_date": [f"{o.get('OBJECT_NAME')} (launched: {o.get('LAUNCH_DATE')})" for o in active[:8]],
                    }
                else:
                    results["resource"] = {"status": "error", "detail": f"HTTP {resp.status_code}"}
            except Exception as e:
                results["resource"] = {"status": "error", "detail": f"{type(e).__name__}: {e}"}

            await asyncio.sleep(2.0)

            # --- science: test additional real, confidently-known names ---
            science_names = ["HST", "CHANDRA", "SWIFT"]
            science_results = {}
            for name in science_names:
                sci_url = f"https://www.space-track.org/basicspacedata/query/class/gp/OBJECT_NAME/~~{name}/OBJECT_TYPE/PAYLOAD/orderby/EPOCH%20desc/limit/3/format/json"
                try:
                    resp = await client.get(sci_url)
                    if resp.status_code == 200:
                        data = resp.json()
                        active = [obj for obj in data if not obj.get("DECAY_DATE")] if isinstance(data, list) else []
                        science_results[name] = [o.get("OBJECT_NAME") for o in active]
                    else:
                        science_results[name] = f"HTTP {resp.status_code}"
                except Exception as e:
                    science_results[name] = f"{type(e).__name__}: {e}"
                await asyncio.sleep(2.0)
            results["science"] = {"status": "success", "results_per_name": science_results}

        return {
            "status": "success",
            "detail": "Real results for both proposed fixes \u2014 review before deciding whether either is ready for the real failover map",
            "results": results,
        }

    except Exception as e:
        return {"status": "error", "detail": f"{type(e).__name__}: {e}"}


@app.get("/api/debug/space-track-broaden-test")
async def space_track_broaden_diagnostic():
    """
    DIAGNOSTIC ONLY \u2014 tests two candidate expansions:
    - science: TESS (a real NASA science satellite) as a third name to add
      alongside HST/SWIFT.
    - stations: TIANGONG (China's space station) searched as a full word,
      not a short abbreviation like "CSS" \u2014 deliberately avoiding the
      exact kind of substring-collision mistake CHANDRA caused earlier.
    Real evidence before adding either, same discipline as every other
    addition in this investigation.
    """
    if not SPACE_TRACK_USERNAME or not SPACE_TRACK_PASSWORD:
        return {"status": "not_configured", "detail": "SPACE_TRACK_USERNAME / SPACE_TRACK_PASSWORD not set"}

    login_url = "https://www.space-track.org/ajaxauth/login"
    results = {}

    try:
        async with httpx.AsyncClient(timeout=25.0) as client:
            login_resp = await client.post(login_url, data={"identity": SPACE_TRACK_USERNAME, "password": SPACE_TRACK_PASSWORD})
            if login_resp.status_code != 200:
                return {"status": "error", "detail": f"Login failed: HTTP {login_resp.status_code}"}

            for label, name in [("science_candidate_TESS", "TESS"), ("stations_candidate_TIANGONG", "TIANGONG")]:
                q_url = f"https://www.space-track.org/basicspacedata/query/class/gp/OBJECT_NAME/~~{name}/OBJECT_TYPE/PAYLOAD/orderby/EPOCH%20desc/limit/5/format/json"
                try:
                    resp = await client.get(q_url)
                    if resp.status_code == 200:
                        data = resp.json()
                        active = [obj for obj in data if not obj.get("DECAY_DATE")] if isinstance(data, list) else []
                        results[label] = {
                            "status": "success",
                            "active_count_returned": len(active),
                            "sample_names": [o.get("OBJECT_NAME") for o in active],
                            "norad_ids": [o.get("NORAD_CAT_ID") for o in active],
                        }
                    else:
                        results[label] = {"status": "error", "detail": f"HTTP {resp.status_code}"}
                except Exception as e:
                    results[label] = {"status": "error", "detail": f"{type(e).__name__}: {e}"}
                await asyncio.sleep(2.0)

        return {
            "status": "success",
            "detail": "Review sample_names carefully for any unexpected/incorrect matches before adding either as a real expansion",
            "results": results,
        }

    except Exception as e:
        return {"status": "error", "detail": f"{type(e).__name__}: {e}"}


@app.get("/api/debug/space-track-nisar-search")
async def space_track_nisar_search():
    """
    DIAGNOSTIC ONLY \u2014 checks whether NISAR (the joint NASA-ISRO SAR
    mission) now has a real, catalogued NORAD ID on Space-Track. Earlier
    in this project, NISAR was excluded from the ISRO fleet preset because
    it only had a placeholder ID ("XXXXX+") at the time \u2014 not yet
    launched/catalogued. This checks for real, current status rather than
    assuming either way.
    """
    if not SPACE_TRACK_USERNAME or not SPACE_TRACK_PASSWORD:
        return {"status": "not_configured", "detail": "SPACE_TRACK_USERNAME / SPACE_TRACK_PASSWORD not set"}

    login_url = "https://www.space-track.org/ajaxauth/login"
    query_url = "https://www.space-track.org/basicspacedata/query/class/gp/OBJECT_NAME/~~NISAR/orderby/EPOCH%20desc/limit/5/format/json"

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            login_resp = await client.post(login_url, data={"identity": SPACE_TRACK_USERNAME, "password": SPACE_TRACK_PASSWORD})
            if login_resp.status_code != 200:
                return {"status": "error", "detail": f"Login failed: HTTP {login_resp.status_code}"}

            resp = await client.get(query_url)
            if resp.status_code != 200:
                return {"status": "error", "detail": f"Query failed: HTTP {resp.status_code}"}

            data = resp.json()
            active = [obj for obj in data if not obj.get("DECAY_DATE")] if isinstance(data, list) else []

            if not active:
                return {
                    "status": "success",
                    "found": False,
                    "detail": "No real, active NISAR object found on Space-Track \u2014 either not yet launched/catalogued, or catalogued under a different name",
                }

            return {
                "status": "success",
                "found": True,
                "results": [
                    {"name": o.get("OBJECT_NAME"), "norad_id": o.get("NORAD_CAT_ID"), "launch_date": o.get("LAUNCH_DATE"), "epoch": o.get("EPOCH")}
                    for o in active
                ],
            }

    except Exception as e:
        return {"status": "error", "detail": f"{type(e).__name__}: {e}"}


@app.get("/api/celestrak-health")
def celestrak_health():
    """
    Real Celestrak status check for the dashboard's health indicator \u2014
    reads from EXISTING cache data (already collected by every real
    /api/celestrak/{group} call), zero additional load on Celestrak no
    matter how often this is polled.

    Reports PROPORTIONALLY across every category that's been checked so
    far (group_name is the primary key in the cache table, so there's
    exactly one row per category \u2014 its most recent real status).

    Fixed TWICE now, both times from real bugs the user caught directly:
    (1) the original version only looked at the single most-recently-
    checked category, which could show "fully healthy" while another
    category was actively on the Space-Track failover, just because it
    was checked slightly earlier. (2) even after fixing that, it still
    counted a category as "healthy" if is_error was False \u2014 but a
    SUCCESSFUL failover response is also stored as is_error=False (it
    genuinely did return valid data), so failover successes were being
    silently counted as if they were real Celestrak successes. Now uses
    the dedicated `source` field (added specifically to fix this) to
    count only genuine source=="celestrak" rows as healthy \u2014 a failover
    success is correctly NOT counted as Celestrak being healthy.
    """
    db = SessionLocal()
    try:
        all_entries = db.query(CelestrakCacheEntry).all()
        if not all_entries:
            return {
                "healthy_count": 0, "total_checked": 0, "fully_healthy": None,
                "unhealthy_categories": [],
                "detail": "No category has been checked yet since this server last restarted",
            }

        healthy_count = sum(1 for e in all_entries if e.source == "celestrak")
        total_checked = len(all_entries)
        unhealthy_categories = sorted(e.group_name for e in all_entries if e.source != "celestrak")

        return {
            "healthy_count": healthy_count,
            "total_checked": total_checked,
            "fully_healthy": healthy_count == total_checked,
            "unhealthy_categories": unhealthy_categories,
        }
    finally:
        db.close()


@app.get("/api/debug/my-outbound-ip")
async def my_outbound_ip():
    """
    DIAGNOSTIC ONLY \u2014 reports this server's real, current outbound IP
    address, by asking a real external "what's my IP" service directly
    (the same real network path Celestrak itself would see), rather than
    guessing from Render's dashboard. Needed to send Celestrak's own
    maintainer the IP he asked for directly.

    Honest note: on Render's free tier, outbound IPs are commonly shared/
    pooled rather than a fixed, dedicated address \u2014 this reports the real
    current one, but it may not be permanent.
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get("https://api.ipify.org?format=json")
            if resp.status_code == 200:
                return {"status": "success", "outbound_ip": resp.json().get("ip")}
            return {"status": "error", "detail": f"HTTP {resp.status_code}"}
    except Exception as e:
        return {"status": "error", "detail": f"{type(e).__name__}: {e}"}


@app.get("/api/debug/celestrak-recovery-check-all")
async def celestrak_recovery_check_all():
    """
    DIAGNOSTIC ONLY \u2014 checks all 10 real Celestrak categories directly,
    with a real, forced-fresh request each (bypassing cache), to confirm
    Celestrak's actual recovery across everything this app uses \u2014 not
    just the couple of categories already spot-checked manually.

    Respectfully paced: a 2-second gap between each of the 10 real
    requests, same discipline as every other multi-request diagnostic in
    this project. This is a one-time confirmation sweep, not something
    meant to be run repeatedly.
    """
    categories = ["stations", "starlink", "weather", "gps-ops", "geo", "resource", "science",
                  "cosmos-2251-debris", "iridium-33-debris", "fengyun-1c-debris"]
    results = {}

    # Fixed after a real failure caught in testing: the first version of
    # this sweep reused ONE shared httpx.AsyncClient (one long-lived
    # connection) across all 10 sequential requests, and every single one
    # failed with ConnectTimeout \u2014 while the exact same category, checked
    # individually via /api/celestrak/{group} (which opens a FRESH
    # connection per request), succeeded immediately afterward. That
    # proved the failure was specific to this sweep's connection-reuse
    # pattern, not a real Celestrak issue. Fixed by opening a fresh client
    # for each request below, matching the proven-working pattern exactly.
    for group in categories:
        url = f"https://celestrak.org/NORAD/elements/gp.php?GROUP={group}&FORMAT=TLE"
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(url, headers=CELESTRAK_HEADERS)
            if resp.status_code == 200 and resp.text.strip():
                lines = [l.strip() for l in resp.text.strip().split("\n") if l.strip()]
                object_count = len(lines) // 3
                sample_names = [lines[i] for i in range(0, min(len(lines), 9), 3)]
                results[group] = {"status": "success", "object_count": object_count, "sample_names": sample_names}
            else:
                results[group] = {"status": "error", "detail": f"HTTP {resp.status_code}"}
        except Exception as e:
            results[group] = {"status": "error", "detail": f"{type(e).__name__}: {e}"}

        await asyncio.sleep(2.0)  # respectful pacing between every request, success or failure alike

    all_ok = all(r["status"] == "success" for r in results.values())
    return {
        "status": "success",
        "detail": "All 10 categories checked directly against live Celestrak (cache bypassed)" if all_ok else "Some categories still failing \u2014 review individually",
        "all_recovered": all_ok,
        "results": results,
    }


@app.get("/api/debug/raw-cache-contents")
def raw_cache_contents():
    """
    DIAGNOSTIC ONLY \u2014 shows the RAW, actual contents of every row in the
    celestrak_cache table directly, bypassing all aggregation logic.
    Built specifically to debug a real discrepancy: a fresh, confirmed-
    successful Celestrak fetch wasn't being reflected as healthy in
    /api/celestrak-health, and this shows exactly what's actually stored
    for each category, so the real cause can be seen directly rather than
    guessed at further.
    """
    db = SessionLocal()
    try:
        rows = db.query(CelestrakCacheEntry).all()
        return {
            "row_count": len(rows),
            "rows": [
                {
                    "group_name": r.group_name,
                    "is_error": r.is_error,
                    "source": r.source,
                    "cached_at": r.cached_at.isoformat() if r.cached_at else None,
                    "data_preview": (r.data[:60] if r.data else None),
                }
                for r in rows
            ],
        }
    finally:
        db.close()

