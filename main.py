from fastapi import FastAPI, Response, Request, Depends, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, JSONResponse, HTMLResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Boolean, Float
from sqlalchemy.orm import sessionmaker, declarative_base
from jose import jwt, JWTError
from datetime import datetime, timedelta, timezone
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from sgp4.api import Satrec, jday
import httpx
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

    url = "https://celestrak.org/NORAD/elements/gp.php?GROUP=stations&FORMAT=TLE"
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
                resp = await client.get(url, headers=CELESTRAK_HEADERS)

            if resp.status_code in (301, 403, 404, 500):
                last_error = f"HTTP {resp.status_code} (explicit error response — not retrying, per Celestrak's own stated policy on 301/403/404/500)"
                satellite_load_status["last_error"] = last_error
                logger.warning(f"Celestrak returned an error response: {last_error}. Stopping immediately, no retry.")
                return  # stop entirely — do not retry, do not continue the loop

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
            logger.warning(f"Attempt {attempt}/2 to load real satellite data failed: {last_error}")
            await asyncio.sleep(2 * attempt)

    logger.warning(f"Could not load real satellite data after 2 attempts. Last error: {last_error}. Will check again in 2 hours (matching Celestrak's own update cadence, not more often).")


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
    # a fresh Celestrak request, with no limit — exactly the kind of
    # repeated-hitting pattern Celestrak's own documentation warns can
    # result in an IP being firewalled. Real gap, fixed after reviewing
    # their stated policy: don't attempt again if the last attempt was
    # less than 10 minutes ago, regardless of how many new connections
    # come in during that window.
    now_iso = datetime.now(timezone.utc)
    last_attempt = satellite_load_status["last_attempt_time"]
    cooldown_active = False
    if last_attempt:
        last_attempt_dt = datetime.fromisoformat(last_attempt)
        cooldown_active = (now_iso - last_attempt_dt).total_seconds() < 600

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
    "User-Agent": "Mozilla/5.0 (compatible; KakshyaDashboard/1.0; +https://kakshaya.netlify.app)",
    "Accept": "text/plain",
}

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
            # Failures cached for 1 hour — long enough to genuinely stop
            # repeated hits during an outage or block, short enough to
            # recover reasonably once Celestrak stabilizes.
            cache_window = 14400 if not cached.is_error else 3600
            if age_seconds < cache_window:
                if cached.is_error:
                    return JSONResponse(status_code=502, content={
                        "error": cached.data,
                        "note": "cached failure — not re-attempted yet, to avoid repeatedly hitting Celestrak",
                        "cached_at_utc": cached_at.isoformat(),
                        "cache_age_minutes": round(age_seconds / 60, 1),
                        "will_retry_in_minutes": round((cache_window - age_seconds) / 60, 1),
                    })
                return PlainTextResponse(content=cached.data, headers={"Cache-Control": "public, max-age=14400", "X-Cache": "hit"})

        def save_cache(data: str, is_error: bool):
            existing = db.query(CelestrakCacheEntry).filter(CelestrakCacheEntry.group_name == group).first()
            if existing:
                existing.data = data
                existing.cached_at = now
                existing.is_error = is_error
            else:
                db.add(CelestrakCacheEntry(group_name=group, data=data, cached_at=now, is_error=is_error))
            db.commit()

        try:
            url = f"https://celestrak.org/NORAD/elements/gp.php?GROUP={group}&FORMAT=TLE"
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(url, headers=CELESTRAK_HEADERS)

            if resp.status_code != 200:
                error_msg = f'Celestrak returned HTTP {resp.status_code} for group "{group}"'
                save_cache(error_msg, True)
                return JSONResponse(status_code=502, content={"error": error_msg})
            if not resp.text or not resp.text.strip():
                error_msg = f'Celestrak returned an empty response for group "{group}" — the group name may no longer be valid'
                save_cache(error_msg, True)
                return JSONResponse(status_code=502, content={"error": error_msg})

            save_cache(resp.text, False)
            return PlainTextResponse(
                content=resp.text,
                headers={"Cache-Control": "public, max-age=14400", "X-Cache": "miss"},
            )

        except Exception as e:
            error_msg = str(e)
            save_cache(error_msg, True)
            return JSONResponse(status_code=502, content={"error": error_msg})
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

