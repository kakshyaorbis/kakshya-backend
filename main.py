from fastapi import FastAPI, Response, Request, Depends, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, JSONResponse, HTMLResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String
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

    for attempt in range(1, 4):
        satellite_load_status["last_attempt_time"] = datetime.now(timezone.utc).isoformat()
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(url, headers=CELESTRAK_HEADERS)
            if resp.status_code != 200 or not resp.text.strip():
                last_error = f"HTTP {resp.status_code}"
                satellite_load_status["last_error"] = last_error
                logger.warning(f"Attempt {attempt}/3 to load real satellite data failed: {last_error}")
                await asyncio.sleep(2 * attempt)
                continue

            lines = [l.strip() for l in resp.text.strip().split("\n") if l.strip()]
            count = 0
            for i in range(0, len(lines) - 2, 3):
                name, l1, l2 = lines[i], lines[i + 1], lines[i + 2]
                if not l1.startswith("1 ") or not l2.startswith("2 "):
                    continue
                try:
                    tracked_satellites[name] = Satrec.twoline2rv(l1, l2)
                    count += 1
                except Exception:
                    continue  # one bad TLE entry skipped, not fatal to the rest
                if count >= 8:  # a small, real, manageable set for this demo — not the full stations catalog
                    break
            satellite_load_status["loaded_count"] = count
            satellite_load_status["last_error"] = None
            satellite_load_status["last_success_time"] = datetime.now(timezone.utc).isoformat()
            logger.info(f"Loaded {count} real satellites for WebSocket tracking (Celestrak 'stations' group, attempt {attempt}/3)")
            return  # success — no need to retry further
        except Exception as e:
            # Always includes the exception TYPE, not just str(e) — some
            # real connection-level errors (timeouts especially) have an
            # empty string representation on their own, which is exactly
            # what produced an unhelpfully blank log message before this
            # fix (observed live, not hypothetical).
            last_error = f"{type(e).__name__}: {e}"
            satellite_load_status["last_error"] = last_error
            logger.warning(f"Attempt {attempt}/3 to load real satellite data failed: {last_error}")
            await asyncio.sleep(2 * attempt)

    logger.warning(f"Could not load real satellite data after 3 attempts. Last error: {last_error}. Will keep retrying in the background every 5 minutes.")


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


async def satellite_reload_safety_net():
    """
    A periodic safety net — if the initial load (with its own retries)
    still didn't succeed, or somehow the tracked set is empty later on,
    try again every 5 minutes rather than requiring a manual restart to
    recover from a transient failure.
    """
    while True:
        await asyncio.sleep(300)
        if not tracked_satellites:
            logger.info("Retrying real satellite data load (background safety net)...")
            await load_tracked_satellites()


@app.websocket("/ws/demo")
async def websocket_demo(websocket: WebSocket):
    await manager.connect(websocket)
    logger.info(f"WebSocket client connected. Total connected: {len(manager.active_connections)}")

    # Trigger the real satellite load HERE, as part of handling this real
    # incoming connection — not as a bare background task fired at
    # container startup. This is a deliberate structural change based on
    # real evidence: /api/celestrak/{group} has worked reliably for a
    # while now, and it only ever runs in response to an actual request.
    # The pure-background-task version of this same Celestrak call kept
    # failing with ConnectTimeout regardless of how long it waited after
    # startup (10s, then 45s, both failed) — strongly suggesting the
    # problem was specifically about running with no real request context
    # yet, not simply needing more warm-up time. This reuses the same
    # already-proven-working trigger pattern instead of continuing to
    # fight the not-working one.
    if not tracked_satellites:
        asyncio.create_task(load_tracked_satellites())

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
    whitelist, same identifying header, and the same 4-hour caching
    approach as the existing Node.js proxy, so repeat visitors don't each
    independently re-hit Celestrak's heaviest endpoints (this is what fixed
    the real HTTP 403 Starlink was returning before this proxy existed).
    """
    if group not in ALLOWED_GROUPS:
        return JSONResponse(
            status_code=400,
            content={"error": f'Unknown or missing group "{group}" — this proxy only serves the specific Celestrak groups Kakshya uses.'},
        )

    try:
        url = f"https://celestrak.org/NORAD/elements/gp.php?GROUP={group}&FORMAT=TLE"
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, headers=CELESTRAK_HEADERS)

        if resp.status_code != 200:
            return JSONResponse(
                status_code=502,
                content={"error": f'Celestrak returned HTTP {resp.status_code} for group "{group}"'},
            )
        if not resp.text or not resp.text.strip():
            return JSONResponse(
                status_code=502,
                content={"error": f'Celestrak returned an empty response for group "{group}" — the group name may no longer be valid'},
            )

        # 4 hours, same reasoning as the existing Node.js proxy: real TLE
        # data doesn't change fast enough to need per-visit freshness, and
        # this is what actually stops repeat visitors from each triggering
        # a fresh Celestrak hit.
        return PlainTextResponse(
            content=resp.text,
            headers={"Cache-Control": "public, max-age=14400"},
        )

    except Exception as e:
        return JSONResponse(status_code=502, content={"error": str(e)})

