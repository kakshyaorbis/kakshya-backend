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
import httpx
import bcrypt
import os
import logging
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
# WebSocket demo (Phase 3 proof-of-concept) — DELIBERATELY LABELED AS A
# DEMO. This does NOT push real satellite data. Today's architecture
# computes satellite positions entirely in the browser (via satellite.js on
# the actual dashboard), and this backend has never computed a single real
# position — so there is currently no genuine live data source to push.
# This exists purely to prove the real-time push MECHANISM works
# end-to-end (connect -> receive pushed updates -> disconnect), so it's
# ready to point at a real data source (e.g. real Skyroot telemetry) the
# moment one actually exists, rather than needing to be built from scratch
# at that point.
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


@app.websocket("/ws/demo")
async def websocket_demo(websocket: WebSocket):
    await manager.connect(websocket)
    logger.info(f"WebSocket demo client connected. Total connected: {len(manager.active_connections)}")
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        logger.info(f"WebSocket demo client disconnected. Total connected: {len(manager.active_connections)}")


async def demo_broadcast_loop():
    """
    Runs forever in the background, pushing one clearly-fake demo value to
    every connected client every 3 seconds — the proof that server-pushed
    updates work, using an honestly-labeled counter instead of anything
    dressed up to look like real data.
    """
    counter = 0
    while True:
        await asyncio.sleep(3)
        counter += 1
        if manager.active_connections:
            await manager.broadcast({
                "type": "demo_tick",
                "counter": counter,
                "server_time_utc": datetime.now(timezone.utc).isoformat(),
                "note": "This is a demo value, not real satellite data.",
            })


@app.on_event("startup")
async def start_background_tasks():
    asyncio.create_task(demo_broadcast_loop())


@app.get("/ws-test", response_class=HTMLResponse)
def websocket_test_page():
    """
    A simple, self-contained test page — visit this URL directly in a
    browser to see the WebSocket demo working live, no separate tools or
    coding needed.
    """
    return """
    <!DOCTYPE html>
    <html>
    <head><title>Kakshya WebSocket Demo</title></head>
    <body style="font-family: monospace; background: #0B1220; color: #E6EDF3; padding: 24px;">
        <h2>WebSocket Demo \u2014 Phase 3 Proof of Concept</h2>
        <p style="color: #F5A623;">This shows a fake counter pushed from the server every 3 seconds. It is NOT real satellite data \u2014 that's the whole point of this test.</p>
        <p>Status: <span id="status" style="color: #33D17A;">Connecting...</span></p>
        <div id="messages" style="border: 1px solid #333; padding: 12px; height: 300px; overflow-y: auto; background: #101A2C;"></div>
        <script>
            const statusEl = document.getElementById("status");
            const messagesEl = document.getElementById("messages");
            const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
            const ws = new WebSocket(proto + "//" + window.location.host + "/ws/demo");
            ws.onopen = () => { statusEl.textContent = "Connected \u2014 waiting for the next push..."; };
            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);
                const line = document.createElement("div");
                line.textContent = "[" + data.server_time_utc + "] counter = " + data.counter;
                messagesEl.prepend(line);
                statusEl.textContent = "Connected \u2014 receiving live pushed updates";
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

