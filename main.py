from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, JSONResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base
import httpx
import bcrypt
import os

# This is the entire application, on purpose — the goal right now isn't to
# do anything useful yet, it's to prove the whole chain works end to end:
# code -> deployed on the internet -> responds when you visit it in a
# browser. Everything else gets built on top of this once this part works.

app = FastAPI()

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
def register(req: RegisterRequest):
    """
    Creates a real user account. The password itself is never stored —
    only a one-way bcrypt hash of it, so even someone with direct database
    access can't recover the actual password.
    """
    db = SessionLocal()
    try:
        existing = db.query(User).filter(User.username == req.username).first()
        if existing:
            return JSONResponse(status_code=400, content={"error": "That username is already taken."})

        hashed = bcrypt.hashpw(req.password.encode("utf-8"), bcrypt.gensalt())
        new_user = User(username=req.username, hashed_password=hashed.decode("utf-8"))
        db.add(new_user)
        db.commit()
        return {"status": "ok", "message": f"Account created for {req.username}"}
    finally:
        db.close()


@app.post("/api/login")
def login(req: LoginRequest):
    """
    Checks real credentials against the database. This step deliberately
    stops at "are these credentials correct" — real session tokens (so you
    stay logged in) come in the next step, kept separate so each piece can
    be tested on its own before adding the next.
    """
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.username == req.username).first()
        if not user:
            return JSONResponse(status_code=401, content={"error": "Incorrect username or password."})

        correct = bcrypt.checkpw(req.password.encode("utf-8"), user.hashed_password.encode("utf-8"))
        if not correct:
            return JSONResponse(status_code=401, content={"error": "Incorrect username or password."})

        return {"status": "ok", "message": f"Welcome, {req.username} — credentials verified."}
    finally:
        db.close()


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

