import base64
import os
import secrets
from urllib.parse import urlencode
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, Depends
from fastapi.responses import RedirectResponse, JSONResponse

from sqlalchemy.orm import Session
from sqlalchemy import select, func

from db import engine, SessionLocal, Base
from models import User, Artist, Track, Play

load_dotenv()

# =========================
# Spotify App Credentials
# =========================
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID", "YOUR_CLIENT_ID_HERE")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "YOUR_CLIENT_SECRET_HERE")
REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI", "http://127.0.0.1:8000/callback")

AUTH_URL = "https://accounts.spotify.com/authorize"
TOKEN_URL = "https://accounts.spotify.com/api/token"
API_BASE = "https://api.spotify.com/v1"

SCOPES = "user-read-email user-read-private user-read-recently-played user-top-read"

app = FastAPI()

# Create DB tables
Base.metadata.create_all(bind=engine)

# State for OAuth (fine in memory)
STATE_STORE = set()

# =========================
# DB helper
# =========================
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def now_utc() -> datetime:
    # naive UTC (no tzinfo) to match SQLite DateTime
    return datetime.utcnow()


def parse_played_at(ts: str) -> datetime:
    # e.g. "2026-03-04T20:17:35.123Z"
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    # convert to naive UTC for SQLite consistency
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


# =========================
# Spotify auth helpers
# =========================
def token_exchange(code: str):
    basic = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    headers = {"Authorization": f"Basic {basic}"}
    data = {"grant_type": "authorization_code", "code": code, "redirect_uri": REDIRECT_URI}

    r = requests.post(TOKEN_URL, headers=headers, data=data, timeout=20)
    return r


def refresh_access_token(refresh_token: str):
    basic = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    headers = {"Authorization": f"Basic {basic}"}
    data = {"grant_type": "refresh_token", "refresh_token": refresh_token}

    r = requests.post(TOKEN_URL, headers=headers, data=data, timeout=20)
    return r


def get_current_user(db: Session) -> User | None:
    """
    We treat the *one logged-in* user as the most recently tokened user.
    (Simple single-user local app behavior.)
    """
    return (
        db.execute(
            select(User)
            .where(User.access_token.is_not(None))
            .order_by(User.id.desc())
        )
        .scalars()
        .first()
    )


def ensure_fresh_token(db: Session, user: User) -> str | None:
    """
    Returns a valid access token, refreshing if needed.
    """
    if not user.access_token:
        return None

    # If we don't know expiry, try it anyway.
    if user.token_expires_at is None:
        return user.access_token

    # Refresh if token expired (or about to expire within 30s)
    if now_utc() >= (user.token_expires_at - timedelta(seconds=30)):
        if not user.refresh_token:
            return None

        rr = refresh_access_token(user.refresh_token)
        if rr.status_code != 200:
            # refresh failed, force re-login
            user.access_token = None
            user.refresh_token = None
            user.token_expires_at = None
            db.commit()
            return None

        tj = rr.json()
        user.access_token = tj["access_token"]
        # Spotify may or may not return refresh_token on refresh
        if tj.get("refresh_token"):
            user.refresh_token = tj["refresh_token"]
        user.token_expires_at = now_utc() + timedelta(seconds=int(tj.get("expires_in", 3600)))
        db.commit()

    return user.access_token


def spotify_get(db: Session, endpoint: str, params: dict | None = None):
    user = get_current_user(db)
    if not user:
        return JSONResponse({"error": "Not logged in. Visit /login"}, status_code=401)

    token = ensure_fresh_token(db, user)
    if not token:
        return JSONResponse({"error": "Not logged in. Visit /login"}, status_code=401)

    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(f"{API_BASE}{endpoint}", headers=headers, params=params, timeout=20)

    # If token is rejected, try one refresh + retry once
    if r.status_code == 401 and user.refresh_token:
        rr = refresh_access_token(user.refresh_token)
        if rr.status_code == 200:
            tj = rr.json()
            user.access_token = tj["access_token"]
            if tj.get("refresh_token"):
                user.refresh_token = tj["refresh_token"]
            user.token_expires_at = now_utc() + timedelta(seconds=int(tj.get("expires_in", 3600)))
            db.commit()

            headers = {"Authorization": f"Bearer {user.access_token}"}
            r = requests.get(f"{API_BASE}{endpoint}", headers=headers, params=params, timeout=20)

    if r.status_code == 429:
        return JSONResponse(
            {"error": "rate_limited", "retry_after_seconds": r.headers.get("Retry-After")},
            status_code=429,
        )

    if r.status_code != 200:
        return JSONResponse({"error": "Spotify API error", "details": r.text}, status_code=r.status_code)

    return r.json()


# =========================
# Routes
# =========================
@app.get("/")
def home():
    return {
        "message": "Spotify Analytics Project Running",
        "next": ["GET /login", "POST /sync/recent", "GET /analytics/top-artists", "GET /analytics/top-tracks"],
    }


@app.get("/login-url")
def login_url():
    state = secrets.token_urlsafe(16)
    STATE_STORE.add(state)

    params = {
        "client_id": CLIENT_ID,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPES,
        "state": state,
    }

    url = f"{AUTH_URL}?{urlencode(params)}"
    return {"url": url, "note": "Copy/paste this into a browser tab to log in (Swagger can't follow redirects)."}

@app.get("/login")
def login():
    state = secrets.token_urlsafe(16)
    STATE_STORE.add(state)

    params = {
        "client_id": CLIENT_ID,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPES,
        "state": state,
    }
    return RedirectResponse(f"{AUTH_URL}?{urlencode(params)}")


@app.get("/callback")
def callback(code: str, state: str, db: Session = Depends(get_db)):
    if state not in STATE_STORE:
        return JSONResponse({"error": "Invalid state"}, status_code=400)

    r = token_exchange(code)
    if r.status_code != 200:
        return JSONResponse({"error": "Token exchange failed", "details": r.text}, status_code=400)

    token_json = r.json()
    access_token = token_json["access_token"]
    refresh_token = token_json.get("refresh_token")
    expires_in = int(token_json.get("expires_in", 3600))

    # Fetch /me to know who logged in
    headers = {"Authorization": f"Bearer {access_token}"}
    me = requests.get(f"{API_BASE}/me", headers=headers, timeout=20)
    if me.status_code != 200:
        return JSONResponse({"error": "Could not fetch /me", "details": me.text}, status_code=400)

    me_data = me.json()
    spotify_id = me_data["id"]

    user = db.execute(select(User).where(User.spotify_id == spotify_id)).scalar_one_or_none()
    if not user:
        user = User(
            spotify_id=spotify_id,
            display_name=me_data.get("display_name"),
            email=me_data.get("email"),
        )
        db.add(user)
        db.commit()
        db.refresh(user)

    user.access_token = access_token
    if refresh_token:
        user.refresh_token = refresh_token
    user.token_expires_at = now_utc() + timedelta(seconds=expires_in)
    db.commit()

    return RedirectResponse("/me")


@app.get("/me")
def me(db: Session = Depends(get_db)):
    return spotify_get(db, "/me")


@app.get("/top-artists")
def top_artists(db: Session = Depends(get_db), time_range: str = "medium_term", limit: int = 20):
    # time_range: short_term | medium_term | long_term
    return spotify_get(db, "/me/top/artists", params={"time_range": time_range, "limit": limit})


@app.get("/top-tracks")
def top_tracks(db: Session = Depends(get_db), time_range: str = "medium_term", limit: int = 20):
    return spotify_get(db, "/me/top/tracks", params={"time_range": time_range, "limit": limit})


@app.get("/recent")
def recent_tracks(db: Session = Depends(get_db), limit: int = 20):
    return spotify_get(db, "/me/player/recently-played", params={"limit": limit})


# =========================
# Sync endpoint (API -> DB)
# =========================
@app.post("/sync/recent/incremental")
def sync_recent_incremental(db: Session = Depends(get_db), limit: int = 50):
    """
    Pull only plays newer than the newest play we already have in the DB.
    This makes sync fast + lets you run it daily.
    """
    me_data = spotify_get(db, "/me")
    if isinstance(me_data, JSONResponse):
        return me_data

    spotify_id = me_data["id"]
    user = db.execute(select(User).where(User.spotify_id == spotify_id)).scalar_one_or_none()
    if not user:
        user = User(
            spotify_id=spotify_id,
            display_name=me_data.get("display_name"),
            email=me_data.get("email"),
        )
        db.add(user)
        db.commit()
        db.refresh(user)

    # newest play we already have (naive UTC)
    latest_played_at = db.query(func.max(Play.played_at)).filter(Play.user_id == user.id).scalar()

    params = {"limit": limit}
    if latest_played_at:
        # Spotify expects milliseconds since epoch
        after_ms = int(latest_played_at.replace(tzinfo=timezone.utc).timestamp() * 1000)
        params["after"] = after_ms

    recent_data = spotify_get(db, "/me/player/recently-played", params=params)
    if isinstance(recent_data, JSONResponse):
        return recent_data

    items = recent_data.get("items", [])
    added = 0

    for item in items:
        track_obj = item["track"]
        played_at = parse_played_at(item["played_at"])

        # primary artist
        artist_obj = track_obj["artists"][0]
        artist = db.execute(
            select(Artist).where(Artist.spotify_artist_id == artist_obj["id"])
        ).scalar_one_or_none()
        if not artist:
            artist = Artist(spotify_artist_id=artist_obj["id"], name=artist_obj["name"])
            db.add(artist)
            db.commit()
            db.refresh(artist)

        # track
        track = db.execute(
            select(Track).where(Track.spotify_track_id == track_obj["id"])
        ).scalar_one_or_none()
        if not track:
            track = Track(
                spotify_track_id=track_obj["id"],
                name=track_obj["name"],
                album=track_obj["album"]["name"] if track_obj.get("album") else None,
                primary_artist_id=artist.id,
            )
            db.add(track)
            db.commit()
            db.refresh(track)

        # play dedupe
        existing = db.execute(
            select(Play).where(
                Play.user_id == user.id,
                Play.track_id == track.id,
                Play.played_at == played_at,
            )
        ).scalar_one_or_none()

        if not existing:
            db.add(Play(user_id=user.id, track_id=track.id, played_at=played_at))
            db.commit()
            added += 1

    return {
        "status": "ok",
        "mode": "incremental",
        "plays_added": added,
        "pulled": len(items),
        "latest_in_db": latest_played_at.isoformat() if latest_played_at else None,
        "used_after_ms": params.get("after"),
    }


# =========================
# Analytics (DB)
# =========================
@app.get("/analytics/top-artists")
def analytics_top_artists(db: Session = Depends(get_db), limit: int = 10):
    rows = (
        db.query(Artist.name, func.count(Play.id).label("plays"))
        .join(Track, Track.primary_artist_id == Artist.id)
        .join(Play, Play.track_id == Track.id)
        .group_by(Artist.name)
        .order_by(func.count(Play.id).desc())
        .limit(limit)
        .all()
    )
    return [{"artist": name, "plays": plays} for name, plays in rows]


@app.get("/analytics/top-tracks")
def analytics_top_tracks(db: Session = Depends(get_db), limit: int = 10):
    rows = (
        db.query(Track.name, Artist.name.label("artist"), func.count(Play.id).label("plays"))
        .join(Artist, Track.primary_artist_id == Artist.id)
        .join(Play, Play.track_id == Track.id)
        .group_by(Track.name, Artist.name)
        .order_by(func.count(Play.id).desc())
        .limit(limit)
        .all()
    )
    return [{"track": t, "artist": a, "plays": p} for t, a, p in rows]


@app.get("/analytics/recent-plays")
def analytics_recent_plays(db: Session = Depends(get_db), limit: int = 20):
    rows = (
        db.query(Play.played_at, Track.name, Artist.name.label("artist"))
        .join(Track, Play.track_id == Track.id)
        .join(Artist, Track.primary_artist_id == Artist.id)
        .order_by(Play.played_at.desc())
        .limit(limit)
        .all()
    )
    return [{"played_at": pa.isoformat(), "track": t, "artist": a} for pa, t, a in rows]


@app.get("/analytics/listening-by-day")
def analytics_listening_by_day(db: Session = Depends(get_db), days: int = 30):
    # SQLite: date() extracts date portion
    cutoff = now_utc() - timedelta(days=days)
    rows = (
        db.query(func.date(Play.played_at).label("day"), func.count(Play.id).label("plays"))
        .filter(Play.played_at >= cutoff)
        .group_by(func.date(Play.played_at))
        .order_by(func.date(Play.played_at).asc())
        .all()
    )
    return [{"day": str(day), "plays": plays} for day, plays in rows]


@app.get("/analytics/listening-by-hour")
def analytics_listening_by_hour(db: Session = Depends(get_db), days: int = 30):
    # SQLite: strftime('%H', ts) -> hour in 00-23
    cutoff = now_utc() - timedelta(days=days)
    rows = (
        db.query(func.strftime("%H", Play.played_at).label("hour"), func.count(Play.id).label("plays"))
        .filter(Play.played_at >= cutoff)
        .group_by(func.strftime("%H", Play.played_at))
        .order_by(func.strftime("%H", Play.played_at).asc())
        .all()
    )
    return [{"hour": int(hour), "plays": plays} for hour, plays in rows]