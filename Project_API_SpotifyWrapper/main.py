import base64
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv
from fastapi import Depends, FastAPI
from fastapi.responses import JSONResponse, RedirectResponse
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .db import Base, SessionLocal, engine
from . import models

# ---------------------------------------------------------------------
# Config / Constants
# ---------------------------------------------------------------------

load_dotenv()

CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID", "YOUR_CLIENT_ID_HERE")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "YOUR_CLIENT_SECRET_HERE")
REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI", "http://127.0.0.1:8000/callback")

AUTH_URL = "https://accounts.spotify.com/authorize"
TOKEN_URL = "https://accounts.spotify.com/api/token"
API_BASE = "https://api.spotify.com/v1"

SCOPES = "user-read-email user-read-private user-read-recently-played user-top-read"

app = FastAPI()

# Create DB tables on startup (fine for local dev)
Base.metadata.create_all(bind=engine)

# In-memory OAuth state (fine for local single-user dev)
STATE_STORE: set[str] = set()


# ---------------------------------------------------------------------
# DB session dependency
# ---------------------------------------------------------------------

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------------------------------------------------------------------
# Time helpers (SQLite-friendly)
# ---------------------------------------------------------------------

def now_utc() -> datetime:
    # SQLite DateTime is typically stored as naive datetimes.
    return datetime.utcnow()


def parse_played_at(ts: str) -> datetime:
    # Spotify returns ISO strings like "2026-03-04T20:17:35.123Z"
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


# ---------------------------------------------------------------------
# Auth / Spotify API helpers
# ---------------------------------------------------------------------

def unauthorized(msg: str = "Not logged in. Visit /login or /login-url and complete auth.") -> JSONResponse:
    return JSONResponse({"error": "unauthorized", "message": msg}, status_code=401)


def _basic_auth_header() -> Dict[str, str]:
    basic = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    return {"Authorization": f"Basic {basic}"}


def token_exchange(code: str) -> requests.Response:
    data = {"grant_type": "authorization_code", "code": code, "redirect_uri": REDIRECT_URI}
    return requests.post(TOKEN_URL, headers=_basic_auth_header(), data=data, timeout=20)


def refresh_access_token(refresh_token: str) -> requests.Response:
    data = {"grant_type": "refresh_token", "refresh_token": refresh_token}
    return requests.post(TOKEN_URL, headers=_basic_auth_header(), data=data, timeout=20)


def get_current_user(db: Session) -> Optional[User]:
    """
    Local single-user behavior: last user with a token is treated as "current".
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


def ensure_fresh_token(db: Session, user: User) -> Optional[str]:
    """
    Returns a usable access token if available; refreshes if needed.
    """
    if not user.access_token:
        return None

    if user.token_expires_at is None:
        return user.access_token

    # Refresh if expired or about to expire (within 30s)
    if now_utc() >= (user.token_expires_at - timedelta(seconds=30)):
        if not user.refresh_token:
            return None

        rr = refresh_access_token(user.refresh_token)
        if rr.status_code != 200:
            # Refresh failed => force re-login
            user.access_token = None
            user.refresh_token = None
            user.token_expires_at = None
            db.commit()
            return None

        tj = rr.json()
        user.access_token = tj["access_token"]
        if tj.get("refresh_token"):
            user.refresh_token = tj["refresh_token"]
        user.token_expires_at = now_utc() + timedelta(seconds=int(tj.get("expires_in", 3600)))
        db.commit()

    return user.access_token


def spotify_get(db: Session, endpoint: str, params: dict | None = None) -> Any:
    """
    Wrapper around Spotify GET calls that:
      - validates user auth
      - refreshes tokens when needed
      - returns JSONResponse for auth / error cases
    """
    user = get_current_user(db)
    if not user:
        return unauthorized()

    token = ensure_fresh_token(db, user)
    if not token:
        return unauthorized("Token missing/expired. Re-login via /login or /login-url.")

    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(f"{API_BASE}{endpoint}", headers=headers, params=params, timeout=20)

    # If rejected, attempt one refresh + retry (helps if token became invalid early)
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
        return JSONResponse(
            {"error": "Spotify API error", "details": r.text},
            status_code=r.status_code,
        )

    return r.json()


# ---------------------------------------------------------------------
# Small "get or create" helpers for sync
# ---------------------------------------------------------------------

def get_or_create_user(db: Session, me_data: dict) -> User:
    spotify_id = me_data["id"]
    user = db.execute(select(User).where(User.spotify_id == spotify_id)).scalar_one_or_none()

    if user:
        return user

    user = User(
        spotify_id=spotify_id,
        display_name=me_data.get("display_name"),
        email=me_data.get("email"),
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def get_or_create_artist(db: Session, spotify_artist_id: str, name: str) -> Artist:
    artist = db.execute(
        select(Artist).where(Artist.spotify_artist_id == spotify_artist_id)
    ).scalar_one_or_none()

    if artist:
        return artist

    artist = Artist(spotify_artist_id=spotify_artist_id, name=name)
    db.add(artist)
    db.commit()
    db.refresh(artist)
    return artist


def get_or_create_track(db: Session, track_obj: dict, primary_artist_id: int) -> Track:
    spotify_track_id = track_obj["id"]
    track = db.execute(
        select(Track).where(Track.spotify_track_id == spotify_track_id)
    ).scalar_one_or_none()

    if track:
        return track

    track = Track(
        spotify_track_id=spotify_track_id,
        name=track_obj["name"],
        album=track_obj["album"]["name"] if track_obj.get("album") else None,
        primary_artist_id=primary_artist_id,
    )
    db.add(track)
    db.commit()
    db.refresh(track)
    return track


def play_exists(db: Session, user_id: int, track_id: int, played_at: datetime) -> bool:
    existing = db.execute(
        select(Play).where(
            Play.user_id == user_id,
            Play.track_id == track_id,
            Play.played_at == played_at,
        )
    ).scalar_one_or_none()

    return existing is not None


# ---------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------

@app.get("/")
def home():
    return {
        "message": "Spotify Analytics Project Running",
        "next": [
            "GET /login (browser)",
            "GET /login-url (copy/paste into browser)",
            "GET /auth/status (check auth)",
            "POST /sync/recent",
            "POST /sync/recent/incremental",
            "GET /analytics/dashboard?days=30&limit=10",
        ],
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
    return {
        "url": f"{AUTH_URL}?{urlencode(params)}",
        "note": "Copy/paste into a browser tab to log in (Swagger can't follow redirects).",
    }


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
    # Make state single-use to avoid replays
    if state not in STATE_STORE:
        return JSONResponse({"error": "Invalid state"}, status_code=400)
    STATE_STORE.discard(state)

    r = token_exchange(code)
    if r.status_code != 200:
        return JSONResponse({"error": "Token exchange failed", "details": r.text}, status_code=400)

    token_json = r.json()
    access_token = token_json["access_token"]
    refresh_token = token_json.get("refresh_token")
    expires_in = int(token_json.get("expires_in", 3600))

    # Fetch /me to identify user
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

    # Absolute redirect avoids edge cases
    return RedirectResponse(url="http://127.0.0.1:8000/me")


@app.get("/auth/status")
def auth_status(db: Session = Depends(get_db)):
    user = get_current_user(db)
    if not user:
        return JSONResponse({"logged_in": False, "reason": "no_user_with_token"}, status_code=401)

    token = ensure_fresh_token(db, user)
    if not token:
        return JSONResponse({"logged_in": False, "reason": "token_missing_or_expired"}, status_code=401)

    return {
        "logged_in": True,
        "spotify_id": user.spotify_id,
        "display_name": user.display_name,
        "token_expires_at": user.token_expires_at.isoformat() if user.token_expires_at else None,
    }


@app.get("/me")
def me(db: Session = Depends(get_db)):
    return spotify_get(db, "/me")


@app.get("/top-artists")
def top_artists(db: Session = Depends(get_db), time_range: str = "medium_term", limit: int = 20):
    return spotify_get(db, "/me/top/artists", params={"time_range": time_range, "limit": limit})


@app.get("/top-tracks")
def top_tracks(db: Session = Depends(get_db), time_range: str = "medium_term", limit: int = 20):
    return spotify_get(db, "/me/top/tracks", params={"time_range": time_range, "limit": limit})


@app.get("/recent")
def recent_tracks(db: Session = Depends(get_db), limit: int = 20):
    return spotify_get(db, "/me/player/recently-played", params={"limit": limit})


# ---------------------------------------------------------------------
# Sync endpoints (Spotify API -> DB)
# ---------------------------------------------------------------------

@app.post("/sync/recent")
def sync_recent(db: Session = Depends(get_db), limit: int = 50):
    me_data = spotify_get(db, "/me")
    if isinstance(me_data, JSONResponse):
        return me_data

    user = get_or_create_user(db, me_data)

    recent_data = spotify_get(db, "/me/player/recently-played", params={"limit": limit})
    if isinstance(recent_data, JSONResponse):
        return recent_data

    items = recent_data.get("items", [])
    added = 0

    for item in items:
        track_obj = item["track"]
        played_at = parse_played_at(item["played_at"])

        # Primary artist (Spotify returns multiple sometimes; we use the first)
        artist_obj = track_obj["artists"][0]
        artist = get_or_create_artist(db, artist_obj["id"], artist_obj["name"])

        track = get_or_create_track(db, track_obj, artist.id)

        if not play_exists(db, user.id, track.id, played_at):
            db.add(Play(user_id=user.id, track_id=track.id, played_at=played_at))
            db.commit()
            added += 1

    return {"status": "ok", "plays_added": added, "pulled": len(items)}


@app.post("/sync/recent/incremental")
def sync_recent_incremental(db: Session = Depends(get_db), limit: int = 50):
    me_data = spotify_get(db, "/me")
    if isinstance(me_data, JSONResponse):
        return me_data

    user = get_or_create_user(db, me_data)

    latest_played_at = db.query(func.max(Play.played_at)).filter(Play.user_id == user.id).scalar()

    params: dict[str, Any] = {"limit": limit}
    if latest_played_at:
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

        artist_obj = track_obj["artists"][0]
        artist = get_or_create_artist(db, artist_obj["id"], artist_obj["name"])

        track = get_or_create_track(db, track_obj, artist.id)

        if not play_exists(db, user.id, track.id, played_at):
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


# ---------------------------------------------------------------------
# Analytics (DB)
# ---------------------------------------------------------------------

@app.get("/analytics/summary")
def analytics_summary(db: Session = Depends(get_db)):
    total_plays = db.query(func.count(Play.id)).scalar() or 0

    unique_artists = (
        db.query(func.count(func.distinct(Artist.id)))
        .join(Track, Track.primary_artist_id == Artist.id)
        .join(Play, Play.track_id == Track.id)
        .scalar()
        or 0
    )

    unique_tracks = (
        db.query(func.count(func.distinct(Track.id)))
        .join(Play, Play.track_id == Track.id)
        .scalar()
        or 0
    )

    last_play = db.query(func.max(Play.played_at)).scalar()

    return {
        "total_plays": total_plays,
        "unique_artists": unique_artists,
        "unique_tracks": unique_tracks,
        "last_played_at": last_play.isoformat() if last_play else None,
    }


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


@app.get("/analytics/top-artists-window")
def analytics_top_artists_window(db: Session = Depends(get_db), days: int = 30, limit: int = 10):
    cutoff = now_utc() - timedelta(days=days)
    rows = (
        db.query(Artist.name, func.count(Play.id).label("plays"))
        .join(Track, Track.primary_artist_id == Artist.id)
        .join(Play, Play.track_id == Track.id)
        .filter(Play.played_at >= cutoff)
        .group_by(Artist.name)
        .order_by(func.count(Play.id).desc())
        .limit(limit)
        .all()
    )
    return [{"artist": name, "plays": plays} for name, plays in rows]


@app.get("/analytics/top-tracks-window")
def analytics_top_tracks_window(db: Session = Depends(get_db), days: int = 30, limit: int = 10):
    cutoff = now_utc() - timedelta(days=days)
    rows = (
        db.query(Track.name, Artist.name.label("artist"), func.count(Play.id).label("plays"))
        .join(Artist, Track.primary_artist_id == Artist.id)
        .join(Play, Play.track_id == Track.id)
        .filter(Play.played_at >= cutoff)
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
    cutoff = now_utc() - timedelta(days=days)
    rows = (
        db.query(func.strftime("%H", Play.played_at).label("hour"), func.count(Play.id).label("plays"))
        .filter(Play.played_at >= cutoff)
        .group_by(func.strftime("%H", Play.played_at))
        .order_by(func.strftime("%H", Play.played_at).asc())
        .all()
    )
    return [{"hour": int(hour), "plays": plays} for hour, plays in rows]


@app.get("/analytics/heatmap")
def analytics_heatmap(db: Session = Depends(get_db), days: int = 90):
    cutoff = now_utc() - timedelta(days=days)
    rows = (
        db.query(func.date(Play.played_at).label("day"), func.count(Play.id).label("plays"))
        .filter(Play.played_at >= cutoff)
        .group_by(func.date(Play.played_at))
        .order_by(func.date(Play.played_at).asc())
        .all()
    )
    return [{"day": str(day), "plays": plays} for day, plays in rows]


@app.get("/analytics/dashboard")
def analytics_dashboard(db: Session = Depends(get_db), days: int = 30, limit: int = 10):
    total_plays = db.query(func.count(Play.id)).scalar() or 0

    unique_tracks = (
        db.query(func.count(func.distinct(Track.id)))
        .join(Play, Play.track_id == Track.id)
        .scalar()
        or 0
    )

    unique_artists = (
        db.query(func.count(func.distinct(Artist.id)))
        .join(Track, Track.primary_artist_id == Artist.id)
        .join(Play, Play.track_id == Track.id)
        .scalar()
        or 0
    )

    last_play = db.query(func.max(Play.played_at)).scalar()
    cutoff = now_utc() - timedelta(days=days)

    top_artists = (
        db.query(Artist.name, func.count(Play.id).label("plays"))
        .join(Track, Track.primary_artist_id == Artist.id)
        .join(Play, Play.track_id == Track.id)
        .filter(Play.played_at >= cutoff)
        .group_by(Artist.name)
        .order_by(func.count(Play.id).desc())
        .limit(limit)
        .all()
    )

    top_tracks = (
        db.query(Track.name, Artist.name.label("artist"), func.count(Play.id).label("plays"))
        .join(Artist, Track.primary_artist_id == Artist.id)
        .join(Play, Play.track_id == Track.id)
        .filter(Play.played_at >= cutoff)
        .group_by(Track.name, Artist.name)
        .order_by(func.count(Play.id).desc())
        .limit(limit)
        .all()
    )

    by_day = (
        db.query(func.date(Play.played_at).label("day"), func.count(Play.id).label("plays"))
        .filter(Play.played_at >= cutoff)
        .group_by(func.date(Play.played_at))
        .order_by(func.date(Play.played_at).asc())
        .all()
    )

    by_hour = (
        db.query(func.strftime("%H", Play.played_at).label("hour"), func.count(Play.id).label("plays"))
        .filter(Play.played_at >= cutoff)
        .group_by(func.strftime("%H", Play.played_at))
        .order_by(func.strftime("%H", Play.played_at).asc())
        .all()
    )

    recent = (
        db.query(Play.played_at, Track.name, Artist.name.label("artist"))
        .join(Track, Play.track_id == Track.id)
        .join(Artist, Track.primary_artist_id == Artist.id)
        .order_by(Play.played_at.desc())
        .limit(25)
        .all()
    )

    return {
        "summary": {
            "total_plays": total_plays,
            "unique_artists": unique_artists,
            "unique_tracks": unique_tracks,
            "last_played_at": last_play.isoformat() if last_play else None,
        },
        "top_artists": [{"artist": n, "plays": p} for n, p in top_artists],
        "top_tracks": [{"track": t, "artist": a, "plays": p} for t, a, p in top_tracks],
        "by_day": [{"day": str(d), "plays": p} for d, p in by_day],
        "by_hour": [{"hour": int(h), "plays": p} for h, p in by_hour],
        "recent": [{"played_at": pa.isoformat(), "track": t, "artist": a} for pa, t, a in recent],
    }


# ---------------------------------------------------------------------
# Search + Drilldown
# ---------------------------------------------------------------------

@app.get("/analytics/search/artists")
def search_artists(db: Session = Depends(get_db), q: str = "", limit: int = 10):
    q = q.strip()
    if not q:
        return []

    rows = (
        db.query(Artist.id, Artist.name)
        .filter(Artist.name.ilike(f"%{q}%"))
        .order_by(Artist.name.asc())
        .limit(limit)
        .all()
    )
    return [{"artist_id": aid, "artist": name} for aid, name in rows]


@app.get("/analytics/search/tracks")
def search_tracks(db: Session = Depends(get_db), q: str = "", limit: int = 10):
    q = q.strip()
    if not q:
        return []

    rows = (
        db.query(Track.id, Track.name, Artist.name.label("artist"))
        .join(Artist, Track.primary_artist_id == Artist.id, isouter=True)
        .filter(Track.name.ilike(f"%{q}%"))
        .order_by(Track.name.asc())
        .limit(limit)
        .all()
    )
    return [{"track_id": tid, "track": t, "artist": a} for tid, t, a in rows]


@app.get("/analytics/artist/timeseries")
def artist_timeseries(artist_id: int, db: Session = Depends(get_db), days: int = 90):
    cutoff = now_utc() - timedelta(days=days)

    rows = (
        db.query(func.date(Play.played_at).label("day"), func.count(Play.id).label("plays"))
        .join(Track, Play.track_id == Track.id)
        .filter(Track.primary_artist_id == artist_id)
        .filter(Play.played_at >= cutoff)
        .group_by(func.date(Play.played_at))
        .order_by(func.date(Play.played_at).asc())
        .all()
    )

    return [{"day": str(d), "plays": p} for d, p in rows]


@app.get("/analytics/track/timeseries")
def track_timeseries(track_id: int, db: Session = Depends(get_db), days: int = 90):
    cutoff = now_utc() - timedelta(days=days)

    rows = (
        db.query(func.date(Play.played_at).label("day"), func.count(Play.id).label("plays"))
        .filter(Play.track_id == track_id)
        .filter(Play.played_at >= cutoff)
        .group_by(func.date(Play.played_at))
        .order_by(func.date(Play.played_at).asc())
        .all()
    )

    return [{"day": str(d), "plays": p} for d, p in rows]


# ---------------------------------------------------------------------
# Extra insights
# ---------------------------------------------------------------------

@app.get("/analytics/insights")
def analytics_insights(db: Session = Depends(get_db), days: int = 90):
    cutoff = now_utc() - timedelta(days=days)

    # Top weekday (SQLite %w => 0=Sun..6=Sat)
    weekday_rows = (
        db.query(func.strftime("%w", Play.played_at).label("w"), func.count(Play.id).label("plays"))
        .filter(Play.played_at >= cutoff)
        .group_by(func.strftime("%w", Play.played_at))
        .order_by(func.count(Play.id).desc())
        .all()
    )
    weekday_map = {"0": "Sun", "1": "Mon", "2": "Tue", "3": "Wed", "4": "Thu", "5": "Fri", "6": "Sat"}
    top_weekday = None
    if weekday_rows:
        w, p = weekday_rows[0]
        top_weekday = {"weekday": weekday_map.get(str(w), str(w)), "plays": p}

    # Top hour
    hour_rows = (
        db.query(func.strftime("%H", Play.played_at).label("hour"), func.count(Play.id).label("plays"))
        .filter(Play.played_at >= cutoff)
        .group_by(func.strftime("%H", Play.played_at))
        .order_by(func.count(Play.id).desc())
        .all()
    )
    top_hour = None
    if hour_rows:
        h, p = hour_rows[0]
        top_hour = {"hour": int(h), "plays": p}

    # Daily plays for streaks
    daily = (
        db.query(func.date(Play.played_at).label("day"), func.count(Play.id).label("plays"))
        .filter(Play.played_at >= cutoff)
        .group_by(func.date(Play.played_at))
        .order_by(func.date(Play.played_at).asc())
        .all()
    )
    days_set = {str(d) for d, _ in daily}
    sorted_days = sorted(days_set)

    def to_date(s: str) -> datetime:
        return datetime.fromisoformat(s)

    # Longest streak inside the window
    longest = 0
    cur = 0
    prev = None
    for ds in sorted_days:
        d = to_date(ds)
        if prev is None:
            cur = 1
        elif (d - prev).days == 1:
            cur += 1
        else:
            longest = max(longest, cur)
            cur = 1
        prev = d
    longest = max(longest, cur)

    # Current streak looking back from today
    today = now_utc().date()
    current = 0
    for i in range(days + 1):
        check = (today - timedelta(days=i)).isoformat()
        if check in days_set:
            current += 1
        else:
            break

    total_plays = db.query(func.count(Play.id)).filter(Play.played_at >= cutoff).scalar() or 0
    distinct_artists = (
        db.query(func.count(func.distinct(Artist.id)))
        .join(Track, Track.primary_artist_id == Artist.id)
        .join(Play, Play.track_id == Track.id)
        .filter(Play.played_at >= cutoff)
        .scalar()
        or 0
    )
    diversity = round((distinct_artists / total_plays), 4) if total_plays else 0.0

    return {
        "window_days": days,
        "top_weekday": top_weekday,
        "top_hour": top_hour,
        "current_streak_days": current,
        "longest_streak_days": longest,
        "diversity_ratio": diversity,  
    }
