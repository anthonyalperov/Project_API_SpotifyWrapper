# Spotify Listening Analytics (FastAPI + SQLite + Streamlit)

A full-stack personal analytics platform that ingests Spotify listening history via the Spotify Web API, stores it in a relational database, and serves interactive analytics through a web dashboard.

## Tech Stack
- **Backend:** FastAPI, Requests, SQLAlchemy
- **Auth:** Spotify OAuth (Authorization Code + refresh tokens)
- **Database:** SQLite
- **Analytics:** SQL aggregations + time-window insights
- **Frontend:** Streamlit + Plotly

## Features
- Spotify OAuth login + token refresh (persisted in DB)
- ETL sync pipeline to ingest recently played tracks
- Deduplicated play events (unique constraint)
- Analytics endpoints (top artists/tracks, trends, heatmap, drilldowns)
- Streamlit dashboard: KPIs, charts, heatmap, search + drilldown

## Project Structure
spotify-analytics/
├─ main.py
├─ db.py
├─ models.py
├─ dashboard.py
├─ spotify.db
├─ requirements.txt
└─ .streamlit/config.toml
## Setup (Local)
1) Create a Spotify developer app and add this Redirect URI:
- `http://127.0.0.1:8000/callback`

2) Create `.env`:
```env
SPOTIFY_CLIENT_ID=...
SPOTIFY_CLIENT_SECRET=...
SPOTIFY_REDIRECT_URI=http://127.0.0.1:8000/callback
