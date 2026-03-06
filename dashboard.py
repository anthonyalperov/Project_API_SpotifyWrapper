import math
import os
import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import requests
import streamlit as st

API = os.getenv("API_BASE_URL", "https://project-api-spotifywrapper.onrender.com").rstrip("/")
st.set_page_config(page_title="Spotify // Telemetry", layout="wide")


# ---------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------

def get_json(path: str):
    try:
        r = requests.get(f"{API}{path}", timeout=30)
    except requests.exceptions.RequestException as e:
        return {"_error": True, "_detail": f"API connection failed: {e}"}

    if r.status_code == 401:
        return {"_unauthorized": True, "_detail": r.text}

    if not r.ok:
        return {"_error": True, "_detail": r.text}

    return r.json()


def post_json(path: str):
    try:
        r = requests.post(f"{API}{path}", timeout=60)
    except requests.exceptions.RequestException as e:
        return {"_error": True, "_detail": f"API connection failed: {e}"}

    if r.status_code == 401:
        return {"_unauthorized": True, "_detail": r.text}

    if not r.ok:
        return {"_error": True, "_detail": r.text}

    return r.json()


# ---------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------

def pill(text: str):
    st.markdown(
        f"""
        <span style="
            display:inline-block;
            padding:6px 10px;
            border:1px solid rgba(0,229,255,0.35);
            border-radius:999px;
            background:rgba(0,229,255,0.08);
            font-size:12px;
            letter-spacing:0.4px;">
            {text}
        </span>
        """,
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------

st.markdown(
    """
    <div style="
        padding:18px 18px 10px 18px;
        border:1px solid rgba(0,229,255,0.18);
        border-radius:16px;
        background: radial-gradient(1200px circle at 0% 0%, rgba(0,229,255,0.18), transparent 35%),
                    radial-gradient(1000px circle at 100% 0%, rgba(29,185,84,0.12), transparent 35%),
                    rgba(16,24,39,0.35);
    ">
      <div style="font-size:13px; opacity:0.85; letter-spacing:0.9px;">SPOTIFY // TELEMETRY</div>
      <div style="font-size:34px; font-weight:800; margin-top:4px;">Listening Intelligence Console</div>
      <div style="margin-top:6px; opacity:0.82;">FastAPI • SQLite • ETL Sync • Analytics • Streamlit UI</div>
    </div>
    """,
    unsafe_allow_html=True,
)
st.write("")


# ---------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------

st.sidebar.markdown("## ⚙️ Control Panel")

days = st.sidebar.selectbox("Time window (days)", [7, 30, 90, 365], index=1)
top_n = st.sidebar.slider("Top N", 5, 25, 10)
sync_limit = st.sidebar.selectbox("Sync limit", [10, 20, 30, 50], index=3)

st.sidebar.markdown("---")
pill(f"API: {API}")
pill(f"WINDOW: last {days}d")
pill(f"TOP_N: {top_n}")


# ---------------------------------------------------------------------
# Actions row
# ---------------------------------------------------------------------

c1, c2, c3, c4 = st.columns([1, 1, 1, 2])

with c3:
    login = get_json("/login-url")
    login_url = login.get("url")

    if login_url:
        st.link_button("🔐 Login to Spotify", login_url, use_container_width=True)
    else:
        if login.get("_unauthorized"):
            st.warning("API says unauthorized for login-url (unexpected).")
        else:
            st.warning("Could not fetch login URL.")

with c4:
    me = get_json("/me")
    if me.get("_unauthorized"):
        st.warning("Not logged in. Click **Login to Spotify** and complete auth in your browser.")
    else:
        name = me.get("display_name") or me.get("id")
        st.markdown(
            f"**User:** `{name}`  •  **Plan:** `{me.get('product','?')}`  •  **Country:** `{me.get('country','?')}`"
        )

with c1:
    if st.button("🔄 Incremental Sync", use_container_width=True):
        out = post_json(f"/sync/recent/incremental?limit={sync_limit}")
        if out.get("_unauthorized"):
            st.error("Unauthorized. Click **Login to Spotify**, finish auth in your browser, then try again.")
        else:
            st.success(f"Pulled {out.get('pulled')} • Added {out.get('plays_added')}")

with c2:
    if st.button("📥 Full Sync", use_container_width=True):
        out = post_json(f"/sync/recent?limit={sync_limit}")
        if out.get("_unauthorized"):
            st.error("Unauthorized. Click **Login to Spotify**, finish auth in your browser, then try again.")
        else:
            st.success(f"Pulled {out.get('pulled')} • Added {out.get('plays_added')}")

st.divider()


# ---------------------------------------------------------------------
# Fetch dashboard data
# ---------------------------------------------------------------------

dash = get_json(f"/analytics/dashboard?days={days}&limit={top_n}")
if dash.get("_unauthorized"):
    dash = None

insights = get_json(f"/analytics/insights?days={days}")
if insights.get("_unauthorized"):
    insights = None


# ---------------------------------------------------------------------
# KPIs
# ---------------------------------------------------------------------

if dash:
    s = dash["summary"]

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("TOTAL PLAYS", f"{s['total_plays']:,}")
    k2.metric("UNIQUE ARTISTS", f"{s['unique_artists']:,}")
    k3.metric("UNIQUE TRACKS", f"{s['unique_tracks']:,}")
    k4.metric("LAST PLAY", s["last_played_at"] or "—")
else:
    st.info("Run a sync to populate dashboard data.")


# Extra insights row
if insights:
    i1, i2, i3, i4 = st.columns(4)

    tw = insights.get("top_weekday") or {}
    th = insights.get("top_hour") or {}

    i1.metric("TOP DAY", f"{tw.get('weekday','—')}", f"{tw.get('plays','')} plays" if tw else "")
    i2.metric("TOP HOUR", f"{th.get('hour','—')}", f"{th.get('plays','')} plays" if th else "")
    i3.metric("CURRENT STREAK", f"{insights.get('current_streak_days',0)} days")
    i4.metric("LONGEST STREAK", f"{insights.get('longest_streak_days',0)} days")

st.write("")


# ---------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------

tab_overview, tab_heat, tab_trends, tab_drill, tab_recent = st.tabs(
    ["🧠 Overview", "🧱 Heatmap", "📈 Trends", "🔎 Drilldown", "🕒 Recent"]
)


# ---------------------------------------------------------------------
# Overview
# ---------------------------------------------------------------------

with tab_overview:
    if not dash:
        st.stop()

    left, right = st.columns(2)

    with left:
        st.subheader("Top Artists")

        df_a = pd.DataFrame(dash["top_artists"])
        if df_a.empty:
            st.info("No artist data yet.")
        else:
            df_a["rank"] = range(1, len(df_a) + 1)
            df_a["share_%"] = (df_a["plays"] / df_a["plays"].sum() * 100).round(1)

            st.dataframe(df_a[["rank", "artist", "plays", "share_%"]], use_container_width=True)

            fig = px.bar(
                df_a.sort_values("plays", ascending=True),
                x="plays",
                y="artist",
                orientation="h",
                text="plays",
                title="Top Artists",
            )
            fig.update_traces(textposition="outside", cliponaxis=False)
            fig.update_layout(
                height=420,
                margin=dict(l=10, r=10, t=50, b=10),
                xaxis_title="Plays",
                yaxis_title="",
            )
            st.plotly_chart(fig, use_container_width=True)

    with right:
        st.subheader("Top Tracks")

        df_t = pd.DataFrame(dash["top_tracks"])
        if df_t.empty:
            st.info("No track data yet.")
        else:
            df_t["label"] = df_t["track"] + " — " + df_t["artist"]

            def trunc(s: str, n: int = 60) -> str:
                return s if len(s) <= n else s[: n - 1] + "…"

            df_t["label"] = df_t["label"].apply(trunc)
            df_t["rank"] = range(1, len(df_t) + 1)
            df_t["share_%"] = (df_t["plays"] / df_t["plays"].sum() * 100).round(1)

            st.dataframe(df_t[["rank", "label", "plays", "share_%"]], use_container_width=True)

            fig = px.bar(
                df_t.sort_values("plays", ascending=True),
                x="plays",
                y="label",
                orientation="h",
                text="plays",
                title="Top Tracks",
            )
            fig.update_traces(textposition="outside", cliponaxis=False)
            fig.update_layout(
                height=520,
                margin=dict(l=10, r=10, t=50, b=10),
                xaxis_title="Plays",
                yaxis_title="",
            )
            st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------
# Heatmap (GitHub-style)
# ---------------------------------------------------------------------

with tab_heat:
    st.subheader("Listening Heatmap")
    st.caption("GitHub-style daily listening intensity.")

    hm = get_json(f"/analytics/heatmap?days={max(days, 90)}")

    # NOTE: hm can be dict (unauthorized) OR list (success).
    if isinstance(hm, dict) and hm.get("_unauthorized"):
        st.info("Not logged in. Click **Login to Spotify**, finish auth, then run sync.")
        st.stop()

    if not isinstance(hm, list):
        st.error(f"Unexpected response from /analytics/heatmap: {type(hm).__name__}")
        st.stop()

    df = pd.DataFrame(hm)
    if df.empty:
        st.info("No data yet. Run sync.")
        st.stop()

    df["day"] = pd.to_datetime(df["day"])
    df = df.sort_values("day")

    start = df["day"].min()
    end = df["day"].max()

    # Align start to Monday for a clean week grid
    start = start - pd.to_timedelta(start.weekday(), unit="D")

    all_days = pd.date_range(start=start, end=end, freq="D")
    plays_map = dict(zip(df["day"].dt.date, df["plays"]))

    weeks = math.ceil(len(all_days) / 7)
    grid = [[0 for _ in range(weeks)] for __ in range(7)]

    for idx, day in enumerate(all_days):
        w = idx // 7
        d = day.weekday()
        grid[d][w] = plays_map.get(day.date(), 0)

    fig = plt.figure(figsize=(12, 2.6))
    img = plt.imshow(grid, aspect="auto")
    plt.yticks(range(7), ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"])
    plt.xticks([])
    plt.title("Daily Plays Heatmap")
    plt.colorbar(img, fraction=0.02, pad=0.02)
    plt.tight_layout()
    st.pyplot(fig)


# ---------------------------------------------------------------------
# Trends
# ---------------------------------------------------------------------

with tab_trends:
    if not dash:
        st.stop()

    l, r = st.columns(2)

    with l:
        st.subheader("Plays by Day")

        df_d = pd.DataFrame(dash["by_day"])
        if df_d.empty:
            st.info("No data.")
        else:
            df_d["day"] = pd.to_datetime(df_d["day"])
            df_d = df_d.sort_values("day")

            fig = px.line(df_d, x="day", y="plays", markers=True, title="Plays by Day")
            fig.update_layout(height=360, margin=dict(l=10, r=10, t=50, b=10), xaxis_title="", yaxis_title="Plays")
            st.plotly_chart(fig, use_container_width=True)

    with r:
        st.subheader("Plays by Hour")

        df_h = pd.DataFrame(dash["by_hour"]).sort_values("hour")
        if df_h.empty:
            st.info("No data.")
        else:
            fig = px.bar(df_h, x="hour", y="plays", title="Plays by Hour")
            fig.update_layout(height=360, margin=dict(l=10, r=10, t=50, b=10), xaxis_title="Hour", yaxis_title="Plays")
            st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------
# Drilldown
# ---------------------------------------------------------------------

with tab_drill:
    st.subheader("Search + Drilldown")
    st.caption("Search an artist or track, then view plays over time.")

    mode = st.radio("Mode", ["Artist", "Track"], horizontal=True)
    query = st.text_input("Search", placeholder="type a name…")

    if query.strip():
        if mode == "Artist":
            results = get_json(f"/analytics/search/artists?q={query}&limit=15")
            if results and not isinstance(results, dict):
                label_map = {f"{r['artist']} (id:{r['artist_id']})": r["artist_id"] for r in results}
                selected = st.selectbox("Pick an artist", list(label_map.keys()))
                artist_id = label_map[selected]

                ts = get_json(f"/analytics/artist/timeseries?artist_id={artist_id}&days={days}")
                df_ts = pd.DataFrame(ts if not isinstance(ts, dict) else [])
                if df_ts.empty:
                    st.info("No plays found in this window.")
                else:
                    df_ts["day"] = pd.to_datetime(df_ts["day"])
                    fig = px.line(df_ts, x="day", y="plays", markers=True, title="Artist Plays Over Time")
                    fig.update_layout(height=380, margin=dict(l=10, r=10, t=50, b=10))
                    st.plotly_chart(fig, use_container_width=True)
                    st.dataframe(df_ts.sort_values("day", ascending=False), use_container_width=True)
            else:
                st.info("No matches.")
        else:
            results = get_json(f"/analytics/search/tracks?q={query}&limit=15")
            if results and not isinstance(results, dict):
                label_map = {
                    f"{r['track']} — {r.get('artist','')} (id:{r['track_id']})": r["track_id"]
                    for r in results
                }
                selected = st.selectbox("Pick a track", list(label_map.keys()))
                track_id = label_map[selected]

                ts = get_json(f"/analytics/track/timeseries?track_id={track_id}&days={days}")
                df_ts = pd.DataFrame(ts if not isinstance(ts, dict) else [])
                if df_ts.empty:
                    st.info("No plays found in this window.")
                else:
                    df_ts["day"] = pd.to_datetime(df_ts["day"])
                    fig = px.line(df_ts, x="day", y="plays", markers=True, title="Track Plays Over Time")
                    fig.update_layout(height=380, margin=dict(l=10, r=10, t=50, b=10))
                    st.plotly_chart(fig, use_container_width=True)
                    st.dataframe(df_ts.sort_values("day", ascending=False), use_container_width=True)
            else:
                st.info("No matches.")


# ---------------------------------------------------------------------
# Recent
# ---------------------------------------------------------------------

with tab_recent:
    if not dash:
        st.stop()

    st.subheader("Recent Plays (DB)")

    df_r = pd.DataFrame(dash["recent"])
    if df_r.empty:
        st.info("No recent plays.")
    else:
        df_r["played_at"] = pd.to_datetime(df_r["played_at"])
        df_r = df_r.sort_values("played_at", ascending=False)
        st.dataframe(df_r, use_container_width=True) 