import pandas as pd
import requests
import streamlit as st
import matplotlib.pyplot as plt

API = "http://127.0.0.1:8000"

st.set_page_config(page_title="Spotify Analytics", layout="wide")

st.title("🎧 Spotify Analytics Dashboard")

# ---- Sidebar controls ----
st.sidebar.header("Controls")
sync_limit = st.sidebar.slider("Sync limit", 10, 50, 50, step=10)
top_n = st.sidebar.slider("Top N", 5, 30, 10)

colA, colB, colC = st.columns(3)

def get_json(path: str):
    r = requests.get(f"{API}{path}", timeout=30)
    r.raise_for_status()
    return r.json()

def post_json(path: str):
    r = requests.post(f"{API}{path}", timeout=60)
    r.raise_for_status()
    return r.json()

# ---- Header cards ----
with colA:
    if st.button("🔄 Incremental Sync"):
        out = post_json(f"/sync/recent/incremental?limit={sync_limit}")
        st.success(f"Synced: pulled {out.get('pulled')} | added {out.get('plays_added')}")

with colB:
    if st.button("📥 Full Sync (recent)"):
        out = post_json(f"/sync/recent?limit={sync_limit}")
        st.success(f"Synced: pulled {out.get('pulled')} | added {out.get('plays_added')}")

with colC:
    try:
        me = get_json("/me")
        st.metric("Logged in as", me.get("display_name") or me.get("id"))
    except Exception:
        st.warning("Not logged in. Open FastAPI /login in your browser.")

st.divider()

# ---- Data ----
left, right = st.columns(2)

with left:
    st.subheader("Top Artists")
    artists = get_json(f"/analytics/top-artists?limit={top_n}")
    df_a = pd.DataFrame(artists)
    st.dataframe(df_a, use_container_width=True)

    if not df_a.empty:
        fig = plt.figure()
        plt.barh(df_a["artist"][::-1], df_a["plays"][::-1])
        plt.xlabel("Plays")
        plt.ylabel("Artist")
        st.pyplot(fig)

with right:
    st.subheader("Top Tracks")
    tracks = get_json(f"/analytics/top-tracks?limit={top_n}")
    df_t = pd.DataFrame(tracks)
    st.dataframe(df_t, use_container_width=True)

    if not df_t.empty:
        fig = plt.figure()
        # show "Track — Artist" labels
        labels = (df_t["track"] + " — " + df_t["artist"]).iloc[::-1]
        plt.barh(labels, df_t["plays"].iloc[::-1])
        plt.xlabel("Plays")
        plt.ylabel("Track")
        st.pyplot(fig)

st.divider()

bottom_left, bottom_right = st.columns(2)

with bottom_left:
    st.subheader("Listening by Day (last 30 days)")
    by_day = get_json("/analytics/listening-by-day?days=30")
    df_d = pd.DataFrame(by_day)
    if not df_d.empty:
        df_d["day"] = pd.to_datetime(df_d["day"])
        df_d = df_d.sort_values("day")
        st.line_chart(df_d.set_index("day")["plays"])
    else:
        st.info("No data yet. Run sync.")

with bottom_right:
    st.subheader("Listening by Hour (last 30 days)")
    by_hour = get_json("/analytics/listening-by-hour?days=30")
    df_h = pd.DataFrame(by_hour).sort_values("hour")
    if not df_h.empty:
        fig = plt.figure()
        plt.bar(df_h["hour"], df_h["plays"])
        plt.xlabel("Hour (0-23)")
        plt.ylabel("Plays")
        st.pyplot(fig)
    else:
        st.info("No data yet. Run sync.")

st.divider()

st.subheader("Recent Plays (from DB)")
recent = get_json("/analytics/recent-plays?limit=25")
df_r = pd.DataFrame(recent)
st.dataframe(df_r, use_container_width=True) 