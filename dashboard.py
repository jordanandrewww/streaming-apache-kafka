# dashboard.py
import time
from datetime import datetime, timedelta
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Real-Time Trips Dashboard", layout="wide")
st.title("🚕 Real-Time Ride-Sharing Dashboard")

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5432/kafka_db"

@st.cache_resource
def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True)

engine = get_engine(DATABASE_URL)

def load_data(status_filter: str = None, limit: int = 500):
    base = "SELECT * FROM trips"
    params = {}
    if status_filter and status_filter != "All":
        base += " WHERE status = :status"
        params["status"] = status_filter
    base += " ORDER BY updated_at DESC LIMIT :limit"
    params["limit"] = limit
    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(text(base), conn, params=params)
        return df
    except Exception as e:
        st.error(f"DB error: {e}")
        return pd.DataFrame()

# Sidebar
status_options = ["All", "requested", "accepted", "in_progress", "completed", "cancelled"]
selected_status = st.sidebar.selectbox("Status", status_options)
update_interval = st.sidebar.slider("Update Interval (s)", 2, 30, 5)
limit_records = st.sidebar.number_input("Max records", min_value=100, max_value=5000, value=500, step=100)

if st.sidebar.button("Refresh now"):
    st.experimental_rerun()

placeholder = st.empty()

while True:
    df = load_data(selected_status, int(limit_records))
    with placeholder.container():
        if df.empty:
            st.warning("No trips yet. Run producer & consumer.")
            time.sleep(update_interval)
            continue

        # timestamps
        if "start_time" in df.columns:
            df["start_time"] = pd.to_datetime(df["start_time"], utc=True, errors="coerce")
        if "end_time" in df.columns:
            df["end_time"] = pd.to_datetime(df["end_time"], utc=True, errors="coerce")
        df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)

        total_trips = len(df)
        active = len(df[df["status"]=="in_progress"])
        completed = len(df[df["status"]=="completed"])
        cancelled = len(df[df["status"]=="cancelled"])
        avg_fare = float(df["fare"].dropna().mean() or 0.0)
        avg_distance = float(df["distance_km"].dropna().mean() or 0.0)

        st.subheader(f"Showing {total_trips} trips (Filter: {selected_status})")
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Active (in_progress)", active)
        c2.metric("Completed", completed)
        c3.metric("Cancelled", cancelled)
        c4.metric("Avg Fare", f"${avg_fare:,.2f}")
        c5.metric("Avg Distance (km)", f"{avg_distance:.2f} km")

        st.markdown("### Latest Trips (top 10)")
        st.dataframe(df.head(10)[["trip_id","driver_id","status","fare","distance_km","start_time","end_time"]], use_container_width=True)

        # Map of recent trip starts
        map_df = df.dropna(subset=["start_lat","start_lon"]).head(500)
        if not map_df.empty:
            st.markdown("### Trip start locations (recent)")
            st.map(map_df.rename(columns={"start_lat":"lat","start_lon":"lon"}).loc[:,["lat","lon"]])

        # Time series: trips by minute
        ts = df.set_index("updated_at").resample("1T").size().reset_index(name="count")
        fig_ts = px.line(ts, x="updated_at", y="count", title="Trips per minute (recent)")
        st.plotly_chart(fig_ts, use_container_width=True)

        st.caption(f"Last updated: {datetime.now().isoformat()} • Auto-refresh: {update_interval}s")
    time.sleep(update_interval)
