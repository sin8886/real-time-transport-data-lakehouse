"""Urban Mobility Analytics Streamlit dashboard."""

from __future__ import annotations

import sys
from pathlib import Path

import plotly.express as px
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dashboard.data import (  # noqa: E402
    format_timestamp,
    load_overview,
    load_active_vehicles,
    load_pipeline_runs,
    load_route_metrics,
    load_stop_metrics,
)


st.set_page_config(
    page_title="Urban Mobility Analytics",
    page_icon="●",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    [data-testid="stAppViewContainer"] {
        background: #0b1220;
        color: #e8eef7;
    }
    [data-testid="stSidebar"] {
        background: #111b2e;
        border-right: 1px solid #263550;
    }
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, #16243c, #111b2e);
        border: 1px solid #263b5d;
        border-radius: 14px;
        padding: 18px;
    }
    .eyebrow {
        color: #5eead4;
        font-size: 0.78rem;
        font-weight: 700;
        letter-spacing: 0.16em;
        text-transform: uppercase;
    }
    .muted { color: #93a4bd; }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=30, show_spinner=False)
def get_overview() -> dict:
    return load_overview()


@st.cache_data(ttl=30, show_spinner=False)
def get_routes():
    return load_route_metrics()


@st.cache_data(ttl=30, show_spinner=False)
def get_stops():
    return load_stop_metrics()


@st.cache_data(ttl=30, show_spinner=False)
def get_runs():
    return load_pipeline_runs()


def page_header(title: str, subtitle: str) -> None:
    st.markdown(
        '<div class="eyebrow">Urban Mobility · Auckland</div>', unsafe_allow_html=True
    )
    st.title(title)
    st.markdown(f'<p class="muted">{subtitle}</p>', unsafe_allow_html=True)


def show_connection_error(error: Exception) -> None:
    st.error("Serving database is unavailable.")
    st.caption(
        "Start Docker Compose and load the Auckland Gold output into PostgreSQL "
        "before opening the dashboard."
    )
    with st.expander("Technical details"):
        st.code(str(error))


def overview_page() -> None:
    page_header(
        "Transport intelligence",
        "A calm operational view of the latest Auckland Static Gold snapshot.",
    )
    try:
        overview = get_overview()
    except Exception as error:
        show_connection_error(error)
        return

    daily = overview["daily"]
    runs = overview["latest_run"]
    if daily.empty:
        st.warning("No Auckland Gold snapshot is loaded yet.")
        return

    row = daily.iloc[0]
    cols = st.columns(4)
    cols[0].metric("Scheduled trips", f"{int(row.scheduled_trip_count):,}")
    cols[1].metric("Routes analysed", f"{int(row.active_route_count):,}")
    cols[2].metric("Stops covered", f"{int(row.active_stop_count):,}")
    cols[3].metric("Snapshot date", str(row.service_date))

    st.divider()
    left, right = st.columns([1.25, 1])
    with left:
        st.subheader("What is live today?")
        st.info(
            "The current v1.0 pipeline has a verified Kafka/Spark streaming path, "
            "but it does not yet persist live vehicle events to a dashboard serving "
            "table. This page therefore never presents synthetic values as live."
        )
    with right:
        st.subheader("Latest pipeline run")
        if runs.empty:
            st.caption("No pipeline audit record is available.")
        else:
            run = runs.iloc[0]
            st.metric("Status", str(run.status).upper())
            st.caption(f"Completed: {format_timestamp(run.completed_at)}")


def route_page() -> None:
    page_header(
        "Route performance",
        "Compare scheduled activity and stop coverage from the latest Gold snapshot.",
    )
    try:
        routes = get_routes()
    except Exception as error:
        show_connection_error(error)
        return
    if routes.empty:
        st.warning("No route metrics are available.")
        return
    top = routes.head(20).sort_values("stop_event_count")
    chart = px.bar(
        top,
        x="stop_event_count",
        y="route_id",
        orientation="h",
        color="served_stop_count",
        color_continuous_scale=["#23395d", "#5eead4"],
        labels={"stop_event_count": "Stop events", "route_id": "Route"},
    )
    chart.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=620,
    )
    st.plotly_chart(chart, width="stretch")
    st.dataframe(routes, width="stretch", hide_index=True)


def stops_page() -> None:
    page_header(
        "Stop activity",
        "Identify the busiest stops in the latest Auckland service snapshot.",
    )
    try:
        stops = get_stops()
    except Exception as error:
        show_connection_error(error)
        return
    if stops.empty:
        st.warning("No stop metrics are available.")
        return
    st.dataframe(stops.head(50), width="stretch", hide_index=True)


def live_page() -> None:
    page_header(
        "Live vehicle tracking",
        "A truthful view of the streaming serving boundary.",
    )
    try:
        vehicles = load_active_vehicles()
    except Exception as error:
        show_connection_error(error)
        return
    if vehicles.empty:
        st.warning("No vehicle observations in the last 10 minutes.")
        st.caption(
            "Start the realtime producer and Spark serving job to populate this view."
        )
        return
    cols = st.columns(3)
    cols[0].metric("Active vehicles", f"{len(vehicles):,}")
    cols[1].metric("Routes represented", f"{vehicles.route_id.nunique():,}")
    cols[2].metric("Latest event", format_timestamp(vehicles.event_ts.max()))
    st.map(
        vehicles.rename(columns={"latitude": "lat", "longitude": "lon"})[
            ["lat", "lon"]
        ].dropna(),
        latitude="lat",
        longitude="lon",
        zoom=10,
    )
    st.dataframe(vehicles, width="stretch", hide_index=True)


def platform_page() -> None:
    page_header(
        "Platform health",
        "Audit signals from the PostgreSQL serving layer.",
    )
    try:
        runs = get_runs()
    except Exception as error:
        show_connection_error(error)
        return
    if runs.empty:
        st.info("No pipeline audit records are available.")
        return
    st.dataframe(runs, width="stretch", hide_index=True)


pages = {
    "Overview": overview_page,
    "Route performance": route_page,
    "Stop activity": stops_page,
    "Live vehicles": live_page,
    "Platform health": platform_page,
}

with st.sidebar:
    st.markdown("## URBAN MOBILITY")
    st.caption("DATA PLATFORM · v1.0.3")
    selected_page = st.radio("Navigate", list(pages))
    st.divider()
    st.caption("Data source")
    st.code("PostgreSQL serving", language=None)
    if st.button("Refresh data"):
        st.cache_data.clear()
        st.rerun()

pages[selected_page]()
