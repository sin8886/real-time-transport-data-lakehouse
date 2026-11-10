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
        background: #08111f;
        color: #f5f7fb;
    }
    [data-testid="stSidebar"] {
        background: #101d32;
        border-right: 1px solid #334968;
    }
    [data-testid="stSidebar"] * {
        color: #e8eef7;
    }
    [data-testid="stSidebar"] .stCaption {
        color: #aebdd1;
    }
    [data-testid="stSidebar"] [data-testid="stRadio"] label {
        color: #e8eef7;
    }
    [data-testid="stSidebar"] [data-testid="stRadio"] div[role="radiogroup"] label[data-checked="true"] {
        color: #67e8d1;
        font-weight: 700;
    }
    [data-testid="stSidebar"] .stButton button {
        background: #e8eef7;
        color: #0d192b;
        border: 0;
        font-weight: 700;
    }
    [data-testid="stSidebar"] .stButton button p,
    [data-testid="stSidebar"] .stButton button span {
        color: #0d192b !important;
    }
    [data-testid="stSidebar"] .stCode,
    [data-testid="stSidebar"] code {
        color: #dce8f7;
        background: #1b2c47;
    }
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, #1a2e4d, #14243d);
        border: 1px solid #46658f;
        border-radius: 14px;
        padding: 18px;
    }
    [data-testid="stMetricLabel"] p {
        color: #b7c8dd !important;
        font-weight: 600;
    }
    [data-testid="stMetricValue"] {
        color: #ffffff !important;
        font-weight: 800;
    }
    [data-testid="stMetricDelta"] {
        color: #67e8d1 !important;
    }
    h1, h2, h3 {
        color: #f8fafc !important;
    }
    .stMarkdown p, .stCaption, [data-testid="stCaptionContainer"] {
        color: #bdcbe0;
    }
    [data-testid="stAlert"] p {
        color: #f8fafc;
    }
    [data-testid="stDataFrame"] {
        border: 1px solid #3a5275;
        border-radius: 10px;
    }
    button[kind="secondary"] {
        color: #e8eef7;
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
            "The batch Gold snapshot and the live Kafka/Spark serving path are "
            "separate products. Live vehicle observations appear only while the "
            "producer and serving job are running; this page never presents "
            "synthetic values as live."
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
        font={"color": "#e8eef7"},
        xaxis={"gridcolor": "#304766", "zerolinecolor": "#46658f"},
        yaxis={"gridcolor": "#304766", "zerolinecolor": "#46658f"},
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
    st.caption(
        "Showing real Auckland Transport observations received within the last "
        "10 minutes. Refresh to fetch the latest serving snapshot."
    )
    map_vehicles = vehicles[
        vehicles.latitude.between(-37.5, -36.5)
        & vehicles.longitude.between(174.3, 175.2)
    ].copy()
    if map_vehicles.empty:
        st.warning("No vehicles are currently inside the Auckland map bounds.")
    st.map(
        map_vehicles.rename(columns={"latitude": "lat", "longitude": "lon"})[
            ["lat", "lon"]
        ].dropna(),
        latitude="lat",
        longitude="lon",
        zoom=10,
        size=12,
    )
    if len(map_vehicles) < len(vehicles):
        st.caption(
            f"{len(vehicles) - len(map_vehicles):,} observations were excluded "
            "from the map because their coordinates were outside Auckland."
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
