"""Urban Mobility Analytics Streamlit dashboard."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
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
    load_serving_health,
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
        font-family: Inter, "Segoe UI", Arial, sans-serif;
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
    [data-testid="stAlert"] {
        background: #172945;
        border: 1px solid #42628c;
        color: #f8fafc;
    }
    [data-testid="stAlert"][data-baseweb="notification"] p,
    [data-testid="stAlert"] [data-testid="stMarkdownContainer"] p {
        color: #f8fafc !important;
    }
    [data-testid="stAlert"] svg {
        fill: #67e8d1;
    }
    [data-testid="stExpander"] {
        border-color: #3a5275;
        background: #101d32;
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


def live_metrics(vehicles):
    """Derive transparent, rule-based signals from current vehicle observations."""
    speeds = vehicles["speed"].dropna()
    moving = speeds[speeds > 2]
    stopped = speeds[speeds <= 2]
    congestion_score = round((len(stopped) / len(speeds)) * 100) if len(speeds) else None
    status = (
        "Elevated"
        if congestion_score is not None and congestion_score >= 40
        else "Moderate"
        if congestion_score is not None and congestion_score >= 20
        else "Moving normally"
        if congestion_score is not None
        else "Unavailable"
    )
    return {
        "active_vehicles": len(vehicles),
        "routes": vehicles["route_id"].replace("", None).dropna().nunique(),
        "moving": len(moving),
        "stopped": len(stopped),
        "congestion_score": congestion_score,
        "status": status,
        "average_speed": speeds.mean() if len(speeds) else None,
    }


def route_live_summary(vehicles):
    """Return route-level live activity without inventing delay observations."""
    live = vehicles.copy()
    live["route_id"] = live["route_id"].replace("", "Unassigned").fillna("Unassigned")
    summary = (
        live.groupby("route_id", as_index=False)
        .agg(
            active_vehicles=("vehicle_id", "count"),
            average_observed_speed=("speed", "mean"),
            latest_event=("event_ts", "max"),
        )
        .sort_values("active_vehicles", ascending=False)
    )
    return summary


def localize_timestamps(frame: pd.DataFrame) -> pd.DataFrame:
    """Convert displayed database timestamps to Auckland local time."""
    localized = frame.copy()
    for column in ("event_ts", "ingested_at", "latest_event"):
        if column in localized:
            timestamps = pd.to_datetime(localized[column], utc=True, errors="coerce")
            localized[column] = timestamps.dt.tz_convert(
                "Pacific/Auckland"
            ).dt.strftime("%Y-%m-%d %H:%M:%S")
    return localized


def speed_distribution(vehicles):
    """Classify observed positions using a documented GTFS speed threshold."""
    speeds = vehicles["speed"]
    return pd.DataFrame(
        {
            "status": ["Moving", "Stopped / slow", "Unknown"],
            "vehicles": [
                int((speeds > 2).sum()),
                int((speeds <= 2).sum()),
                int(speeds.isna().sum()),
            ],
        }
    )


def render_live_map(vehicles) -> None:
    map_vehicles = vehicles[
        vehicles.latitude.between(-37.5, -36.5)
        & vehicles.longitude.between(174.3, 175.2)
    ].copy()
    if map_vehicles.empty:
        st.warning("No vehicles are currently inside the Auckland map bounds.")
        return
    map_vehicles = map_vehicles.dropna(subset=["latitude", "longitude"])
    if len(map_vehicles) > 600:
        map_vehicles = map_vehicles.head(600)
        st.caption("Map is capped at the 600 most recent vehicles for browser stability.")
    st.map(
        map_vehicles.rename(columns={"latitude": "lat", "longitude": "lon"})[
            ["lat", "lon"]
        ],
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
    try:
        vehicles = load_active_vehicles()
    except Exception:
        vehicles = pd.DataFrame()
    if vehicles.empty:
        st.warning("Realtime serving is currently offline.")
        live = {
            "active_vehicles": 0,
            "routes": 0,
            "congestion_score": None,
            "status": "Unavailable",
        }
    else:
        live = live_metrics(vehicles)
    cols = st.columns(4)
    cols[0].metric("Active vehicles", f"{live['active_vehicles']:,}")
    cols[1].metric("Live routes", f"{live['routes']:,}")
    cols[2].metric(
        "Congestion risk",
        f"{live['congestion_score']}/100"
        if live["congestion_score"] is not None
        else "Unavailable",
        help="Rule-based proxy: percentage of fresh observations with speed <= 2 m/s.",
    )
    cols[3].metric("Traffic state", live["status"])

    st.divider()
    left, right = st.columns([1.35, 1])
    with left:
        st.subheader("Auckland live traffic")
        if vehicles.empty:
            st.info("Start the realtime producer and Spark serving job to populate this view.")
        else:
            render_live_map(vehicles)
    with right:
        st.subheader("Traffic status")
        if vehicles.empty:
            st.info("No current vehicle observations.")
        else:
            distribution = speed_distribution(vehicles)
            chart = px.pie(
                distribution,
                names="status",
                values="vehicles",
                hole=0.58,
                color="status",
                color_discrete_map={
                    "Moving": "#67e8d1",
                    "Stopped / slow": "#f59e0b",
                    "Unknown": "#64748b",
                },
            )
            chart.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                height=300,
                margin={"t": 20, "b": 20, "l": 10, "r": 10},
                font={"color": "#e8eef7"},
                legend={"font": {"color": "#e8eef7"}},
                showlegend=True,
            )
            st.plotly_chart(chart, width="stretch", config={"displayModeBar": False})
        st.caption(
            f"Static Gold snapshot: {row.service_date} · "
            f"{int(row.scheduled_trip_count):,} scheduled trips · "
            f"{int(row.active_stop_count):,} stops"
        )
    st.subheader("Latest pipeline run")
    with st.container():
        if runs.empty:
            st.caption("No pipeline audit record is available.")
        else:
            run = runs.iloc[0]
            run_cols = st.columns(3)
            run_cols[0].metric("Status", str(run.status).upper())
            run_cols[1].metric("Records out", f"{int(run.records_out):,}")
            run_cols[2].caption(f"Completed: {format_timestamp(run.completed_at)}")
    st.caption(
        "Traffic risk is an estimated operational proxy, not a delay measurement. "
        "Trip Update delay windows are not currently persisted in the dashboard serving table."
    )


def route_page() -> None:
    page_header(
        "Route performance",
        "Combine static Gold route activity with current observed vehicle movement.",
    )
    try:
        routes = get_routes()
    except Exception as error:
        show_connection_error(error)
        return
    if routes.empty:
        st.warning("No route metrics are available.")
        return
    try:
        vehicles = load_active_vehicles()
    except Exception:
        vehicles = pd.DataFrame()
    live_routes = route_live_summary(vehicles) if not vehicles.empty else None
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
    if live_routes is not None:
        st.subheader("Current route activity")
        st.dataframe(
            localize_timestamps(live_routes.head(30)),
            width="stretch",
            hide_index=True,
        )
    st.info(
        "Average delay and reliability rankings require persisted Trip Update "
        "delay observations. The current serving table contains vehicle positions "
        "only, so this page does not fabricate delay values."
    )
    st.dataframe(routes, width="stretch", hide_index=True)


def stops_page() -> None:
    page_header(
        "Stop activity",
        "Identify the busiest stops and explain the current data boundary.",
    )
    try:
        stops = get_stops()
    except Exception as error:
        show_connection_error(error)
        return
    if stops.empty:
        st.warning("No stop metrics are available.")
        return
    st.metric("Stops in static Gold", f"{len(stops):,}")
    st.info(
        "The ranking below is based on scheduled/static Gold stop events. "
        "Realtime stop arrivals require Trip Updates to be persisted separately."
    )
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
        "Real Auckland Transport observations received within the last 10 minutes."
    )
    render_live_map(vehicles)
    st.subheader("Vehicle details")
    st.dataframe(localize_timestamps(vehicles), width="stretch", hide_index=True)


def platform_page() -> None:
    page_header(
        "Platform health",
        "Monitor data freshness, pipeline audit signals, and local platform boundaries.",
    )
    try:
        runs = get_runs()
    except Exception as error:
        show_connection_error(error)
        return
    try:
        health = load_serving_health()
    except Exception as error:
        show_connection_error(error)
        return
    if not health.empty:
        row = health.iloc[0]
        cols = st.columns(4)
        cols[0].metric("PostgreSQL", "Healthy")
        cols[1].metric("Fresh vehicles", f"{int(row.fresh_vehicle_rows):,}")
        cols[2].metric("Latest event", format_timestamp(row.latest_event_ts))
        cols[3].metric("S3", "Not probed")
    st.subheader("Pipeline runs")
    if runs.empty:
        st.info("No pipeline audit records are available.")
    else:
        st.dataframe(runs, width="stretch", hide_index=True)
    st.info(
        "Kafka and Spark health are verified by Docker Compose and Spark UI. "
        "S3 is intentionally not probed from the local dashboard; production "
        "deployment should use an IAM role and private network access."
    )


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
