"""
dashboard/app.py
================
Streamlit live surge pricing dashboard.

Shows real-time surge multipliers across all zones
for all 3 cities on a color-coded map.

Usage:
    streamlit run dashboard/app.py
"""

from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))



import time
from datetime import datetime

import pandas as pd
import streamlit as st
import requests
from loguru import logger

from config.settings import settings


# =============================================================================
# PAGE CONFIG
# =============================================================================

st.set_page_config(
    page_title="Surge Pricing Engine",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================================================================
# CONSTANTS
# =============================================================================

API_BASE = f"http://localhost:{settings.api.port}"
REFRESH_INTERVAL = 3  # seconds

SURGE_COLORS = {
    "normal":  "#2ECC71",   # green
    "low":     "#F1C40F",   # yellow
    "medium":  "#E67E22",   # orange
    "high":    "#E74C3C",   # red
    "maximum": "#8E44AD",   # purple
}


# =============================================================================
# DATA FETCHING
# =============================================================================

def fetch_all_surges() -> dict:
    """Fetch current surge data for all zones from API."""
    try:
        response = requests.get(f"{API_BASE}/surge/all", timeout=5)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"API fetch failed: {e}")
        return {"zones": {}, "total_zones": 0, "surging_zones": 0}


def fetch_health() -> dict:
    """Fetch API health status."""
    try:
        response = requests.get(f"{API_BASE}/health", timeout=5)
        return response.json()
    except Exception:
        return {"status": "unreachable"}


def fetch_zone_history(zone_id: str) -> list:
    """Fetch historical surge for a specific zone."""
    try:
        response = requests.get(
            f"{API_BASE}/surge/{zone_id}/history",
            params={"limit": 50},
            timeout=5,
        )
        return response.json().get("history", [])
    except Exception:
        return []


# =============================================================================
# SIDEBAR
# =============================================================================

def render_sidebar(health: dict) -> str:
    """Render sidebar with health status and controls."""
    st.sidebar.title("🚗 Surge Pricing Engine")
    st.sidebar.markdown("---")

    # Health status
    st.sidebar.subheader("System Status")
    status = health.get("status", "unknown")
    color = "🟢" if status == "healthy" else "🟡" if status == "degraded" else "🔴"
    st.sidebar.write(f"{color} API: **{status.upper()}**")

    if "kafka_connected" in health:
        st.sidebar.write(f"{'🟢' if health['kafka_connected'] else '🔴'} Kafka")
        st.sidebar.write(f"{'🟢' if health['cassandra_connected'] else '🔴'} Cassandra")
        st.sidebar.write(f"{'🟢' if health['redis_connected'] else '🔴'} Redis")
        st.sidebar.write(f"{'🟢' if health['ml_model_loaded'] else '🟡'} ML Model")

    st.sidebar.markdown("---")

    # City filter
    st.sidebar.subheader("Filters")
    selected_city = st.sidebar.selectbox(
        "City",
        ["All Cities", "new_york", "san_francisco", "chicago"],
    )

    st.sidebar.markdown("---")
    st.sidebar.write(f"🔄 Refreshing every {REFRESH_INTERVAL}s")
    st.sidebar.write(f"🕐 Last update: {datetime.now().strftime('%H:%M:%S')}")

    return selected_city


# =============================================================================
# METRICS ROW
# =============================================================================

def render_metrics(surge_data: dict, city_filter: str) -> None:
    """Render top-level metrics row."""
    zones = surge_data.get("zones", {})

    if city_filter != "All Cities":
        zones = {
            k: v for k, v in zones.items()
            if v.get("city") == city_filter
        }

    total = len(zones)
    surging = sum(1 for v in zones.values() if v.get("final_multiplier", 1.0) > 1.0)
    avg_surge = (
        sum(v.get("final_multiplier", 1.0) for v in zones.values()) / total
        if total > 0 else 1.0
    )
    max_surge = max(
        (v.get("final_multiplier", 1.0) for v in zones.values()),
        default=1.0,
    )

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Zones", total)
    with col2:
        st.metric("Surging Zones", surging)
    with col3:
        st.metric("Avg Surge", f"{avg_surge:.2f}x")
    with col4:
        st.metric("Max Surge", f"{max_surge:.2f}x")


# =============================================================================
# SURGE MAP TABLE
# =============================================================================

def render_surge_table(surge_data: dict, city_filter: str) -> None:
    """Render surge data as a sortable table."""
    zones = surge_data.get("zones", {})

    if city_filter != "All Cities":
        zones = {
            k: v for k, v in zones.items()
            if v.get("city") == city_filter
        }

    if not zones:
        st.warning("No surge data available. Is the pipeline running?")
        return

    rows = []
    for zone_id, data in zones.items():
        multiplier = data.get("final_multiplier", 1.0)
        tier = data.get("surge_tier", "normal")
        rows.append({
            "Zone ID": zone_id,
            "City": data.get("city", "unknown").replace("_", " ").title(),
            "Final Surge": f"{multiplier:.2f}x",
            "Rule-Based": f"{data.get('rule_multiplier', 1.0):.2f}x",
            "ML Predicted": f"{data.get('ml_multiplier', 1.0):.2f}x",
            "ML Confidence": f"{data.get('ml_confidence', 0.0):.0%}",
            "Tier": tier.upper(),
            "Updated": data.get("updated_at", "")[:19],
        })

    df = pd.DataFrame(rows)
    df = df.sort_values("Final Surge", ascending=False)

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
    )


# =============================================================================
# SURGE DISTRIBUTION CHART
# =============================================================================

def render_distribution_chart(surge_data: dict) -> None:
    """Render bar chart of surge tier distribution."""
    zones = surge_data.get("zones", {})
    if not zones:
        return

    tier_counts = {"normal": 0, "low": 0, "medium": 0, "high": 0, "maximum": 0}
    for data in zones.values():
        tier = data.get("surge_tier", "normal")
        tier_counts[tier] = tier_counts.get(tier, 0) + 1

    df = pd.DataFrame({
        "Tier": list(tier_counts.keys()),
        "Zones": list(tier_counts.values()),
    })

    st.bar_chart(df.set_index("Tier"))


# =============================================================================
# ZONE DETAIL
# =============================================================================

def render_zone_detail(surge_data: dict) -> None:
    """Render detailed view for a selected zone."""
    zones = surge_data.get("zones", {})
    if not zones:
        return

    zone_ids = list(zones.keys())
    selected_zone = st.selectbox("Select Zone for Detail View", zone_ids)

    if selected_zone:
        data = zones[selected_zone]
        col1, col2 = st.columns(2)

        with col1:
            st.subheader(f"Zone: {selected_zone}")
            st.write(f"**City:** {data.get('city', 'unknown').replace('_', ' ').title()}")
            st.write(f"**Final Surge:** {data.get('final_multiplier', 1.0):.2f}x")
            st.write(f"**Rule-Based:** {data.get('rule_multiplier', 1.0):.2f}x")
            st.write(f"**ML Predicted:** {data.get('ml_multiplier', 1.0):.2f}x")
            st.write(f"**ML Confidence:** {data.get('ml_confidence', 0.0):.0%}")
            st.write(f"**Tier:** {data.get('surge_tier', 'normal').upper()}")

        with col2:
            history = fetch_zone_history(selected_zone)
            if history:
                hist_df = pd.DataFrame(history)
                if "timestamp" in hist_df.columns:
                    hist_df["timestamp"] = pd.to_datetime(hist_df["timestamp"])
                    hist_df = hist_df.sort_values("timestamp")
                    st.subheader("Surge History")
                    st.line_chart(
                        hist_df.set_index("timestamp")[
                            ["final_multiplier", "rule_multiplier", "ml_multiplier"]
                        ]
                    )


# =============================================================================
# MAIN APP
# =============================================================================

def main() -> None:
    """Main Streamlit app entry point."""
    st.title("🚗 Real-Time Ride Surge Pricing Engine")
    st.markdown(
        "Live surge pricing dashboard powered by "
        "**Kafka → Spark → XGBoost → FastAPI**"
    )

    # Fetch data
    health = fetch_health()
    surge_data = fetch_all_surges()

    # Sidebar
    selected_city = render_sidebar(health)

    # Main content
    st.markdown("---")
    st.subheader("📊 Live Metrics")
    render_metrics(surge_data, selected_city)

    st.markdown("---")
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("🗺️ Surge by Zone")
        render_surge_table(surge_data, selected_city)

    with col2:
        st.subheader("📈 Tier Distribution")
        render_distribution_chart(surge_data)

    st.markdown("---")
    st.subheader("🔍 Zone Detail")
    render_zone_detail(surge_data)

    # Auto refresh
    time.sleep(REFRESH_INTERVAL)
    st.rerun()


if __name__ == "__main__":
    main()