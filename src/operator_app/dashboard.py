"""
CNC Operator Dashboard – MQTT-first with fallback
================================================
Prefers live data from the Mission Coordinator over MQTT.
Falls back to the original simulation when MQTT is unavailable.
"""

import streamlit as st
import folium
from streamlit_folium import st_folium
from pathlib import Path
import sys
import time
import uuid
import json
from datetime import datetime

sys.path.append(str(Path(__file__).parent.parent.parent))

from simulations.simple_assets import create_demo_fleet
from src.communication.mqtt_client import MQTTClient
from src.data_services.event_store import event_store, EventType
from src.data_services.db import db
from src.asset_integration.models import AssetStatus
from src.command_control.mission_manager import mission_manager, MissionStatus

st.set_page_config(
    page_title="Unified Mission Autonomy Platform | RCI-DRDO",
    page_icon="🛰️",
    layout="wide"
)

# ---------- Header ----------
col_logo, col_title = st.columns([1, 5])
with col_logo:
    logo_path = Path("assets/drdologo1.webp")
    if logo_path.exists():
        st.image(str(logo_path), width=160)
    else:
        st.markdown("### 🇮🇳")
with col_title:
    st.markdown("""
    <h2 style='margin-bottom:0; padding-top:12px;'>RCI - DRDO</h2>
    <h3 style='margin-top:0; color:#1f4e79;'>Unified Mission Autonomy Platform</h3>
    """, unsafe_allow_html=True)
st.caption("Operator Dashboard | Heterogeneous Asset Command & Control")
st.divider()

# ---------- Session State ----------
if "fleet" not in st.session_state:
    st.session_state.fleet = create_demo_fleet()
    event_store.add(EventType.INFO, "System started – 2 UAVs + 2 UGVs online")

if "mqtt" not in st.session_state:
    st.session_state.mqtt = None
    st.session_state.mqtt_ok = False
    st.session_state.mqtt_telemetry = {}      # asset_id → latest dict
    st.session_state.mqtt_mission = None      # latest mission/status

    try:
        client = MQTTClient(client_id="cnc-dashboard")
        client.connect()

        def on_telemetry(topic, payload):
            try:
                parts = topic.split("/")
                if len(parts) >= 2:
                    asset_id = parts[1]
                    st.session_state.mqtt_telemetry[asset_id] = payload
            except Exception:
                pass

        def on_mission(topic, payload):
            st.session_state.mqtt_mission = payload

        client.subscribe("assets/+/telemetry", on_telemetry)
        client.subscribe("mission/status", on_mission)

        st.session_state.mqtt = client
        st.session_state.mqtt_ok = True
    except Exception as e:
        print(f"[Dashboard] MQTT init failed: {e}")
        st.session_state.mqtt = None
        st.session_state.mqtt_ok = False

if "auto_refresh" not in st.session_state:
    st.session_state.auto_refresh = True

fleet = st.session_state.fleet
mqtt = st.session_state.mqtt
mqtt_telem = st.session_state.get("mqtt_telemetry", {})
mqtt_mission = st.session_state.get("mqtt_mission")
using_mqtt = st.session_state.mqtt_ok and len(mqtt_telem) > 0

# ---------- Update assets (MQTT-first) ----------
if using_mqtt:
    # Live data from Coordinator
    for asset_id, payload in mqtt_telem.items():
        # Map Coordinator IDs (UAV-01) to existing fleet keys if needed
        # For now we try direct match and also common variants
        candidates = [asset_id, asset_id.lower(), asset_id.replace("-", "").lower()]
        matched = None
        for key in fleet.keys():
            if key in candidates or key.lower() in candidates:
                matched = key
                break

        if matched:
            asset = fleet[matched]
            # Update position
            if hasattr(asset, "position"):
                asset.position.lat = float(payload.get("lat", asset.position.lat))
                asset.position.lon = float(payload.get("lon", asset.position.lon))
                asset.position.alt = float(payload.get("alt", getattr(asset.position, "alt", 0)))
            # Keep a simple trail
            if hasattr(asset, "trail"):
                asset.trail.append((asset.position.lat, asset.position.lon))
                if len(asset.trail) > 40:
                    asset.trail = asset.trail[-40:]
else:
    # Fallback to original simulation
    for asset in fleet.values():
        prev = getattr(asset, "status", None)
        if hasattr(asset, "update"):
            asset.update(dt=2.0)
        t = asset.get_telemetry()
        try:
            db.insert_telemetry(
                asset_id=t.asset_id,
                timestamp=t.timestamp,
                lat=t.position.lat,
                lon=t.position.lon,
                alt=t.position.alt,
                heading=getattr(t.position, "heading", 0),
                speed=t.speed,
                battery=t.battery_percent,
                status=t.status.value if hasattr(t.status, "value") else str(t.status),
                mode=getattr(t, "mode", "unknown")
            )
        except Exception:
            pass

# ---------- Mission status from Coordinator ----------
st.subheader("Fleet Summary")
total = len(fleet)
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Assets", total)
c2.metric("MQTT Source", "Live Coordinator" if using_mqtt else "Local Simulation")
c3.metric("MQTT Status", "Connected" if st.session_state.mqtt_ok else "Offline")
c4.metric("Live Telemetry Assets", len(mqtt_telem))

# ---------- Mission Control ----------
st.divider()
st.subheader("Mission Control")

if mqtt_mission:
    st.success("Receiving live mission data from Coordinator")
    tasks = mqtt_mission.get("tasks", {})
    cols = st.columns(min(4, max(1, len(tasks))))
    for idx, (tid, tinfo) in enumerate(tasks.items()):
        with cols[idx % len(cols)]:
            status = tinfo.get("status", "unknown").upper()
            st.metric(tid, status)
            st.caption(tinfo.get("description", ""))
else:
    # Fallback to original mission manager
    mission = mission_manager.check_progress(fleet)
    mcol1, mcol2 = st.columns([3, 1])
    with mcol1:
        if mission and mission.status != MissionStatus.IDLE:
            st.info(f"**Current Mission:** {mission.name} | Status: `{mission.status.value.upper()}`")
        else:
            st.caption("No active mission from Coordinator or MissionManager")
    with mcol2:
        if st.button("▶ Start Reconnaissance Mission", type="primary", use_container_width=True):
            mission_manager.start_reconnaissance(fleet)
            st.rerun()

# ---------- Map ----------
st.divider()
col_map, col_side = st.columns([2.3, 1])

with col_map:
    st.subheader("Common Operational Picture")
    m = folium.Map(location=[17.3850, 78.4867], zoom_start=13, tiles="CartoDB positron")

    for asset in fleet.values():
        try:
            t = asset.get_telemetry()
            lat = t.position.lat
            lon = t.position.lon
            name = getattr(asset.info, "name", str(asset))
            atype = getattr(asset.info, "asset_type", None)
            color = "blue" if atype and atype.value == "uav" else "green"
            icon = "plane" if atype and atype.value == "uav" else "truck"

            if hasattr(asset, "trail") and len(asset.trail) > 1:
                folium.PolyLine(asset.trail, color=color, weight=3, opacity=0.7).add_to(m)

            folium.Marker(
                [lat, lon],
                popup=f"<b>{name}</b><br>{getattr(t.status, 'value', t.status)}",
                tooltip=name,
                icon=folium.Icon(color=color, icon=icon, prefix="fa"),
            ).add_to(m)
        except Exception:
            continue

    st_folium(m, width=980, height=520, returned_objects=[])

with col_side:
    st.subheader("Source & Controls")
    if using_mqtt:
        st.success("Live data from Coordinator (MQTT)")
    else:
        st.warning("Using local simulation (MQTT offline or no data)")

    st.session_state.auto_refresh = st.toggle("Auto Refresh", value=st.session_state.auto_refresh)

    st.divider()
    st.caption("Asset list")
    for key, asset in fleet.items():
        try:
            t = asset.get_telemetry()
            st.write(f"**{getattr(asset.info, 'name', key)}** — {getattr(t.status, 'value', t.status)}")
        except Exception:
            st.write(key)

# ---------- Event History ----------
st.divider()
st.subheader("Event History")
for e in event_store.get_recent(12):
    icon = {"info": "ℹ️", "warning": "⚠️", "critical": "🔴"}.get(e.severity, "ℹ️")
    st.markdown(f"{icon} `{e.timestamp.strftime('%H:%M:%S')}` {e.message}")

if st.session_state.auto_refresh:
    time.sleep(2.0)
    st.rerun()
    