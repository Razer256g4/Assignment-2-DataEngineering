

from __future__ import annotations

import json
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import folium
import numpy as np
import pandas as pd
import streamlit as st
from streamlit_folium import st_folium

try:
    import paho.mqtt.client as mqtt
except Exception as e:
    mqtt = None

# ----------------------------
# Page config
# ----------------------------
st.set_page_config(
    page_title="NEM Live Map — Power & Emissions",
    page_icon="⚡",
    layout="wide",
)

# ----------------------------
# Data structures & app state
# ----------------------------
@dataclass
class AppState:
    lock: threading.RLock = field(default_factory=threading.RLock)
    latest_by_facility: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    history: deque = field(default_factory=lambda: deque(maxlen=5000))
    facility_lookup: Optional[pd.DataFrame] = None  # indexed by facility_id
    connected: bool = False
    client: Optional[Any] = None
    topic: str = ""
    price_demand: dict = field(default_factory=dict)


@st.cache_resource(show_spinner=False)
def get_state() -> AppState:
    state = AppState()
    lk = pd.read_csv("fac.csv")
    lk['fuel_tech'] = lk['fuel_tech'].apply(json.loads)
    state.facility_lookup = lk.set_index("facility_id")
    return state


# ----------------------------
# MQTT glue
# ----------------------------

def _normalize_payload(msg: Dict[str, Any]) -> Dict[str, Any]:
    """Tolerant schema normalizer for incoming MQTT JSON payloads.

    Expected fields (preferred names):
    - facility_id (str)
    - facility_name (str)
    - lat (float), lon (float)
    - region (str) e.g., QLD, NSW, VIC, SA, TAS
    - fuel_tech (list[str]) e.g., Coal, Gas, Hydro, Wind, Solar, Battery
    - timestamp (ISO8601 string)
    - power_mw (float)
    - co2_tonnes (float)
    """
    d = {**msg}

    # Common aliases
    aliases = {
        "facilityId": "facility_id",
        "name": "facility_name",
        "lng": "lon",
        "longitude": "lon",
        "latitude": "lat",
        "fuel": "fuel_tech",
        "fuel_type": "fuel_tech",
        "co2": "co2_tonnes",
        "emissions_t": "co2_tonnes",
        "emissions": "co2_tonnes",
        "power": "power_mw",
        "power_mwh": "power_mw",  # if a publisher sends MWh per 5-min, convert if needed upstream
    }
    for k, v in list(d.items()):
        if k in aliases:
            d[aliases[k]] = v

    # Coerce types where possible
    for f in ["lat", "lon", "power_mw", "co2_tonnes"]:
        if f in d and d[f] is not None:
            try:
                d[f] = float(d[f])
            except Exception:
                d[f] = None

    # Strip whitespace in categoricals
    for f in ["facility_id", "facility_name", "region", "fuel_tech"]:
        if f in d and isinstance(d[f], str):
            d[f] = d[f].strip()

    return d


def _on_connect(client, userdata: AppState, flags, rc, properties=None):
    st.toast(f"MQTT connected (rc={rc}) | subscribing to {userdata.topic}")
    client.subscribe(userdata.topic, qos=1)
    userdata.connected = True

def _on_subscribe(client, userdata, mid, reason_codes, properties):
    print(f"Subscribed successfully!")

def _on_message(client, userdata: AppState, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except Exception as e:
        print("paylod decode error: ", e)
        return

    if "price_dmwh" in payload:
        with userdata.lock:
            userdata.price_demand = payload
            print("Price Demand Updated")

    elif payload.get("facility_id"):
        d = _normalize_payload(payload) 
        # print(d)

        # Enrich with lookup if lat/lon missing
        if userdata.facility_lookup is not None:
            fid = d.get("facility_id")
            if fid and (pd.isna(d.get("lat")) or pd.isna(d.get("lon")) or d.get("lat") is None or d.get("lon") is None):
                try:
                    row = userdata.facility_lookup.loc[fid]
                    # print(row)
                    d.setdefault("facility_name", row.get("facility_name"))
                    d.setdefault("region", row.get("region"))
                    d.setdefault("fuel_tech", row.get("fuel_tech"))
                    d.setdefault("lat", float(row.get("lat")))
                    d.setdefault("lon", float(row.get("lon")))
                except Exception as e:
                    print("Exception encountered: ", e)
                    pass

        fid = d.get("facility_id")
        if not fid:
            return

        with userdata.lock:
            # Keep latest per facility
            d.pop('facility_id', None)
            prev = userdata.latest_by_facility.get(fid, {})
            userdata.latest_by_facility[fid] = {**prev, **d}
            # Append to short history for optional charts/debug
            userdata.history.append({**d, "_ts": time.time()})

        print("Check latest_by_facility len: ", len(userdata.latest_by_facility))

@st.cache_resource(show_spinner=False)
def start_mqtt_client(broker: str, port: int, topic: str, username: str | None, password: str | None) -> AppState:
    state = get_state()
    state.topic = topic

    if mqtt is None:
        st.error("paho-mqtt is not installed. Add it to requirements.txt and reinstall.")
        return state

    # Avoid duplicate clients if user edits settings repeatedly
    if state.client is not None:
        try:
            state.client.loop_stop()
            state.client.disconnect()
        except Exception:
            pass
        state.client = None
        state.connected = False

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if username:
        client.username_pw_set(username, password)

    client.user_data_set(state)
    client.on_connect = _on_connect
    client.on_message = _on_message
    client.on_subscribe = _on_subscribe

    try:
        client.connect(broker, port, keepalive=60)
        client.loop_start()
        state.client = client
    except Exception as e:
        st.error(f"Failed to connect to MQTT broker at {broker}:{port} — {e}")

    return state


# ----------------------------
# Helpers for UI/rendering
# ----------------------------

def dataframe_from_state(state: AppState) -> pd.DataFrame:
    with state.lock:
        if not state.latest_by_facility:
            return pd.DataFrame(columns=[
                "facility_id",
                "facility_name",
                "region",
                "fuel_tech",
                "lat",
                "lon",
                "timestamp",
                "power_mw",
                "co2_tonnes",
            ])
        df = pd.DataFrame.from_dict(state.latest_by_facility, orient="index").reset_index(names=["facility_id"])  # type: ignore

    # Ensure consistent columns
    for c in ["facility_name", "region", "fuel_tech", "timestamp"]:
        if c not in df.columns:
            df[c] = None
    for c in ["lat", "lon", "power_mw", "co2_tonnes"]:
        if c not in df.columns:
            df[c] = np.nan

    # Drop entries without coordinates
    df = df.dropna(subset=["lat", "lon"], how="any")
    return df


FUEL_COLORS = {
    "Coal": "#3e3e3e",
    "Black coal": "#3e3e3e",
    "Brown coal": "#6b4226",
    "Gas": "#d97706",
    "Hydro": "#2563eb",
    "Wind": "#16a34a",
    "Solar": "#f59e0b",
    "Battery": "#7c3aed",
    "Bioenergy": "#15803d",
    "Diesel": "#ef4444",
}


def make_map(df: pd.DataFrame, metric: str) -> folium.Map:
    # Fallback center: Australia
    center = (-35.0, 147.0)
    if not df.empty:
        center = (float(df["lat"].mean()), float(df["lon"].mean()))

    fmap = folium.Map(location=center, zoom_start=6, tiles="cartodbpositron")

    if df.empty:
        return fmap

    # Scale marker radius by metric (p95 as reference)
    vals = df[metric].fillna(0).astype(float)
    ref = max(1e-6, float(np.percentile(vals[vals > 0], 95)) if (vals > 0).any() else 1.0)

    for _, r in df.iterrows():
        val = float(r.get(metric, 0.0) or 0.0)
        val_ratio = np.clip(val / ref if ref != 0 else 0, 0.0, 1.0)
        radius = 4.0 + 10.0 * np.sqrt(val_ratio)
        # radius = 4.0 + 10.0 * np.sqrt(min(val / ref, 1.0))

        # fuel = (r.get("fuel_tech") or "").strip()
        fuel = (r.get("fuel_tech")[0] or "").strip()
        color = FUEL_COLORS.get(fuel, "#64748b")

        name = r.get("facility_name") or r.get("facility_id")
        region = r.get("region") or "—"
        ts = r.get("timestamp") or "—"
        other_metric = "co2_tonnes" if metric == "power_mw" else "power_mw"
        other_val = r.get(other_metric)

        popup_html = f"""
        <div style='font-family:Inter,system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif;min-width:220px'>
          <div style='font-weight:700;margin-bottom:4px'>{name}</div>
          <div style='font-size:12px;color:#475569;margin-bottom:6px'>Region: {region} • Fuel: {fuel or '—'}</div>
          <div style='font-size:13px'><b>{'Power (MW)' if metric=='power_mw' else 'CO₂ (t)'}</b>: {val:,.3f}</div>
          <div style='font-size:13px'><b>{'CO₂ (t)' if metric=='power_mw' else 'Power (MW)'}</b>: {other_val if other_val is not None else '—'}</div>
          <div style='font-size:12px;color:#64748b;margin-top:6px'>Latest at: {ts}</div>
        </div>
        """

        label = f"{name}<br>{val}{'MW' if metric == 'power_mw' else 't'}"
        if show_labels:
            folium.Marker(
                location=(float(r["lat"]), float(r["lon"])),
                icon=folium.DivIcon(html=f"""
                    <div style="
                        display: inline-block;
                        font-size:14px;
                        color:{color};
                        background:white;
                        border-radius:4px;
                        padding:4px 8px;
                        white-space:nowrap;
                        box-shadow:0 1px 4px #0002
                        margin-bottom: 28px; 
                        text-align:center;
                        transform:translate(-100%, -100%);
                        position:relative;
                    ">{label}</div>""")
            ).add_to(fmap)

        folium.CircleMarker(
            location=(float(r["lat"]), float(r["lon"])),
            radius=radius,
            color=color,
            weight=1,
            fill=True,
            fill_opacity=0.7,
            popup=folium.Popup(popup_html, max_width=320),
        ).add_to(fmap)

    folium.LayerControl().add_to(fmap)
    return fmap


# ----------------------------
# Sidebar controls
# ----------------------------
with st.sidebar:
    st.markdown("### Connection")
    broker = st.text_input("MQTT broker", value="test.mosquitto.org")
    port = st.number_input("Port", value=1883, min_value=1, max_value=65535, step=1)
    topic = st.text_input("Topic", value="nem/power_emissions")

    col_a, col_b = st.columns(2)
    with col_a:
        username = st.text_input("Username", value="", placeholder="optional")
    with col_b:
        password = st.text_input("Password", value="", type="password", placeholder="optional")

    connect = st.button("Connect / Reconnect", width='stretch')

    st.markdown("---")
    show_labels = st.checkbox("Show marker labels", value=False)
    st.markdown("### Data & Filters")
    metric = st.radio("Bubble metric", ["power_mw", "co2_tonnes"], index=0, horizontal=True)
    live = st.checkbox("Live refresh", value=True)
    refresh_sec = st.slider("Refresh every (s)", min_value=1, max_value=10, value=3)

    st.markdown("#### Facility lookup (optional)")
    up = st.file_uploader("Upload facility_lookup.csv", type=["csv"], help="Columns: facility_id, facility_name, region, fuel_tech, lat, lon")

# Start / restart client if requested
if connect:
    state = start_mqtt_client(broker, int(port), topic, username or None, password or None)
else:
    state = get_state()

# Attach uploaded lookup once; update state if provided
if up is not None:
    try:
        df_lookup = pd.read_csv(up)
        df_lookup['fuel_tech'] = df_lookup['fuel_tech'].apply(json.loads)
        # print(df_lookup['fuel_tech'].head())
        if "facility_id" not in df_lookup.columns:
            st.warning("Missing 'facility_id' column in lookup CSV. Ignored.")
        else:
            state.facility_lookup = df_lookup.set_index("facility_id")
            st.success(f"Loaded lookup: {len(df_lookup):,} rows")
    except Exception as e:
        st.warning(f"Failed to read lookup CSV — {e}")

# ----------------------------
# Main area
# ----------------------------
st.title("⚡ NEM Live Map — Power & Emissions")
sub = st.container()

with sub:
    if not state.connected:
        st.info("Not connected yet. Set broker/port/topic in the sidebar and click **Connect / Reconnect**.")

    df = dataframe_from_state(state)

    # Optional selectors only when we have data
    regions = sorted([r for r in df["region"].dropna().unique()]) if not df.empty else []
    # fuels = sorted([f for f in df["fuel_tech"].dropna().unique()]) if not df.empty else []
    fuels: list = sorted(set(f for sublist in df["fuel_tech"] for f in sublist)) if not df.empty else []
    # print(df['fuel_tech'].head())

    c1, c2, c3, c4 = st.columns([2, 2, 3, 3])
    with c1:
        sel_regions = st.multiselect("Region", options=regions, default=regions)
    with c2:
        sel_fuels = st.multiselect("Fuel", options=fuels, default=fuels)
    with c3:
        st.caption("rs (filtered)")
        fdf = df.copy()
        if sel_regions:
            fdf = fdf[fdf["region"].isin(sel_regions)]
        if sel_fuels:
            fdf = fdf[fdf["fuel_tech"].isin(sel_fuels)]
        total_power = float(fdf["power_mw"].fillna(0).sum()) if not fdf.empty else 0.0
        total_co2 = float(fdf["co2_tonnes"].fillna(0).sum()) if not fdf.empty else 0.0
        m1, m2= st.columns(2)
        m1.metric("Total Power (MW)", f"{total_power:,.1f}")
        m2.metric("Total CO₂ (t)", f"{total_co2:,.1f}")
    with c4:
        
        st.caption(f"NEM Market:  {state.price_demand.get('timestamp', 'pending data...')}")
        m3, m4 = st.columns(2)
        m3.metric("Price ($/MWh)", f"{state.price_demand.get('price_dmwh', 0):,.1f}")
        m4.metric("Demand (MW)", f"{state.price_demand.get('demand_mw', 0):,.1f}")

    # Apply filters for map render
    if not df.empty:
        df = df[df["region"].isin(sel_regions)] if sel_regions else df
        # df = df[df["fuel_tech"].isin(sel_fuels)] if sel_fuels else df
        df = df[df["fuel_tech"].apply(lambda fuels: any(f in sel_fuels for f in fuels))] if sel_fuels else df

    st.markdown(":earth_asia: **Live map**")
    
    fmap = make_map(df, metric)
    st_folium(fmap, width=None, height=640)
    with st.expander("Latest readings (table)"):
        if not df.empty:
            show_cols = [
                "facility_id",
                "facility_name",
                "region",
                "fuel_tech",
                "timestamp",
                "power_mw",
                "co2_tonnes",
                "lat",
                "lon",
            ]
            st.dataframe(df[show_cols].sort_values("facility_id"), width='stretch')
        else:
            st.caption("Waiting for messages…")

# Auto-refresh to pull in new MQTT updates pushed into cached state
if live:
    time.sleep(int(refresh_sec))
    try:
        # Streamlit >= 1.30
        st.rerun()
    except Exception:
        # Older versions
        st.experimental_rerun()
