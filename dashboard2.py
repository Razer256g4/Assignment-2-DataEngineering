# dashboard_streamlit.py — smooth (no-flicker) map using pydeck
# -------------------------------------------------------------
# - Demo mode: generates dummy facility updates + market stats (no MQTT needed)
# - MQTT mode: subscribe to facility + optional market topics
# - Map: pydeck ScatterplotLayer (no iframe), smooth updates on reruns
# - Filters, totals, market panel; accepts facility_id or facility_code
# -------------------------------------------------------------

from __future__ import annotations

import json
import math
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List

import numpy as np
import pandas as pd
import streamlit as st
import pydeck as pdk

try:
    import paho.mqtt.client as mqtt
except Exception:
    mqtt = None

# ----------------------------
# Page config
# ----------------------------
st.set_page_config(page_title="NEM Live Map — Power & Emissions", page_icon="⚡", layout="wide")

# ----------------------------
# Dummy facilities (one per NEM region)
# ----------------------------
DUMMY_FACILITIES: List[Dict[str, Any]] = [
    {"facility_id": "QLD_SOLAR_01", "facility_name": "Sunvale Solar Farm", "region": "QLD", "fuel_tech": "Solar", "lat": -27.4705, "lon": 153.0260},
    {"facility_id": "NSW_COAL_01",  "facility_name": "Ironbark Coal",      "region": "NSW", "fuel_tech": "Coal",  "lat": -33.8688, "lon": 151.2093},
    {"facility_id": "VIC_WIND_01",  "facility_name": "Bass Strait Wind",    "region": "VIC", "fuel_tech": "Wind",  "lat": -37.8136, "lon": 144.9631},
    {"facility_id": "SA_GAS_01",    "facility_name": "Sturt Gas Turbine",   "region": "SA",  "fuel_tech": "Gas",   "lat": -34.9285, "lon": 138.6007},
    {"facility_id": "TAS_HYDRO_01", "facility_name": "Derwent Hydro",       "region": "TAS", "fuel_tech": "Hydro", "lat": -42.8821, "lon": 147.3272},
]

EMISSION_FACTOR_T_PER_MWH = {"Coal": 0.9, "Gas": 0.4, "Hydro": 0.02, "Wind": 0.0, "Solar": 0.0, "Battery": 0.1}

# ----------------------------
# App state
# ----------------------------
@dataclass
class AppState:
    lock: threading.RLock = field(default_factory=threading.RLock)
    latest_by_facility: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    history: deque = field(default_factory=lambda: deque(maxlen=5000))
    facility_lookup: Optional[pd.DataFrame] = None  # indexed by facility_id
    # MQTT
    connected: bool = False
    client: Optional[Any] = None
    topic: str = ""
    market_topic: str = ""
    market_by_region: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    # Demo
    demo_running: bool = False
    demo_thread: Optional[threading.Thread] = None
    demo_interval: float = 0.1

@st.cache_resource(show_spinner=False)
def get_state() -> AppState:
    state = AppState()
    df = pd.DataFrame(DUMMY_FACILITIES)
    state.facility_lookup = df.set_index("facility_id")
    return state

# ----------------------------
# MQTT glue (optional)
# ----------------------------
def _normalize_payload(msg: Dict[str, Any]) -> Dict[str, Any]:
    d = {**msg}
    aliases = {
        "facilityId":"facility_id","name":"facility_name","lng":"lon","longitude":"lon","latitude":"lat",
        "fuel":"fuel_tech","fuel_type":"fuel_tech","co2":"co2_tonnes","emissions_t":"co2_tonnes","emissions":"co2_tonnes",
        "power":"power_mw","power_mwh":"power_mw","facility_code":"facility_id"
    }
    for k, v in list(d.items()):
        if k in aliases: d[aliases[k]] = v
    for f in ["lat","lon","power_mw","co2_tonnes"]:
        if f in d and d[f] is not None:
            try: d[f] = float(d[f])
            except Exception: d[f] = None
    for f in ["facility_id","facility_name","region","fuel_tech"]:
        if f in d and isinstance(d[f], str): d[f] = d[f].strip()
    return d

def _apply_message_to_state(d: Dict[str, Any], state: AppState) -> None:
    if state.facility_lookup is not None:
        fid = d.get("facility_id")
        if fid and (pd.isna(d.get("lat")) or pd.isna(d.get("lon")) or d.get("lat") is None or d.get("lon") is None):
            try:
                row = state.facility_lookup.loc[fid]
                d.setdefault("facility_name", row.get("facility_name"))
                d.setdefault("region", row.get("region"))
                d.setdefault("fuel_tech", row.get("fuel_tech"))
                d.setdefault("lat", float(row.get("lat")))
                d.setdefault("lon", float(row.get("lon")))
            except Exception:
                pass
    fid = d.get("facility_id")
    if not fid: return
    with state.lock:
        prev = state.latest_by_facility.get(fid, {})
        state.latest_by_facility[fid] = {**prev, **d}
        state.history.append({**d, "_ts": time.time()})

def _on_connect(client, userdata: AppState, flags, rc, properties=None):
    topics = [(userdata.topic, 1)]
    if userdata.market_topic: topics.append((userdata.market_topic, 1))
    st.toast(f"MQTT rc={rc} | subscribing to {', '.join(t for t,_ in topics)}")
    client.subscribe(topics)
    userdata.connected = True

def _on_message(client, userdata: AppState, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except Exception:
        return
    if userdata.market_topic and msg.topic == userdata.market_topic:
        reg = (payload.get("region") or "").strip()
        if reg:
            with userdata.lock:
                userdata.market_by_region[reg] = {
                    "price": float(payload.get("price") or 0.0),
                    "demand": float(payload.get("demand") or 0.0),
                    "timestamp": payload.get("timestamp"),
                }
        return
    d = _normalize_payload(payload)
    _apply_message_to_state(d, userdata)

@st.cache_resource(show_spinner=False)
def start_mqtt_client(broker: str, port: int, topic: str, market_topic: str, username: str | None, password: str | None) -> AppState:
    state = get_state()
    state.topic = topic
    state.market_topic = market_topic
    if mqtt is None:
        st.error("paho-mqtt not installed.")
        return state
    if state.client is not None:
        try:
            state.client.loop_stop(); state.client.disconnect()
        except Exception: pass
        state.client = None; state.connected = False
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if username: client.username_pw_set(username, password)
    client.user_data_set(state)
    client.on_connect = _on_connect
    client.on_message = _on_message
    try:
        client.connect(broker, port, keepalive=60)
        client.loop_start()
        state.client = client
    except Exception as e:
        st.error(f"MQTT connect failed: {e}")
    return state

# ----------------------------
# Demo generator (facilities + market)
# ----------------------------
def _baseline_for_fuel(fuel: str) -> float:
    return {"Coal":900.0, "Gas":400.0, "Wind":200.0, "Solar":120.0, "Hydro":300.0, "Battery":60.0}.get(fuel, 150.0)

def _demo_loop(state: AppState):
    bases = {f["facility_id"]:_baseline_for_fuel(f["fuel_tech"]) for f in DUMMY_FACILITIES}
    t0 = time.time()
    while state.demo_running:
        now_iso = pd.Timestamp.now(tz="Australia/Sydney").isoformat(timespec="seconds")
        t = time.time() - t0

        # facilities
        for f in DUMMY_FACILITIES:
            fid = f["facility_id"]; base = bases[fid]; fuel = f["fuel_tech"]
            swing = 0.2 if fuel in {"Wind","Solar"} else 0.1
            power_mw = max(0.0, base*(1 + swing*math.sin(t/20)) + np.random.uniform(-0.05, 0.05)*base)
            ef = EMISSION_FACTOR_T_PER_MWH.get(fuel, 0.0)
            co2_t = ef * (power_mw/12.0)
            _apply_message_to_state({
                "facility_id": fid, "facility_name": f["facility_name"], "region": f["region"], "fuel_tech": fuel,
                "lat": f["lat"], "lon": f["lon"], "timestamp": now_iso,
                "power_mw": round(float(power_mw),3), "co2_tonnes": round(float(co2_t),3),
            }, state)
            time.sleep(max(0.01, state.demo_interval))

        # market
        regions = {f["region"] for f in DUMMY_FACILITIES}
        for r in regions:
            base_price = {"NSW":95,"VIC":90,"QLD":105,"SA":110,"TAS":85}.get(r, 90)
            base_demand = {"NSW":8000,"VIC":6500,"QLD":7000,"SA":1500,"TAS":1000}.get(r, 2000)
            price = max(0.0, base_price*(1 + 0.05*np.sin(t/10)) + np.random.uniform(-3,3))
            demand = max(0.0, base_demand*(1 + 0.03*np.sin(t/15)) + np.random.uniform(-100,100))
            with state.lock:
                state.market_by_region[r] = {"price": round(float(price),1), "demand": round(float(demand),0), "timestamp": now_iso}
        time.sleep(0.4)

def start_demo(state: AppState, interval: float = 0.1):
    if state.client is not None:
        try: state.client.loop_stop(); state.client.disconnect()
        except Exception: pass
        state.client = None; state.connected = False
    state.demo_interval = interval
    if state.demo_thread and state.demo_thread.is_alive():
        state.demo_running = False
        try: state.demo_thread.join(timeout=0.5)
        except Exception: pass
    state.demo_running = True
    th = threading.Thread(target=_demo_loop, args=(state,), daemon=True)
    state.demo_thread = th; th.start()

def stop_demo(state: AppState):
    state.demo_running = False
    if state.demo_thread and state.demo_thread.is_alive():
        try: state.demo_thread.join(timeout=0.5)
        except Exception: pass

# ----------------------------
# Dataframe helper
# ----------------------------
def dataframe_from_state(state: AppState) -> pd.DataFrame:
    with state.lock:
        if not state.latest_by_facility:
            return pd.DataFrame(columns=["facility_id","facility_name","region","fuel_tech","lat","lon","timestamp","power_mw","co2_tonnes"])
        tmp = pd.DataFrame.from_dict(state.latest_by_facility, orient="index")
        if "facility_id" in tmp.columns:
            tmp.index.name = "_idx"; df = tmp.reset_index().drop(columns=["_idx"])
        else:
            tmp.index.name = "facility_id"; df = tmp.reset_index()
    for c in ["facility_name","region","fuel_tech","timestamp"]:
        if c not in df.columns: df[c] = None
    for c in ["lat","lon","power_mw","co2_tonnes"]:
        if c not in df.columns: df[c] = np.nan
    return df.dropna(subset=["lat","lon"], how="any")

# ----------------------------
# Map (pydeck)
# ----------------------------
COLOR = {
    "Coal":[62,62,62], "Gas":[217,119,6], "Hydro":[37,99,235],
    "Wind":[22,163,74], "Solar":[245,158,11], "Battery":[124,58,237],
    "Bioenergy":[21,128,61], "Diesel":[239,68,68]
}

def deck_from_df(df: pd.DataFrame, metric: str) -> pdk.Deck:
    if df.empty:
        view = pdk.ViewState(latitude=-25.0, longitude=133.0, zoom=4)
        return pdk.Deck(layers=[], initial_view_state=view, map_style="carto-positron")

    # ensure numeric metric
    m = pd.to_numeric(df[metric], errors="coerce").fillna(0.0)
    # robust p95
    pos = m[m > 0]
    p95 = float(np.percentile(pos, 95)) if len(pos) else 1.0

    data = df.copy()

    # bubble radius (meters)
    data["_radius"] = 1000.0 * np.sqrt(np.clip(m / p95, 0.0, 1.0)) + 1000.0

    # per-row RGB list — avoid fillna(list) which errors
    def _color_for(fuel):
        key = str(fuel).strip() if pd.notna(fuel) else ""
        return COLOR.get(key, [100, 100, 100])  # default grey

    data["_color"] = data["fuel_tech"].apply(_color_for)

    # sanity for coords
    data["lat"] = pd.to_numeric(data["lat"], errors="coerce")
    data["lon"] = pd.to_numeric(data["lon"], errors="coerce")
    data = data.dropna(subset=["lat", "lon"])

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=data,
        get_position='[lon, lat]',
        get_radius='_radius',
        get_fill_color='_color',
        pickable=True,
        radius_min_pixels=3,
        radius_max_pixels=60,
        auto_highlight=True,
    )

    view = pdk.ViewState(
        latitude=float(data["lat"].mean()),
        longitude=float(data["lon"].mean()),
        zoom=4,
    )
    tooltip = {"text": "{facility_name}\n{region} • {fuel_tech}\nPower: {power_mw} MW\nCO₂: {co2_tonnes} t\n{timestamp}"}

    # Use Carto basemap (works without a key)
    deck_kwargs = {}
    
    deck_kwargs["map_style"] = "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"

    return pdk.Deck(
        layers=[layer],
        initial_view_state=view,
        tooltip=tooltip,
        **deck_kwargs,
    )

# ----------------------------
# Sidebar
# ----------------------------
with st.sidebar:
    st.markdown("### Demo or MQTT")
    demo_mode = st.toggle("Use demo data (no MQTT)", value=True)
    demo_interval = st.slider("Demo interval (s)", 0.05, 1.0, 0.10, 0.05)

    st.markdown("---")
    st.markdown("### MQTT connection")
    broker = st.text_input("Broker", value="localhost")
    port = st.number_input("Port", value=1883, min_value=1, max_value=65535, step=1)
    topic = st.text_input("Facility topic", value="nem/power_emissions")
    market_topic = st.text_input("Market topic (optional)", value="nem/market")
    col_a, col_b = st.columns(2)
    with col_a:
        username = st.text_input("Username", value="", placeholder="optional")
    with col_b:
        password = st.text_input("Password", value="", type="password", placeholder="optional")
    connect = st.button("Connect / Reconnect", use_container_width=True, disabled=demo_mode)

    st.markdown("---")
    st.markdown("### Data & Filters")
    metric = st.radio("Bubble metric", ["power_mw", "co2_tonnes"], index=0, horizontal=True)
    live = st.checkbox("Live refresh", value=True)
    refresh_sec = st.slider("Refresh every (s)", min_value=1, max_value=10, value=2)

    st.markdown("#### Facility lookup (optional)")
    up = st.file_uploader(
        "Upload facility_lookup.csv",
        type=["csv"],
        help="Columns: facility_id (or facility_code), facility_name, region, fuel_tech, lat, lon"
    )

state = get_state()

# Demo vs MQTT lifecycle
if demo_mode and not state.demo_running:
    start_demo(state, demo_interval)
elif demo_mode and state.demo_running and abs(state.demo_interval - demo_interval) > 1e-6:
    state.demo_interval = demo_interval
elif not demo_mode and state.demo_running:
    stop_demo(state)

# Start / restart MQTT client if requested
if connect:
    state = start_mqtt_client(broker, int(port), topic.strip(), market_topic.strip(), username or None, password or None)

# Attach uploaded lookup
if up is not None:
    try:
        df_lookup = pd.read_csv(up)
        cols = set(df_lookup.columns.str.lower())
        if "facility_id" not in cols and "facility_code" in cols:
            df_lookup = df_lookup.rename(columns={"facility_code":"facility_id"})
        if "facility_id" not in df_lookup.columns:
            st.warning("Missing 'facility_id' (or 'facility_code') column. Ignored.")
        else:
            state.facility_lookup = df_lookup.set_index("facility_id")
            st.success(f"Loaded lookup: {len(df_lookup):,} rows")
    except Exception as e:
        st.warning(f"Failed to read lookup CSV — {e}")

# ----------------------------
# Main area
# ----------------------------
st.title("⚡ NEM Live Map — Power & Emissions")

if not demo_mode and not state.connected:
    st.info("Not connected yet. Enable demo or set broker/port/topic then click **Connect / Reconnect**.")

df = dataframe_from_state(state)

# Filters & totals
regions = sorted([r for r in df["region"].dropna().unique()]) if not df.empty else []
fuels = sorted([f for f in df["fuel_tech"].dropna().unique()]) if not df.empty else []

c1, c2, c3 = st.columns([2, 2, 3])
with c1:
    sel_regions = st.multiselect("Region", options=regions, default=regions)
with c2:
    sel_fuels = st.multiselect("Fuel", options=fuels, default=fuels)
with c3:
    st.caption("Totals (filtered)")
    fdf = df.copy()
    if sel_regions: fdf = fdf[fdf["region"].isin(sel_regions)]
    if sel_fuels:   fdf = fdf[fdf["fuel_tech"].isin(sel_fuels)]
    total_power = float(fdf["power_mw"].fillna(0).sum()) if not fdf.empty else 0.0
    total_co2 = float(fdf["co2_tonnes"].fillna(0).sum()) if not fdf.empty else 0.0
    m1, m2 = st.columns(2)
    m1.metric("Total Power (MW)", f"{total_power:,.1f}")
    m2.metric("Total CO₂ (t)", f"{total_co2:,.1f}")

# Market panel
if state.market_by_region:
    st.caption("Market (latest)")
    regs = sel_regions or sorted(state.market_by_region.keys())
    cols = st.columns(min(4, max(1, len(regs))))
    for i, r in enumerate(regs[: len(cols)]):
        m = state.market_by_region.get(r, {})
        with cols[i]:
            st.metric(f"{r} Price ($/MWh)", f"{m.get('price', 0):,.1f}")
            st.write(f"Demand: {int(m.get('demand', 0)):,} MW")

# Apply filters for map
if not df.empty:
    if sel_regions: df = df[df["region"].isin(sel_regions)]
    if sel_fuels:   df = df[df["fuel_tech"].isin(sel_fuels)]

st.markdown(":earth_asia: **Live map**")
deck = deck_from_df(df, metric)
st.pydeck_chart(deck, use_container_width=True, key="deck_map")

with st.expander("Latest readings (table)"):
    if not df.empty:
        show_cols = ["facility_id","facility_name","region","fuel_tech","timestamp","power_mw","co2_tonnes","lat","lon"]
        st.dataframe(df[show_cols].sort_values("facility_id"), use_container_width=True)
    else:
        st.caption("Waiting for messages…")

# Auto-refresh
if live:
    time.sleep(int(refresh_sec))
    try: st.rerun()
    except Exception: st.experimental_rerun()
