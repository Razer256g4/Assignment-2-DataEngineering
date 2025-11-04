

from __future__ import annotations

import json
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, Optional
import altair as alt


import numpy as np
import pandas as pd
import streamlit as st
import pydeck as pdk


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
def _to_float(x, default=0.0):
    try:
        if x is None:
            return default
        if isinstance(x, str):
            x = x.strip().replace("$", "").replace(",", "")
            if x == "" or x.lower() in {"nan", "inf", "-inf"}:
                return default
        v = float(x)
        if np.isnan(v) or np.isinf(v):
            return default
        return v
    except Exception:
        return default


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
        print("payload decode error:", e)
        return

    # ----- MARKET SNAPSHOT (keep publisher unchanged) -----
    # Any packet that carries price/demand and has no facility_id is treated as market.
    if (("price_dmwh" in payload) or ("demand_mw" in payload) or ("price" in payload)) \
       and not payload.get("facility_id"):
        ts = payload.get("timestamp")
        if ts != "starting...":  # ignore warm-start zeros
            with userdata.lock:
                userdata.price_demand = {
                    "timestamp": ts,
                    "price_dmwh": _to_float(payload.get("price_dmwh", payload.get("price"))),
                    "demand_mw":  _to_float(payload.get("demand_mw", payload.get("demand"))),
                    "region": payload.get("region"),
                }
                userdata.market_history.append({
                "_ts": time.time(),
                "price_dmwh": userdata.price_demand["price_dmwh"],
                "demand_mw": userdata.price_demand["demand_mw"],
    })
            print("Market update:", userdata.price_demand)
        return  # <<< don't fall through

    # ----- FACILITY SNAPSHOT -----
    if payload.get("facility_id"):
        d = _normalize_payload(payload)

        # Enrich with lookup if needed
        if userdata.facility_lookup is not None:
            fid = d.get("facility_id")
            if fid and (pd.isna(d.get("lat")) or pd.isna(d.get("lon")) or d.get("lat") is None or d.get("lon") is None):
                try:
                    row = userdata.facility_lookup.loc[fid]
                    d.setdefault("facility_name", row.get("facility_name"))
                    d.setdefault("region", row.get("region"))
                    d.setdefault("fuel_tech", row.get("fuel_tech"))
                    d.setdefault("lat", float(row.get("lat")))
                    d.setdefault("lon", float(row.get("lon")))
                except Exception as e:
                    print("lookup enrich error:", e)

        fid = d.get("facility_id")
        if not fid:
            return

        with userdata.lock:
            d.pop("facility_id", None)
            prev = userdata.latest_by_facility.get(fid, {})
            userdata.latest_by_facility[fid] = {**prev, **d}
            userdata.history.append({**d, "_ts": time.time()})
        return

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
def totals_timeseries(state: AppState, sel_regions=None, sel_fuels=None,
                      bucket: str = "10s", horizon_min: int = 15) -> pd.DataFrame:
    with state.lock:
        hist = list(state.history)
    if not hist:
        return pd.DataFrame()

    h = pd.DataFrame(hist)
    # make sure numeric
    for c in ("power_mw", "co2_tonnes"):
        h[c] = pd.to_numeric(h.get(c), errors="coerce").fillna(0.0)

    # filters (optional)
    if sel_regions:
        h = h[h.get("region").isin(sel_regions)]
    if sel_fuels:
        h["fuel_tech"] = h["fuel_tech"].apply(lambda x: x if isinstance(x, list)
                                              else ([x] if pd.notna(x) and x != "" else []))
        h = h[h["fuel_tech"].apply(lambda xs: any(x in sel_fuels for x in xs))]

    # time window + resample
    tz = "Australia/Sydney"
    h["_ts"] = pd.to_datetime(h["_ts"], unit="s", utc=True).dt.tz_convert(tz)
    cutoff = pd.Timestamp.now(tz=tz) - pd.Timedelta(minutes=horizon_min)
    h = h[h["_ts"] >= cutoff]

    if h.empty:
        return pd.DataFrame()

    out = (h.set_index("_ts")[["power_mw", "co2_tonnes"]]
             .resample(bucket).sum()
             .reset_index())
    return out

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

    df = df.dropna(subset=["lat", "lon"], how="any")
    # NEW: always make fuel_tech a list so filters & labels work
    df["fuel_tech"] = df["fuel_tech"].apply(
        lambda x: x if isinstance(x, list) else ([x] if pd.notna(x) and x != "" else [])
    )
    return df


# FUEL_COLORS = {
#     "Coal": "#3e3e3e",
#     "Black coal": "#3e3e3e",
#     "Brown coal": "#6b4226",
#     "Gas": "#d97706",
#     "Hydro": "#2563eb",
#     "Wind": "#16a34a",
#     "Solar": "#f59e0b",
#     "Battery": "#7c3aed",
#     "Bioenergy": "#15803d",
#     "Diesel": "#ef4444",
# }

COLOR = {
    "Coal": [62, 62, 62],
    "Black coal": [62, 62, 62],
    "Brown coal": [107, 66, 38],
    "Gas": [217, 119, 6],
    "Hydro": [37, 99, 235],
    "Wind": [22, 163, 74],
    "Solar": [245, 158, 11],
    "Battery": [124, 58, 237],
    "Bioenergy": [21, 128, 61],
    "Diesel": [239, 68, 68],
}


def deck_from_df(df: pd.DataFrame, metric: str, show_labels: bool = False) -> pdk.Deck:
    if df.empty:
        view = pdk.ViewState(latitude=-25.0, longitude=133.0, zoom=4)
        return pdk.Deck(layers=[], initial_view_state=view, map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json")

    # robust numeric + p95 ref
    m = pd.to_numeric(df[metric], errors="coerce").fillna(0.0)
    pos = m[m > 0]
    p95 = float(np.percentile(pos, 95)) if len(pos) else 1.0

    data = df.copy()

    # bubble radius (meters): floor + sqrt scaling
    data["_radius"] = 1000.0 * np.sqrt(np.clip(m / p95, 0.0, 1.0)) + 1000.0

    # color from first fuel in list (or string), fallback grey
    def _color_for(ft):
        key = ""
        if isinstance(ft, list) and ft:
            key = str(ft[0]).strip()
        elif isinstance(ft, str):
            key = ft.strip()
        return COLOR.get(key, [100, 100, 100])

    data["_color"] = data["fuel_tech"].apply(_color_for)

    # coords sanity
    data["lat"] = pd.to_numeric(data["lat"], errors="coerce")
    data["lon"] = pd.to_numeric(data["lon"], errors="coerce")
    data = data.dropna(subset=["lat", "lon"])

    scatter = pdk.Layer(
        "ScatterplotLayer",
        data=data,
        id="facilities",
        get_position='[lon, lat]',
        get_radius="_radius",
        get_fill_color="_color",
        pickable=True,
        radius_min_pixels=3,
        radius_max_pixels=60,
        auto_highlight=True,
        # smooth transitions between reruns
        transitions={"get_radius": 300, "get_fill_color": 300},
    )

    layers = [scatter]

    if show_labels:
        labels = pdk.Layer(
            "TextLayer",
            data=data,
            id="labels",
            get_position='[lon, lat]',
            get_text="facility_name",
            get_size=12,
            get_color=[0, 0, 0],
            get_alignment_baseline="'bottom'",
        )
        layers.append(labels)

    view = pdk.ViewState(
        latitude=float(data["lat"].mean()),
        longitude=float(data["lon"].mean()),
        zoom=4,
    )
    tooltip = {
        "text": "{facility_name}\n{region} • {fuel_tech}\nPower: {power_mw} MW\nCO₂: {co2_tonnes} t\n{timestamp}"
    }

    return pdk.Deck(
        layers=layers,
        initial_view_state=view,
        tooltip=tooltip,
        map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
    )


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

# Start / restart client if requested
if connect:
    state = start_mqtt_client(broker, int(port), topic, username or None, password or None)
else:
    state = get_state()

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
            fdf = fdf[fdf["fuel_tech"].apply(lambda xs: any(x in sel_fuels for x in xs))]

        total_power = float(fdf["power_mw"].fillna(0).sum()) if not fdf.empty else 0.0
        total_co2 = float(fdf["co2_tonnes"].fillna(0).sum()) if not fdf.empty else 0.0
        m1, m2= st.columns(2)
        m1.metric("Total Power (MW)", f"{total_power:,.1f}")
        m2.metric("Total CO₂ (t)", f"{total_co2:,.1f}")
    ts = totals_timeseries(state, sel_regions=sel_regions, sel_fuels=sel_fuels,
                       bucket="10s", horizon_min=15)

    st.markdown("#### Totals over time (last 15 min)")
    if not ts.empty:
        cts1, cts2 = st.columns(2)
        with cts1:
                st.line_chart(ts.set_index("_ts")["power_mw"], height=220)
        with cts2:
                st.line_chart(ts.set_index("_ts")["co2_tonnes"], height=220)
    else:
        st.caption("Waiting for enough data to draw totals…")
    with c4:
        pdict = state.price_demand or {}
        price_val  = _to_float(pdict.get("price_dmwh"))
        demand_val = _to_float(pdict.get("demand_mw"))
        ts_label   = pdict.get("timestamp", "pending data...")
        st.caption(f"NEM Market: {ts_label}")
        m3, m4 = st.columns(2)
        m3.metric("Price ($/MWh)", f"{price_val:,.1f}")
        m4.metric("Demand (MW)",   f"{demand_val:,.1f}")

    # Apply filters for map render
    if not df.empty:
        df = df[df["region"].isin(sel_regions)] if sel_regions else df
        # df = df[df["fuel_tech"].isin(sel_fuels)] if sel_fuels else df
        df = df[df["fuel_tech"].apply(lambda fuels: any(f in sel_fuels for f in fuels))] if sel_fuels else df

    st.markdown(":earth_asia: **Live map**")
    deck = deck_from_df(df, metric, show_labels=show_labels)
    st.pydeck_chart(deck, use_container_width=True, key="deck_map")

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
