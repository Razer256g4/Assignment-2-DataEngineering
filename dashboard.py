import threading
import time
import json
import pandas as pd
import numpy as np
import emoji
import constants


from collections import deque
from dataclasses import dataclass, field
from typing import Annotated, Dict, Any, Optional
from pydantic import BaseModel, Field, StringConstraints, ValidationError, constr, field_validator

import paho.mqtt.client as mqtt
from sqlalchemy import create_engine, text
import streamlit as st
import pydeck as pdk

MSG_BROKER_HOSTNAME = "test.mosquitto.org"
MSG_BROKER_TOPIC = "comp5339/t01_group8"

## Helpers for Map Visualization

def build_region_hue_layer_from_geojson(allowed_region_names: list[str] | None = None):
    if not constants.REGION_GEOJSON:
        return None

    # turn Region multiselect (names) into allowed NEM ids using your lookup
    st_state = get_state()
    allowed_ids = None
    if allowed_region_names:
        allowed_ids = {
            rid for rid in st_state.region_lookup.index
            if st_state.region_lookup.loc[rid, "region_name"] in set(allowed_region_names)
        }

    # clone features and stamp colors
    feats = []
    for f in constants.REGION_GEOJSON.get("features", []):
        name = (f.get("properties") or {}).get("STATE_NAME", "")
        rid = constants.STATE_TO_REGION_ID.get(name)  # None for WA/NT/ACT
        if not rid:
            continue  # skip non-NEM
        if allowed_ids and rid not in allowed_ids:
            continue

        fill = constants.REGION_COLORS.get(rid, [200, 200, 200, 70])
        props = {**f.get("properties", {}),
                 "region_id": rid,
                 "region_name": st_state.region_lookup.loc[rid, "region_name"],
                 "fill_color": fill,
                 "line_color": [0, 0, 0, 160]}
        feats.append({**f, "properties": props})

    if not feats:
        return None

    gj = {"type": "FeatureCollection", "features": feats}
    return pdk.Layer(
        "GeoJsonLayer",
        gj,
        id="region-hues",
        stroked=True,
        filled=True,
        get_fill_color="properties.fill_color",
        get_line_color="properties.line_color",
        line_width_min_pixels=2,
        pickable=False,
        opacity=0.30,
    )

FUEL_TO_EMOJI_ALIAS: Dict[str, str] = {
    "Coal": ":rock:", "Black coal": ":rock:", "Brown coal": ":rock:",
    "Gas": ":fire:", "Hydro": ":droplet:", "Wind": ":dash:",
    "Solar": ":sun_with_face:", "Battery": ":battery:",
    "Bioenergy": ":seedling:", "Diesel": ":fuel_pump:",
}

def emojialias_to_char(alias: str) -> str:
    return emoji.emojize(alias, language="alias")

def emoji_char_to_twemoji_url(ch: str) -> str:
    codepoints = "-".join(f"{ord(c):x}" for c in ch).replace("-fe0f", "")
    return f"https://cdn.jsdelivr.net/gh/twitter/twemoji@14.0.2/assets/72x72/{codepoints}.png"


def get_icon_url_from_fuel(fuel_tech_value: Any) -> str:
    name = (fuel_tech_value[0] if isinstance(fuel_tech_value, list) and fuel_tech_value
            else str(fuel_tech_value or "")).strip()
    alias = FUEL_TO_EMOJI_ALIAS.get(name, ":zap:")
    return emoji_char_to_twemoji_url(emojialias_to_char(alias))

def compute_marker_size(metric_values: pd.Series, pctl: float = 95.0) -> np.ndarray:
    x = pd.to_numeric(metric_values, errors="coerce").fillna(0.0)
    p = float(np.percentile(x[x > 0], pctl)) if (x > 0).any() else 1.0
    return (12 + 24 * np.sqrt(np.clip(x / p, 0.0, 1.0))).astype(float)

def sanitize_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    return df.dropna(subset=["lat", "lon"])
# ----------------------------
# Page config
# ----------------------------
st.set_page_config(
  page_title="NEM Live Map — Power & Emissions",
  page_icon="⚡",
  layout="wide",
)

# --
# Simulate Data Fetched from PostgreSQL
# --
def get_region_lookup_from_postgresql():
  rdf = pd.DataFrame([
    ["NSW1", "New South Wales"],
    ["QLD1", "Queensland"],
    ["VIC1", "Victoria"],
    ["SA1", "South Australia"],
    ["TAS1", "Tasmania"]
  ], columns = ["region_id", "region_name"])
  print("[postgresql] region_lookup fetched")
  return rdf.set_index("region_id")

def get_facility_lookup_from_postgresql():
  fdf = pd.read_csv("data/facility_lookup.csv")
  fdf['fuel_tech'] = fdf['fuel_tech'].apply(json.loads) # json str to list
  print("[postgresql] facility_lookup fetched")
  return fdf.set_index("facility_id")

# ---
# App State
# ---
@dataclass
class AppState:
  lock: threading.RLock = field(default_factory=threading.RLock)
  latest_by_facility: Dict[str, Dict[str, Any]] = field(default_factory=dict)
  facility_lookup: pd.DataFrame = field(default_factory=get_facility_lookup_from_postgresql)
  latest_by_region: Dict[str, Dict[str, Any]] = field(default_factory=dict)
  region_lookup: pd.DataFrame = field(default_factory=get_region_lookup_from_postgresql)
  connected: bool = False
  client: Optional[Any] = None
  topic: str = ""
  history: deque = field(default_factory=lambda: deque(maxlen=5000))
  price_demand_history: deque = field(default_factory=lambda: deque(maxlen=5000))


@st.cache_resource(show_spinner=False)
def get_state() -> AppState:
  return AppState()

# --
# Integration with Assignment 1
# -- 
def enrich_facility_lookup(userdata: AppState, fid: str, payload: dict):
  """ Check for facility information in NGER if OEM does nnot have """
  
  get_facility_sql = f"""
    SELECT 
      f.facility_name,
      f.fuel_type_id as fuel_tech, 
      ST_Y(l.geom) as lat,
      ST_X(l.geom) as lon,
      l.region
    FROM ner.facility f JOIN ner.location l ON f.location_id = l.location_id
    WHERE f.facility_id == {fid}
  """
  insert_facility_sql = f"""
    INSERT INTO oem.facility_lookup (facility_id, facility_name, region, lat, lon, fuel_tech)
    VALUES (:facility_id, :facility_name, :region, :lat, :lon, :fuel_tech)
    ON CONFLICT (facility_id) DO NOTHING
  """
  engine = create_engine("postgresql+psycopg2://postgres:comp5339@localhost:5432/renewable_energy")

  with engine.begin() as conn:
    # Check if facility_id exist in NGER
    facility: dict|None = conn.execute(text(get_facility_sql)).one_or_none()

    # Insert in oem.facility_lookup since facility exist
    if facility:
      conn.execute(text(insert_facility_sql), {"facility_id": fid ,**facility})
      print(f"[postgresql] {fid} inserted to oem.facility_lookup")
  
      # Add to app's state
      userdata.facility_lookup.loc[fid] = facility

# --
# Data Validation
# -- 
class FacilityPayload(BaseModel):
  facility_id: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
  timestamp: Annotated[str, StringConstraints(strip_whitespace=True, min_length=20)]
  power_mw: float = 0.0
  co2_tonnes: float = 0.0

class MarketPayload(BaseModel):
  region_id: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
  timestamp: Annotated[str, StringConstraints(strip_whitespace=True, min_length=20)]
  price_dmwh: float = 0.0
  demand_mw: float = 0.0

# --
# MQTT Subscriber
# -- 
def on_connect(client, userdata: AppState, flags, rc, properties=None):
  st.toast(f"MQTT connected (rc={rc}) | subscribing to {userdata.topic}")
  client.subscribe(userdata.topic, qos=1)
  userdata.connected = True

def on_subscribe(client, userdata, mid, reason_codes, properties):
  print(f"Subscribed successfully!")

def on_message(client, userdata: AppState, msg):
  try: 

    payload: dict = json.loads(msg.payload.decode("utf-8"))
    
    # Handle Facility Event
    if "facility_id" in payload:
      # print(f"[on_message] facility event: {payload}") # debug
      validated: dict = FacilityPayload(**payload).model_dump()
      fid: str = validated.pop("facility_id", None)
      
      if fid not in userdata.facility_lookup.index:
        enrich_facility_lookup(userdata, fid, payload)

      try:
        row = userdata.facility_lookup.loc[fid]
        validated.setdefault("facility_name", row.get("facility_name"))
        validated.setdefault("region", row.get("region"))
        validated.setdefault("fuel_tech", row.get("fuel_tech")) # list
        validated.setdefault("lat", float(row.get("lat")))
        validated.setdefault("lon", float(row.get("lon")))
      except KeyError:
        # fid does not exist in NGER dataset as well
        print(f"[on_message] {fid} does not exist in facility lookup")
        return

      with userdata.lock:
        prev = userdata.latest_by_facility.get(fid, {})
        userdata.latest_by_facility[fid] = {**prev, **validated}
        userdata.history.append({**validated, '_ts': time.time()}) #! using transaction time is wrong

    # Handle Market Event
    elif "region_id" in payload:
    #   print(f"[on_message] market event: {payload}") # debug
      validated: dict = MarketPayload(**payload).model_dump()
      rid = validated.pop("region_id", None)

      if rid in userdata.region_lookup.index:
        row = userdata.region_lookup.loc[rid]
        validated.setdefault("region_name", row.get("region_name"))
        
        with userdata.lock:
          prev = userdata.latest_by_region.get(rid, {})
          userdata.latest_by_region[rid] = {**prev, **validated}
          userdata.price_demand_history.append({**validated, '_ts': time.time()}) #! using transaction time is wrong

    elif payload['timestamp'] == "starting...":
      print("[on_message] warm start event received")
      pass
    
    return 


  except json.JSONDecodeError as e:
    print(f"[on_message] Invalid JSON: {e}")
    return 
  
  except ValidationError as e:
    print(f"[on_message] Validation error: {e.error_count()} errors")
    for error in e.errors():
      print(f"  - {error['loc']}: {error['msg']}")
    return
  
  except Exception as e:
    print(f"[on_message] Unexpected error")
    return


# @st.cache_resource(show_spinner=False)
def start_mqtt_client(broker: str, port: int, topic: str) -> AppState:
  state = get_state()
  state.topic = topic

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
  client.user_data_set(state)
  client.on_connect = on_connect
  client.on_subscribe = on_subscribe
  client.on_message = on_message

  try:
    client.connect(broker, port, keepalive=60)
    client.loop_start()
    state.client = client
  except Exception as e:
    st.error(f"Failed to connect to MQTT broker at {broker}:{port} — {e}")

  return state



# ---
# Frontend Development
# ---
# facility_df
# facility_id, facility_name, region, timestamp, power_mw, co2_tonnes, lat, lon, fuel_tech
# market_data
# region_id, region_name, timestamp, price_dmwh, demand_mw

def prepare_data_from_state(state: AppState):
  """ Prepare the data in the format ready to use for visualization """
  if not state.latest_by_facility:
    return (
      pd.DataFrame(columns=["facility_id", "facility_name", "region", "timestamp", "power_mw", "co2_tonnes", "lat", "lon", "fuel_tech"]), 
      pd.DataFrame(columns=["region_id", "region_name", "timestamp", "price_dmwh", "demand_mw"]), 
      [], []
    )

  facility_df = pd.DataFrame.from_dict(state.latest_by_facility, orient="index").reset_index(names=['facility_id'])
  facility_df['region'] = facility_df['region'].apply(lambda code: state.region_lookup.loc[code, 'region_name']) # so as to filter on selected region names
  fuel_types: list = sorted(facility_df['fuel_tech'].explode().dropna().unique().tolist()) # options for fuel selector
  region_names:list = facility_df['region'].unique().tolist()
  

  if not state.latest_by_region:
     return (
      facility_df, 
      pd.DataFrame(columns=["region_id", "region_name", "timestamp", "price_dmwh", "demand_mw"]),
      region_names, fuel_types
     )
  
  market_df = pd.DataFrame.from_dict(state.latest_by_region, orient="index").reset_index(names=['region_id'])
  region_names: list = sorted(market_df['region_name'].tolist()) # options for region selector
  
  return facility_df, market_df, region_names, fuel_types

# copied over
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

    if h.empty:
        return pd.DataFrame()
    
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

# copied over
def deck_from_df(facility_df: pd.DataFrame, metric: str, show_labels: bool = False, show_region_hues: bool = False) -> pdk.Deck:
    if facility_df.empty:
        view = pdk.ViewState(latitude=-25.0, longitude=133.0, zoom=4)
        return pdk.Deck(layers=[], initial_view_state=view,
                        map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json")

    data = sanitize_coordinates(facility_df.copy())
    data["icon_url"] = data["fuel_tech"].apply(get_icon_url_from_fuel)
    data["_icon_size"] = compute_marker_size(data[metric])
    data["icon_data"] = data["icon_url"].apply(lambda url: {"url": url, "width": 72, "height": 72, "anchorY": 72})

    icon_layer = pdk.Layer(
        "IconLayer",
        data=data,
        id="facility-icons",
        get_icon="icon_data",
        get_size="_icon_size",
        size_scale=1,
        get_position='[lon, lat]',
        pickable=True,
        billboard=True,
        transitions={"get_size": 300},
    )

    layers = [icon_layer]
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
    if show_region_hues:                    
        hue_layer = build_region_hue_layer_from_geojson(allowed_region_names=selected_regions)
        if hue_layer:
            layers.append(hue_layer)

    view = pdk.ViewState(latitude=float(data["lat"].mean()),
                         longitude=float(data["lon"].mean()), zoom=4)
    tooltip = {"text": "{facility_name}\n{region} • {fuel_tech}\n"
                       f"{'Power' if metric=='power_mw' else 'CO₂'}: {{{metric}}}\n"
                       "{timestamp}"}
    return pdk.Deck(layers=layers, initial_view_state=view, tooltip=tooltip,
                    map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json")




# Side Bar
with st.sidebar:
  st.markdown("### Connection")
  broker = st.text_input("MQTT broker", value=MSG_BROKER_HOSTNAME)
  port = st.number_input("Port", value=1883, min_value=1, max_value=65535, step=1)
  topic = st.text_input("Topic", value=MSG_BROKER_TOPIC)
  
  connect = st.button("Connect / Reconnect", width = "stretch")
  st.markdown("---")
  st.markdown("### Zoning")
  show_region_hues = st.checkbox("Show region hues", value=True)
  show_labels = st.checkbox("Show marker labels", value=False)
  st.markdown("### Data & Filters")
  metric = st.radio("Bubble metric", ["power_mw", "co2_tonnes"], index=0, horizontal=True)
  live = st.checkbox("Live refresh", value=True)
  refresh_sec = st.slider("Refresh every (s)", min_value=1, max_value=10, value=3)

# Returns new app state when restart
state: AppState = start_mqtt_client(broker, int(port), topic) if connect else get_state()
st.sidebar.text(f"State ID: {id(state)}")  # Should be same ID across reruns
st.sidebar.text(f"Facilities: {len(state.latest_by_facility)}")  # Should grow over time

# Main Area
st.title("⚡ NEM Live Map — Power & Emissions")
with st.container():

  if not state.connected:
    st.info("Not connected yet. Set broker/port/topic in the sidebar and click **Connect / Reconnect**.")
  
  # Data Preparation
  facility_df, market_df, region_names, fuel_types = prepare_data_from_state(state)
  
  # Control & Metric Section

  c1, c2, c3, c4 = st.columns([2, 2, 3, 3])
  ## Filters
  with c1: selected_regions: list[str] = st.multiselect("Region", options=region_names, default=region_names)
  with c2: selected_fueltypes: list[str] = st.multiselect("Fuel", options=fuel_types, default=fuel_types)
  
  ### Filtered Data
  fdf = facility_df.copy()
  rdf = market_df.copy()
  if selected_fueltypes: fdf = fdf[fdf['fuel_tech'].apply(lambda ft: bool(set(selected_fueltypes) & set(ft)))]
  if selected_regions: 
    fdf = fdf[fdf['region'].isin(selected_regions)]
    rdf = rdf[rdf['region_name'].isin(selected_regions)]

  ## Metrics
  with c3:
    st.caption("rs (filtered)")
    m1, m2 = st.columns(2)
    m1.metric("Total Power (MW)", f"{fdf['power_mw'].sum():,.1f}")
    m2.metric("Total CO₂ (t)", f"{fdf['co2_tonnes'].sum():,.1f}")

  ## Market Data
  with c4:
    market_price: float = rdf['price_dmwh'].mean()
    market_price = market_price if not pd.isna(market_price) else 0.0
    market_demand: float = rdf['demand_mw'].sum() or 0
    ts_label: str = rdf['timestamp'].mode()[0] if not rdf.empty else "pending data..."
   
    st.caption(f"NEM Market: {ts_label}")
    m3, m4 = st.columns(2)
    m3.metric("Average Price ($/MWh)", f"{market_price:,.1f}")
    m4.metric("Total Demand (MW)",   f"{market_demand:,.1f}")

  ### Line Plots
  st.markdown("#### Totals over time (last 15 min)")
  ts = totals_timeseries(state, sel_regions=selected_regions, sel_fuels=selected_fueltypes, bucket="10s", horizon_min=15)
  if not ts.empty:
      cts1, cts2 = st.columns(2)
      with cts1: st.line_chart(ts.set_index("_ts")["power_mw"], height=220)
      with cts2: st.line_chart(ts.set_index("_ts")["co2_tonnes"], height=220)
  else:
      st.caption("Waiting for enough data to draw totals…")

  # Map Display
  st.markdown(":earth_asia: **Live map**")
  deck = deck_from_df(fdf, metric, show_labels=show_labels,show_region_hues =show_region_hues)
  st.pydeck_chart(deck,width='stretch', key="deck_map")

  # DataFrame Display
  with st.expander("Latest readings (table)", expanded=False):
    if not fdf.empty:
      st.dataframe(fdf, width='stretch')
    else: 
      st.caption("Waiting for messages…")

if live:
  time.sleep(int(refresh_sec))
  st.rerun()
