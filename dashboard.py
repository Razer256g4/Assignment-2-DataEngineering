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
## Map Legends and Helpers
SYD_TZ = "Australia/Sydney"

def parse_event_ts(ts_str: str) -> pd.Timestamp:
    """
    Accepts ISO 8601 strings like '2025-10-29T01:20:00+10:00' or '...Z'.
    Returns a timezone-aware UTC timestamp.
    """
    ts = pd.to_datetime(ts_str, utc=True, errors="coerce")
    return ts
# ---- Legends (single overlay) ----
def _legend_css() -> str:
    return """
    <style>
      .nem-legend-wrap{
        position:fixed; right:16px; bottom:16px; z-index:9999;
        max-width:280px; color:#111; pointer-events:none;
      }
      .nem-legend{
        background:rgba(255,255,255,.92);
        border:1px solid rgba(0,0,0,.12);
        border-radius:12px; padding:10px 12px;
        box-shadow:0 6px 20px rgba(0,0,0,.18);
        font-size:12px; line-height:1.35;
        backdrop-filter:saturate(1.2) blur(4px);
      }
      .nem-legend h4{
        margin:0 0 8px 0; font-weight:700; font-size:12.5px;
      }
      .nem-legend .sec{ margin-top:10px; }
      .nem-legend .row{ display:flex; align-items:center; gap:8px; margin:3px 0; }
      .nem-legend .sw{ width:14px; height:14px; border-radius:3px;
        border:1px solid rgba(0,0,0,.25); display:inline-block }
      .nem-ico{ width:18px; height:18px; display:inline-block; }
      .nem-b{ display:inline-block; border-radius:999px;
        border:1px solid rgba(0,0,0,.25); background:rgba(0,0,0,.06); }
      .nem-b-row{ display:flex; align-items:center; gap:8px; margin:4px 0; }
      .nem-b-label{ min-width:56px; text-align:right; }
      @media (prefers-color-scheme: dark){
        .nem-legend{ background:rgba(20,20,20,.88); color:#eee;
          border-color:rgba(255,255,255,.12); }
        .nem-legend .sw{ border-color:rgba(255,255,255,.25); }
        .nem-b{ background:rgba(255,255,255,.08); border-color:rgba(255,255,255,.25); }
      }
    </style>
    """

def _legend_regions_html(selected_region_names: list[str]) -> str:
    if not selected_region_names:
        return ""
    lookup = get_state().region_lookup
    name_to_id = {lookup.loc[rid, "region_name"]: rid for rid in lookup.index}
    rows = []
    for name in selected_region_names:
        rid = name_to_id.get(name)
        if not rid:
            continue
        r,g,b,a = constants.REGION_COLORS.get(rid, [200,200,200,90])
        rows.append(
            f'<div class="row"><span class="sw" '
            f'style="background: rgba({r},{g},{b},{a/255:.2f});"></span>{name}</div>'
        )
    return "" if not rows else f'<div class="sec"><h4>Regions</h4>{"".join(rows)}</div>'

def _legend_fuels_html(facilities_df: pd.DataFrame, selected_fuels: list[str]) -> str:
    if facilities_df.empty or "fuel_tech" not in facilities_df:
        return ""
    fuels = sorted(set(facilities_df["fuel_tech"].explode().dropna().astype(str)))
    if selected_fuels:
        keep = set(selected_fuels)
        fuels = [f for f in fuels if f in keep]
    if not fuels:
        return ""
    rows = []
    for f in fuels:
        rows.append(
            f'<div class="row"><img class="nem-ico" src="{get_icon_url_from_fuel(f)}" alt="{f}"/>'
            f'<span>{f}</span></div>'
        )
    return f'<div class="sec"><h4>Fuel icons</h4>{"".join(rows)}</div>'

def _legend_sizes_html(values: pd.Series, metric_label: str) -> str:
    x = pd.to_numeric(values, errors="coerce").fillna(0.0)
    pos = x[x > 0]
    if pos.empty:
        return ""
    p95 = float(np.percentile(pos, 95))
    def px(v: float) -> int:
        return int(12 + 24 * np.sqrt(np.clip(v/p95, 0.0, 1.0)))
    ticks = [0.1*p95, 0.3*p95, 0.6*p95, 1.0*p95]
    bubbles = [
        f'<div class="nem-b-row"><span class="nem-b" style="width:{px(t)}px;height:{px(t)}px;"></span>'
        f'<span class="nem-b-label">{t:,.0f}</span></div>'
        for t in ticks
    ]
    return f'<div class="sec"><h4>Marker size → {metric_label}</h4>{"".join(bubbles)}</div>'

def show_legend(render_legends, show_region_hues, metric, selected_regions, selected_fueltypes, fdf):
   
    render_legends(
              show_region_hues,
              selected_regions,
              selected_fueltypes,
              fdf,
              metric,
          )
            
def render_legends(show_region_hues: bool,
                   selected_region_names: list[str],
                   selected_fuels: list[str],
                   facilities_df: pd.DataFrame,
                   metric: str) -> None:
    """Render a single fixed overlay with optional sections; outputs nothing else."""
    parts = [_legend_css(), '<div class="nem-legend-wrap"><div class="nem-legend">']
    if show_region_hues:
        parts.append(_legend_regions_html(selected_region_names))
    parts.append(_legend_fuels_html(facilities_df, selected_fuels))
    label = "Power (MW)" if metric == "power_mw" else "CO₂ (t)"
    if not facilities_df.empty and metric in facilities_df.columns:
        parts.append(_legend_sizes_html(facilities_df[metric], label))
    parts.append("</div></div>")
    html = "".join(p for p in parts if p)
    if html:
        st.markdown(html, unsafe_allow_html=True)


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
    "Solar": ":sun:", "Battery": ":battery:",
    "Bioenergy": ":seedling:", "Diesel": ":fuel_pump:",
    "Pumps": ":droplet:", "Distillate": ":alembic:",
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
  client.subscribe(userdata.topic, qos=1)
  userdata.connected = True

def on_subscribe(client, userdata, mid, reason_codes, properties):
  print(f"Subscribed successfully!")

def on_message(client, userdata: AppState, msg):
    try:
        payload: dict = json.loads(msg.payload.decode("utf-8"))

        # ----------------------------
        # FACILITY EVENT
        # ----------------------------
        if "facility_id" in payload:
            validated: dict = FacilityPayload(**payload).model_dump()
            fid: str = validated.pop("facility_id")

            # Enrich lookup if missing
            if fid not in userdata.facility_lookup.index:
                enrich_facility_lookup(userdata, fid, payload)

            try:
                row = userdata.facility_lookup.loc[fid]
                validated.setdefault("facility_name", row.get("facility_name"))
                # keep region **ID** in state/history; map to name later for UI
                validated.setdefault("region", row.get("region"))
                validated.setdefault("fuel_tech", row.get("fuel_tech"))
                validated.setdefault("lat", float(row.get("lat")))
                validated.setdefault("lon", float(row.get("lon")))
            except KeyError:
                print(f"[on_message] {fid} not found in facility lookup")
                return

            evt_ts_utc = parse_event_ts(validated.get("timestamp"))
            if evt_ts_utc is pd.NaT:
                evt_ts_utc = pd.Timestamp.utcnow().tz_localize("UTC")

            with userdata.lock:
                prev = userdata.latest_by_facility.get(fid, {})
                userdata.latest_by_facility[fid] = {**prev, **validated}
                userdata.history.append({
                    **validated,
                    "facility_id": fid,
                    "_event_ts": evt_ts_utc.isoformat(),
                })
            return

        # ----------------------------
        # MARKET EVENT
        # ----------------------------
        if "region_id" in payload:
            validated: dict = MarketPayload(**payload).model_dump()
            rid: Optional[str] = validated.pop("region_id", None)
            if not rid:
                return

            # attach region_name if known
            try:
                validated.setdefault("region_name", userdata.region_lookup.loc[rid, "region_name"])
            except Exception:
                pass

            evt_ts_utc = parse_event_ts(validated.get("timestamp"))
            if evt_ts_utc is pd.NaT:
                evt_ts_utc = pd.Timestamp.utcnow().tz_localize("UTC")

            with userdata.lock:
                prev = userdata.latest_by_region.get(rid, {})
                userdata.latest_by_region[rid] = {**prev, **validated}
                userdata.price_demand_history.append({
                    **validated,
                    "region_id": rid,
                    "_event_ts": evt_ts_utc.isoformat(),
                })
            return

        # ----------------------------
        # WARM START / OTHER
        # ----------------------------
        if isinstance(payload, dict) and payload.get("timestamp") == "starting...":
            print("[on_message] warm start event received")
            return

    except json.JSONDecodeError as e:
        print(f"[on_message] Invalid JSON: {e}")
    except ValidationError as e:
        print(f"[on_message] Validation error: {e.error_count()} errors")
        for error in e.errors():
            print(f"  - {error['loc']}: {error['msg']}")
    except Exception as e:
        print(f"[on_message] Unexpected error: {e}")



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
    """Build facility & market frames independently; never gate one on the other."""
    # Facilities
    if state.latest_by_facility:
        facility_df = (pd.DataFrame.from_dict(state.latest_by_facility, orient="index")
                       .reset_index(names=["facility_id"]))
        # map region IDs -> names for UI filtering
        if "region" in facility_df.columns:
            facility_df["region"] = facility_df["region"].apply(
                lambda rid: state.region_lookup.loc[rid, "region_name"]
                if (isinstance(rid, str) and rid in state.region_lookup.index) else rid
            )
        try:
            fuel_types = sorted(
                facility_df["fuel_tech"].explode().dropna().astype(str).unique().tolist()
            )
        except Exception:
            fuel_types = []
        fac_region_names = sorted(facility_df.get("region", pd.Series([], dtype=str)).unique().tolist())
    else:
        facility_df = pd.DataFrame(
            columns=["facility_id","facility_name","region","timestamp",
                     "power_mw","co2_tonnes","lat","lon","fuel_tech"]
        )
        fuel_types = []
        fac_region_names = []

    # Market
    if state.latest_by_region:
        market_df = (pd.DataFrame.from_dict(state.latest_by_region, orient="index")
                     .reset_index(names=["region_id"]))
        mkt_region_names = sorted(market_df.get("region_name", pd.Series([], dtype=str)).tolist())
    else:
        market_df = pd.DataFrame(
            columns=["region_id","region_name","timestamp","price_dmwh","demand_mw"]
        )
        mkt_region_names = []

    # Region selector options: prefer market regions; fall back to facility-derived
    region_names = mkt_region_names or fac_region_names

    return facility_df, market_df, region_names, fuel_types

# copied over
def totals_timeseries(state: AppState, sel_region_ids=None, sel_fuels=None,
                      bucket: str = "5min", horizon_min: int = 60) -> pd.DataFrame:
    # copy history
    with state.lock:
        hist = list(state.history)
    if not hist:
        return pd.DataFrame()

    h = pd.DataFrame(hist)

    # numeric
    for c in ("power_mw", "co2_tonnes"):
        h[c] = pd.to_numeric(h.get(c), errors="coerce").fillna(0.0)

    # event time
    if "_event_ts" in h.columns:
        h["_ts"] = pd.to_datetime(h["_event_ts"], utc=True, errors="coerce")
    else:
        h["_ts"] = pd.to_datetime(h.get("timestamp"), utc=True, errors="coerce")
    h = h.dropna(subset=["_ts"])
    if h.empty:
        return pd.DataFrame()

    # ---- region filter (IDs) ----
    if sel_region_ids:
        # history may have "region" as an ID; also accept "region_id" if present
        candidates = []
        if "region" in h.columns:
            candidates.append(h["region"].isin(sel_region_ids))
        if "region_id" in h.columns:
            candidates.append(h["region_id"].isin(sel_region_ids))
        if candidates:
            mask = candidates[0]
            for m in candidates[1:]:
                mask = mask | m
            h = h[mask]
        if h.empty:
            return pd.DataFrame()

    # ---- fuel filter (unchanged) ----
    if sel_fuels:
        h["fuel_tech"] = h["fuel_tech"].apply(lambda x: x if isinstance(x, list)
                                              else ([x] if pd.notna(x) and x != "" else []))
        h = h[h["fuel_tech"].apply(lambda xs: any(x in sel_fuels for x in xs))]
        if h.empty:
            return pd.DataFrame()

    # window
    max_ts = h["_ts"].max()
    cutoff = max_ts - pd.Timedelta(minutes=horizon_min)
    h = h[h["_ts"] >= cutoff]
    if h.empty:
        return pd.DataFrame()

    # bucket + sum
    h["_bucket"] = h["_ts"].dt.floor(bucket)
    out = (h.groupby("_bucket", as_index=False)[["power_mw", "co2_tonnes"]].sum()
             .rename(columns={"_bucket": "_ts"}))

    # to Sydney time for display
    out["_ts"] = out["_ts"].dt.tz_convert("Australia/Sydney")
    return out.sort_values("_ts")



# copied over
def deck_from_df(
    facility_df: pd.DataFrame,
    metric: str,
    show_labels: bool = False,
    show_region_hues: bool = False,
    allowed_region_names: Optional[list[str]] = None,
) -> pdk.Deck:
    """
    Build a deck.gl map. If show_labels is True, labels display the selected metric:
      - power_mw -> '12,345 MW'
      - co2_tonnes -> '12,345 t CO₂'
    """
    if facility_df.empty:
        view = pdk.ViewState(latitude=-25.0, longitude=133.0, zoom=4)
        return pdk.Deck(layers=[], initial_view_state=view,
                        map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json")

    data = sanitize_coordinates(facility_df.copy())
    data["icon_url"] = data["fuel_tech"].apply(get_icon_url_from_fuel)
    data["_icon_size"] = compute_marker_size(data[metric])
    data["icon_data"] = data["icon_url"].apply(lambda url: {"url": url, "width": 72, "height": 72, "anchorY": 72})

    # ---- metric label text for TextLayer ----
    def _fmt_metric(val: Any, which: str) -> str:
        try:
            v = float(val)
        except Exception:
            v = 0.0
        if which == "power_mw":
            return f"{v:,.1f} MW" if abs(v) < 1000 else f"{v:,.0f} MW"
        else:
            return f"{v:,.1f} t CO₂" if abs(v) < 1000 else f"{v:,.0f} t CO₂"

    def _shorten(name: Any) -> str:
        s = str(name or "").strip()
        return (s[:22] + "…") if len(s) > 23 else s
    data["label_text"] = data.apply(
        lambda r: f"{_shorten(r.get('facility_name'))}\n{_fmt_metric(r.get(metric), metric)}",
        axis=1,
    )


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
        autoHighlight=True,
        transitions={"get_size": 300},
    )

    layers = [icon_layer]

    if show_labels:
        labels = pdk.Layer(
            "TextLayer",
            data=data,
            id="labels",
            get_position='[lon, lat]',
            get_text="label_text",
            get_size=12,
            get_color=[0, 0, 0],
            get_alignment_baseline="'top'",     
            get_text_anchor="'middle'",
            get_pixel_offset='[0, -6]',    
        )
        layers.append(labels)


    if show_region_hues:
        hue_layer = build_region_hue_layer_from_geojson(allowed_region_names=allowed_region_names)
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
  if state.connected:
    st.success("Connected to MQTT broker!")
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
st.markdown("#### Totals over time (last 1 hour)")
# selected_regions are NAMES from the UI; convert to IDs for history filtering
region_name_to_id = {state.region_lookup.loc[rid, "region_name"]: rid
                     for rid in state.region_lookup.index}
selected_region_ids = [region_name_to_id[n]
                       for n in (selected_regions or [])
                       if n in region_name_to_id]

ts = totals_timeseries(
    state,
    sel_region_ids=selected_region_ids,
    sel_fuels=selected_fueltypes,
    bucket="5min",
    horizon_min=60
)
if not ts.empty:
    cts1, cts2 = st.columns(2)
    with cts1: st.line_chart(ts.set_index("_ts")["power_mw"], height=220)
    with cts2: st.line_chart(ts.set_index("_ts")["co2_tonnes"], height=220)
else:
    st.caption("Waiting for enough data to draw totals…")
# Map Display
st.markdown(":earth_asia: **Live map**")
deck = deck_from_df(
    fdf,
    metric,
    show_labels=show_labels,
    show_region_hues=show_region_hues,
    allowed_region_names=selected_regions,
)

st.pydeck_chart(deck,width='stretch')
  # ---- init once (near top) ----
show_legend(render_legends, show_region_hues, metric, selected_regions, selected_fueltypes, fdf)
  # DataFrame Display
# --- Latest readings (one block, two tabs) ---
with st.expander("Latest readings", expanded=False):
    tab_fac, tab_mkt = st.tabs(["Facilities", "Market"])

    with tab_fac:
        if not fdf.empty:
            st.dataframe(fdf, width="stretch")
        else:
            st.caption("Waiting for facility messages…")

    with tab_mkt:
        if not rdf.empty:
            st.dataframe(rdf, width="stretch")
        else:
            st.caption("Waiting for market messages…")

if live:
  time.sleep(int(refresh_sec))
  st.rerun()
