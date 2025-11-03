#!/usr/bin/env python3
"""
NEM Tasks 1–3 (mosquitto): fetch + cache + MQTT publisher
---------------------------------------------------------
- Task 1: Retrieve per-facility 5‑minute power + CO₂ emissions from OpenElectricity for a date range.
- Task 2: Clean & integrate to a single CSV cache (one row per facility per timestamp). Optionally save a facility lookup.
- Task 3: Publish combined messages to an MQTT broker (e.g., mosquitto), in event‑time order, 0.1s between messages.
- Continuous mode: loop forever; after each full publish pass, sleep 60s (meets the assignment’s delay requirement).

Usage examples
--------------
# 0) Set your API key (required)
export OPENELECTRICITY_API_KEY=...  # see docs

# 1) Fetch one week from October 2025 (fits 5‑minute limit) and cache
python nem_tasks_1_3.py fetch \
  --start 2025-10-07T00:00:00+11:00 \
  --end   2025-10-14T00:00:00+11:00 \
  --network NEM \
  --fueltechs Coal,Gas,Wind,Solar,Hydro,Battery \
  --out-csv data/power_emissions.csv \
  --facility-csv data/facility_lookup.csv

# 2) Publish from the cached CSV to mosquitto (local broker)
#    One full pass → sleep 60s → repeat
python nem_tasks_1_3.py publish \
  --csv data/power_emissions.csv \
  --facility-csv data/facility_lookup.csv \
  --broker localhost --port 1883 --topic nem/power_emissions \
  --sleep-between 0.1 --round-delay 60

# Optional: limit facilities during development to stay well under request budgets
python nem_tasks_1_3.py fetch --limit 25 --fueltechs Wind,Solar

Requirements
------------
- Python 3.10+
- pip install: requests, pandas, paho-mqtt, python-dateutil, pyarrow (for faster CSV/parquet IO)

Notes
-----
- API limits: 5‑minute interval queries allow up to ~8 days in a single request (we chunk by facility codes).
- Endpoint shapes can evolve; the parser handles both “table/datatable” and nested series forms.
- Facility coordinates come from the facilities endpoint + optional facility CSV from Assignment 1.
"""
from __future__ import annotations

import argparse
import dataclasses
import itertools
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import requests
from dateutil import parser as dtparse

try:
    import paho.mqtt.client as mqtt
except Exception:  # pragma: no cover
    mqtt = None

API_BASE = os.environ.get("OPENELECTRICITY_API_URL", "https://api.openelectricity.org.au/v4")
API_KEY = os.environ.get("OPENELECTRICITY_API_KEY")

DEFAULT_TOPIC = "nem/power_emissions"

# -------------------------------
# Helpers
# -------------------------------

def chunked(seq: Sequence[Any], n: int) -> Iterable[Sequence[Any]]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def ensure_api_key():
    if not API_KEY:
        sys.exit("Set OPENELECTRICITY_API_KEY in your environment.")


def http_get(path: str, params: List[Tuple[str, Any]] | Dict[str, Any]) -> Dict[str, Any]:
    ensure_api_key()
    url = f"{API_BASE}{path}"
    headers = {"Authorization": f"Bearer {API_KEY}", "Accept": "application/json"}
    r = requests.get(url, headers=headers, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


# -------------------------------
# Facility catalog
# -------------------------------

def get_facilities(network_id: str = "NEM", fueltechs: Optional[List[str]] = None, status_id: Optional[List[str]] = None) -> pd.DataFrame:
    """Fetch facilities with coordinates and attributes.
    Returns DataFrame indexed by facility_code.
    """
    params: List[Tuple[str, Any]] = [("network_id", network_id)]
    if fueltechs:
        for f in fueltechs:
            params.append(("fueltech_id", f))
    if status_id:
        for s in status_id:
            params.append(("status_id", s))

    js = http_get("/facilities/", params)

    # Normalize — API may return {data:[...]}, or {table:[...]}
    records = []
    if isinstance(js, dict):
        if "table" in js:
            records = js["table"]
        elif "data" in js and isinstance(js["data"], list):
            records = js["data"]

    if not records:
        raise RuntimeError("No facilities returned; check filters/API key.")

    # Pick common fields (fallbacks are kept if present).
    df = pd.DataFrame.from_records(records)
    # Common column names seen in docs/clients
    colmap = {
        "code": "facility_code",
        "facility_code": "facility_code",
        "name": "facility_name",
        "facility_name": "facility_name",
        "network_region": "region",
        "region": "region",
        "fueltech_id": "fuel_tech",
        "fuel_tech": "fuel_tech",
        "latitude": "lat",
        "lat": "lat",
        "longitude": "lon",
        "lng": "lon",
        "lon": "lon",
    }
    for old, new in list(colmap.items()):
        if old in df.columns and new not in df.columns:
            df[new] = df[old]

    keep = ["facility_code", "facility_name", "region", "fuel_tech", "lat", "lon"]
    for k in keep:
        if k not in df.columns:
            df[k] = pd.NA
    df = df[keep].dropna(subset=["facility_code"]).drop_duplicates("facility_code").set_index("facility_code").sort_index()
    return df


# -------------------------------
# Facility time series: power + emissions @ 5m
# -------------------------------

def fetch_facility_timeseries(
    network_code: str,
    facility_codes: List[str],
    date_start: datetime,
    date_end: datetime,
    interval: str = "5m",
) -> pd.DataFrame:
    """Fetch power & emissions for facility code list; return long DataFrame.
    The API supports multiple facility_code and metrics in one request; we handle chunking.
    Output columns: [timestamp, facility_code, unit_code?, metric, value]
    """
    all_rows: List[Dict[str, Any]] = []

    # Batch facility codes to keep payloads sane
    for batch in chunked(facility_codes, 40):
        params: List[Tuple[str, Any]] = []
        # Metrics: power + emissions
        for m in ("power", "emissions"):
            params.append(("metrics", m))
        # Facilities
        for fc in batch:
            params.append(("facility_code", fc))
        params.extend(
            [
                ("interval", interval),
                ("date_start", date_start.isoformat()),
                ("date_end", date_end.isoformat()),
            ]
        )

        js = http_get(f"/data/facilities/{network_code}", params)

        # Parser tries multiple shapes. Prefer tabular if present.
        if isinstance(js, dict) and ("datatable" in js or "table" in js):
            table = js.get("datatable") or js.get("table")
            df = pd.DataFrame.from_records(table)
            # Heuristic: standardize column names
            colmap = {
                "period": "timestamp",
                "timestamp": "timestamp",
                "facility_code": "facility_code",
                "unit_code": "unit_code",
                "power": "power",
                "emissions": "emissions",
                "value": "value",
                "metric": "metric",
            }
            for old, new in list(colmap.items()):
                if old in df.columns and new not in df.columns:
                    df[new] = df[old]

            # If wide shape (power/emissions columns), melt to long
            if {"power", "emissions"}.issubset(df.columns):
                df_long = df.melt(
                    id_vars=[c for c in ["timestamp", "facility_code", "unit_code"] if c in df.columns],
                    value_vars=["power", "emissions"],
                    var_name="metric",
                    value_name="value",
                )
            elif "value" in df.columns and "metric" in df.columns:
                df_long = df[[c for c in ["timestamp", "facility_code", "unit_code", "metric", "value"] if c in df.columns]].copy()
            else:
                # Fallback: keep as-is
                df_long = df.copy()

            all_rows.extend(df_long.to_dict(orient="records"))
            continue

        # Nested shape: { data: [ {metric, unit, series/results:[{name, columns, data:[{timestamp,value}]}]} ] }
        if isinstance(js, dict) and isinstance(js.get("data"), list):
            for series in js["data"]:
                metric = series.get("metric")
                # some variants: series["series"] or series["results"]
                results = series.get("series") or series.get("results") or []
                for res in results:
                    name = res.get("name") or res.get("id")
                    cols = res.get("columns", {})
                    fac = cols.get("facility_code") or cols.get("facility") or name
                    unit_code = cols.get("unit_code") or cols.get("unit")
                    for point in res.get("data", []):
                        # point could be list [ts, val] or dict
                        if isinstance(point, (list, tuple)) and len(point) >= 2:
                            ts, val = point[0], point[1]
                        elif isinstance(point, dict):
                            ts = point.get("timestamp") or point.get("period")
                            val = point.get("value")
                        else:
                            continue
                        all_rows.append({
                            "timestamp": ts,
                            "facility_code": fac,
                            "unit_code": unit_code,
                            "metric": metric,
                            "value": val,
                        })
            continue

        raise RuntimeError("Unexpected facility timeseries response shape. Inspect API output.")

    if not all_rows:
        return pd.DataFrame(columns=["timestamp", "facility_code", "unit_code", "metric", "value"])

    df_long = pd.DataFrame.from_records(all_rows)
    # Coerce types
    df_long["timestamp"] = pd.to_datetime(df_long["timestamp"], utc=True, errors="coerce")
    df_long = df_long.dropna(subset=["timestamp", "facility_code", "metric"]).reset_index(drop=True)
    return df_long


def to_facility_level(df_long: pd.DataFrame) -> pd.DataFrame:
    """Aggregate unit-level to facility-level wide form with power_mw + co2_tonnes."""
    if df_long.empty:
        return pd.DataFrame(columns=["timestamp", "facility_code", "power_mw", "co2_tonnes"])

    # Pivot metric -> columns, sum across units
    df_piv = (
        df_long.pivot_table(
            index=["timestamp", "facility_code"],
            columns="metric",
            values="value",
            aggfunc="sum",
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )
    # Rename to expected output names
    if "power" in df_piv.columns:
        df_piv = df_piv.rename(columns={"power": "power_mw"})
    if "emissions" in df_piv.columns:
        df_piv = df_piv.rename(columns={"emissions": "co2_tonnes"})

    # Keep only useful columns
    for c in ["power_mw", "co2_tonnes"]:
        if c not in df_piv.columns:
            df_piv[c] = 0.0

    # Sort by event time
    return df_piv.sort_values(["timestamp", "facility_code"]).reset_index(drop=True)


# -------------------------------
# MQTT Publisher
# -------------------------------

def make_payload(row: pd.Series, flookup: Optional[pd.DataFrame]) -> Dict[str, Any]:
    fid = row["facility_code"]
    payload = {
        "facility_id": fid,
        "timestamp": row["timestamp"].isoformat(),
        "power_mw": float(row.get("power_mw", 0.0) or 0.0),
        "co2_tonnes": float(row.get("co2_tonnes", 0.0) or 0.0),
    }
    if flookup is not None and fid in flookup.index:
        r = flookup.loc[fid]
        payload.update({
            "facility_name": r.get("facility_name"),
            "region": r.get("region"),
            "fuel_tech": r.get("fuel_tech"),
            "lat": float(r.get("lat")) if pd.notna(r.get("lat")) else None,
            "lon": float(r.get("lon")) if pd.notna(r.get("lon")) else None,
        })
    return payload


def publish_csv(
    csv_path: str,
    facility_csv: Optional[str],
    broker: str,
    port: int,
    topic: str,
    sleep_between: float = 0.1,
    round_delay: float = 60.0,
):
    if mqtt is None:
        raise RuntimeError("paho-mqtt not installed. pip install paho-mqtt")

    df = pd.read_csv(csv_path, parse_dates=["timestamp"], dtype={"facility_code": str})
    if df.empty:
        print("No rows in cache; nothing to publish.")
        time.sleep(round_delay)
        return

    flookup = None
    if facility_csv and os.path.exists(facility_csv):
        flookup = pd.read_csv(facility_csv).set_index("facility_code")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect(broker, port, keepalive=60)
    client.loop_start()

    try:
        while True:
            # Publish in event-time order
            for _, row in df.sort_values(["timestamp", "facility_code"]).iterrows():
                payload = make_payload(row, flookup)
                client.publish(topic, json.dumps(payload), qos=1)
                print("→", payload)
                time.sleep(max(0.001, sleep_between))
            # After a full pass, wait
            print(f"Round complete. Sleeping {round_delay}s…")
            time.sleep(max(0.0, round_delay))
    finally:
        client.loop_stop()
        client.disconnect()


# -------------------------------
# CLI
# -------------------------------
@dataclasses.dataclass
class FetchArgs:
    start: str
    end: str
    network: str = "NEM"
    fueltechs: Optional[str] = None  # comma‑sep
    limit: Optional[int] = None
    out_csv: str = "data/power_emissions.csv"
    facility_csv: Optional[str] = None


def cmd_fetch(a: FetchArgs):
    os.makedirs(os.path.dirname(a.out_csv) or ".", exist_ok=True)
    fuel = [s.strip() for s in a.fueltechs.split(",") if s.strip()] if a.fueltechs else None

    # 1) Facilities
    fac_df = get_facilities(network_id=a.network, fueltechs=fuel, status_id=["operating"])
    if a.limit:
        fac_df = fac_df.head(a.limit)
    if a.facility_csv:
        os.makedirs(os.path.dirname(a.facility_csv) or ".", exist_ok=True)
        fac_df.reset_index().to_csv(a.facility_csv, index=False)
        print(f"Saved facility lookup → {a.facility_csv}  ({len(fac_df):,} facilities)")

    # 2) Timeseries (5‑minute)
    start_dt = dtparse.isoparse(a.start)
    end_dt = dtparse.isoparse(a.end)

    ts_long = fetch_facility_timeseries(a.network, fac_df.index.tolist(), start_dt, end_dt, interval="5m")
    fac_ts = to_facility_level(ts_long)

    fac_ts.to_csv(a.out_csv, index=False)
    print(f"Saved power+emissions cache → {a.out_csv}  ({len(fac_ts):,} rows)")


@dataclasses.dataclass
class PublishArgs:
    csv: str
    facility_csv: Optional[str]
    broker: str = "localhost"
    port: int = 1883
    topic: str = DEFAULT_TOPIC
    sleep_between: float = 0.1
    round_delay: float = 60.0


def cmd_publish(a: PublishArgs):
    publish_csv(
        csv_path=a.csv,
        facility_csv=a.facility_csv,
        broker=a.broker,
        port=a.port,
        topic=a.topic,
        sleep_between=a.sleep_between,
        round_delay=a.round_delay,
    )


def main(argv: Optional[List[str]] = None):
    p = argparse.ArgumentParser(description="NEM Tasks 1–3: fetch + cache + MQTT publisher (mosquitto)")
    sp = p.add_subparsers(dest="cmd", required=True)

    p_fetch = sp.add_parser("fetch", help="Fetch week of 5‑minute power+emissions and cache as CSV")
    p_fetch.add_argument("--start", required=True, help="ISO8601 start e.g. 2025-10-07T00:00:00+11:00")
    p_fetch.add_argument("--end", required=True, help="ISO8601 end   e.g. 2025-10-14T00:00:00+11:00")
    p_fetch.add_argument("--network", default="NEM")
    p_fetch.add_argument("--fueltechs", default="Coal,Gas,Wind,Solar,Hydro,Battery", help="Comma‑separated fueltech filters (optional)")
    p_fetch.add_argument("--limit", type=int, default=None, help="Limit number of facilities (for testing)")
    p_fetch.add_argument("--out-csv", default="data/power_emissions.csv")
    p_fetch.add_argument("--facility-csv", default="data/facility_lookup.csv")

    p_pub = sp.add_parser("publish", help="Publish cached CSV to MQTT in event‑time order; loop forever with round delay")
    p_pub.add_argument("--csv", required=True)
    p_pub.add_argument("--facility-csv", default="data/facility_lookup.csv")
    p_pub.add_argument("--broker", default="localhost")
    p_pub.add_argument("--port", type=int, default=1883)
    p_pub.add_argument("--topic", default=DEFAULT_TOPIC)
    p_pub.add_argument("--sleep-between", type=float, default=0.1, help="Delay between messages (seconds)")
    p_pub.add_argument("--round-delay", type=float, default=60.0, help="Delay after finishing a full pass (seconds)")

    args = p.parse_args(argv)

    if args.cmd == "fetch":
        cmd_fetch(FetchArgs(
            start=args.start,
            end=args.end,
            network=args.network,
            fueltechs=args.fueltechs,
            limit=args.limit,
            out_csv=args.out_csv,
            facility_csv=args.facility_csv,
        ))
    elif args.cmd == "publish":
        cmd_publish(PublishArgs(
            csv=args.csv,
            facility_csv=args.facility_csv,
            broker=args.broker,
            port=args.port,
            topic=args.topic,
            sleep_between=args.sleep_between,
            round_delay=args.round_delay,
        ))


if __name__ == "__main__":
    main()
