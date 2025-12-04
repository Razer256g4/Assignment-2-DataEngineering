# Events
### power emission event
```
{
  "facility_id": "NEM_VIC_HAZELWOOD_1",
  # "facility_name": "Hazelwood",
  # "lat": -38.266,
  # "lon": 146.394,
  # "region": "VIC",
  # "fuel_tech": ["Coal"],
  "timestamp": "2025-10-15T10:05:00+11:00",
  "power_mw": 245.7,
  "co2_tonnes": 212.48
}
```
### price demand event
```
{
  "region_id": "VIC1",
  "timestamp": "2025-10-15T10:05:00+11:00",
  "price_dmwh": 34.12,
  "demand_mw": 142.0
}
```

# Documents to submit
1. requirements.txt
2. context.py
2. extractor.py
2. publisher.py
3. dashboard.py

# Run
```
python version 3.12.12

cd Assignment-2-DataEngineering
# activate python env with vene or conda

# From new terminal, extract data from OEM
python extractor.py

# From new terminal, start Dashboard
streamlit run dashboard.py

# From new terminal, start Publishing from Cache file
python publisher.py
```
## 1. Project Overview

This project simulates a near-real-time data pipeline for the Australian National Electricity Market (NEM):

- Historical **per-facility power & emissions** and **per-region price & demand** are pulled from the OpenElectricity (OE) API.
- The data is cleaned, aggregated, and saved into a compact cache file.
- A **publisher** replays these records as a live stream to a public MQTT broker.
- A **Streamlit dashboard** subscribes to this topic, enriches messages with lookup metadata, and renders an interactive map and time-series charts.

The system is designed so that you can run everything locally with three Python processes: extractor, dashboard, and publisher. :contentReference[oaicite:1]{index=1}

---

## 2. System Architecture

The system has four main components:

1. **Data Extraction (`extractor.py`)**
   - Downloads one week of 5-minute data from OpenElectricity:
     - Unit-level power & emissions for each NEM facility.
     - Region-level price & demand.
   - Aggregates units into **facility-level** readings.
   - Produces a single consolidated cache file (`consolidate.csv`) indexed by timestamp.

2. **Publisher (`publisher.py`)**
   - Loads `consolidate.csv` into memory.
   - For each timestamp, emits:
     - Per-facility events: `{facility_code, timestamp, power, emissions}`
     - Per-region events: `{region_code, timestamp, price, demand}`
   - Publishes messages sequentially to a public Eclipse Mosquitto broker (QoS 1).
   - Replays the entire week in a loop with a short delay between “cycles” to simulate continuous streaming.

3. **MQTT Broker**
   - Uses the public **Eclipse Mosquitto** broker (no self-hosted broker required).
   - The publisher and dashboard share a topic (e.g. `comp5339/t01_group8`).

4. **Dashboard (`dashboard.py`)**
   - Streamlit app that:
     - Connects to the MQTT broker in a background thread.
     - Subscribes to the topic and validates messages using **Pydantic** models.
     - Enriches events with static metadata from lookup tables (facility & region).
     - Maintains rolling windows of recent data in an application-level state object.
     - Renders:
       - A PyDeck map of facilities.
       - Aggregated metrics for power, emissions, price, and demand.
       - Time-series charts for selected metrics.
       - Tables of the most recent facility or market events. :contentReference[oaicite:2]{index=2}

---

## 3. Data Pipeline Details

### 3.1 Facility Lookup and Metadata

- Facilities are initially obtained from the OE facilities API.
- Raw responses are at **unit level**; a facility can have multiple units.
- A facility is treated as **operational** if at least one of its units is operating.
- Unit-level fueltech codes are mapped to broader fuel categories using OE’s fueltech mapping.
- A facility-level lookup table is built with:
  - Name, NEM code, latitude, longitude, NEM region, and list of fueltech groups.
- A unit → facility hashmap is created so that later unit-level readings can be rolled up to facility level. :contentReference[oaicite:3]{index=3}

### 3.2 Power & Emission Data

- Facility codes are split into batches to comply with OE’s API limits.
- For each batch:
  - Unit-level records are pulled with:
    - 7-day window.
    - 5-minute interval.
  - Responses are zipped:
    - First by unit to pair power and emissions.
    - Then by timestamp so all records at the same instant are grouped.
- Each record is mapped via the unit → facility hashmap, then grouped by:
  - `(facility_code, timestamp)` with sums over power and emissions.
- The output is a **facility-level time series** for power and CO₂ emissions. :contentReference[oaicite:4]{index=4}

### 3.3 Region-level Price & Demand Data

- Region-level market data is pulled from OE’s network API:
  - Same 7-day, 5-minute granularity.
  - Grouped by **primary NEM region**.
- Similar zipping logic is used to group region price and demand by timestamp. :contentReference[oaicite:5]{index=5}

### 3.4 Consolidated Cache (`consolidate.csv`)

- Facility and region dataframes are pivoted:
  - Index: timestamp.
  - Columns: facility or region codes.
  - Values: power/emissions (for facilities), price/demand (for regions).
- The pivot tables are merged on timestamp (left join to keep all facility records).
- The final joined table is written as `consolidate.csv`.

This pivoted design avoids storing the same timestamp and code strings repeatedly; in the report we estimate roughly a **75% reduction** in file size (around 8MB vs ~32MB naive layout). :contentReference[oaicite:6]{index=6}

---

## 4. Streaming & Dashboard Behaviour

### 4.1 Publisher Loop

- Reads `consolidate.csv` into a Pandas dataframe sorted by timestamp.
- For each row (timestamp), iterates through:
  - Facility columns → emits power/emission events.
  - Region columns → emits price/demand events.
- Inserts a short delay (e.g. 0.1s) between events to mimic streaming.
- After reaching the end of the dataframe:
  - Waits a fixed cooldown (e.g. 60s).
  - Starts again from the first timestamp.
- In a real deployment, a set of `(timestamp, facility_code)` pairs would be maintained so duplicate events are not re-sent. :contentReference[oaicite:7]{index=7}

### 4.2 Dashboard Logic

- On startup:
  - Loads static lookup tables (from PostgreSQL in A1; here simulated via CSV/context).
  - Initialises a cached application state object holding:
    - Lookup tables.
    - Rolling deques of recent records.
    - Maps of latest values by facility and region.
- When the user presses **Connect / Reconnect**:
  - A background MQTT client is created.
  - It subscribes to the configured topic (QoS 1) and starts processing messages.
- For each message:
  - Determined as either facility or market event.
  - Parsed and validated with a Pydantic model.
  - Enriched with facility/region metadata.
  - Under a thread lock, the shared state is updated:
    - Latest readings dicts.
    - Historical deques for plotting.

The Streamlit app reruns on a live refresh timer (e.g. every 3 seconds). Each rerun:

- Builds dataframes from shared state.
- Computes aggregated metrics.
- Renders:
  - **Filters** for region and fuel type.
  - A **PyDeck map**:
    - Icons for facilities.
    - Icon size scales with power / emissions.
    - GeoJSON polygons delineating NEM regions.
    - Optional labels and marker visibility toggles.
  - Time-series charts for power and emissions.
  - A table of recent facility or market events. :contentReference[oaicite:8]{index=8}

---

## 5. Optimisation & Efficiency

Key decisions to keep the system responsive and lightweight:

- **API robustness**
  - OE endpoints can be unreliable; the extraction code uses a retry loop with delays and a maximum retry count for price/demand queries.

- **Compact cache design**
  - Using timestamp-indexed pivot tables dramatically reduces repetition of timestamp and code strings.
  - The final cache is roughly 8MB for a week of data at 5-minute resolution.

- **Lean event payloads**
  - Events carry just identifiers plus changing values (e.g. `facility_code`, `power`, `emissions`).
  - Static metadata stays in lookup tables, lowering bandwidth and processing overhead.

- **In-memory lookups**
  - Facility and region metadata are loaded once into memory when the dashboard starts.
  - This avoids frequent database round-trips and keeps the UI responsive.

- **Data validation**
  - Pydantic models ensure each incoming message matches the expected schema before it reaches the shared state. :contentReference[oaicite:9]{index=9}

---

## 6. Project Structure

A typical repository layout for this assignment might look like:

```text
.
├── extractor.py        # Fetch & integrate data from OpenElectricity
├── publisher.py        # MQTT publisher (replays consolidate.csv)
├── dashboard.py        # Streamlit dashboard (subscriber + visualisation)
├── context.py          # Cached lookup data / simulated DB tables
├── consolidate.csv     # Generated consolidated time series cache
├── requirements.txt    # Python dependencies
├── README.md           # This file
