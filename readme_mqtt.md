How to run it (quick)

Install deps

pip install requests pandas paho-mqtt python-dateutil pyarrow


Run a local mosquitto (Docker):

docker run --rm -p 1883:1883 eclipse-mosquitto:2


Fetch one week (Oct 7 → Oct 14, 2025)

export OPENELECTRICITY_API_KEY=YOUR_KEY
python nem_tasks_1_3.py fetch \
  --start 2025-10-07T00:00:00+11:00 \
  --end   2025-10-14T00:00:00+11:00 \
  --network NEM \
  --out-csv data/power_emissions.csv \
  --facility-csv data/facility_lookup.csv


Publish to MQTT (loops forever; after each pass sleeps 60 s)

python nem_tasks_1_3.py publish \
  --csv data/power_emissions.csv \
  --facility-csv data/facility_lookup.csv \
  --broker localhost --port 1883 --topic nem/power_emissions \
  --sleep-between 0.1 --round-delay 60

JSON payload (what my Streamlit dashboard already accepts)
{
  "facility_id": "VIC_WIND_01",
  "facility_name": "Bass Strait Wind",
  "lat": -37.8136,
  "lon": 144.9631,
  "region": "VIC",
  "fuel_tech": "Wind",
  "timestamp": "2025-10-07T12:05:00+11:00",
  "power_mw": 182.4,
  "co2_tonnes": 0.0
}

Notes

Auth & base URL: Authorization: Bearer <token>, base https://api.openelectricity.org.au/v4. 
docs.openelectricity.org.au

Endpoints used:

Facilities list: GET /v4/facilities (filter by network_id=NEM, status_id=operating, fueltech_id=*). 
docs.openelectricity.org.au

Facility data: GET /v4/data/facilities/{network_code} with metrics=power&metrics=emissions, interval=5m, date_start, date_end, facility_code=.... 
docs.openelectricity.org.au

Range: 5-minute requests allow ~8 days per call; the script chunks facility codes and fits the week in one interval window. 
docs.openelectricity.org.au

Want me to add a --live-tail mode that fetches the latest 5-minute window every 60 s and only publishes new rows?