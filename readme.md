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
cd Assignment-2-DataEngineering
# activate python env with vene or conda

# From new terminal, extract data from OEM
python extractor.py

# From new terminal, start Dashboard
streamlit run dashboard.py

# From new terminal, start Publishing from Cache file
python publisher.py
```