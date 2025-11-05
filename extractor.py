import os
import json
import requests
import pandas as pd

from typing import Literal
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()
""" ENVIRONMENT VARIABLE SETUP
You need a .env file in project directory with OPENELECTRICITY_API_KEY
Sign up & create API KEY for Open Electricity https://platform.openelectricity.org.au/"
"""

BASE_URL = "https://api.openelectricity.org.au/v4"
HEADERS = {"Authorization" : f"Bearer {os.environ['OPENELECTRICITY_API_KEY']}"}
PATH_FACILITY_BATCHES = "data/facility_batches"
PATH_POWER_EMISSIONS = "data/power_emissions"
PATH_PRICE_DEMAND = "data/price_demand"
# PATH_TO_FACILITIES_JSON = "nem_facilities.json"
# PATH_TO_FUELTECH_JSON = "fueltech_map.json"
from context import nem_facilities_json, fueltech_map_json
PATH_TO_FACILITY_LOOKUP_CSV = "data/facility_lookup.csv"
PATH_TO_CONSOLIDATE_CSV = "data/consolidate.csv"

def queryStringFormulator(
        interval: Literal["5m", "15m", "30m", "1h", "6h", "12h", "1d"] = None,
        metrics: list[str] = None,
        network_region: list[str] = None, 
        facility_code: list[str] = None,
        primary_grouping: Literal['network', 'network_region'] = None,
        date_start: str = None,
        date_end: str = None
):
    """ General URL Query String for Open Electricity """
    params = []
    if interval:
        params.append(f"interval={interval}")
    if primary_grouping:
        params.append(f"primary_grouping={primary_grouping}")
    if date_start:
        params.append(f"date_start={date_start}")
    if date_end:
        params.append(f"date_end={date_end}")
    # Open Electricity Platform uses repeated query parameters for multiple metrics instead of comman-separated values
    if metrics:
        [ params.append(f"metrics={metric}") for metric in metrics ]            
    if network_region:
        [ params.append(f"network_region={reg}") for reg in network_region ]
    if facility_code:
        [ params.append(f"facility_code={fac}") for fac in facility_code ]

    return "?" + "&".join(params) if params else ""


def batch_facilities(batch_size: int = 25) -> dict:
    """ 
    1. Filter for NEM facilities with at least 1 operating unit
    2. Batch facility_codes into batches of batch_size; saved to data/facility_batches
    3. Return a map for {unit_code: facility_code}
    """
    
    # 1. Filter for NEM facilities with at least 1 operating unit
    # facilities: dict = json.load(open(PATH_TO_FACILITIES_JSON, "r")).get("data")
    facilities: dict = nem_facilities_json.get("data")
    assert facilities != None, "facilities should not be None"
    # fueltech_map: dict = json.load(open(PATH_TO_FUELTECH_JSON, "r")) # https://docs.openelectricity.org.au/guides/fueltechs
    fueltech_map: dict = fueltech_map_json
    facility_table = []
    unit_to_facility_map = {}

    for fac in facilities: # facility level
        
      fueltech = []
      facility_has_operating_unit = False
      loc = fac.get("location")

      for unit in fac.get("units"):  # unit level
          
        if unit.get("status_id") == "operating":
          facility_has_operating_unit = True
          fueltech.append(unit.get("fueltech_id"))
          unit_to_facility_map[unit.get("code")] = fac.get("code")

      if facility_has_operating_unit:
          facility_table.append(dict(
            facility_id = fac.get("code"),
            facility_name = fac.get("name"),
            lat = loc.get("lat"),
            lon = loc.get("lng"),
            region = fac.get("network_region"),
            fuel_tech = [ff for ff in {fueltech_map[f].get("label") for f in fueltech} if ff != '-'],
         ))
          
    # 2. Batch facility_codes into batches of batch_size; saved to data/facility_batches
    os.makedirs(PATH_FACILITY_BATCHES, exist_ok=True)
    facility_codes = [ fac.get("facility_id") for fac in facility_table]
    for batch_num, i in enumerate(range(0, len(facility_codes), batch_size), start=1):
       # i: 0, 25, 50 ...
       # batch_num: 1, 2, 2
       batch = facility_codes[i: i+batch_size]
       json.dump(batch, open(f"{PATH_FACILITY_BATCHES}/batch_{batch_num}.json", "w"), indent =2)
    print(f"[batch_facilities] {len(os.listdir(PATH_FACILITY_BATCHES))} batches of facility code")

    # Facility Lookup for dashboard.py
    fdf = pd.DataFrame(facility_table)
    fdf['fuel_tech'] = fdf['fuel_tech'].apply(json.dumps)
    fdf.to_csv(PATH_TO_FACILITY_LOOKUP_CSV, index=False)
    
    #3. Return a map for {unit_code: facility_code}
    return unit_to_facility_map


def fetch_with_retry(url: str, query_params: dict, retry_limit:int =3):
  """ Retry mechanism to overcome instability of OEM API Server """
  query_string = queryStringFormulator(**query_params)
  retry_counter = 0

  while (retry_counter < retry_limit):
    response = requests.get(url+query_string, headers=HEADERS, allow_redirects=True)
    
    if response.status_code!= 200:
      print(f"[fetch_with_retry] error on attempt {retry_counter}: [{response.status_code}] {response.text}")
      retry_counter+=1

      if retry_counter == retry_limit:
         print(f"[fetch_with_retry] exit after {retry_limit} attempts")
         return None
      
      continue

    retry_counter = 999
  return response.json()


def fetch_facility_data(interval=7):
  """ Fetch NEM facility data in batches <br>
  Refer to https://docs.openelectricity.org.au/api-reference/data/get-facility-data
  """
   
  os.makedirs(PATH_POWER_EMISSIONS, exist_ok=True)
  date_start = (datetime.today().date() - timedelta(days=interval)).isoformat()
  date_end = datetime.today().date().isoformat()
  print(f"[fetch_facility_data] fetching facility data from {date_start} to {date_end}")
   
  for file_name in os.listdir(PATH_FACILITY_BATCHES):
    batch_num = int(file_name.removeprefix('batch_').removesuffix(".json"))
    facility_codes = json.load(open(PATH_FACILITY_BATCHES + "/" + file_name, "r"))
    
    print("[fetch_facility_data] Batch Number ", batch_num)
    response: dict = fetch_with_retry(
      f"{BASE_URL}/data/facilities/NEM",
      dict(
        interval = '5m',
        metrics = ['power', 'emissions'],
        date_start = date_start,
        date_end = date_end,
        facility_code = facility_codes
      )
    )

    if response:
        json.dump(response, open(PATH_POWER_EMISSIONS + f"/batch_{batch_num}.json", "w"))


def fetch_market_data(interval=7):
  """ Fetch NEM market data 
  Refer to https://docs.openelectricity.org.au/api-reference/market/get-network-data
  """

  os.makedirs(PATH_PRICE_DEMAND, exist_ok=True)

  response: dict = fetch_with_retry(
    f"{BASE_URL}/market/network/NEM",
    dict(
      interval = '5m',
      metrics = ['price', 'demand'],
      date_start = (datetime.today().date() - timedelta(days=interval)).isoformat(),
      date_end = datetime.today().date().isoformat(),
      primary_grouping="network_region"
    )
  )

  if response:
    json.dump(response, open(PATH_PRICE_DEMAND + f"/market_by_region.json", "w"))
    print(f"[fetch_market_data] Market data fetched")
     

def transform_facility_data(unit_to_facility: dict):
  """ Returns a facility level dataset<br>
  <b>facility_df</b>
  - facility_code (str)
  - timestamp (str)
  - power (float)
  - emission (float)
  """
  unit_records = []
  facility_set = set()
  for file_name in os.listdir(PATH_POWER_EMISSIONS):
    
    print(f"[transform_facility_data] transforming facility data", file_name)
    batch_records = []
    power, emissions = json.load(open(PATH_POWER_EMISSIONS + "/" + file_name, "r")).get("data")
    assert len(power.get("results")) == len(emissions.get("results")), "power and emissions should have same length"
    power_emission_by_unit = list(zip(power.get("results"), emissions.get("results")))

    for pow, emi in power_emission_by_unit: # unit level
       
      pow_unit_code = pow.get("columns").get("unit_code")
      emi_unit_code = emi.get("columns").get("unit_code")
      assert pow_unit_code == emi_unit_code, f"Power Unit: {pow_unit_code} | Emission Unit: {emi_unit_code}"
      
      if unit_to_facility.get(pow_unit_code) == None:
        print(f"[transform_facility_data] {pow_unit_code} not in operation anymore")
        continue
      
      facility_set.add(unit_to_facility[pow_unit_code])
      interval_list = list(zip(pow.get("data"), emi.get("data")))

      for p_read, e_read in interval_list: # timestamp level
        
        p_time, p_mw = p_read
        e_time, e_t = e_read
        assert p_time == e_time, f"Power Timestamp: {p_time} | Emission Timestamp {e_time}"

        batch_records.append(dict(
          facility_code = unit_to_facility[pow_unit_code],
          unit_code = pow_unit_code,
          timestamp = p_time,
          power = p_mw,
          emission = e_t
        ))

      unit_records.extend(batch_records)

  print(f"[transform_facility_data] fetched facility data has {len(facility_set)} facility codes")
  
  # sum up unit(s) power and emission for a facility at a timestamp
  facility_df = (
    pd.DataFrame(unit_records)
    .groupby(["facility_code", "timestamp"])
    .agg({"power": "sum", "emission": "sum"})
  ).reset_index()

  return facility_df


def transform_market_data():
  """ Returns a region level dataset<br>
  <b>market_df</b>
  - region_code (str)
  - timestamp (str)
  - price (float)
  - demand (float)
  """
  region_records = []
  region_set = set()
  price, demand =  json.load(open(PATH_PRICE_DEMAND + "/market_by_region.json", "r")).get("data")
  assert len(price) == len(demand), "price and demand should have same number of regions"
  price_demand_by_region = list(zip(price.get("results"), demand.get("results")))

  for p_region, d_region in price_demand_by_region: # region level
     
    p_region_code = p_region.get("name","").removeprefix("price_")
    d_region_code = d_region.get("name","").removeprefix("demand_")
    assert p_region_code == d_region_code, f"region code should be the same. price: {p_region.get('name')} | demand: {d_region.get('name')}"
    interval_list = list(zip(p_region.get("data"), d_region.get("data")))
    region_set.add(p_region_code)

    for p_read, d_read in interval_list: # timestamp level
       
      p_time, p_dmwh = p_read
      d_time, d_mw = d_read
      assert p_time == d_time, "timestamp should be the same"

      region_records.append(dict(
        region_code = p_region_code,
        timestamp = p_time,
        price = p_dmwh,
        demand = d_mw
      ))
  
  print(f"[transform_market_data] fetched market data has {len(region_set)} regions")

  return pd.DataFrame(region_records)


def main():

  # Batch Requests
  unit_to_facility: dict = batch_facilities()
  # Fetch Data
  fetch_facility_data()
  fetch_market_data()
  # Transforma
  facility_df = transform_facility_data(unit_to_facility)
  market_df = transform_market_data()
  # Optimize
  facility_pivot = facility_df.pivot_table(index="timestamp", values=['power', 'emission'], columns="facility_code")
  facility_pivot.columns = [f"{col[0]}_{col[1]}" for col in facility_pivot.columns]
  market_pivot = market_df.pivot_table(index="timestamp", values=['price', 'demand'], columns="region_code")
  market_pivot.columns = [f"{col[0]}_{col[1]}" for col in market_pivot.columns]
  # Merge
  cache_df = facility_pivot.merge(market_pivot, on="timestamp", how="left")
  cache_df.to_csv(PATH_TO_CONSOLIDATE_CSV)
  print(f"[extractor] success")


if __name__ == "__main__":
  main()