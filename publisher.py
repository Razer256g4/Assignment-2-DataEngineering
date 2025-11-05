import atexit
from datetime import datetime
import pandas as pd
import paho.mqtt.client as mqtt
import json
import time

PATH_TO_CACHE_FILE = "data/consolidate.csv"

class MqttPublisher:
  def __init__(self, broker:str, topic:str):
    self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    self.client.connect(broker, 1883, 60)
    self.client.loop_start()
    self.topic = topic
    time.sleep(1)
    self.warm_start()
    atexit.register(self.close)

  def publish(self, data: dict):
    result = self.client.publish(self.topic, json.dumps(data), qos=1) 
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
      print(f"Error publishing: {result.rc}")
    else:
      time.sleep(0.1)
    #   print(f"Publish result: {result}")

  def warm_start(self):
    data = {
      "timestamp": "starting...",
      "price_dmwh": 0,
      "demand_mw": 0
    }
    self.client.publish(self.topic, json.dumps(data), qos=1) 
    time.sleep(2)
    print(f"[MqttPublisher] MQTT connected")

  def close(self):
    self.client.loop_stop()
    self.client.disconnect()
    print(f"[MqttPublisher] loop stop and disconnect")


def main():
  # read cache data
  cache_df = pd.read_csv(PATH_TO_CACHE_FILE)
  print(f"[main] cache file has {len(cache_df)} records")
  facility_codes = sorted( col.removeprefix('emission_') for col in cache_df.columns if 'emission' in col )
  region_codes = sorted( col.removeprefix('price_') for col in cache_df.columns if 'price' in col )
  
  # connect to message broker
  pub = MqttPublisher("test.mosquitto.org", "nem/power_emissions")
  
  while (1): 

    # For each timestamp
    for idx, row in cache_df.iterrows():
      timestamp: str = row.pop("timestamp")
      formatted_time: str = datetime.fromisoformat(timestamp).strftime("%d-%b-%Y %H:%M")

      # publish per-facility power-emission data
      for fac in facility_codes[:2]:
        pub.publish(dict(
          # format of power-emission event
          facility_id = fac,
          timestamp = timestamp,
          power_mw = row.get(f"power_{fac}", 0),
          co2_tonnes = row.get(f"emission_{fac}", 0),
        ))

      # publish per-region price-demand data
      for reg in region_codes[:2]:
        pub.publish(dict(
          # format of price-demand event
          region_id = reg,
          timestamp = timestamp,
          price_dmwh = row.get(f"price_{reg}", 0),
          demand_mw = row.get(f"demand_{reg}", 0)
        ))

      print(f"[{idx}/{len(cache_df)}] published {len(facility_codes) + len(region_codes)} events from {formatted_time}")
      time.sleep(0.1)

    # delay before replay cache data
    print(f"[publisher] beginning 60 second delay")
    time.sleep(60)
    print(f"[publisher] re-publishing consolidate.csv")




if __name__ == "__main__":

  main()