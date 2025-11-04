import pandas as pd
import paho.mqtt.client as mqtt
import json
import time

df = pd.read_csv("consolidate.csv")
facilities = sorted( col.split('_', 1)[-1] for col in df.columns if 'emission' in col)

class MqttPublisher:
  def __init__(self):
    self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    self.client.connect("test.mosquitto.org", 1883, 60)
    # self.client.connect("localhost", 1883, 60)
    self.client.loop_start()
    self.topic = "nem/power_emissions"
    time.sleep(1)
    self.warm_start()

  def publish(self, data: dict):
    result = self.client.publish(self.topic, json.dumps(data), qos=1) 
    # status = result.wait_for_publish(timeout=5)
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
      print(f"Error publishing: {result.rc}")
    else:
      print(f"Publish result: {result}")
    # if not status:
    #   print(f"Warning: Publish timeout for {data}")
    # time.sleep(1)

  def warm_start(self):
    data = {
      "kind": "market",       
      "market_id": "NEM",
      "timestamp": "starting...",
      "price_dmwh": 0,
      "demand_mw": 0
    }
    self.client.publish(self.topic, json.dumps(data), qos=1) 
    time.sleep(2)

  def close(self):
    self.client.loop_stop()
    self.client.disconnect()


if __name__ == "__main__":
  pub = MqttPublisher()
  
  # For each timestamp
  for idx, row in df.iterrows():
    timestamp = row.pop("timestamp")
    
    # Publish per-facility power-emission data
    for fac in facilities:
      data = dict(
          kind="facility",
        facility_id = fac,
        timestamp = timestamp,
        power_mw = row.get(f"power_{fac}", 0),
        co2_tonnes = row.get(f"emission_{fac}", 0)
      )
      # print(data)
      pub.publish(data)

    # Publish per-market price-demand data
    data = dict(
      kind="market",
      market_id="NEM",
      timestamp = timestamp,
      price_dmwh = row.get('NEM_price',  row.get('price_NEM', 0)),
      demand_mw = row.get('NEM_demand', row.get('demand_NEM', 0)),
    )
    # print(data)
    pub.publish(data)
    print(f"row {idx}/{len(df)} published")
    time.sleep(0.1)