
import json
import time
import uuid

import paho.mqtt.client as mqtt

BROKER = "localhost"
PORT = 1883

truck_id = "TRUCK-INVALID-TEST"

payload = {
    "eventId": str(uuid.uuid4()),
    "truckId": truck_id,
    "deviceId": f"DEVICE-{truck_id}",
    "bootId": str(uuid.uuid4()),
    "sequenceNumber": 1,
    "schemaVersion": 1,
    "eventTime": int(time.time() * 1000),
    "timestamp": "2026-11-24T17:00:00+00:00",
    "latitude": 120.0,       # Deliberately invalid
    "longitude": 72.95,
    "speed": 60.0
}

client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    client_id=f"invalid-test-{uuid.uuid4()}"
)

client.connect(BROKER, PORT, 60)
client.loop_start()

info = client.publish(
    f"fleet/company-1/{truck_id}/gps",
    json.dumps(payload),
    qos=1
)

info.wait_for_publish()

print("Published deliberately invalid GPS telemetry:")
print(json.dumps(payload, indent=2))

client.loop_stop()
client.disconnect()
