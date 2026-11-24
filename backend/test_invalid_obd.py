
import json
import time
import uuid
import paho.mqtt.client as mqtt

truck_id = "TRUCK-INVALID-OBD-TEST"

payload = {
    "eventId": str(uuid.uuid4()),
    "truckId": truck_id,
    "deviceId": f"DEVICE-{truck_id}",
    "bootId": str(uuid.uuid4()),
    "sequenceNumber": 1,
    "schemaVersion": 1,
    "eventTime": int(time.time() * 1000),
    "timestamp": "2026-11-24T17:00:00+00:00",
    "engineRPM": 15000,
    "fuelLevel": 50.0,
    "engineTemperature": 80.0,
    "batteryVoltage": 13.5
}

client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    client_id=f"invalid-obd-{uuid.uuid4()}"
)

client.connect("localhost", 1883, 60)
client.loop_start()

info = client.publish(
    f"fleet/company-1/{truck_id}/obd",
    json.dumps(payload),
    qos=1
)

info.wait_for_publish()

print("Published invalid OBD telemetry:")
print(json.dumps(payload, indent=2))

client.loop_stop()
client.disconnect()
