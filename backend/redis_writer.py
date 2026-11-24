
import json
from datetime import datetime

import redis
from kafka import KafkaConsumer


# Connect to Redis
redis_client = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=True
)

# Only update a field if the incoming message is newer.
UPDATE_LUA = """
local old_timestamp = redis.call('HGET', KEYS[1], ARGV[2])

if old_timestamp == false or tonumber(ARGV[3]) > tonumber(old_timestamp) then
    redis.call('HSET', KEYS[1],
        ARGV[1], ARGV[4],
        ARGV[2], ARGV[3]
    )
    return 1
end

return 0
"""

update_script = redis_client.register_script(UPDATE_LUA)


def timestamp_to_seconds(timestamp):
    dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    return dt.timestamp()


# Connect to both processed Kafka topics
consumer = KafkaConsumer(
    "gps-latest",
    "obd-processed",
    bootstrap_servers="localhost:9092",
    group_id="fleet-redis-writer",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

print("Redis writer started. Waiting for GPS and OBD messages...")

for message in consumer:
    data = message.value
    truck_id = data["truckId"]

    redis_key = f"truck:{truck_id}"
    timestamp = timestamp_to_seconds(data["timestamp"])

    if message.topic == "gps-latest":
        field = "gps"
        timestamp_field = "gps_timestamp"

        value = {
            "latitude": data["latitude"],
            "longitude": data["longitude"],
            "speed": data["speed"]
        }

    elif message.topic == "obd-processed":
        field = "obd"
        timestamp_field = "obd_timestamp"

        value = {
            "engineRPM": data["engineRPM"],
            "fuelLevel": data["fuelLevel"],
            "engineTemperature": data["engineTemperature"],
            "batteryVoltage": data["batteryVoltage"]
        }

    else:
        continue

    updated = update_script(
        keys=[redis_key],
        args=[
            field,
            timestamp_field,
            timestamp,
            json.dumps(value)
        ]
    )

    if updated:
        print(f"Updated {truck_id}: {field}")
