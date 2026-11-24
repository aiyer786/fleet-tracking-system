
import json
import time

import redis
from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata


# Redis connection
redis_client = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=True,
    socket_connect_timeout=5,
    socket_timeout=5
)

redis_client.ping()


# Atomically:
# 1. Reject duplicate event IDs.
# 2. Reject old sequence numbers.
# 3. Handle a new device session.
# 4. Update Redis.
# 5. Publish the accepted event.

UPDATE_LUA = """
local truck_key = KEYS[1]
local event_key = KEYS[2]

local prefix = ARGV[1]
local boot_id = ARGV[2]
local sequence = tonumber(ARGV[3])
local event_time = tonumber(ARGV[4])
local event_id = ARGV[5]
local value = ARGV[6]
local timestamp_seconds = ARGV[7]
local notification = ARGV[8]

-- Duplicate event
if redis.call('EXISTS', event_key) == 1 then
    return 0
end

local old_boot = redis.call('HGET', truck_key, prefix .. '_boot_id')
local old_sequence = tonumber(
    redis.call('HGET', truck_key, prefix .. '_sequence') or '-1'
)
local old_event_time = tonumber(
    redis.call('HGET', truck_key, prefix .. '_event_time') or '0'
)

if old_boot then

    if old_boot == boot_id then
        -- Same device session: sequence must increase.
        if sequence <= old_sequence then
            return 0
        end
    else
        -- New session: accept only if its event time is newer.
        if event_time <= old_event_time then
            return 0
        end
    end
end

-- Update latest truck state.
redis.call('HSET', truck_key,
    prefix, value,
    prefix .. '_timestamp', timestamp_seconds,
    prefix .. '_boot_id', boot_id,
    prefix .. '_sequence', sequence,
    prefix .. '_event_time', event_time,
    prefix .. '_event_id', event_id
)

-- Remember this event for seven days.
redis.call('SET', event_key, '1', 'EX', 604800)

-- Publish only accepted updates.
redis.call('PUBLISH', 'truck-updates', notification)

return 1
"""

update_script = redis_client.register_script(UPDATE_LUA)


# Kafka consumer
consumer = KafkaConsumer(
    "gps-latest",
    "obd-processed",
    bootstrap_servers="localhost:9092",
    group_id="fleet-redis-writer-v2",
    auto_offset_reset="latest",
    enable_auto_commit=False,
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    )
)


def process_message(message):
    data = message.value

    if not isinstance(data, dict):
        raise ValueError("Telemetry must be a JSON object")

    required = [
        "eventId",
        "truckId",
        "bootId",
        "sequenceNumber",
        "eventTime",
        "schemaVersion"
    ]

    for field in required:
        if field not in data or data[field] is None:
            raise ValueError(f"Missing field: {field}")

    if data["schemaVersion"] != 1:
        raise ValueError("Unsupported schema version")

    truck_id = data["truckId"]
    event_id = data["eventId"]
    boot_id = data["bootId"]

    sequence = int(data["sequenceNumber"])
    event_time = int(data["eventTime"])

    if not truck_id or not event_id or not boot_id:
        raise ValueError("Empty telemetry identifier")

    if sequence <= 0 or event_time <= 0:
        raise ValueError("Invalid sequence number or event time")

    if message.topic == "gps-latest":

        prefix = "gps"

        value = {
            "latitude": data["latitude"],
            "longitude": data["longitude"],
            "speed": data["speed"]
        }

    elif message.topic == "obd-processed":

        prefix = "obd"

        value = {
            "engineRPM": data["engineRPM"],
            "fuelLevel": data["fuelLevel"],
            "engineTemperature": data["engineTemperature"],
            "batteryVoltage": data["batteryVoltage"]
        }

    else:
        return 0

    timestamp_seconds = event_time / 1000

    notification = {
        "truckId": truck_id,
        "type": prefix,
        "data": value,
        "timestamp": timestamp_seconds,
        "eventId": event_id,
        "sequenceNumber": sequence
    }

    result = update_script(
        keys=[
            f"truck:{truck_id}",
            f"event:seen:{event_id}"
        ],
        args=[
            prefix,
            boot_id,
            sequence,
            event_time,
            event_id,
            json.dumps(value),
            timestamp_seconds,
            json.dumps(notification)
        ]
    )

    if result == 1:
        print(f"ACCEPTED {truck_id}: {prefix} #{sequence}")
    else:
        print(f"REJECTED {truck_id}: duplicate/stale {prefix} #{sequence}")

    return result


print("Redis writer started. Waiting for telemetry...")

try:
    while True:

        # Process one message at a time so we can commit
        # exactly the offset we have successfully processed.
        records = consumer.poll(
            timeout_ms=1000,
            max_records=1
        )

        for topic_partition, messages in records.items():

            for message in messages:

                try:
                    process_message(message)

                    # Commit only after Redis processing succeeds.
                    consumer.commit({
                        TopicPartition(
                            message.topic,
                            message.partition
                        ): OffsetAndMetadata(
                            message.offset + 1,
                            "",
                            -1
                        )
                    })

                except Exception as error:
                    print(
                        f"Processing failed for "
                        f"{message.topic}, offset {message.offset}: {error}"
                    )

                    # Do not commit. Kafka can redeliver this message.
                    raise

except KeyboardInterrupt:
    print("Stopping Redis writer...")

finally:
    consumer.close()
    redis_client.close()
    print("Redis writer stopped.")
