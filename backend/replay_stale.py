
import json
import redis

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

TOPIC = "gps-latest"
TRUCK_ID = "TRUCK-00001"
BOOTSTRAP = "localhost:9092"

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

current = r.hgetall(f"truck:{TRUCK_ID}")

current_sequence = int(current["gps_sequence"])
current_boot = current["gps_boot_id"]

print("Current Redis sequence:", current_sequence)

consumer = KafkaConsumer(
    bootstrap_servers=BOOTSTRAP,
    enable_auto_commit=False
)

partitions = [
    TopicPartition(TOPIC, p)
    for p in consumer.partitions_for_topic(TOPIC)
]

consumer.assign(partitions)
end_offsets = consumer.end_offsets(partitions)

for partition in partitions:
    consumer.seek(partition, max(0, end_offsets[partition] - 2000))

stale_event = None

while True:
    records = consumer.poll(timeout_ms=500, max_records=2000)

    for messages in records.values():
        for message in messages:
            try:
                data = json.loads(message.value.decode("utf-8"))

                if (
                    data.get("truckId") == TRUCK_ID
                    and data.get("bootId") == current_boot
                    and 0 < int(data.get("sequenceNumber", 0)) < current_sequence
                ):
                    if (
                        stale_event is None
                        or int(data["sequenceNumber"])
                        > int(stale_event["sequenceNumber"])
                    ):
                        stale_event = data

            except (ValueError, TypeError, KeyError):
                continue

    if all(consumer.position(p) >= end_offsets[p] for p in partitions):
        break

consumer.close()

if stale_event is None:
    raise SystemExit("No older event found. No message was published.")

print("Replaying older event:")
print("Sequence:", stale_event["sequenceNumber"])
print("Event ID:", stale_event["eventId"])

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)

producer.send(
    TOPIC,
    value=json.dumps(stale_event).encode("utf-8")
).get(timeout=10)

producer.close()

print("Older event successfully replayed.")
