
import json
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

TOPIC = "gps-latest"
TARGET_TRUCK = "TRUCK-00001"
BOOTSTRAP = "localhost:9092"

consumer = KafkaConsumer(
    bootstrap_servers=BOOTSTRAP,
    enable_auto_commit=False,
    consumer_timeout_ms=10000
)

partitions = [
    TopicPartition(TOPIC, p)
    for p in consumer.partitions_for_topic(TOPIC)
]

consumer.assign(partitions)

end_offsets = consumer.end_offsets(partitions)

# Search recent messages across all partitions.
for partition in partitions:
    start = max(0, end_offsets[partition] - 2000)
    consumer.seek(partition, start)

found = None

print(f"Searching recent GPS events for {TARGET_TRUCK}...")

while True:
    records = consumer.poll(timeout_ms=500, max_records=2000)

    for messages in records.values():
        for message in messages:
            try:
                data = json.loads(message.value.decode("utf-8"))

                if (
                    data.get("truckId") == TARGET_TRUCK
                    and data.get("eventId")
                    and data.get("bootId")
                ):
                    found = message.value
            except Exception:
                continue

    finished = all(
        consumer.position(p) >= end_offsets[p]
        for p in partitions
    )

    if finished:
        break

consumer.close()

if found is None:
    raise SystemExit("No recent metadata-compatible event found.")

event = json.loads(found.decode("utf-8"))

print("Found event:")
print(json.dumps(event, indent=2))

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)

producer.send(TOPIC, value=found).get(timeout=10)
producer.flush()
producer.close()

print("Duplicate event successfully replayed to Kafka.")
