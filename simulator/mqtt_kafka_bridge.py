
import json
import paho.mqtt.client as mqtt
from kafka import KafkaProducer

MQTT_BROKER = "localhost"
MQTT_PORT = 1883

KAFKA_BROKER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    acks="all",
    retries=10,
    key_serializer=lambda key: key.encode("utf-8")
)

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("Connected to EMQX")

        client.subscribe("fleet/company-1/+/gps", qos=1)
        client.subscribe("fleet/company-1/+/obd", qos=1)

        print("Subscribed to GPS and OBD topics")
    else:
        print("MQTT connection failed:", reason_code)


def on_message(client, userdata, message):
    try:
        payload = json.loads(message.payload.decode("utf-8"))

        truck_id = payload["truckId"]

        if message.topic.endswith("/gps"):
            kafka_topic = "gps-telemetry"

        elif message.topic.endswith("/obd"):
            kafka_topic = "obd-telemetry"

        else:
            return

        future = producer.send(
            kafka_topic,
            key=truck_id,
            value=message.payload
        )

        future.add_errback(
            lambda error: print("Kafka delivery failed:", error)
        )

    except (ValueError, KeyError) as error:
        print("Invalid telemetry message:", error)


client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    client_id="mqtt-kafka-bridge"
)

client.on_connect = on_connect
client.on_message = on_message

print("Starting MQTT-to-Kafka bridge...")

client.connect(MQTT_BROKER, MQTT_PORT, 60)

try:
    client.loop_forever()
except KeyboardInterrupt:
    print("Stopping bridge...")
finally:
    client.disconnect()
    producer.flush()
    producer.close()
    print("Bridge stopped.")
