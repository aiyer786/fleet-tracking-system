
import paho.mqtt.client as mqtt

def on_connect(client, userdata, flags, reason_code, properties):
    print("Connected to EMQX:", reason_code)
    client.subscribe("fleet/company-1/+/gps", qos=1)

def on_message(client, userdata, message):
    print("\nTopic:", message.topic)
    print("Payload:", message.payload.decode())

client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    client_id="fleet-inspector"
)

client.on_connect = on_connect
client.on_message = on_message

client.connect("localhost", 1883, 60)
print("Waiting for GPS messages...")
client.loop_forever()
