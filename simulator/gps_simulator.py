import uuid
import asyncio
import json
import random
import time
import argparse
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

BROKER = "localhost"
PORT = 1883

BASE_LAT = 19.0760
BASE_LON = 72.8777
BOOT_ID = str(uuid.uuid4())


class Truck:
    def event_metadata(self):
        self.sequence += 1

        return {
            "eventId": str(uuid.uuid4()),
            "bootId": BOOT_ID,
            "sequenceNumber": self.sequence,
            "eventTime": int(time.time() * 1000),
            "schemaVersion": 1
        }
    def __init__(self, truck_id):
        self.truck_id = truck_id
        self.latitude = BASE_LAT + random.uniform(-0.1, 0.1)
        self.longitude = BASE_LON + random.uniform(-0.1, 0.1)
        self.speed = random.uniform(20, 80)
        self.sequence = 0

    def generate_gps(self):
        self.latitude += random.uniform(-0.0005, 0.0005)
        self.longitude += random.uniform(-0.0005, 0.0005)
        self.speed = max(0, min(120, self.speed + random.uniform(-5, 5)))

        return {
            **self.event_metadata(),
            "truckId": self.truck_id,
            "latitude": round(self.latitude, 6),
            "longitude": round(self.longitude, 6),
            "speed": round(self.speed, 2),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    def generate_obd(self):
        return {
            **self.event_metadata(),
            "truckId": self.truck_id,
            "engineRPM": random.randint(800, 3000),
            "fuelLevel": round(random.uniform(20, 100), 2),
            "engineTemperature": round(random.uniform(75, 105), 2),
            "batteryVoltage": round(random.uniform(12, 14.5), 2),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }


async def main(number_of_trucks):
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="fleet-gps-simulator"
    )

    client.connect(BROKER, PORT, keepalive=60)
    client.loop_start()

    trucks = [
        Truck(f"TRUCK-{i:05d}")
        for i in range(1, number_of_trucks + 1)
    ]

    now = time.monotonic()

    # Each truck gets its own GPS and OBD schedule.
    schedules = {
        truck.truck_id: {
            "next_gps": now,
            "next_obd": now + random.uniform(5, 10)
        }
        for truck in trucks
    }

    print(f"Connected to EMQX: {BROKER}:{PORT}")
    print(f"Simulating {number_of_trucks} trucks")
    print("GPS interval: 2 seconds")
    print("OBD interval: randomly 5–10 seconds")

    gps_count = 0
    obd_count = 0

    try:
        while True:
            now = time.monotonic()

            for truck in trucks:
                schedule = schedules[truck.truck_id]

                if now >= schedule["next_gps"]:
                    client.publish(
                        f"fleet/company-1/{truck.truck_id}/gps",
                        json.dumps(truck.generate_gps()),
                        qos=1
                    )
                    gps_count += 1
                    schedule["next_gps"] = now + 2

                if now >= schedule["next_obd"]:
                    client.publish(
                        f"fleet/company-1/{truck.truck_id}/obd",
                        json.dumps(truck.generate_obd()),
                        qos=1
                    )
                    obd_count += 1
                    schedule["next_obd"] = now + random.uniform(5, 10)

            if gps_count or obd_count:
                print(
                    f"GPS messages: {gps_count} | "
                    f"OBD messages: {obd_count}"
                )

            await asyncio.sleep(0.05)

    except asyncio.CancelledError:
        raise
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--trucks", type=int, default=100)
    args = parser.parse_args()

    asyncio.run(main(args.trucks))
