
import json
import gzip
import uuid
import threading
import time
from io import BytesIO
from datetime import datetime, timezone

import boto3
from botocore.client import Config
from kafka import KafkaConsumer


# Silo connection (S3-compatible API)
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("SILO_ENDPOINT_URL", "http://localhost:9000"),
aws_access_key_id=os.getenv("SILO_ACCESS_KEY"),
aws_secret_access_key=os.getenv("SILO_SECRET_KEY"),
    region_name="us-east-1",
    config=Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"}
    )
)

BATCH_SIZE = 100
FLUSH_INTERVAL = 10

STREAMS = {
    "gps-telemetry": "gps-history",
    "obd-telemetry": "obd-history"
}


def archive_stream(topic, bucket):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        group_id=f"fleet-history-{topic}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    batch = []
    last_flush = time.time()

    print(f"Archiver started: {topic} -> {bucket}")

    while True:
        records = consumer.poll(timeout_ms=1000)

        for messages in records.values():
            for message in messages:
                batch.append(message.value)

        should_flush = (
            len(batch) >= BATCH_SIZE
            or (batch and time.time() - last_flush >= FLUSH_INTERVAL)
        )

        if should_flush:
            now = datetime.now(timezone.utc)
            date_path = now.strftime("%Y/%m/%d/%H")

            content = "\n".join(
                json.dumps(record) for record in batch
            ).encode("utf-8")

            compressed = gzip.compress(content)
            filename = f"{date_path}/{uuid.uuid4().hex}.jsonl.gz"

            try:
                s3.upload_fileobj(
                    BytesIO(compressed),
                    bucket,
                    filename
                )

                consumer.commit()

                print(
                    f"Archived {len(batch)} records "
                    f"from {topic} to {bucket}/{filename}"
                )

                batch.clear()
                last_flush = time.time()

            except Exception as error:
                print(f"Archive failed for {topic}: {error}")
                time.sleep(3)


def main():
    for bucket in STREAMS.values():
        s3.head_bucket(Bucket=bucket)

    threads = []

    for topic, bucket in STREAMS.items():
        thread = threading.Thread(
            target=archive_stream,
            args=(topic, bucket),
            daemon=True
        )
        thread.start()
        threads.append(thread)

    print("Historical archiver is running.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping historical archiver...")


if __name__ == "__main__":
    main()
