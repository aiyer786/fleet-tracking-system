$topics = @(
    "gps-telemetry",
    "obd-telemetry",
    "gps-latest",
    "obd-processed",
    "telemetry-dlq"
)

foreach ($topic in $topics) {
    docker exec fleet-kafka /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:9092 `
        --create `
        --if-not-exists `
        --topic $topic `
        --partitions 12 `
        --replication-factor 1

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create Kafka topic: $topic"
    }
}

Write-Host "Kafka topic setup completed."