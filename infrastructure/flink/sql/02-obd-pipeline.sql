CREATE TABLE obd_source (
    truckId STRING,
    engineRPM INT,
    fuelLevel DOUBLE,
    engineTemperature DOUBLE,
    batteryVoltage DOUBLE,
    `timestamp` STRING,
    eventId STRING,
    bootId STRING,
    sequenceNumber BIGINT,
    eventTime BIGINT,
    schemaVersion INT
) WITH (
    'connector' = 'kafka',
    'topic' = 'obd-telemetry',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-obd-validator',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE obd_sink (
    truckId STRING,
    engineRPM INT,
    fuelLevel DOUBLE,
    engineTemperature DOUBLE,
    batteryVoltage DOUBLE,
    `timestamp` STRING,
    eventId STRING,
    bootId STRING,
    sequenceNumber BIGINT,
    eventTime BIGINT,
    schemaVersion INT
) WITH (
    'connector' = 'kafka',
    'topic' = 'obd-processed',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json'
);

INSERT INTO obd_sink
SELECT
    truckId,
    engineRPM,
    fuelLevel,
    engineTemperature,
    batteryVoltage,
    `timestamp`,
    eventId,
    bootId,
    sequenceNumber,
    eventTime,
    schemaVersion
FROM obd_source
WHERE
    truckId IS NOT NULL
    AND eventId IS NOT NULL
    AND bootId IS NOT NULL
    AND sequenceNumber > 0
    AND eventTime > 0
    AND schemaVersion = 1
    AND engineRPM BETWEEN 0 AND 10000
    AND fuelLevel BETWEEN 0 AND 100
    AND engineTemperature BETWEEN -40 AND 200
    AND batteryVoltage BETWEEN 0 AND 30;