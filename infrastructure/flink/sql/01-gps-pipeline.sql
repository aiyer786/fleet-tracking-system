CREATE TABLE gps_source (
    truckId STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    speed DOUBLE,
    `timestamp` STRING,
    eventId STRING,
    bootId STRING,
    sequenceNumber BIGINT,
    eventTime BIGINT,
    schemaVersion INT
) WITH (
    'connector' = 'kafka',
    'topic' = 'gps-telemetry',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-gps-validator',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE gps_sink (
    truckId STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    speed DOUBLE,
    `timestamp` STRING,
    eventId STRING,
    bootId STRING,
    sequenceNumber BIGINT,
    eventTime BIGINT,
    schemaVersion INT
) WITH (
    'connector' = 'kafka',
    'topic' = 'gps-latest',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json'
);

INSERT INTO gps_sink
SELECT
    truckId,
    latitude,
    longitude,
    speed,
    `timestamp`,
    eventId,
    bootId,
    sequenceNumber,
    eventTime,
    schemaVersion
FROM gps_source
WHERE
    truckId IS NOT NULL
    AND eventId IS NOT NULL
    AND bootId IS NOT NULL
    AND sequenceNumber > 0
    AND eventTime > 0
    AND schemaVersion = 1
    AND latitude BETWEEN -90 AND 90
    AND longitude BETWEEN -180 AND 180
    AND speed >= 0;