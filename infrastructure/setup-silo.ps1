$ErrorActionPreference = "Stop"

$accessKey = Read-Host "Enter MINIO_ROOT_USER"
$secretKey = Read-Host "Enter MINIO_ROOT_PASSWORD"

docker run --rm --network infrastructure_default `
    -e MC_HOST_fleet="http://${accessKey}:${secretKey}@fleet-minio:9000" `
    minio/mc mb --ignore-existing fleet/gps-history

if ($LASTEXITCODE -ne 0) {
    throw "Failed to create gps-history"
}

docker run --rm --network infrastructure_default `
    -e MC_HOST_fleet="http://${accessKey}:${secretKey}@fleet-minio:9000" `
    minio/mc mb --ignore-existing fleet/obd-history

if ($LASTEXITCODE -ne 0) {
    throw "Failed to create obd-history"
}

Write-Host "Silo bucket setup completed."