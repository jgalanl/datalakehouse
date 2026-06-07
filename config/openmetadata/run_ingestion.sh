#!/usr/bin/env bash
# Run the OpenMetadata ingestion workflows via the official ingestion image.
set -euo pipefail
cd "$(dirname "$0")/../.."

NETWORK="${OM_NETWORK:-datalakehouse_datalakehouse}"
IMAGE="${OM_INGESTION_IMAGE:-datalakehouse/om-ingestion:1.12.1}"

# Pull the secrets from .env 
envval() { grep -aE "^$1=" .env | tail -1 | cut -d= -f2- | tr -d '\r"'; }
JWT="$(envval OM_INGESTION_BOT_JWT)"
[ -n "$JWT" ] || { echo "missing OM_INGESTION_BOT_JWT in .env (ingestion-bot JWT)"; exit 1; }

# Ingest the metadata for each service (iceberg and mlflow)
for KIND in iceberg mlflow; do
  SRC="config/openmetadata/${KIND}_metadata.yaml"
  OUT="config/openmetadata/${KIND}_metadata.runtime.yaml"
  sed -e "s|__OM_JWT__|${JWT}|g" \
      -e "s|__MINIO_KEY__|$(envval MINIO_ROOT_USER)|g" \
      -e "s|__MINIO_SECRET__|$(envval MINIO_ROOT_PASSWORD)|g" \
      "$SRC" > "$OUT"
  echo ">> ingesting $KIND"
  MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*' \
  docker run --rm --network "$NETWORK" \
    --entrypoint metadata \
    -v "$(pwd)/config/openmetadata:/cfg" \
    "$IMAGE" \
    ingest -c "/cfg/$(basename "$OUT")"
done
