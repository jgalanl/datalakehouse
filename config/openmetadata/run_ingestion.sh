#!/usr/bin/env bash
# Run an OpenMetadata ingestion workflow via the official ingestion image.
# There is no Airflow/ingestion container in this stack, so we run the `metadata`
# CLI ad-hoc on the compose network. Secrets are injected into a gitignored
# *.runtime.yaml copy (the committed YAMLs keep placeholders only).
#
# Usage:
#   config/openmetadata/run_ingestion.sh iceberg  # data (Iceberg via Nessie REST)
#   config/openmetadata/run_ingestion.sh mlflow   # models (MLflow registry)
set -euo pipefail
cd "$(dirname "$0")/../.."          # repo root

KIND="${1:?usage: run_ingestion.sh <iceberg|mlflow>}"
SRC="config/openmetadata/${KIND}_metadata.yaml"
OUT="config/openmetadata/${KIND}_metadata.runtime.yaml"
NETWORK="${OM_NETWORK:-datalakehouse_datalakehouse}"
# Locally-built ingestion image (docker/om-ingestion) — patched for our MLflow 2.20.3
# server + alias-managed registry. Build with: docker build -t "$IMAGE" docker/om-ingestion
IMAGE="${OM_INGESTION_IMAGE:-datalakehouse/om-ingestion:1.12.1}"

[ -f "$SRC" ] || { echo "missing $SRC"; exit 1; }
[ -f config/openmetadata/.om-token ] || { echo "missing config/openmetadata/.om-token (ingestion-bot JWT)"; exit 1; }
# Pull just the secrets we need from .env (avoid sourcing — the file has a UTF-8 BOM).
envval() { grep -aE "^$1=" .env | tail -1 | cut -d= -f2- | tr -d '\r"'; }
MINIO_KEY="$(envval MINIO_ROOT_USER)"
MINIO_SECRET="$(envval MINIO_ROOT_PASSWORD)"

JWT="$(cat config/openmetadata/.om-token)"
sed -e "s|__OM_JWT__|${JWT}|g" \
    -e "s|__MINIO_KEY__|${MINIO_KEY:-}|g" \
    -e "s|__MINIO_SECRET__|${MINIO_SECRET:-}|g" \
    "$SRC" > "$OUT"

# The image's default entrypoint is `airflow`; override it to the metadata CLI.
# MSYS_* vars stop Git-Bash (Windows) from rewriting the in-container /cfg path;
# they're harmless on Linux/macOS.
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*' \
docker run --rm --network "$NETWORK" \
  --entrypoint metadata \
  -v "$(pwd)/config/openmetadata:/cfg" \
  "$IMAGE" \
  ingest -c "/cfg/$(basename "$OUT")"
