#!/usr/bin/env bash
# Ejecuta los flujos de ingesta de OpenMetadata mediante la imagen oficial de ingesta.
#   - Iceberg: un servicio de catálogo por rama del medallón (bronze/silver/gold),
#     inyectando la referencia de Nessie vía NESSIE_ICEBERG_PREFIX (el conector OSS no
#     expone el prefix Iceberg-REST — ver docker/om-ingestion/Dockerfile).
#   - MLflow: todos los modelos registrados (los tres experimentos), sin filtro.
set -euo pipefail
cd "$(dirname "$0")/../.."

NETWORK="${OM_NETWORK:-datalakehouse_datalakehouse}"
IMAGE="${OM_INGESTION_IMAGE:-datalakehouse/om-ingestion:1.12.1}"

# Extrae los secretos de .env
envval() { grep -aE "^$1=" .env | tail -1 | cut -d= -f2- | tr -d '\r"'; }
JWT="$(envval OM_INGESTION_BOT_JWT)"
[ -n "$JWT" ] || { echo "Falta OM_INGESTION_BOT_JWT en .env (JWT del ingestion-bot)"; exit 1; }
MINIO_KEY="$(envval MINIO_ROOT_USER)"
MINIO_SECRET="$(envval MINIO_ROOT_PASSWORD)"

render() {  # render <src> <out> [extra sed exprs...]
  local src="$1" out="$2"; shift 2
  sed -e "s|__OM_JWT__|${JWT}|g" \
      -e "s|__MINIO_KEY__|${MINIO_KEY}|g" \
      -e "s|__MINIO_SECRET__|${MINIO_SECRET}|g" \
      "$@" "$src" > "$out"
}

ingest() {  # ingest <rendered-yaml> [docker-run extra args...]
  local out="$1"; shift
  MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*' \
  docker run --rm --network "$NETWORK" \
    --entrypoint metadata \
    -v "$(pwd)/config/openmetadata:/cfg" \
    "$@" \
    "$IMAGE" \
    ingest -c "/cfg/$(basename "$out")"
}

# --- Iceberg: una capa (rama de Nessie) por servicio ---
# rama de Nessie  ->  nombre del servicio en OpenMetadata
declare -A ICEBERG_SERVICES=(
  [bronze]=iceberg_bronze
  [silver]=iceberg_silver
  [main]=iceberg_gold
)
for REF in bronze silver main; do
  SVC="${ICEBERG_SERVICES[$REF]}"
  OUT="config/openmetadata/iceberg_${REF}.runtime.yaml"
  render config/openmetadata/iceberg_metadata.yaml "$OUT" \
    -e "s|__ICEBERG_SERVICE__|${SVC}|g"
  echo ">> ingestando iceberg ref=${REF} servicio=${SVC}"
  ingest "$OUT" -e "NESSIE_ICEBERG_PREFIX=${REF}"
done

# --- MLflow: todos los modelos registrados (fare/duration/demand) ---
OUT="config/openmetadata/mlflow_metadata.runtime.yaml"
render config/openmetadata/mlflow_metadata.yaml "$OUT"
echo ">> ingestando mlflow (todos los modelos registrados)"
ingest "$OUT"

# --- Linaje del medallón + modelos ---
# OM no infiere linaje tabla-a-tabla desde Iceberg/MLflow (ingesta solo de entidad),
# así que las aristas del flujo bronze->silver->gold->modelos se declaran explícitamente
# (idempotente). Se corre en la imagen de ingesta, sobre la red de Compose.
echo ">> declaring lineage edges (bronze -> silver -> gold -> modelos)"
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*' \
docker run --rm --network "$NETWORK" \
  --entrypoint python3 \
  -e OM_HOST="http://openmetadata:8585" \
  -e OM_JWT="${JWT}" \
  -v "$(pwd)/config/openmetadata:/cfg" \
  "$IMAGE" \
  /cfg/lineage.py
