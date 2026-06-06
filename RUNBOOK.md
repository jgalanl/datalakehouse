# Runbook

Secuencia completa para ejecutar el pipeline de extremo a extremo. Cada bloque
es copia-pega directa.

## 0. Reset opcional (Nessie limpio)

Solo si quieres arrancar desde cero (borra todo el catálogo Nessie y los volúmenes).

```bash
docker compose down
rm -rf ./nessie-data
```

## 1. Levantar la plataforma

```bash
# Bronze/Silver/Gold + ML: core (MinIO/Nessie) + Spark + Dremio + MLflow
docker compose --profile ml up -d

# Verificar healthchecks
docker compose ps
```

> `minio`, `minio-init` y `nessie` son *profile-less* (núcleo siempre activo). El resto
> está gateado: Spark + Dremio en `ingest` (y `ml`), MLflow en `ml` (y `governance`), la
> pila OpenMetadata en `governance`, y el servidor REST del modelo (`model-server`) en
> `api`. Para solo ejecutar pipelines de ingesta basta `--profile ingest`; el
> entrenamiento ML necesita `--profile ml`; servir un modelo entrenado, `--profile api`
> (§8). Añade `--profile governance` si vas a usar OpenMetadata (los perfiles se unen).

## 2. Bronze — ingesta cruda

```bash
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/bronze/ingest_yellow_taxi.py
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/bronze/ingest_zone_lookup.py
```

## 3. Silver — limpieza y conformación

```bash
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/silver/clean_yellow_taxi.py
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/silver/clean_zone_lookup.py
```

## 4. Gold — curación

```bash
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/gold/build_yellow_taxi_trips.py
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/gold/aggregate_demand_zone_hour.py
```

## 5. Entrenamiento ML (requiere `--profile ml` activo)

El split es un **last-year holdout**: `common_test` es toda la fila de `TEST_YEAR=2025` (totalmente reservado — el modelo no ve ninguna fila de 2025 ni durante entrenamiento ni en selección), y `train`/`val` se obtienen por split aleatorio estratificado por año dentro de `TRAIN_VAL_YEARS=[2023, 2024]` (`val_frac=0.15`).

Un único script dispatcheado por caso de uso. Cada invocación corre Phase 0 (drop de categóricas redundantes — borough es derivable de location_id) → construye un `Dataset` de MLflow por split (`from_spark`, reutilizado entre runs, registrado como inputs `training`/`validation`/`testing` en cada run) → baseline `mean-por-grupo` (evaluado sobre `train`/`val`/`common_test`, el mismo conjunto de métricas que los runs de modelo; la media es el predictor constante por grupo óptimo para RMSE; la mediana sería óptima para MAE — métrica equivocada) → Phase 1 Training (CV(k=3) por algoritmo sobre su `FULL_PARAM_GRIDS` completo — 4-pt GLR, 4-pt GBT, 4-pt RFR = 12 fits/algoritmo; el ganador es el algoritmo cuyo bestModel del CV obtiene menor `val_rmse`; cada algoritmo queda registrado con su mejor punto correctamente afinado) → Phase 2 More-data (A entrena con `SUBSET_A_YEARS=[2023]`, B es el ganador de Phase 1 entrenado con todo `TRAIN_VAL_YEARS`; ambos se evalúan en `common_test = TEST_YEAR`) → Phase 3 Registration: cada candidato (4 por invocación: 3 training + 1 more-data-A) se registra como una nueva versión del modelo en el MLflow Model Registry. El ganador de la invocación (menor `val_rmse`) pasa a `@champion` solo si su `val_rmse` es estrictamente menor que el `val_rmse` del campeón anterior (mejora estricta del punto); en caso contrario se etiqueta `validation_status="challenger"`. El resto de candidatos se etiquetan siempre `validation_status="challenger"`, y el alias `@challenger` apunta al mejor de ellos. `common_test` queda *held out* — sus métricas se registran en cada versión pero no se usan para rankear: son la estimación de generalización imparcial que se reporta en el notebook de diagnóstico. Cada run (baseline + los 4 candidatos) registra además sus splits como *Datasets* de MLflow (campo *Datasets used*) y comparte el mismo conjunto de tags/métricas — `more_data_subset` por defecto `none` (solo la comparación A/B de Phase 2 lo sobreescribe), `training_data` como tag en cada run, y métricas `train`/`val`/`common_test` en todos; solo difieren las métricas `cv_*` y los `best_<hp>` propios de cada algoritmo afinado.

Cada versión del registro lleva además el **hash de Nessie `main`** (`nessie_main_hash`) y el **snapshot_id de Iceberg** de la tabla origen (`source_snapshot_id`) en el momento del entrenamiento — basta con el model card para reconstruir el input exacto más adelante. Cuando hay promoción, el nuevo `@champion` recibe tres tags de latencia (`inference_latency_ms_per_row` / `inference_latency_sample_n` / `inference_latency_total_s`) sobre una muestra de 1000 filas de `common_test` — chequeo de regresión de coste entre campeones, no un SLO de servicio. Al final de cada invocación el registro se poda al top `REGISTRY_MAX_VERSIONS = 20` no-aliadas (las versiones con alias `@champion`/`@challenger` se conservan siempre).

```bash
docker exec -it spark-master spark-submit /opt/spark/src/ml/train.py fare
docker exec -it spark-master spark-submit /opt/spark/src/ml/train.py duration
docker exec -it spark-master spark-submit /opt/spark/src/ml/train.py demand
# Smoke test (1% sample, grids tiny, CV k=2) — corre en minutos:
docker exec -it spark-master spark-submit /opt/spark/src/ml/train.py fare --fast
```

## 6. Verificación interactiva

Jupyter en http://localhost:8888 — abre los notebooks en orden:

- [src/notebooks/bronze/explore.ipynb](src/notebooks/bronze/explore.ipynb)
- [src/notebooks/bronze/preprocess.ipynb](src/notebooks/bronze/preprocess.ipynb)
- [src/notebooks/bronze/verify_iceberg.ipynb](src/notebooks/bronze/verify_iceberg.ipynb)
- [src/notebooks/silver/verify_iceberg.ipynb](src/notebooks/silver/verify_iceberg.ipynb)
- [src/notebooks/gold/verify_iceberg.ipynb](src/notebooks/gold/verify_iceberg.ipynb)
- [src/notebooks/ml/fare_diagnostics.ipynb](src/notebooks/ml/fare_diagnostics.ipynb) / [duration_diagnostics.ipynb](src/notebooks/ml/duration_diagnostics.ipynb) / [demand_diagnostics.ipynb](src/notebooks/ml/demand_diagnostics.ipynb) — un notebook de diagnóstico por caso de uso.

## 7. UIs

| Servicio         | URL                                  |
| ---------------- | ------------------------------------ |
| MLflow           | http://localhost:5000                |
| Spark Master     | http://localhost:8080                |
| Spark Worker 1   | http://localhost:8081                |
| Spark Worker 2   | http://localhost:8082                |
| Jupyter Notebook | http://localhost:8888                |
| MinIO Console    | http://localhost:9001                |
| Dremio           | http://localhost:9047                |
| Nessie API (v2)  | http://localhost:19120/api/v2/config |
| OpenMetadata     | http://localhost:8585                |

## 8. Servir un modelo como REST (perfil `api`)

Un modelo registrado se expone como endpoint REST con el servidor integrado de
MLflow (`mlflow models serve`), definido como el servicio **`model-server`** en
`docker-compose.yml` bajo el perfil **`api`**. Se construye desde la imagen de Spark
(los modelos son `PipelineModel` de Spark MLlib y cargarlos requiere una
SparkSession); corre `local[*]` (driver == executor), lo que evita el problema de
staging driver/executor. El puerto 5001 está mapeado al host, con healthcheck `/ping`
y `restart: on-failure`.

El servicio está en su **propio perfil** a propósito: sirve un modelo *registrado*,
así que debe arrancar solo después de que exista (si no, entra en bucle de reinicio
por alias inexistente). Entrena un modelo primero (§5).

```bash
# El modelo/alias a servir se fija en .env (por defecto fare_prediction / champion):
#   SERVE_MODEL=fare_prediction   SERVE_ALIAS=champion

# Arranca el servidor + el MLflow tracking del que lee (+ núcleo MinIO/Nessie):
docker compose --profile api up -d model-server
# docker compose ps  -> espera a que aparezca "healthy" (~60-90 s en frío)

# Predicción de ejemplo (3 filas)
curl -s -X POST http://localhost:5001/invocations \
  -H 'Content-Type: application/json' -d @src/ml/api/examples/fare_request.json
# -> {"predictions": [20.99, 8.05, 70.46]}   (la fila 3 es tarifa plana JFK ≈ $70)

# O con el cliente Python incluido (solo stdlib):
python src/ml/api/client.py fare
```

El campeón de `fare` es un GBT (árbol), así que el payload son las columnas crudas de
gold **sin** codificación cíclica: `trip_distance`, `passenger_count`,
`pickup_year/month/week/dow/hour`, `pu_location_id`, `do_location_id`, `vendor_id`,
`ratecode_id`, `payment_type`. Un campeón GLR esperaría en su lugar las columnas
`*_sin`/`*_cos`. Detalles y ejemplos por caso de uso en
[src/ml/api/README.md](src/ml/api/README.md).

## 9. Catalogar en OpenMetadata — datos + modelos (requiere perfil `governance`)

La ingesta corre con el CLI `metadata` vía una imagen de ingesta propia
(`docker/om-ingestion`), sobre la red de compose — no hay contenedor Airflow.

```bash
# 1. Levanta governance (openmetadata + postgres + elasticsearch + mlflow);
#    añadimos `ml` para tener también Spark/Dremio disponibles durante la sesión.
docker compose --profile ml --profile governance up -d

# 2. Construye la imagen de ingesta parcheada (una sola vez)
docker build -t datalakehouse/om-ingestion:1.12.1 docker/om-ingestion

# 3. Guarda el JWT del ingestion-bot en config/openmetadata/.om-token (gitignored)
#    UI: Settings > Bots > ingestion-bot ; o API /api/v1/users/auth-mechanism/<botId>

# 4. Ingesta de modelos (MLflow) y de datos (Iceberg vía Nessie REST)
config/openmetadata/run_ingestion.sh mlflow
config/openmetadata/run_ingestion.sh iceberg
```

Notas (decisiones no obvias):

- **Datos vía Iceberg/Nessie REST, no Dremio.** El conector Dremio de OpenMetadata
  1.12.1 es solo Collate (no está en la imagen OSS: existe el *schema* de conexión
  pero no el módulo de ingesta). Se usa el conector nativo `iceberg` apuntando al
  endpoint Iceberg-REST **integrado de Nessie** (`http://nessie:19120/iceberg`), que
  expone el mismo store —no es un segundo catálogo—. Configurado en
  `config/nessie/application.properties` (warehouse + credenciales MinIO).
- **Desambiguación REST vs Hive.** `catalog.connection` es una unión no discriminada
  `[Hive, Rest, ...]`; con solo `uri` resuelve a Hive (cuelga intentando Thrift). El
  campo `token` en `iceberg_metadata.yaml` la fuerza a REST (Nessie ignora el token).
- **Imagen de ingesta parcheada** (`docker/om-ingestion/Dockerfile`): el cliente
  MLflow del contenedor (3.6.0) es incompatible con el servidor 2.20.3 (la búsqueda en
  el registry devuelve 0 modelos) → se fija a 2.20.3; además se corrige la selección de
  versión del conector MLflow (asumía `last_updated_timestamp` exacto).
- **`yellow_taxi_trips`**: su snapshot actual quedó cacheado como *fallo* en Nessie
  durante la puesta a punto de credenciales (Nessie persiste tareas de catálogo
  fallidas y no reintenta el mismo snapshot). Se resuelve generando un snapshot nuevo
  (re-ejecutar el pipeline gold, o un `ALTER TABLE … SET TBLPROPERTIES` no-op) y
  re-lanzando `run_ingestion.sh iceberg`. `pickup_demand_zone_hour` se cataloga sin
  problema.
- **Elasticsearch 9.x es obligatorio (OM 1.12).** Si tras la ingesta no ves las tablas
  ni los modelos en **Explore** (pero sí responden por API, p. ej.
  `/api/v1/tables?database=iceberg_lakehouse.nessie`), el problema es el índice de
  búsqueda, no la ingesta. OM 1.12.1 trae el cliente Java de Elasticsearch `9.2.4`
  (protocolo `compatible-with=9`); un servidor ES **8.x** lo rechaza con
  `media_type_header_exception` y OM no puede escribir el índice → Explore vacío. El
  `docker-compose.yml` fija **Elasticsearch 9.3.0**. Si cambiaste de versión o el
  índice quedó vacío, relanza el reindex y verifícalo:

  ```bash
  TOKEN=$(cat config/openmetadata/.om-token | tr -d '\r\n')
  curl -s -X POST -H "Authorization: Bearer $TOKEN" \
    http://localhost:8585/api/v1/apps/trigger/SearchIndexingApplication
  # estado del job (sinkStats debe terminar en failedRecords: 0):
  curl -s -H "Authorization: Bearer $TOKEN" \
    "http://localhost:8585/api/v1/apps/name/SearchIndexingApplication/status?limit=1"
  ```

  (También en UI: Settings → Applications → Search Indexing → Run.) Estado `yellow` del
  clúster ES en single-node es normal.

## 10. Apagar

```bash
docker compose down            # mantiene volúmenes y ./nessie-data
docker compose down -v         # limpieza total (volúmenes nombrados)
```

> `docker compose down -v` no borra `./nessie-data` (es bind mount). Para wipe
> total de Nessie usa el bloque 0.
