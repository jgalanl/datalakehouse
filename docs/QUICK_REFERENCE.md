# Quick Reference Guide

## Service Access

| Service              | URL                           | Credentials              |
| -------------------- | ----------------------------- | ------------------------ |
| MinIO Console        | http://localhost:9001         | See `.env`               |
| MinIO S3 API         | http://localhost:9000         | See `.env`               |
| Spark Master         | http://localhost:8080         | N/A                      |
| Spark Worker 1       | http://localhost:8081         | N/A                      |
| Spark driver UI      | http://localhost:4040         | N/A                      |
| Spark notebook UI    | http://localhost:4041         | N/A                      |
| Jupyter Notebook     | http://localhost:8888         | N/A                      |
| Dremio Console       | http://localhost:9047         | Configured on first boot |
| Dremio Flight        | grpc://localhost:31010        | N/A                      |
| Dremio REST API      | http://localhost:32010        | N/A                      |
| Dremio ODBC          | localhost:45678               | N/A                      |
| OpenMetadata*        | http://localhost:8585         | See `.env`               |
| MLflow Tracking**    | http://localhost:5000         | N/A                      |
| Model REST API***    | http://localhost:5001         | N/A                      |
| Nessie API (v2)      | http://localhost:19120/api/v2 | N/A                      |
| Nessie API (v1)      | http://localhost:19120/api/v1 | N/A                      |

\* OpenMetadata is gated behind the `governance` Compose profile.
\** MLflow + its PostgreSQL are gated behind the `ml` and `governance` Compose profiles.
\*** The `model-server` (MLflow `mlflow models serve`) is gated behind the `api` profile.
Spark + Dremio are gated behind `ingest` and `ml`; only MinIO, minio-init and Nessie
are profile-less (always-on core).

## Database Access

| Database                  | Host      | Port | Credentials | Profile      |
| ------------------------- | --------- | ---- | ----------- | ------------ |
| PostgreSQL (MLflow)       | localhost | 5435 | See `.env`  | `ml`, `governance` |
| PostgreSQL (OpenMetadata) | localhost | 5437 | See `.env`  | `governance` |
| Elasticsearch (OM)        | localhost | 9200 | N/A         | `governance` |

## Common Commands

### Docker Compose

```bash
# Core only (MinIO, minio-init, Nessie) — profile-less, no compute
docker compose up -d

# Ingest: core + Spark (master/notebook/workers) + Dremio
docker compose --profile ingest up -d

# ML: core + Spark + Dremio + MLflow (+ its PostgreSQL)
docker compose --profile ml up -d

# Governance: core + MLflow + OpenMetadata (+ PostgreSQL + Elasticsearch)
docker compose --profile governance up -d

# API: core + MLflow + model-server (serves a registered model over REST; see below)
docker compose --profile api up -d model-server

# Multiple profiles union their service sets
docker compose --profile ml --profile governance up -d

# Stop all services
docker compose stop

# View logs
docker compose logs -f <service>

# Remove containers and volumes (clean slate)
docker compose down -v

# Check service status
docker compose ps
```

### MinIO Client

```bash
# Create alias (use credentials from .env)
mc alias set minio http://localhost:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD

# List buckets
mc ls minio/

# Create bucket
mc mb minio/my-bucket

# Upload file
mc cp file.txt minio/bucket/
```

### Spark Submit

```bash
# Submit job to cluster
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark/work-dir/path/to/script.py

# Access Spark Shell
docker exec -it spark-master spark-shell
```

### Model Serving (REST) — `api` profile

```bash
# Serve the model in .env (SERVE_MODEL/SERVE_ALIAS, default fare_prediction@champion)
# as the `model-server` service on host:5001. Train a model first. See RUNBOOK §8.
docker compose --profile api up -d model-server
curl -s -X POST http://localhost:5001/invocations \
  -H 'Content-Type: application/json' -d @src/ml/api/examples/fare_request.json
# or: python src/ml/api/client.py fare
```

### OpenMetadata Ingestion — `governance` profile

```bash
# One-time: build the patched ingestion image (see docker/om-ingestion). RUNBOOK §9.
docker build -t datalakehouse/om-ingestion:1.12.1 docker/om-ingestion
# Save ingestion-bot JWT to config/openmetadata/.om-token, then:
config/openmetadata/run_ingestion.sh mlflow    # models  (MLflow registry)
config/openmetadata/run_ingestion.sh iceberg   # data    (Iceberg via Nessie REST)
```

### Database Access

```bash
# PostgreSQL (MLflow) — `ml` or `governance` profile must be active
psql -h localhost -p 5435 -U $MLFLOW_DB_USER -d $MLFLOW_DB_NAME

# PostgreSQL (OpenMetadata) — `governance` profile must be active
psql -h localhost -p 5437 -U $DB_USER -d openmetadata_db
```

## S3/MinIO S3 CLI

### Using AWS CLI with MinIO

```bash
# Configure (use credentials from .env)
aws configure
# Endpoint URL: http://localhost:9000
# Access Key: $MINIO_ROOT_USER
# Secret Key: $MINIO_ROOT_PASSWORD

# List buckets
aws s3 --endpoint-url http://localhost:9000 ls

# Upload file
aws s3 --endpoint-url http://localhost:9000 cp file.txt s3://warehouse/bronze/

# Download file
aws s3 --endpoint-url http://localhost:9000 cp s3://warehouse/bronze/file.txt .
```

## Spark SQL Examples

```python
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder \
    .appName("test") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v2") \
    .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "<MINIO_ROOT_USER>") \
    .config("spark.hadoop.fs.s3a.secret.key", "<MINIO_ROOT_PASSWORD>") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Create the domain namespace — one namespace `nyc`; the medallion layer is
# the Nessie branch (bronze/silver/main), not the namespace.
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.nyc")

# Create bronze table with hidden partitioning (faithful to source: all NULLABLE).
# Note the Iceberg-Spark partition transform is `months(...)` (plural), not `month(...)`.
# The explicit LOCATION keeps the layer folders distinct under the shared namespace.
spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.nyc.yellow_taxi (
        VendorID BIGINT,
        tpep_pickup_datetime TIMESTAMP_NTZ,
        tpep_dropoff_datetime TIMESTAMP_NTZ,
        passenger_count DOUBLE,
        trip_distance DOUBLE,
        fare_amount DOUBLE,
        tip_amount DOUBLE,
        total_amount DOUBLE
    )
    USING iceberg
    PARTITIONED BY (months(tpep_pickup_datetime))
    LOCATION 's3a://warehouse/bronze/nyc/yellow_taxi'
    TBLPROPERTIES ('format-version' = '2')
""")

# Silver and gold use NOT NULL on every column the pipeline provably guarantees.
# Each pipeline declares its StructType and feeds it to common.iceberg_schema.schema_to_ddl()
# (see DEVELOPMENT.md → "Declaring a Schema in a Pipeline").

# Read data (the session ref selects the layer — switch with `USE REFERENCE`)
spark.sql("SELECT * FROM nessie.nyc.yellow_taxi").show()

# Time travel
spark.sql("SELECT * FROM nessie.nyc.yellow_taxi VERSION AS OF 0").show()
```

## Dremio SQL Examples

```sql
-- Nessie data source configured via Dremio UI

-- Query silver table (silver lives on the `silver` branch — qualify with AT BRANCH)
SELECT * FROM nessie.nyc.yellow_taxi AT BRANCH "silver"
WHERE tpep_pickup_datetime >= '2023-01-01'
LIMIT 10;

-- Gold trips already include denormalized zone columns (no join needed).
-- Gold lives on `main`, Dremio's default branch — no AT BRANCH needed.
SELECT pickup_datetime, pu_zone, do_zone, fare_amount, trip_duration_minutes
FROM nessie.nyc.yellow_taxi_trips
WHERE pickup_year = 2023
LIMIT 10;

-- Aggregate by pickup zone (top zones by trip count and avg fare)
SELECT pu_zone, COUNT(*) AS trip_count, AVG(fare_amount) AS avg_fare
FROM nessie.nyc.yellow_taxi_trips
GROUP BY pu_zone
ORDER BY trip_count DESC
LIMIT 20;

-- Hourly demand from the aggregate gold table
SELECT pu_zone, hour_of_day, SUM(n_pickups) AS total_pickups
FROM nessie.nyc.pickup_demand_zone_hour
GROUP BY pu_zone, hour_of_day
ORDER BY total_pickups DESC
LIMIT 20;
```

## MLflow / ML Training

The three use cases share four focused modules under `src/ml/`:

- `splits.py` — **last-year holdout** (`make_splits` → `common_test` = every row of `TEST_YEAR=2025` (fully held out); `train` / `val` = stratified-by-year random split of `TRAIN_VAL_YEARS=[2023, 2024]` at `val_frac=0.15`). The model is never exposed to any `TEST_YEAR` row during training or selection. Cyclical encoding for hour/dow.
- `pipelines.py` — `build_regression_pipeline` (StringIndexer → optional OneHot → numerical scaling → assemble → optional VectorIndexer → estimator).
- `algorithms.py` — `glr` (`GeneralizedLinearRegression` with use-case family/link — gamma/log for fare and duration, poisson/log for `n_pickups`), `gbt` (`GBTRegressor`, bias-reducing boosting), `rfr` (`RandomForestRegressor`, variance-reducing bagging — same tree preprocessing as GBT). `TREE_ALGORITHMS = {"gbt", "rfr"}` exposes the tree set. Single grid set `FULL_PARAM_GRIDS` (4-pt GLR, 4-pt GBT, 4-pt RFR — 4 × `CV_NUM_FOLDS=3` = 12 fits/algorithm) + fast-mode variants. Each algorithm is CV-tuned over its full grid in one pass (collapses the previous Phase-1 screening + Phase-2 winner-only optimization into a single training step).
- `tracking.py` — `get_ml_spark` / `setup_mlflow`, `evaluate_regression` (rmse/mae/r2 point estimates), `train_and_log_run`, `tune_with_cv` (hand-rolled k-fold CV — fits the shared feature stages once per fold, caches the transformed feature vectors, and refits only the bare estimator per grid point, `CV_PARALLELISM=2` fits at a time; avoids `CrossValidator`'s redundant per-(fold × point) preprocessing with no cross-fold leakage) — return the in-memory `PipelineModel` alongside run/metrics; `compute_baseline_metrics` (single split, still used by the diagnostic notebook) / `log_baseline_run` (scores `train`/`val`/`common_test`, same metric set as the model runs), `log_input_datasets` (logs the train/val/test MLflow `Dataset`s on every run so the *Datasets used* field is populated), `measure_inference_latency(model, df)` (1000-row sample, one warm-up + one timed `transform.collect()`), `prune_old_versions(client, name, keep_recent=REGISTRY_MAX_VERSIONS)` (always retains aliased versions), `register_candidates` (registers every candidate from one run; strict `@champion` promotion — contender's `val_rmse` must be strictly below the previous champion's `val_rmse`; on promotion the new @champion gets the `inference_latency_*` tags; all non-champion candidates tagged `validation_status="challenger"`; `@challenger` alias = best non-champion of the run; every version carries the per-split point metrics + the reproducibility pins `nessie_main_hash` / `source_snapshot_id`; the registry is pruned to `REGISTRY_MAX_VERSIONS = 20` non-aliased versions at the end). `common_test` is held out — its metrics are logged but never used to rank candidates.

One unified `train.py` dispatches by use case and runs the same workflow inside one MLflow experiment per use case:

0. **Phase 0 — drop redundant categoricals.** `borough` (and `pu_service_zone` for demand) are derivable from `pu_location_id` via the silver zone lookup, so they're dropped up-front for every algorithm/phase.
1. **Baseline.** Mean-of-label per `(group_cols)`, scored on `train`/`val`/`common_test` (same metric set as the model runs). Floor reference; the per-group mean is the RMSE-optimal constant-per-group predictor (median would be MAE-optimal — wrong loss for our metric).
2. **Phase 1 — Training.** One `CV(k=3)` per algorithm over `FULL_PARAM_GRIDS[algo]` (4-pt GLR, 4-pt GBT, 4-pt RFR = 12 fits/algorithm). The within-run winner is whichever algorithm's CV bestModel scores lowest on `val_rmse`. Each algorithm gets a fairly-tuned best registered (not a 3-pt screening point like the old Phase 1 design).
3. **Phase 2 — More-data.** Refit the Phase-1 winner on `SUBSET_A_YEARS` (the earliest training year); subset B is the Phase-1 winner trained on all of `TRAIN_VAL_YEARS` and reused without refit. Both score on the same `common_test = TEST_YEAR`, so neither sees the held-out year — the contrast isolates "does more training data help?".
4. **Phase 3 — Registration.** Every candidate (4 per run: 3 training + 1 more-data-A) is registered as a new MLflow Model Registry version under the use-case name. The within-run winner (lowest `val_rmse`) becomes `@champion` only if its `val_rmse` is **strictly below the previous champion's `val_rmse`** (strict point-estimate improvement). Otherwise it stays `validation_status="challenger"`. Every other candidate is always tagged `validation_status="challenger"`. The `@challenger` alias points to the best non-champion version from this run. Every version carries the `nessie_main_hash` + `source_snapshot_id` pins so the exact training input is reproducible. On promotion the new `@champion` also gets `inference_latency_*` tags from a 1000-row `common_test` sample. The registry is finally pruned to the top `REGISTRY_MAX_VERSIONS = 20` non-aliased versions (aliased ones always kept). **`common_test` is held out** — its metrics are logged on every version and shown in the diagnostic notebook as the unbiased generalization estimate, but never used to rank candidates.

Every run (baseline + the four candidates) logs its `training`/`validation`/`testing` splits as MLflow `Dataset` inputs (so the *Datasets used* field is populated everywhere) and shares the same tag/metric field set — `more_data_subset` defaults to `none`, `training_data` is a tag on every run, and `train`/`val`/`common_test` metrics are logged on all runs. The only run-type-specific fields are the tuned-run `cv_*` metrics and per-algorithm `best_<hp>` params.

No PCA. PCA on ~7-9 numericals was considered (an earlier dim-reduction probe) and removed — wrong feature-count regime, interpretability cost, no measurable benefit.

```bash
# Train (one experiment per use case in MLflow; --fast = smoke test on a 1% sample)
docker exec -it spark-master spark-submit /opt/spark/src/ml/train.py fare
docker exec -it spark-master spark-submit /opt/spark/src/ml/train.py duration
docker exec -it spark-master spark-submit /opt/spark/src/ml/train.py demand

# Smoke test (1% sample, tiny grids, CV k=2) — runs in minutes:
docker exec -it spark-master spark-submit /opt/spark/src/ml/train.py fare --fast
```

Open the MLflow UI at http://localhost:5000 to inspect runs and the registered
models (`fare_prediction`, `trip_duration`, `demand_forecast`), then open
the diagnostic notebook for that use case —
[fare_diagnostics.ipynb](../src/notebooks/ml/fare_diagnostics.ipynb) /
[duration_diagnostics.ipynb](../src/notebooks/ml/duration_diagnostics.ipynb) /
[demand_diagnostics.ipynb](../src/notebooks/ml/demand_diagnostics.ipynb)
— each loads `<experiment>@champion`, the per-year breakdown of
`common_test`, predicted vs actual, residuals, feature importance and the
Phase 2 more-data comparison.

To serve a registered model over REST, see the *Model Serving* command above
(`api` profile) and [src/ml/api/README.md](../src/ml/api/README.md).

### Programmatic MLflow access

```python
import mlflow
from mlflow.tracking import MlflowClient

mlflow.set_tracking_uri("http://localhost:5000")
client = MlflowClient()

exp = client.get_experiment_by_name("fare_prediction")
runs = client.search_runs(
    experiment_ids=[exp.experiment_id],
    order_by=["attributes.start_time DESC"],
    max_results=1,
)
last_run = runs[0]
model = mlflow.spark.load_model(f"runs:/{last_run.info.run_id}/model")
```

## ETL Pipelines

```bash
# Bronze — download from TLC, write Iceberg via WAP per month
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/bronze/ingest_yellow_taxi.py
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/bronze/ingest_zone_lookup.py

# Silver — clean / conform from bronze, WAP per month with audit + quarantine.
# clean() = validity filtering (non-positive fare/total, bad timestamps, off-dict
# codes, ...) + per-RatecodeID IQR outlier removal on
# fare_amount/total_amount/trip_distance/duration. Months above 35 % drop ratio
# → review/silver-... tag.
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/silver/clean_yellow_taxi.py
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/silver/clean_zone_lookup.py

# Gold — join + aggregate from silver, WAP per month against `main`
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/gold/build_yellow_taxi_trips.py
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/gold/aggregate_demand_zone_hour.py
```

### Nessie catalog structure (layer-first medallion)

The medallion layer is the Nessie **branch**; all tables share one namespace `nyc`.

```
nessie (catalog)
└── nyc (namespace, on every branch)
    ├── on branch `bronze`
    │   ├── yellow_taxi                 (monthly partitioned by tpep_pickup_datetime)
    │   └── taxi_zone_lookup            (unpartitioned, small reference table)
    ├── on branch `silver`
    │   ├── yellow_taxi                 (cleaned, typed, deduplicated)
    │   └── taxi_zone_lookup            (standardized)
    └── on branch `main` (gold)
        ├── yellow_taxi_trips           (trip-level: zone denorm + time features, partitioned by month(pickup_datetime))
        └── pickup_demand_zone_hour     ((zone, hour_bucket) aggregate, partitioned by month(hour_bucket))
```

Each table has an explicit `LOCATION` of `s3a://warehouse/{layer}/nyc/{table}`, so MinIO
keeps a `bronze/nyc/`, `silver/nyc/`, `gold/nyc/` layout. Within each table folder Iceberg
manages the physical file layout automatically — no manual folder organization.

### Nessie branching (WAP)

- Persistent branches (always present): `bronze`, `silver`, `main` (gold layer).
- Short-lived WAP branches: `wap/{layer}-{table}-{YYYY}-{MM}` — created from the
  target layer, validated, merged back, then dropped.
- Yearly tags: `{layer}-{table}-{YYYY}` — one per year after the last merged month.
- Quarantine tags: `review/{layer}-{table}-{YYYY}-{MM}` — created from the WAP branch
  head when audit fails; the branch is dropped and the run continues.

## Troubleshooting Commands

```bash
# Check if container is running
docker ps | grep service-name

# View container logs
docker logs container-id

# Execute command in container
docker exec -it container-id command

# Check network connectivity
docker exec container-id curl service-name:port

# View environment variables
docker exec container-id env

# Check disk usage
docker exec container-id df -h

# Check memory usage
docker stats container-id
```

## Performance Checks

```bash
# Spark cluster status
curl http://localhost:8080/api/v1/applications

# Nessie health
curl http://localhost:19120/api/v2/config

# Dremio status
curl http://localhost:9047/api/v3/source

# MinIO status
curl http://localhost:9000/minio/health/live

# OpenMetadata health
curl http://localhost:8585/health

# MLflow health
curl http://localhost:5000
```
