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
| Nessie API (v2)      | http://localhost:19120/api/v2 | N/A                      |
| Nessie API (v1)      | http://localhost:19120/api/v1 | N/A                      |

\* OpenMetadata is gated behind the `governance` Compose profile.
\** MLflow + its PostgreSQL are gated behind the `ml` Compose profile.

## Database Access

| Database                  | Host      | Port | Credentials | Profile      |
| ------------------------- | --------- | ---- | ----------- | ------------ |
| PostgreSQL (MLflow)       | localhost | 5435 | See `.env`  | `ml`         |
| PostgreSQL (OpenMetadata) | localhost | 5437 | See `.env`  | `governance` |
| Elasticsearch (OM)        | localhost | 9200 | N/A         | `governance` |

## Common Commands

### Docker Compose

```bash
# Core stack (MinIO, Nessie, Spark, Dremio)
docker compose up -d

# With ML services (MLflow + its PostgreSQL)
docker compose --profile ml up -d

# With governance (OpenMetadata + PostgreSQL + Elasticsearch)
docker compose --profile governance up -d

# Everything
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

### Database Access

```bash
# PostgreSQL (MLflow) — `ml` profile must be active
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

# Create medallion namespaces
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

# Create bronze table with hidden partitioning (faithful to source: all NULLABLE)
# Note the Iceberg-Spark partition transform is `months(...)` (plural), not `month(...)`.
spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.bronze.yellow_taxi (
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
    TBLPROPERTIES ('format-version' = '2')
""")

# Silver and gold use NOT NULL on every column the pipeline provably guarantees.
# Each pipeline declares its StructType and feeds it to common.iceberg_schema.schema_to_ddl()
# (see DEVELOPMENT.md → "Declaring a Schema in a Pipeline").

# Read data
spark.sql("SELECT * FROM nessie.bronze.yellow_taxi").show()

# Time travel
spark.sql("SELECT * FROM nessie.bronze.yellow_taxi VERSION AS OF 0").show()
```

## Dremio SQL Examples

```sql
-- Nessie data source configured via Dremio UI

-- Query silver table
SELECT * FROM nessie.silver.yellow_taxi
WHERE tpep_pickup_datetime >= '2019-01-01'
LIMIT 10;

-- Gold trips already include denormalized zone columns (no join needed)
SELECT pickup_datetime, pu_zone, do_zone, fare_amount, trip_duration_minutes
FROM nessie.gold.yellow_taxi_trips
WHERE pickup_year = 2019
LIMIT 10;

-- Aggregate by pickup zone (top zones by trip count and avg fare)
SELECT pu_zone, COUNT(*) AS trip_count, AVG(fare_amount) AS avg_fare
FROM nessie.gold.yellow_taxi_trips
GROUP BY pu_zone
ORDER BY trip_count DESC
LIMIT 20;

-- Hourly demand from the aggregate gold table
SELECT pu_zone, hour_of_day, SUM(n_pickups) AS total_pickups
FROM nessie.gold.pickup_demand_zone_hour
GROUP BY pu_zone, hour_of_day
ORDER BY total_pickups DESC
LIMIT 20;
```

## MLflow / ML Training

The three ML use cases share the same skeleton via `train_regression(...)` in
`src/ml/common.py`: pin Spark to `main`, set up MLflow, load a gold table
(optionally pre-filter), do a temporal split (train: 2019-2023, test: 2025),
build a `StringIndexer → VectorAssembler → GBTRegressor` pipeline, train,
evaluate, and log params + metrics + model. Each use-case script just declares
its source/label/feature lists and calls the helper.

```bash
# Train (one experiment per use case in MLflow)
docker exec -it spark-master spark-submit /opt/spark/src/ml/fare_prediction.py    # fare_prediction
docker exec -it spark-master spark-submit /opt/spark/src/ml/trip_duration.py       # trip_duration
docker exec -it spark-master spark-submit /opt/spark/src/ml/demand_forecast.py     # demand_forecast
```

Open the MLflow UI at http://localhost:5000 to inspect runs, then open the
corresponding `src/notebooks/ml/*_diagnostics.ipynb` to plot predicted vs actual,
residuals, feature importance and per-year drift on the latest run.

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

# Silver — clean / conform from bronze, WAP per month with audit + quarantine
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/silver/clean_yellow_taxi.py
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/silver/clean_zone_lookup.py
# Reset the silver layer (drops all silver refs, wipes s3a://warehouse/silver/, recreates from main)
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/silver/reset_silver.py

# Gold — join + aggregate from silver, WAP per month against `main`
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/gold/build_yellow_taxi_trips.py
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/gold/aggregate_demand_zone_hour.py
```

### Nessie namespace structure (layer-first medallion)

```
nessie (catalog)
├── bronze
│   ├── yellow_taxi                     (monthly partitioned by tpep_pickup_datetime)
│   └── taxi_zone_lookup                (unpartitioned, small reference table)
├── silver
│   ├── yellow_taxi                     (cleaned, typed, deduplicated)
│   └── taxi_zone_lookup                (standardized)
└── gold
    ├── yellow_taxi_trips               (trip-level: zone denorm + time features, partitioned by month(pickup_datetime))
    └── pickup_demand_zone_hour         ((zone, hour_bucket) aggregate, partitioned by month(hour_bucket))
```

Iceberg manages the physical file layout in MinIO (`s3a://warehouse/`) automatically.
No manual folder organization — hidden partitioning handles it.

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

---

**Last Updated**: May 2026
