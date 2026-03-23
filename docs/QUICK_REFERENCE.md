# Quick Reference Guide

## Service Access

| Service         | URL                           | Credentials              |
| --------------- | ----------------------------- | ------------------------ |
| MinIO Console   | http://localhost:9001         | See `.env`               |
| MinIO S3 API    | http://localhost:9000         | See `.env`               |
| Spark Master    | http://localhost:8080         | N/A                      |
| Spark Worker 1  | http://localhost:8081         | N/A                      |
| Spark Worker 2  | http://localhost:8082         | N/A                      |
| Dremio Console  | http://localhost:9047         | Configured on first boot |
| Dremio Flight   | grpc://localhost:31010        | N/A                      |
| Dremio ODBC     | localhost:45678               | N/A                      |
| OpenMetadata    | http://localhost:8585         | See `.env`               |
| MLflow Tracking | http://localhost:5000         | N/A                      |
| Nessie API      | http://localhost:19120/api/v2 | N/A                      |
| Jupyter Notebook| http://localhost:8888         | N/A                      |

## Database Access

| Database                  | Host      | Port | Credentials |
| ------------------------- | --------- | ---- | ----------- |
| PostgreSQL (MLflow)       | localhost | 5435 | See `.env`  |
| PostgreSQL (OpenMetadata) | localhost | 5432 | See `.env`  |
| Elasticsearch (OM)        | localhost | 9200 | N/A         |

## Common Commands

### Docker Compose

```bash
# Start all services
docker-compose up -d

# Stop all services
docker-compose stop

# View logs
docker-compose logs -f <service>

# Remove containers and volumes
docker-compose down -v

# Check service status
docker-compose ps
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
# PostgreSQL (MLflow)
psql -h localhost -p 5435 -U mlflow -d mlflow

# PostgreSQL (OpenMetadata)
psql -h localhost -p 5432 -U openmetadata_user -d openmetadata_db
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

# Create bronze table with hidden partitioning
spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.bronze.yellow_taxi (
        VendorID INT,
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP,
        passenger_count DOUBLE,
        trip_distance DOUBLE,
        fare_amount DOUBLE,
        tip_amount DOUBLE,
        total_amount DOUBLE
    )
    USING iceberg
    PARTITIONED BY (month(tpep_pickup_datetime))
""")

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
WHERE tpep_pickup_datetime >= '2024-01-01'
LIMIT 10;

-- Join with zone lookup
SELECT t.*, z.Zone, z.Borough
FROM nessie.silver.yellow_taxi t
JOIN nessie.silver.taxi_zone_lookup z ON t.PULocationID = z.LocationID;

-- Aggregate by zone
SELECT z.Zone, COUNT(*) as trip_count, AVG(t.fare_amount) as avg_fare
FROM nessie.gold.taxi_analytics t
JOIN nessie.silver.taxi_zone_lookup z ON t.PULocationID = z.LocationID
GROUP BY z.Zone
ORDER BY trip_count DESC;
```

## MLflow Examples

```python
import mlflow
from mlflow.tracking import MlflowClient

# Set tracking URI
mlflow.set_tracking_uri("http://localhost:5000")

# Create experiment
experiment_id = mlflow.create_experiment("my_experiment")

# Start run
with mlflow.start_run(experiment_id=experiment_id, run_name="run_1"):
    mlflow.log_param("learning_rate", 0.01)
    mlflow.log_metric("accuracy", 0.95)

    # Log model
    mlflow.sklearn.log_model(model, "model")

# Get best run
client = MlflowClient()
runs = client.search_runs(experiment_ids=[experiment_id])
best_run = max(runs, key=lambda r: r.data.metrics.get("accuracy", 0))
```

## Bronze Ingestion

```bash
# Ingest yellow taxi data: download from TLC, write to nessie.bronze.yellow_taxi
docker exec -it spark-master python3 /opt/spark/work-dir/src/etl/bronze/ingest_yellow_taxi.py

# Ingest zone lookup: download from TLC, write to nessie.bronze.taxi_zone_lookup
docker exec -it spark-master python3 /opt/spark/work-dir/src/etl/bronze/ingest_zone_lookup.py
```

### Nessie namespace structure (layer-first medallion)

```
nessie (catalog)
├── bronze
│   ├── yellow_taxi              (monthly partitioned by tpep_pickup_datetime)
│   └── taxi_zone_lookup         (unpartitioned, small reference table)
├── silver
│   ├── yellow_taxi              (cleaned, typed, deduplicated)
│   └── taxi_zone_lookup         (standardized)
└── gold
    └── taxi_analytics           (joined, aggregated, ML features)
```

Iceberg manages the physical file layout in MinIO (`s3a://warehouse/`) automatically.
No manual folder organization — hidden partitioning handles it.

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

**Last Updated**: January 2026
