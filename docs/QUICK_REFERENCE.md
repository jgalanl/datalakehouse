# Quick Reference Guide

## Service Access

| Service         | URL                           | Username          | Password        |
| --------------- | ----------------------------- | ----------------- | --------------- |
| MinIO Console   | http://localhost:9001         | minioadmin        | minioadmin123   |
| MinIO S3 API    | http://localhost:9000         | minioadmin        | minioadmin123   |
| Spark Master    | http://localhost:8080         | N/A               | N/A             |
| Spark Worker 1  | http://localhost:8081         | N/A               | N/A             |
| Spark Worker 2  | http://localhost:8082         | N/A               | N/A             |
| Dremio Console  | http://localhost:9047         | admin@example.com | dremio123       |
| Dremio Flight   | grpc://localhost:31010        | N/A               | N/A             |
| Dremio ODBC     | localhost:45678               | N/A               | N/A             |
| OpenMetadata    | http://localhost:8585         | admin@example.com | openmetadata123 |
| MLflow Tracking | http://localhost:5000         | N/A               | N/A             |
| Nessie API      | http://localhost:19120/api/v2 | N/A               | N/A             |
| Iceberg REST    | http://localhost:8181         | N/A               | N/A             |

## Database Access

| Database             | Host      | Port | Username | Password        |
| -------------------- | --------- | ---- | -------- | --------------- |
| PostgreSQL (Nessie)  | localhost | 5432 | nessie   | nessie123       |
| PostgreSQL (Iceberg) | localhost | 5433 | iceberg  | iceberg123      |
| PostgreSQL (Dremio)  | localhost | 5434 | dremio   | dremio123       |
| PostgreSQL (MLflow)  | localhost | 5435 | mlflow   | mlflow123       |
| MySQL (OpenMetadata) | localhost | 3306 | root     | openmetadata123 |

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
# Create alias
mc alias set minio http://localhost:9000 minioadmin minioadmin123

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
docker exec datalakehouse-spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /path/to/script.py

# Access Spark Shell
docker exec -it datalakehouse-spark-master spark-shell
```

### Database Access

```bash
# PostgreSQL (Nessie)
psql -h localhost -p 5432 -U nessie -d nessie

# MySQL (OpenMetadata)
mysql -h localhost -u root -p
```

## S3/MinIO S3 CLI

### Using AWS CLI with MinIO

```bash
# Configure
aws configure
# Endpoint URL: http://localhost:9000
# Access Key: minioadmin
# Secret Key: minioadmin123

# List buckets
aws s3 --endpoint-url http://localhost:9000 ls

# Upload file
aws s3 --endpoint-url http://localhost:9000 cp file.txt s3://warehouse/

# Download file
aws s3 --endpoint-url http://localhost:9000 cp s3://warehouse/file.txt .
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
    .config("spark.sql.catalog.nessie.uri", "http://nessie-server:19120/api/v2") \
    .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS nessie.demo")

# Create table
spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.demo.users (
        id BIGINT,
        name STRING,
        email STRING,
        created_at TIMESTAMP
    )
    USING iceberg
""")

# Write data
spark.createDataFrame([
    (1, "Alice", "alice@example.com", "2024-01-01"),
    (2, "Bob", "bob@example.com", "2024-01-02"),
]).toDF("id", "name", "email", "created_at") \
    .write.mode("append") \
    .format("iceberg") \
    .saveAsTable("nessie.demo.users")

# Read data
spark.sql("SELECT * FROM nessie.demo.users").show()

# Time travel
spark.sql("SELECT * FROM nessie.demo.users VERSION AS OF 0").show()
```

## Dremio SQL Examples

```sql
-- Create Iceberg Catalog connection
-- (Done through UI)

-- Query Iceberg table
SELECT * FROM iceberg_nessie.demo.users
LIMIT 10;

-- Join tables
SELECT u.id, u.name, o.total
FROM iceberg_nessie.demo.users u
JOIN iceberg_nessie.demo.orders o ON u.id = o.user_id;

-- Aggregate
SELECT name, COUNT(*) as order_count
FROM iceberg_nessie.demo.users u
JOIN iceberg_nessie.demo.orders o ON u.id = o.user_id
GROUP BY name;
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
