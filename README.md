# Data Lakehouse Platform - Master's Degree Final Project

A comprehensive, production-ready data lakehouse platform demonstrating technical rigor across all components. This architecture integrates open-source technologies to create a complete data platform suitable for proof-of-concept testing and advanced analytics.

## Architecture Overview

The platform consists of seven core components organized into logical layers:

### Storage Layer

- **MinIO**: S3-compatible object storage serving as the data lake foundation

### Metadata & Cataloging Layer

- **Project Nessie**: Git-like versioning and metadata cataloging for data
- **Apache Iceberg**: Open table format enabling ACID transactions on data lakes
- **Iceberg REST Catalog**: OpenAPI-compliant metadata management

### Compute Layer

- **Apache Spark**: Distributed data processing engine (3.5.0)
  - 1 Master node
  - 2 Worker nodes
  - Configured with Iceberg and Nessie support

### Query & Analytics Layer

- **Dremio OSS**: Self-service analytics SQL engine with semantic layer

### Data Governance Layer

- **OpenMetadata**: Data documentation, lineage, and governance platform

### ML Operations Layer

- **MLflow**: ML experiment tracking and model registry

## System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    Client Applications                           │
└──────────┬──────────────────────────┬──────────────────┬─────────┘
           │                          │                  │
    ┌──────▼────────┐    ┌───────────▼────────┐   ┌─────▼──────────┐
    │ Spark (Batch)  │    │ Dremio (SQL/BI)    │   │ MLflow (ML)    │
    │ Processing     │    │ Analytics Engine   │   │ Operations     │
    └──────┬────────┘    └──────┬─────────────┘   └─────┬──────────┘
           │                     │                       │
    ┌──────▼────────────────────▼───────────────────────▼──────────┐
    │           Iceberg + Nessie Layer                             │
    │  (Table Format + Versioning + Metadata Cataloging)           │
    └──────┬─────────────────────────────────────────────┬─────────┘
           │                                             │
    ┌──────▼─────────────────────────────────────────────▼──────────┐
    │  MinIO Object Storage (S3-Compatible)                         │
    │  - warehouse/ (Iceberg tables)                                │
    │  - mlflow-artifacts/ (ML models)                              │
    │  - spark-logs/ (Application logs)                             │
    │  - dremio-uploads/ (Query results)                            │
    └───────────────────────────────────────────────────────────────┘

Supporting Services:
├── PostgreSQL (Nessie catalog DB)
├── PostgreSQL (Iceberg REST catalog DB)
├── PostgreSQL (Dremio coordination DB)
├── PostgreSQL (MLflow tracking DB)
├── MySQL (OpenMetadata DB)
└── Elasticsearch (OpenMetadata search)
```

## Prerequisites

- **Docker**: Version 20.10+
- **Docker Compose**: Version 2.0+
- **System Resources**:
  - Minimum: 8 GB RAM, 20 GB disk space
  - Recommended: 16 GB RAM, 50 GB disk space
- **Network**: Ports 9000-9001, 5432-5435, 8080-8082, 8181, 8585-8586, 9047, 19120, 5000, 31010, 45678, 3306, 9200

## Quick Start

### 1. Clone and Setup

```bash
# Clone the repository
git clone <repository-url>
cd datalakehouse

# Copy environment configuration
cp .env.example .env

# Configure credentials (optional, defaults are provided)
# Edit .env with your preferred credentials
```

### 2. Initialize the Platform

#### On Linux/macOS:

```bash
chmod +x scripts/init-platform.sh
./scripts/init-platform.sh
```

#### On Windows (PowerShell):

```powershell
.\scripts\init-platform.bat
```

#### Or manually with Docker Compose:

```bash
docker-compose up -d
```

### 3. Verify Services

```bash
# Check container status
docker-compose ps

# View service logs
docker-compose logs -f <service-name>

# Test MinIO connectivity
curl http://localhost:9000/minio/health/live
```

### 4. Access Web Interfaces

| Service       | URL                           | Credentials                         |
| ------------- | ----------------------------- | ----------------------------------- |
| MinIO Console | http://localhost:9001         | minioadmin / minioadmin123          |
| Spark Master  | http://localhost:8080         | N/A                                 |
| Dremio        | http://localhost:9047         | admin@example.com / dremio123       |
| OpenMetadata  | http://localhost:8585         | admin@example.com / openmetadata123 |
| MLflow        | http://localhost:5000         | N/A                                 |
| Nessie API    | http://localhost:19120/api/v2 | N/A                                 |

## Service Details

### MinIO (Storage Layer)

**Purpose**: S3-compatible object storage foundation

**Configuration**:

- S3 API: http://localhost:9000
- Console: http://localhost:9001
- Default Buckets: warehouse, mlflow-artifacts, spark-logs, dremio-uploads

**Documentation**: https://min.io/docs/minio/linux/

### Project Nessie (Metadata Versioning)

**Purpose**: Git-like versioning for data objects and metadata

**Features**:

- Atomic transactions and ACID guarantees
- Time travel queries
- Multi-user collaboration
- Branch/tag support for data versions

**API**: http://localhost:19120/api/v2
**Documentation**: https://projectnessie.org/

### Apache Iceberg (Table Format)

**Purpose**: Open table format enabling ACID transactions on object storage

**Features**:

- Schema evolution
- Hidden partitioning
- Partition pruning
- Time travel

**REST Catalog**: http://localhost:8181
**Documentation**: https://iceberg.apache.org/

### Apache Spark (Compute)

**Purpose**: Distributed data processing and batch operations

**Configuration**:

- Master: spark://spark-master:7077
- Master UI: http://localhost:8080
- Worker 1 UI: http://localhost:8081
- Worker 2 UI: http://localhost:8082

**Installed Libraries**:

- Iceberg Spark Runtime
- Nessie Spark Extensions
- AWS SDK for S3
- Hadoop AWS plugin

**Documentation**: https://spark.apache.org/

### Dremio OSS (Query Engine)

**Purpose**: SQL query engine with semantic layer for analytics

**Features**:

- Distributed SQL query engine
- Iceberg and Nessie integration
- Physical optimization
- Flight RPC for high-performance queries

**Console**: http://localhost:9047
**Flight Endpoint**: grpc://localhost:31010
**Documentation**: https://docs.dremio.com/

### OpenMetadata (Governance)

**Purpose**: Data documentation, lineage, and governance

**Features**:

- Automated metadata extraction
- Data lineage tracking
- Collaboration and annotations
- Data quality monitoring
- Glossary and policy management

**Console**: http://localhost:8585
**Documentation**: https://docs.open-metadata.org/

### MLflow (ML Operations)

**Purpose**: ML experiment tracking and model registry

**Features**:

- Experiment tracking
- Parameter and metric logging
- Artifact storage
- Model registry with versioning
- Model serving capabilities

**Tracking Server**: http://localhost:5000
**Backend**: PostgreSQL
**Artifact Storage**: S3 (MinIO)
**Documentation**: https://mlflow.org/docs/

## Configuration Files

### Environment Variables (`.env`)

```bash
# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123

# Database Passwords
NESSIE_DB_PASSWORD=nessie123
ICEBERG_DB_PASSWORD=iceberg123
DREMIO_DB_PASSWORD=dremio123
OPENMETADATA_DB_PASSWORD=openmetadata123
MLFLOW_DB_PASSWORD=mlflow123
```

### Spark Configuration (`config/spark/spark-defaults.conf`)

Key configurations:

- Iceberg SQL extensions enabled
- Nessie catalog integration
- S3/MinIO endpoint configuration
- Performance tuning parameters

### Dremio Configuration (`config/dremio/dremio.conf`)

Key configurations:

- Iceberg REST Catalog source
- MinIO S3 source
- PostgreSQL backend for coordination

## Development Workflow

### 1. Data Ingestion

```python
from pyspark.sql import SparkSession
from src.core import SparkSessionBuilder, CatalogManager
from src.ingestion.data_ingestion import DataIngestionService

# Initialize Spark
spark = SparkSessionBuilder.build(
    app_name="data_ingestion",
    use_nessie=True
)

# Create catalog and database
catalog = CatalogManager(spark, catalog_name="nessie")
catalog.create_database("raw_data", location="s3a://warehouse/raw_data/")

# Ingest data
ingestion = DataIngestionService(spark, catalog_name="nessie")
ingestion.ingest_csv(
    file_path="data/raw/input.csv",
    database_name="raw_data",
    table_name="source_data",
    overwrite=True
)
```

### 2. Data Processing

```python
# Process with Spark
df = spark.read.table("nessie.raw_data.source_data")
processed_df = df.filter(df.value > 0).select("id", "value")

# Write processed data
processed_df.write.mode("overwrite").format("iceberg") \
    .option("write.format.default", "parquet") \
    .saveAsTable("nessie.processed_data.cleaned_data")
```

### 3. Query with Dremio

```sql
-- Query through Dremio SQL
SELECT * FROM iceberg_nessie.raw_data.source_data
WHERE value > 100
LIMIT 1000;
```

### 4. ML Experiment Tracking

```python
from src.ml.mlflow_ops import MLflowManager, MLExperimentTracker

# Initialize
manager = MLflowManager(tracking_uri="http://mlflow:5000")
tracker = MLExperimentTracker(manager, "customer_segmentation")

# Start experiment
run_id = tracker.start_training(
    run_name="baseline_model",
    params={"n_clusters": 5, "random_state": 42},
    tags={"model_type": "kmeans"}
)

# Train model (your code)
# ... model training ...

# Log metrics
tracker.log_training_metrics(
    train_metrics={"silhouette_score": 0.65},
    val_metrics={"silhouette_score": 0.62},
    step=0
)

# Register model
model_version = manager.register_model(
    model_uri=f"runs://{run_id}/model",
    model_name="customer_segmentation_v1",
    description="Baseline K-means clustering model"
)

tracker.end_run("FINISHED")
```

### 5. Data Governance with OpenMetadata

Access OpenMetadata UI to:

- Register data assets
- Add descriptions and documentation
- Track data lineage
- Create data quality rules
- Collaborate with team members

## Project Structure

```
datalakehouse/
├── docker-compose.yml          # Complete service orchestration
├── .env                        # Environment configuration
├── .env.example               # Configuration template
├── config/
│   ├── spark/
│   │   ├── spark-defaults.conf
│   │   └── log4j.properties
│   ├── dremio/
│   │   └── dremio.conf
│   ├── mlflow/
│   ├── openmetadata/
│   └── nessie/
├── docker/
│   ├── spark/
│   │   └── Dockerfile         # Custom Spark image with Iceberg
│   └── mlflow/
│       └── Dockerfile         # MLflow tracking server
├── src/
│   ├── core.py               # Core utilities and configuration
│   ├── ingestion/
│   │   └── data_ingestion.py # Data ingestion service
│   ├── processing/           # Data processing modules
│   └── ml/
│       └── mlflow_ops.py    # MLflow operations
├── notebooks/                # Jupyter notebooks for exploration
├── tests/                    # Unit tests
├── scripts/
│   ├── init-platform.sh     # Linux/macOS initialization
│   ├── init-platform.bat    # Windows initialization
│   └── cleanup.sh           # Service cleanup
├── data/
│   ├── raw/                 # Sample raw data
│   └── processed/           # Processed data outputs
└── docs/                    # Additional documentation
```

## Key Features & Technical Highlights

### ACID Transactions on Data Lakes

- **Iceberg + Nessie**: Ensure data consistency with ACID guarantees
- **Atomic updates**: Multi-table transactions with rollback capability

### Data Versioning & Time Travel

- **Nessie**: Git-like branching and tagging
- **Iceberg**: Snapshot isolation and time travel queries

### Distributed Processing

- **Spark**: Scalable batch processing with Iceberg native support
- **Nessie Integration**: Unified metadata across compute nodes

### Semantic Layer & Analytics

- **Dremio**: Self-service analytics with optimized query execution
- **Iceberg Pushdown**: Filter pushdown and projection pruning for performance

### ML Operations Pipeline

- **MLflow**: End-to-end ML lifecycle management
- **Model Registry**: Version control and staging for models
- **S3 Integration**: Artifact storage with MinIO

### Data Governance

- **OpenMetadata**: Comprehensive data cataloging and lineage
- **Quality Monitoring**: Data quality assertions and tracking
- **Collaboration**: Team-based governance workflows

## Proof-of-Concept Testing Scenarios

### Scenario 1: Multi-Table Transactions

```python
# Demonstrate ACID transactions with Iceberg + Nessie
# Create related tables, perform atomic updates, verify consistency
```

### Scenario 2: Data Versioning & Time Travel

```python
# Show version history with Nessie branches
# Query historical data versions
# Compare data across branches
```

### Scenario 3: ETL Pipeline

```python
# Ingest raw data → Process with Spark → Query with Dremio
# Track data lineage in OpenMetadata
# Monitor pipeline with MLflow
```

### Scenario 4: ML Model Management

```python
# Track experiments with MLflow
# Register and version models
# Transition through staging to production
```

## Common Operations

### View Service Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f spark-master

# Last N lines
docker-compose logs --tail=100 dremio
```

### Stop Services

```bash
docker-compose stop
```

### Restart Services

```bash
docker-compose restart spark-master
```

### Remove All Data

```bash
docker-compose down -v
```

### Access Container Shell

```bash
docker exec -it datalakehouse-spark-master /bin/bash
```

## Troubleshooting

### Services Won't Start

```bash
# Check Docker daemon
docker ps

# Check logs
docker-compose logs <service-name>

# Verify ports are available
netstat -tulpn | grep -E ':(9000|8080|5432)'
```

### MinIO Bucket Creation Fails

```bash
# Verify MinIO is running
curl http://localhost:9000/minio/health/live

# Manually create buckets
docker exec datalakehouse-minio mc alias set minio http://localhost:9000 minioadmin minioadmin123
docker exec datalakehouse-minio mc mb minio/warehouse
```

### Spark Can't Access MinIO

```bash
# Check S3 credentials in spark-defaults.conf
# Verify MinIO endpoint: http://minio:9000 (from Spark container perspective)
# Test connection from Spark container
docker exec datalakehouse-spark-master curl http://minio:9000/minio/health/live
```

### Database Connection Issues

```bash
# Test PostgreSQL connection
docker exec -it datalakehouse-nessie-postgres psql -U nessie -d nessie -c "SELECT version();"

# Check connection settings
grep POSTGRES config/spark/spark-defaults.conf
```

## Performance Tuning

### Spark Configuration

- Adjust executor memory based on available resources
- Enable adaptive query execution (already configured)
- Tune shuffle partitions based on data size

### MinIO Performance

- Use SSD for storage
- Configure appropriate block size
- Monitor network bandwidth

### Dremio Optimization

- Enable query result caching
- Configure appropriate reflection policies
- Monitor query performance

## Security Considerations

**Development Environment**:

- Default credentials are suitable for development only
- All communications are unencrypted
- No authentication on internal services

**Production Deployment** (not included):

- Use strong, unique credentials for all services
- Enable TLS/SSL for all communications
- Implement network isolation and firewalls
- Use secrets management (HashiCorp Vault, AWS Secrets Manager)
- Enable audit logging across all components
- Implement role-based access control (RBAC)
- Regular security updates and vulnerability scanning

## Contributing

1. Follow Python PEP 8 style guide
2. Add tests for new functionality
3. Document functions with docstrings
4. Update README with any new features
5. Commit messages should be descriptive

## License

See LICENSE file for details.

## References

### Official Documentation

- **MinIO**: https://min.io/docs/
- **Project Nessie**: https://projectnessie.org/docs/
- **Apache Iceberg**: https://iceberg.apache.org/docs/
- **Apache Spark**: https://spark.apache.org/docs/
- **Dremio**: https://docs.dremio.com/
- **OpenMetadata**: https://docs.open-metadata.org/
- **MLflow**: https://mlflow.org/docs/

### Academic References

- Iceberg: Efficient Iceberg Table Format for Analytics
- Nessie: Versioning and Branching for Data Lake Tables
- Delta Lake: High-Performance ACID Table Storage
- Apache Spark: Rethinking Analytics with Accelerated Machine Learning

## Support & Feedback

For issues, questions, or suggestions, please refer to the project documentation or create an issue in the repository.

---

**Project Status**: Development/Proof-of-Concept

**Last Updated**: January 2026

**Version**: 1.0.0
