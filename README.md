# Data Lakehouse Platform - Master's Degree Final Project

A comprehensive, production-ready data lakehouse platform demonstrating technical rigor across all components. This architecture integrates open-source technologies to create a complete data platform suitable for proof-of-concept testing and advanced analytics.

## Architecture Overview

The platform consists of seven core components organized into logical layers:

### Storage Layer

- **MinIO**: S3-compatible object storage serving as the data lake foundation

### Metadata & Cataloging Layer

- **Project Nessie**: Git-like versioning and metadata cataloging for data (single Iceberg catalog)
- **Apache Iceberg**: Open table format enabling ACID transactions on data lakes

### Compute Layer

- **Apache Spark**: Distributed data processing engine (3.5.8)
  - 1 Master node
  - 1 Worker node
  - 1 Jupyter notebook driver (interactive development)
  - Configured with Iceberg 1.10.1 and Nessie 0.106.0 support

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
    │  Buckets:                                                     │
    │  - warehouse (all Iceberg tables — physical layout by Iceberg)│
    │  - mlflow-artifacts (ML model artifacts, versioned)           │
    │  - spark-logs (Spark event logs)                              │
    └───────────────────────────────────────────────────────────────┘

    Nessie catalog — single namespace `nyc`, layer = branch (layer-first medallion):
    ├── nessie.nyc.yellow_taxi              on bronze  (raw ingestion, monthly partitioned)
    ├── nessie.nyc.taxi_zone_lookup         on bronze  (reference, unpartitioned)
    ├── nessie.nyc.yellow_taxi              on silver  (cleaned, typed, deduplicated)
    ├── nessie.nyc.taxi_zone_lookup         on silver  (standardized)
    ├── nessie.nyc.yellow_taxi_trips        on main    (trip-level enriched: zone denorm + time features)
    └── nessie.nyc.pickup_demand_zone_hour  on main    (aggregated by pickup zone × hour bucket)

Supporting Services:
├── PostgreSQL (MLflow tracking DB) — `ml` profile
├── PostgreSQL (OpenMetadata DB) — `governance` profile
└── Elasticsearch (OpenMetadata search) — `governance` profile
```

## Prerequisites

- **Docker**: Version 20.10+
- **Docker Compose**: Version 2.0+
- **System Resources**:
  - Minimum: 8 GB RAM, 20 GB disk space
  - Recommended: 18 GB RAM and 6 CPUs allocated to Docker, 50 GB disk space
- **Network**: Ports 9000-9001, 5432, 5435-5437, 8080-8081, 8585, 8888, 9047, 19120, 5000, 31010, 32010, 45678, 9200, 9300, 4040-4041

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

Bring the platform up directly with Docker Compose profiles:

```bash
# Core stack only (MinIO, Nessie, Spark, Dremio)
docker compose up -d

# Including ML services (MLflow + its PostgreSQL)
docker compose --profile ml up -d

# Including governance (OpenMetadata + PostgreSQL + Elasticsearch)
docker compose --profile governance up -d

# Everything
docker compose --profile ml --profile governance up -d
```

See [RUNBOOK.md](RUNBOOK.md) for the end-to-end execution sequence (bronze → silver → gold → ML).

> The `mlflow` and `mlflow-postgres` services are gated behind the `ml` profile.
> The OpenMetadata stack lives behind the `governance` profile. ML training scripts
> require the `ml` profile to be active.

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

| Service          | URL                           | Credentials              |
| ---------------- | ----------------------------- | ------------------------ |
| MinIO Console    | http://localhost:9001         | See `.env`               |
| Spark Master     | http://localhost:8080         | N/A                      |
| Spark Worker 1   | http://localhost:8081         | N/A                      |
| Jupyter Notebook | http://localhost:8888         | N/A                      |
| Dremio           | http://localhost:9047         | Configured on first boot |
| OpenMetadata     | http://localhost:8585         | See `.env`               |
| MLflow           | http://localhost:5000         | N/A                      |
| Nessie API       | http://localhost:19120/api/v2 | N/A                      |

## Service Details

### MinIO (Storage Layer)

**Purpose**: S3-compatible object storage foundation

**Configuration**:

- S3 API: http://localhost:9000
- Console: http://localhost:9001
- Default Buckets: warehouse (Iceberg tables), mlflow-artifacts (ML artifacts), spark-logs

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

**Documentation**: https://iceberg.apache.org/

### Apache Spark (Compute)

**Purpose**: Distributed data processing and batch operations

**Configuration**:

- Master: spark://spark-master:7077
- Master UI: http://localhost:8080
- Worker 1 UI: http://localhost:8081
- Notebook driver UI: http://localhost:4041 (when a Jupyter cell holds an active SparkContext)

**Installed Libraries**:

- Iceberg Spark Runtime
- Nessie Spark Extensions
- AWS SDK for S3
- Hadoop AWS plugin
- MLflow client (for the ML training scripts)

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

Copy `.env.example` to `.env` and fill in your credentials. See `.env.example` for the full list of required variables.

### Spark Configuration (`config/spark/spark-defaults.conf`)

Key configurations:

- Iceberg SQL extensions enabled
- Nessie catalog integration
- S3/MinIO endpoint configuration
- Performance tuning parameters

### Dremio Configuration (`config/dremio/dremio.conf`)

Key configurations:

- Single-node deployment (coordinator + executor)
- Embedded RocksDB for metadata (no external PostgreSQL)
- Nessie data source configured via Dremio UI

## Development Workflow

### 1. Bronze Ingestion

Bronze ingestion downloads NYC TLC parquet files for Q1 of 2019/2020/2022/2023/2025
and writes them as Iceberg tables on the `bronze` Nessie branch (WAP per month).
`BRONZE_SCHEMA` (declared in [src/pipelines/bronze/ingest_yellow_taxi.py](src/pipelines/bronze/ingest_yellow_taxi.py))
reconciles the year-over-year drift; `cbd_congestion_fee` (added by TLC in Jan 2025)
is handled via Iceberg `ALTER TABLE ADD COLUMNS` on the WAP branch. Bronze stays
faithful to the source — every column is `NULLABLE`. Cleaning rules run in silver.

```bash
# Yellow taxi: download from TLC, normalize schema, write to nessie.nyc.yellow_taxi
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/bronze/ingest_yellow_taxi.py

# Zone lookup: small reference table → nessie.nyc.taxi_zone_lookup
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/bronze/ingest_zone_lookup.py
```

Verify the bronze layer via `src/notebooks/bronze/verify_iceberg.ipynb`. The exploration
notebook (`explore.ipynb`) gives a first look at the raw TLC parquet (without going
through bronze), and `preprocess.ipynb` derives the silver cleaning rules from the
full dataset.

### 2. Silver Processing

Silver ETL follows the Write-Audit-Publish (WAP) pattern: each run branches off the
persistent `silver` branch, writes the cleaned output, audits it, and only merges back
on success. Failing audits are quarantined as immutable `review/` tags and the run
continues (see `src/pipelines/silver/clean_yellow_taxi.py` and `clean_zone_lookup.py`).

`SILVER_SCHEMA` is declared **per pipeline** with explicit `NOT NULL` on every column
that the `clean()` function provably filters (datetimes, IDs, amounts, `passenger_count`
between 1 and 6, etc.). Unfiltered surcharges (`extra`, `mta_tax`, `improvement_surcharge`)
and year-dependent fields (`congestion_surcharge`, `airport_fee`, `cbd_congestion_fee`)
remain nullable. Tables are created via `CREATE TABLE ... USING iceberg` SQL DDL so the
`NOT NULL` constraints land in Iceberg's metadata.

```bash
# Yellow taxi (monthly, partitioned, WAP with 20% drop-ratio threshold)
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/silver/clean_yellow_taxi.py

# Zone lookup (small reference, full refresh)
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/silver/clean_zone_lookup.py

# Reset the silver layer (drops all silver refs, wipes s3a://warehouse/silver/, recreates silver from main)
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/silver/reset_silver.py
```

Verify the silver layer interactively via `src/notebooks/silver/verify_iceberg.ipynb`
(Jupyter at http://localhost:8888): Nessie refs, schema, snapshots, yearly growth,
cleaning-rule assertions, bronze↔silver retention, and zone-lookup orphan detection.

### 3. Gold Build

Gold ETL follows the same WAP pattern as silver: each (year, month) is processed on
its own WAP branch off `main`, audited, and merged on success. Failures are quarantined
as `review/gold-...` tags and the loop continues. Each gold pipeline declares its
own `GOLD_SCHEMA` with `NOT NULL` on every derived column (year/month/week/dow/hour,
trip_duration_minutes, group-by keys, aggregated metrics) and on every zone field
the audit gate guarantees non-null (`pu_borough`, `pu_zone`, `do_borough`, `do_zone`).
Two scripts produce two gold tables:

```bash
# Trip-level gold (silver yellow_taxi enriched with zone denorm + derived time features)
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/gold/build_yellow_taxi_trips.py

# Aggregated gold (pickups per (zone, hour bucket) — target for the demand model)
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/gold/aggregate_demand_zone_hour.py
```

Verify the gold layer interactively via `src/notebooks/gold/verify_iceberg.ipynb`:
schemas, snapshots, partitions, silver↔gold row-count parity for the trips table,
and `sum(n_pickups)` conservation per month for the demand table.

### 4. Query with Dremio

```sql
-- Query through Dremio (Nessie source configured via UI)
SELECT * FROM nessie.nyc.yellow_taxi AT BRANCH "silver"
WHERE tpep_pickup_datetime >= '2024-01-01'
LIMIT 1000;
```

### 5. ML Training and Diagnostics

ML pipelines read **only from gold on `main`** (strict medallion). All three scripts
use the same temporal split — train on Q1 of 2019/2020/2022/2023, hold out Q1 2025
as a drift test set — and log to MLflow.

```bash
# Use case 1 — fare regression (target: fare_amount)
docker exec -it spark-master spark-submit /opt/spark/src/ml/fare_prediction.py

# Use case 2 — duration regression (target: trip_duration_minutes)
docker exec -it spark-master spark-submit /opt/spark/src/ml/trip_duration.py

# Use case 3 — demand forecast (target: n_pickups per zone × hour)
docker exec -it spark-master spark-submit /opt/spark/src/ml/demand_forecast.py
```

Each run logs parameters, train/test metrics (RMSE, MAE, R²) and the trained
`PipelineModel` to MLflow at http://localhost:5000. Diagnostic notebooks under
`src/notebooks/ml/` load the latest run from MLflow, plot predicted vs actual,
residuals, feature importance and per-year drift.

### 6. Data Governance with OpenMetadata

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
│   │   └── log4j2.properties
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
│   ├── common/              # Shared utilities — SparkSession factory, Nessie branch helpers,
│   │                        #   Iceberg DDL generator (spark_factory.py, nessie_manager.py, iceberg_schema.py)
│   ├── pipelines/
│   │   ├── bronze/          # Download + Iceberg ingestion (ingest_yellow_taxi.py, ingest_zone_lookup.py)
│   │   ├── silver/          # Bronze → Silver (clean_yellow_taxi.py, clean_zone_lookup.py, reset_silver.py)
│   │   └── gold/            # Silver → Gold (build_yellow_taxi_trips.py, aggregate_demand_zone_hour.py)
│   ├── ml/                  # ML training scripts on gold (ml_common.py, fare_prediction.py, trip_duration.py, demand_forecast.py)
│   └── notebooks/           # Jupyter notebooks, split per medallion layer
│       ├── bronze/          # explore (pandas, raw parquet first look), preprocess (Spark, silver-rule evidence), verify_iceberg
│       ├── silver/          # verify_iceberg
│       ├── gold/            # verify_iceberg (trips + demand audits)
│       └── ml/              # *_diagnostics.ipynb (one per ML use case, loads latest MLflow run)
├── RUNBOOK.md               # End-to-end execution sequence (bronze → silver → gold → ML)
└── docs/                    # Architecture / development / quick reference
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
docker exec -it spark-master /bin/bash
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

# Manually create buckets (use credentials from .env)
docker exec minio mc alias set datalakehouse http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD
docker exec minio mc mb datalakehouse/warehouse
```

### Spark Can't Access MinIO

```bash
# Check S3 credentials in spark-defaults.conf
# Verify MinIO endpoint: http://minio:9000 (from Spark container perspective)
# Test connection from Spark container
docker exec spark-master curl http://minio:9000/minio/health/live
```

### Database Connection Issues

```bash
# Test MLflow PostgreSQL connection
docker exec -it mlflow-postgres psql -U mlflow -d mlflow -c "SELECT version();"

# Test OpenMetadata PostgreSQL connection
docker exec -it openmetadata-postgres psql -U openmetadata_user -d openmetadata_db -c "SELECT 1;"
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
2. Keep comments and docstrings concise — explain *why*, not *what*
3. Update README and RUNBOOK if behaviour changes
4. Commit messages should be descriptive

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

**Last Updated**: May 2026

**Version**: 1.0.0
