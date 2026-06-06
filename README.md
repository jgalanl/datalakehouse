# Data Lakehouse Platform - Master's Degree Final Project

A local, batch-oriented data lakehouse built for a master's thesis (TFM). It runs a
medallion (bronze/silver/gold) pipeline over the NYC TLC yellow-taxi dataset and trains
three Spark MLlib models, integrating seven open-source components under Docker Compose.

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
- **model-server**: serves a registered model over REST (`mlflow models serve`, `api` profile)

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
├── PostgreSQL (MLflow tracking DB) — `ml` + `governance` profiles
├── PostgreSQL (OpenMetadata DB) — `governance` profile
├── Elasticsearch (OpenMetadata search) — `governance` profile
└── model-server (MLflow REST serving on :5001) — `api` profile
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
# Core only (MinIO, minio-init, Nessie) — no compute. Rarely useful on its own.
docker compose up -d

# Ingest: core + Spark (master/notebook/workers) + Dremio
docker compose --profile ingest up -d

# ML: core + Spark + Dremio + MLflow (+ its PostgreSQL)
docker compose --profile ml up -d

# Governance: core + MLflow + OpenMetadata (+ PostgreSQL + Elasticsearch)
docker compose --profile governance up -d

# API: core + MLflow + model-server (serves a registered model over REST on :5001)
docker compose --profile api up -d model-server

# Multiple profiles union their service sets (e.g. ML + governance)
docker compose --profile ml --profile governance up -d
```

See [RUNBOOK.md](RUNBOOK.md) for the end-to-end execution sequence (bronze → silver → gold → ML → serving).

> `minio`, `minio-init` and `nessie` are profile-less — the always-on core. Everything
> else is gated: Spark + Dremio behind `ingest` (and `ml`), MLflow behind `ml` (and
> `governance`), the OpenMetadata stack behind `governance`, and the model REST server
> behind `api`. A bare `docker compose up` brings up only the core, so always pass a
> profile to get compute. Spark-submit pipelines need `ingest` or `ml`; ML training
> needs `ml`; serving a trained model needs `api` (see [src/ml/api/](src/ml/api/)).

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
- Model serving over REST (`mlflow models serve`) via the `model-server` service (`api` profile) — see [src/ml/api/](src/ml/api/)

**Tracking Server**: http://localhost:5000
**Model REST API** (when the `api` profile is up): http://localhost:5001
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

Bronze ingestion downloads NYC TLC parquet files for Q1 of 2023/2024/2025
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

`clean()` runs in two stages: **validity filtering** drops rows that are wrong
(NULL datetimes, `pickup >= dropoff`, non-positive `fare_amount` / `total_amount`,
negative `trip_distance` / `tip_amount` / `tolls_amount`, location IDs outside
`1..265`, categorical codes off the TLC dictionary, `passenger_count` outside
`1..6`, exact 20-column duplicates), then **IQR outlier removal** trims both tails
on `fare_amount`, `total_amount`, `trip_distance` and trip duration using Tukey
"far out" fences `[Q1 − 3·IQR, Q3 + 3·IQR]` **computed per `RatecodeID`** (metered,
flat-fare JFK/Newark, negotiated, group, unknown — each is a distinct fare
population). Groups with fewer than 50 rows skip the fence to avoid unstable
quartiles. The audit gate quarantines any month whose total drop ratio exceeds
35 %.

`SILVER_SCHEMA` is declared **per pipeline** with explicit `NOT NULL` on every
column the `clean()` filters provably make non-null (datetimes, IDs, `> 0` /
`>= 0` amounts, `passenger_count` between 1 and 6, etc.). Unfiltered surcharges
(`extra`, `mta_tax`, `improvement_surcharge`) and year-dependent fields
(`congestion_surcharge`, `airport_fee`, `cbd_congestion_fee`) remain nullable.
Tables are created via `CREATE TABLE ... USING iceberg` SQL DDL so the
`NOT NULL` constraints land in Iceberg's metadata.

```bash
# Yellow taxi (monthly, partitioned, WAP with validity + per-RatecodeID IQR
# outlier removal; audit gate at 35% drop ratio)
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/silver/clean_yellow_taxi.py

# Zone lookup (small reference, full refresh)
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/silver/clean_zone_lookup.py
```

Verify the silver layer interactively via `src/notebooks/silver/verify_iceberg.ipynb`
(Jupyter at http://localhost:8888): Nessie refs, schema, snapshots, yearly growth,
cleaning-rule assertions (validity + per-month per-`RatecodeID` IQR fence
verification), bronze↔silver retention, and zone-lookup orphan detection.

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

ML pipelines read **only from gold on `main`** (strict medallion). A single
unified `train.py` dispatches by use case (`fare` / `duration` / `demand`)
and runs the same workflow end-to-end inside one MLflow experiment per use
case. The split is a **last-year holdout**: `common_test` is every row of
`TEST_YEAR = 2025` (fully held out — the model never sees any 2025 row
during training or selection), while `train` / `val` are a
stratified-by-year random split of `TRAIN_VAL_YEARS = [2023, 2024]` at `val_frac = 0.15` per year. The metric on `common_test` is
therefore an unbiased generalization estimate to a future year, and the
diagnostic notebook tells the drift story by comparing the per-year
breakdown of TRAIN_VAL_YEARS (in-sample-ish) to TEST_YEAR (held out).

```bash
# Use case 1 — fare regression (target: fare_amount on yellow_taxi_trips)
docker exec -it spark-master spark-submit /opt/spark/src/ml/train.py fare

# Use case 2 — duration regression (target: trip_duration_minutes on yellow_taxi_trips)
docker exec -it spark-master spark-submit /opt/spark/src/ml/train.py duration

# Use case 3 — demand forecast (target: n_pickups on pickup_demand_zone_hour)
docker exec -it spark-master spark-submit /opt/spark/src/ml/train.py demand

# Smoke test (1 % sample, tiny grids, CV k=2 — runs in minutes):
docker exec -it spark-master spark-submit /opt/spark/src/ml/train.py fare --fast
```

Each invocation runs five phases plus a baseline:

- **Phase 0 — drop redundant categoricals.** `borough` (and `pu_service_zone`
  for demand) are derivable from `pu_location_id` via the silver zone lookup,
  so they're dropped up-front for every algorithm/phase.
- **Baseline.** Mean-of-label per `(group_cols)` on `common_test`. Floor
  reference; the per-group mean is the RMSE-optimal constant-per-group
  predictor — the previous median version was MAE-optimal, wrong loss for
  our reported metric.
- **Phase 1 — Training.** One `CV(k=3)` per algorithm over its
  `FULL_PARAM_GRIDS` entry (4-pt GLR, 4-pt GBT, 4-pt RFR = 12 fits/algorithm). The within-run
  winner is whichever algorithm's CV bestModel scores lowest on `val_rmse`.
  Each algorithm gets its fairly-tuned best registered — the previous
  design (Phase 1 screening with 3-pt grids + Phase 2 winner-only
  optimization) was collapsed into a single training step.
- **Phase 2 — More-data.** Refit the Phase-1 winner on `SUBSET_A_YEARS`
  (the earliest training year); subset B is the Phase-1 winner trained
  on all of `TRAIN_VAL_YEARS` and reused without refit. Both score on the
  same `common_test = TEST_YEAR`, so neither sees the held-out year — the
  contrast isolates "does more training data help generalize to a future
  year?".
- **Phase 3 — Registration.** Every candidate trained in this invocation
  (4 per run: 3 training + 1 more-data-A) becomes a new MLflow Model Registry version under the
  experiment name (`fare_prediction`, `trip_duration`, `demand_forecast`).
  The within-run winner (lowest `val_rmse`) gets `@champion` **only if its
  `val_rmse` is strictly below the previous registry champion's
  `val_rmse`** (strict point-estimate improvement); otherwise it is tagged
  `validation_status="challenger"`. Every other candidate is always tagged
  `validation_status="challenger"`. The `@challenger` alias points to the
  best non-champion version from this run. **`common_test` is held out and
  never used to rank candidates** — the test metrics on each registered
  version are an unbiased generalization estimate, not a selection signal.
  Every version is also pinned to the data state at training time via
  `nessie_main_hash`
  and `source_snapshot_id` tags (reproducibility — the model card alone
  is enough to rebuild the exact training input). On promotion the new
  `@champion` gets three latency tags from a 1000-row `common_test`
  sample (`inference_latency_ms_per_row` / `..._sample_n` /
  `..._total_s`) — a between-champion cost regression check, not a
  production SLO. The registry is pruned to the top
  `REGISTRY_MAX_VERSIONS = 20` non-aliased versions at the end of every
  run; `@champion` / `@challenger` aliased versions are always kept.

Open MLflow at http://localhost:5000 and the per-use-case diagnostic notebook
for the model you trained —
[fare_diagnostics.ipynb](src/notebooks/ml/fare_diagnostics.ipynb) /
[duration_diagnostics.ipynb](src/notebooks/ml/duration_diagnostics.ipynb) /
[demand_diagnostics.ipynb](src/notebooks/ml/demand_diagnostics.ipynb)
— each loads `<experiment>@champion` from the registry and plots the per-year
breakdown of `common_test`, predicted vs actual, residuals, feature
importance and the Phase 2 more-data comparison.

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
│   │   ├── silver/          # Bronze → Silver (clean_yellow_taxi.py, clean_zone_lookup.py)
│   │   └── gold/            # Silver → Gold (build_yellow_taxi_trips.py, aggregate_demand_zone_hour.py)
│   ├── ml/                  # ML training on gold — one unified train.py dispatched by use case
│   │                        #   (train.py {fare|duration|demand} [--fast]); shared building blocks
│   │                        #   split across splits.py / pipelines.py / algorithms.py / tracking.py
│   │   └── api/             # REST serving — client.py + example payloads + README (model-server, `api` profile)
│   └── notebooks/           # Jupyter notebooks, split per medallion layer
│       ├── bronze/          # explore (pandas, raw parquet first look), preprocess (Spark, silver-rule evidence), verify_iceberg
│       ├── silver/          # verify_iceberg
│       ├── gold/            # verify_iceberg (trips + demand audits)
│       └── ml/              # {fare,duration,demand}_diagnostics.ipynb — one per use case; each loads <experiment>@champion
├── RUNBOOK.md               # End-to-end execution sequence (bronze → silver → gold → ML → serving)
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

- **OpenMetadata**: catalogs the gold Iceberg tables (via Nessie's Iceberg-REST endpoint)
  and the registered MLflow models; Explore/search backed by Elasticsearch

The end-to-end execution sequence (bronze → silver → gold → ML → cataloging) is in
[RUNBOOK.md](RUNBOOK.md).

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
