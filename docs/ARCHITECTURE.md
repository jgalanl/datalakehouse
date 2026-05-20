# Architecture & Design Decisions

## Technical Architecture

This document details the architectural decisions and technical rationale behind the data lakehouse platform.

## 1. Storage Layer: MinIO

### Decision Rationale

- **S3 Compatibility**: MinIO provides an S3-compatible interface, enabling seamless integration with AWS SDK
- **Self-Hosted**: Full control over data locality and compliance
- **Performance**: Multi-node support for horizontal scaling
- **Simplicity**: Single-binary deployment

### Trade-offs

- **vs. AWS S3**: MinIO offers on-premise control but requires infrastructure management
- **vs. HDFS**: S3 provides better compatibility with modern big data tools

### Configuration

- Path-style access enabled for better compatibility
- Virtual host-style addressing not required
- Configurable storage backend

## 2. Metadata Layer: Nessie + Iceberg

### Decision Rationale

#### Nessie (Versioning)

- **Git-like semantics**: Familiar branching and tagging paradigm
- **ACID transactions**: Atomic changes across multiple tables
- **Time travel**: Query historical data versions
- **Multi-user collaboration**: Isolated development branches

#### Iceberg (Table Format)

- **Open table format**: Not vendor-locked
- **ACID guarantees**: Full transaction support
- **Schema evolution**: Non-breaking schema updates. Demonstrated in bronze ingestion: the `cbd_congestion_fee` field (added by NYC TLC in Jan 2025) is added via `ALTER TABLE ADD COLUMNS` on the WAP branch when the first 2025 file is ingested. Pre-2025 data returns `NULL` for this column automatically.
- **Hidden partitioning**: Partition pruning without exposing partition columns
- **Snapshot isolation**: Multiple readers/writers without conflicts

### Integration Pattern

```
Data Writers → Iceberg → Nessie (Single Catalog) → MinIO (Storage)
                                    ↓
                        Data Readers (Spark, Dremio)
```

### Data Model (Layer-First Medallion)

Following Strengholt's centralized medallion pattern, the medallion **layer is the Nessie
branch** — all tables share one namespace `nyc` (the data domain), so an identifier never
echoes its layer. The same `nessie.nyc.yellow_taxi` resolves to a different physical table
on the `bronze` and `silver` branches.

```
branch   table identifier                       — description
bronze   nessie.nyc.yellow_taxi                  — raw ingestion, PARTITIONED BY month(tpep_pickup_datetime)
bronze   nessie.nyc.taxi_zone_lookup             — reference table, unpartitioned (~265 rows)
silver   nessie.nyc.yellow_taxi                  — cleaned, typed, deduplicated
silver   nessie.nyc.taxi_zone_lookup             — standardized zone names
main     nessie.nyc.yellow_taxi_trips            — trip-level: silver enriched with denormalized zones (broadcast left join)
                                                   and derived time features (year/month/week/dow/hour, trip_duration_minutes);
                                                   partitioned by month(pickup_datetime)
main     nessie.nyc.pickup_demand_zone_hour      — aggregated to (pu_location_id, hour_bucket) where
                                                   hour_bucket = date_trunc('hour', tpep_pickup_datetime);
                                                   partitioned by month(hour_bucket); target dataset for the demand model
```

Each table is created with an explicit `LOCATION` of `s3a://warehouse/{layer}/nyc/{table}`,
so MinIO keeps a layer-then-domain folder structure even though the namespace is shared.

No separate landing zone: bronze ingestion downloads source data and writes Iceberg tables
directly. Iceberg hidden partitioning manages the physical file layout in MinIO.

### Layer Consumption Policy

- **ML pipelines must read only from gold** (the `nessie.nyc.*` tables on the `main`
  branch). Anything under `src/ml/` is restricted to gold tables — silver is internal
  to the data platform.
- **Data engineers and data scientists** may read from any layer for ad-hoc exploration,
  EDA or rule justification. Notebooks under `src/notebooks/bronze/` and
  `src/notebooks/silver/` exist for exactly this.
- This is the strict interpretation of Strengholt's medallion pattern: production-like
  consumers (ML) only see curated, validated data, while humans retain full visibility
  into the lower layers for debugging and design decisions.

### Schema Strictness Across Layers

Each pipeline declares its own `StructType` schema with explicit nullability and
creates the Iceberg table via `CREATE TABLE ... USING iceberg` SQL DDL — generated
from the schema by `src/common/iceberg_schema.py.schema_to_ddl()`. This puts the
`NOT NULL` constraints in Iceberg's column metadata, not just in convention.

| Layer       | Policy                                                                                       |
|-------------|----------------------------------------------------------------------------------------------|
| Bronze      | All columns `NULLABLE` — bronze is faithful to source, including its missing values.         |
| Silver      | `NOT NULL` on every column the `clean()` function provably filters (datetimes, IDs, `fare_amount` / `total_amount` checked `> 0`, `trip_distance` / `tip_amount` / `tolls_amount` checked `>= 0`, fields with `isin` / `between` / `isNotNull`). Surcharges not filtered (`extra`, `mta_tax`, `improvement_surcharge`) and year-dependent fields (`congestion_surcharge`, `airport_fee`, `cbd_congestion_fee`) stay `NULLABLE`. |
| Gold        | `NOT NULL` on every column traceable to a silver `NOT NULL` column, on every derived time feature (year/month/week/dow/hour, `trip_duration_minutes`), and on every zone field the audit gate guarantees (`pu_borough`, `pu_zone`, `do_borough`, `do_zone`). `pu_service_zone` and `do_service_zone` stay `NULLABLE` because silver's `taxi_zone_lookup.service_zone` is not filtered. |

**`passenger_count` is `NOT NULL` from silver onward** — the existing
`between(1, 6)` filter in `clean()` drops `NULL` implicitly (`NULL BETWEEN 1 AND 6 → NULL`).
Trips with no passenger count never enter silver.

### Data Cleaning & Outlier Policy (silver)

`clean_yellow_taxi.py.clean()` runs in two stages:

1. **Validity filtering** drops rows that are *wrong*: NULL pickup/dropoff
   timestamps, `pickup >= dropoff`, negative `trip_distance` / `tip_amount` /
   `tolls_amount`, **non-positive `fare_amount` / `total_amount`** (a 0 fare /
   total is a recording error, not a real trip), location IDs outside
   `1..265`, categorical codes off the TLC 2024 dictionary (`VendorID`,
   `RatecodeID`, `payment_type`, `store_and_fwd_flag`), `passenger_count`
   outside `1..6`, exact 20-column duplicates.
2. **IQR outlier removal** then trims **both tails** on `fare_amount`,
   `total_amount`, `trip_distance` and trip duration using Tukey "far out"
   fences `[Q1 − 3·IQR, Q3 + 3·IQR]` (`IQR_K = 3.0`). Fences are computed
   **per `RatecodeID`** (metered, JFK flat, Newark flat, Nassau/Westchester,
   negotiated, group ride, unknown) because each rate code is a distinct
   fare population; a single global fence is dominated by `RatecodeID = 1`
   (~99.5 % of rows) and would clip legitimate airport / negotiated trips.
   Groups with fewer than 50 rows skip the fence to avoid unstable quartiles.
   Implementation: one `groupBy("RatecodeID").agg(percentile_approx(...))`
   aggregation is `.collect()`-ed to Python and the per-code fences are baked
   into a single `CASE WHEN` per column, so the filter runs in one pass with
   no join.

The audit gate (`MAX_DROP_RATIO = 0.35`) quarantines any month whose total
drop ratio exceeds 35 % — raised from the pre-IQR threshold of 20 % to give
the IQR stage headroom. Quarantined months freeze the WAP snapshot as a
`review/silver-...` tag and the loop continues; failures are reported in an
end-of-run summary, not raised.

**Iceberg property `write.spark.check-nullability = false`** is set on every table.
This disables Spark/Iceberg's planning-time nullability check (which would otherwise
reject `nullable=True` DataFrames into `NOT NULL` columns, since Spark's `filter()`
does not propagate non-null predicates into the DataFrame schema). The row-level
check in the Iceberg/Parquet writer is the real safety net: if any actual `NULL`
value reached a `NOT NULL` column, the write would fail at row level.

### Trade-offs

- **vs. Delta Lake**: Delta is coupled to Parquet; Iceberg is format-agnostic
- **vs. Apache Hudi**: Iceberg has better analytical query support; Hudi better for streaming
- **Complexity**: Multiple systems to manage vs. single unified solution

## 3. Compute Layer: Apache Spark

### Architecture

#### Master-Worker Model

```
Spark Master (8080, 7077)
    ├── Worker 1 (8081)
    ├── Driver in spark-master (4040)        — invoked by `spark-submit` from inside spark-master
    └── Driver in spark-notebook (4041)      — Jupyter-resident driver for interactive work
```

#### Configuration Strategy

- **Standalone cluster mode**: Simpler than Kubernetes for PoC
- **Pre-configured Iceberg**: Jars included in custom Docker image
- **Nessie integration**: REST-based, no local state required

### Resource Allocation

- Master: 1 core (orchestration only)
- Worker: 2 cores, 2GB memory (Spark heap 1536m, ~500MB headroom for JVM overhead)
- Notebook container (`spark-notebook`): 2 cores / 4GB OS memory — wider than the
  Spark driver heap to leave room for pandas-based exploration on a full month
  (~7.7M rows) and for MLflow client serialization during model logging

### Trade-offs

- **vs. Kubernetes**: Standalone simpler for PoC; K8s better for production
- **vs. Flink**: Spark better batch/SQL support; Flink better streaming

## 4. Query Engine: Dremio

### Design Rationale

- **Semantic layer**: Abstract complex data models
- **Iceberg-native**: First-class support for Iceberg tables
- **Distributed query execution**: Fault-tolerant analytics
- **Multiple data sources**: Query across heterogeneous systems

### Catalog Integration

```
Dremio UI/SQL
    ↓
Nessie Catalog (REST API, port 19120)
    ↓
MinIO (Storage)
```

### Query Optimization

- Predicate pushdown to Iceberg
- Partition pruning
- Columnar data skipping

### Trade-offs

- **vs. Apache Drill**: Dremio has better Iceberg integration
- **vs. Presto**: Dremio includes semantic layer out-of-box
- **vs. Tableau/Power BI**: Open source but less BI features

## 5. Governance Layer: OpenMetadata

### Decision Rationale

- **Metadata extraction**: Automated discovery from catalogs
- **Lineage tracking**: End-to-end data flow visualization
- **Collaboration**: Team-based data documentation
- **Quality monitoring**: Data quality assertions

### Architecture

```
OpenMetadata Server
    ├── PostgreSQL (Metadata store)
    ├── Elasticsearch (Search)
    └── Connectors
        ├── Iceberg connector
        ├── Dremio connector
        └── Spark connector
```

### Trade-offs

- **vs. Collibra**: OpenMetadata open source; Collibra enterprise features
- **vs. Apache Atlas**: OpenMetadata better UI; Atlas more mature lineage

## 6. ML Operations Layer: MLflow

### Decision Rationale

- **Experiment tracking**: Reproducible ML research
- **Model registry**: Version control for models
- **Artifact storage**: S3 integration for large model files
- **Standardized format**: MLmodel standard for serving

### Pipeline Integration

```
src/ml/{fare_prediction,trip_duration,demand_forecast}.py
    ↓ (reads only the gold tables — nessie.nyc.* on main — strict medallion)
Spark MLlib pipeline (StringIndexer → optional OneHot/Scaler →
    VectorAssembler → optional VectorIndexer → estimator)
    estimator ∈ { Ridge (squared loss, pure L2), RandomForest, GBT }
    (tree models use maxBins=300 for the ~265-level location IDs)
    ↓
MLflow (Tracking Server, behind the `ml` profile)
    ├── PostgreSQL (run metadata, params, metrics, tags)
    └── MinIO S3 (PipelineModel artifacts in mlflow-artifacts bucket)
        ↓
    Model Registry (MLflow) — Pareto champion / challenger
        ↓
    Diagnostics notebooks load `<model>@champion` from the registry
    (val ↔ test_id selection-overfitting check, test_id ↔ test_ood drift,
     predicted vs actual, residuals, feature importance, per-year drift)
```

### ML Use Cases

| # | Script                  | Target                  | Source table                          | Year split                       |
|---|-------------------------|-------------------------|---------------------------------------|----------------------------------|
| 1 | `fare_prediction.py`    | `fare_amount`           | `nessie.nyc.yellow_taxi_trips` (main) | train: 2019/2020/2022/2023; test: 2025 |
| 2 | `trip_duration.py`      | `trip_duration_minutes` | `nessie.nyc.yellow_taxi_trips` (main) | same                             |
| 3 | `demand_forecast.py`    | `n_pickups`             | `nessie.nyc.pickup_demand_zone_hour` (main) | same (year column: `hour_year`)  |

All three use cases share the building blocks in `src/ml/ml_common.py`
(`make_splits`, `build_regression_pipeline`, `make_estimator`,
`train_and_log_run`, `register_final_model`) and run the same **three-phase
workflow** end-to-end:

1. **Algorithm comparison** — train Ridge, RandomForest and GBT with
   `DEFAULT_PARAMS`; pick the winner by val RMSE.
2. **Hyperparameter tuning** — grid-search the winner over
   `PARAM_GRIDS[winner]`; pick the best config by val RMSE.
3. **Learning curve** — train every algorithm on each `TRAIN_YEAR_SUBSETS`
   slice (the winner with its Phase-2 best params, the rest with
   `DEFAULT_PARAMS`).

The winner's largest-subset run is registered into the MLflow Model Registry
under the use-case name (`fare_prediction`, `trip_duration`,
`demand_forecast`). The registry applies a Pareto-style champion / challenger
gate: promote to `@champion` only if `val_rmse` strictly improves on the
current champion **and** `test_id_rmse` does not degrade beyond 2 % of the
champion's; otherwise the new version stays as `@challenger`. Version
description carries source + features + target + algorithm + scores; version
tags expose algorithm + train years + per-split RMSE/MAE/R² for filtering in
the registry UI.

The `ridge` estimator uses ordinary squared-error loss (no Huber): silver
removes upper-tail outliers via the per-`RatecodeID` IQR fences, so the
squared-error objective is no longer dragged by a few extreme rows and the
robust loss is unnecessary. Tree models (`rf`, `gbt`) set
`maxBins = TREE_MAX_BINS = 300` so the ~265-level `pu_location_id` /
`do_location_id` clear Spark's `buildMetadata` arity check.

Pre-trip / known-at-pickup features only — no leakage from post-trip amounts.

Every script accepts `--fast` (or `ML_FAST_MODE=1`) for smoke tests: samples
1 % of gold and shrinks grids/year-subsets so the full three-phase workflow +
a throwaway registry version run in minutes. Metrics are not meaningful in
fast mode.

### Trade-offs

- **vs. Kubeflow**: MLflow simpler; Kubeflow more production-ready
- **vs. Neptune**: MLflow open source; Neptune more collaboration features

## 7. Database Backend Strategy

### PostgreSQL Services

| Service      | Purpose        | Database   | Port |
| ------------ | -------------- | ---------- | ---- |
| MLflow       | Tracking       | PostgreSQL | 5435 |
| OpenMetadata | Metadata store | PostgreSQL | 5432 |

**Rationale**: PostgreSQL chosen for reliability, ACID guarantees, JSONB support, and wide adoption in open-source projects.

### Embedded Storage (no external database)

| Service | Storage    | Notes                                              |
| ------- | ---------- | -------------------------------------------------- |
| Nessie  | RocksDB    | Embedded; state persisted in `./nessie-data` mount |
| Dremio  | RocksDB    | Embedded; state in `dremio_data` Docker volume     |

## 8. Network Architecture

### Docker Network

```
Bridge Network (172.20.0.0/16)
    ├── Storage: minio
    ├── Metadata: nessie
    ├── Compute: spark-master, spark-worker-1
    ├── Query: dremio
    ├── Governance: openmetadata
    └── ML: mlflow

Service Discovery: Internal DNS (e.g., minio:9000)
```

### Port Mapping Strategy

- **Public ports**: User-facing services (9001, 8080, 8585, 5000)
- **Internal ports**: Container-to-container (9000, 19120, 8181, 31010)
- **Avoidance**: No port conflicts between services

## 9. Data Flow Patterns

### Batch Ingestion Pattern (Medallion + WAP)

Every layer transition runs as Write-Audit-Publish on a short-lived WAP branch:

```
1. Source Data (NYC TLC Parquet/CSV)
2. Bronze: Spark downloads + writes Iceberg table (nessie.nyc.* on the `bronze` branch)
           — WAP branch off `bronze`, audit, merge back, drop branch
3. Silver: Spark reads bronze, cleans/deduplicates, writes (nessie.nyc.* on `silver`)
           — non-raising audit; failed months → `review/silver-...` quarantine tag
4. Gold:   Spark reads silver, joins/aggregates, writes (nessie.nyc.* on `main`)
           — strict audit (row-count parity for trips, pickup conservation for demand)
5. Dremio Query Layer / ML training
```

### ML Pipeline Pattern

```
1. Read gold (only nessie.nyc.* on `main`)         — src/ml/ml_common.py.get_ml_spark
2. Stratified split into train/val/test_id/test_ood — src/ml/ml_common.py.make_splits
3. Three-phase workflow                            — algorithm comparison → tuning → year-subset curve
4. Spark MLlib pipeline                            — StringIndexer (+ optional OHE/Scaler) → VectorAssembler → estimator
5. Train + evaluate                                — RMSE, MAE, R² on every split
6. Log to MLflow                                   — params, metrics, PipelineModel artifact per run
7. Register the final model                        — Pareto champion/challenger (val_rmse + test_id_rmse guard)
8. Diagnostics notebook                            — load via models:/{name}@champion, drift analysis
```

### Query Pattern

```
1. User SQL Query (Dremio Web/API)
2. Query Planning (Dremio)
3. Predicate Pushdown (Iceberg)
4. Partition Pruning (Nessie metadata)
5. S3 Read (MinIO)
6. Results Return
```

## 10. Security Architecture

### Development Mode

- All services accessible on localhost
- Default credentials (no enforcement)
- No TLS/encryption
- Internal Docker network for inter-service communication

### Production Considerations (Future)

1. **Authentication**
   - OAuth2 for user authentication
   - Service-to-service JWT tokens
   - LDAP/Active Directory integration

2. **Encryption**
   - TLS 1.3 for all external communications
   - Encryption at rest for S3 data
   - Field-level encryption for sensitive data

3. **Authorization**
   - Role-based access control (RBAC)
   - Attribute-based access control (ABAC)
   - Data masking policies

4. **Audit Logging**
   - Immutable audit trails
   - Data access logging
   - Configuration change tracking

## 11. Scalability Considerations

### Current Limits

- Single MinIO node (not distributed)
- 1 Spark worker + 1 notebook driver
- Single OpenMetadata instance
- Single MLflow instance

### Scaling Strategies

1. **Storage**: MinIO distributed deployment with multiple nodes
2. **Compute**: Kubernetes cluster for dynamic Spark deployment
3. **Query**: Dremio executor node scaling
4. **Governance**: OpenMetadata high-availability setup
5. **ML**: MLflow server load balancing

## 12. Monitoring & Observability

### Current Monitoring

- Service health checks (Docker healthcheck)
- Web UI dashboards
- Log aggregation (Docker logs)

### Production Enhancements

- Prometheus metrics collection
- Grafana dashboards
- ELK stack for centralized logging
- Distributed tracing (Jaeger/Zipkin)
- Alert management (AlertManager)

## References

### Academic Papers

- Iceberg: Open Table Formats for Big Data
- Nessie: Branching and Versioning for Data Lakes
- Delta Lake: High-Performance ACID Table Storage

### Technical Specifications

- Apache Iceberg Spec: https://iceberg.apache.org/spec/
- S3 API Reference: https://docs.aws.amazon.com/s3/latest/API/
- Spark Documentation: https://spark.apache.org/docs/

---

**Last Updated**: May 2026
