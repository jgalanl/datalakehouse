# Data Lakehouse Platform - Project Summary

## Overview

A local, batch-oriented data lakehouse built for a master's thesis (TFM). It wires
together seven open-source components — MinIO, Iceberg, Nessie, Spark, Dremio,
OpenMetadata and MLflow — into a medallion (bronze/silver/gold) pipeline over the
NYC TLC yellow-taxi dataset, ending in three Spark MLlib models. Everything runs
under Docker Compose; nothing is deployed to a cloud or a cluster.

## Architecture Components

### 1. **Storage Layer: MinIO**

- S3-compatible object storage
- Self-hosted data lake foundation
- Single `warehouse` bucket for all Iceberg tables (physical layout managed by Iceberg)
- Auxiliary buckets: `mlflow-artifacts` (versioned), `spark-logs`

### 2. **Metadata & Cataloging: Nessie + Iceberg**

- **Nessie**: Git-like versioning for data objects (single Iceberg catalog, embedded RocksDB)
- **Iceberg**: Open table format with ACID guarantees and hidden partitioning

### 3. **Compute Layer: Apache Spark**

- Distributed batch processing
- 1 Master + 1 Worker + 1 Jupyter notebook driver
- Native Iceberg and Nessie integration
- Spark MLlib used by the ML training scripts (no separate ML stack)

### 4. **Query Engine: Dremio OSS**

- Self-service analytics SQL engine
- Iceberg and Nessie support
- Physical query optimization

### 5. **Data Governance: OpenMetadata**

- Metadata extraction and cataloging (data tables via Nessie's Iceberg-REST endpoint,
  models via the MLflow connector)
- Search/Explore backed by Elasticsearch

### 6. **ML Operations: MLflow**

- Experiment tracking and management
- Model registry with versioning
- S3 artifact storage

### 7. **Supporting Infrastructure**

- PostgreSQL for MLflow tracking (`ml` + `governance` profiles)
- PostgreSQL + Elasticsearch for OpenMetadata (`governance` profile)
- Docker Compose orchestration with four profiles — `ingest`, `ml`, `governance`, `api` — over a
  profile-less core (MinIO, minio-init, Nessie); each profile opts into the compute/services it needs
  (`api` runs the `model-server` that serves a registered model over REST)

## Technical Highlights

### ACID Transactions & Consistency

- Iceberg provides ACID guarantees on object storage
- Nessie enables atomic multi-table transactions
- Snapshot isolation for concurrent access

### Data Versioning & Time Travel

- Git-like branches and tags with Nessie
- Complete audit trail of data changes
- Query historical data versions

### Semantic Layer & Analytics

- Dremio's semantic layer for business logic
- Predicate pushdown for query optimization
- Support for multiple data sources

### Distributed Processing

- Spark's distributed SQL processing
- Automatic partition pruning
- Columnar data optimization

### ML Lifecycle Management

- End-to-end experiment tracking
- Model registry with staging/production
- Reproducible model management

## Project Structure

```
datalakehouse/
├── docker-compose.yml          # Complete infrastructure
├── .env                        # Environment configuration
├── RUNBOOK.md                  # End-to-end execution sequence
├── config/                     # Service configurations
│   ├── spark/                  # Spark configs & logging
│   ├── dremio/                 # Dremio configuration
│   ├── mlflow/                 # MLflow setup
│   └── openmetadata/           # OpenMetadata configs
├── docker/                     # Custom Dockerfiles
│   ├── spark/                  # Spark with Iceberg + Nessie
│   └── mlflow/                 # MLflow tracking server
├── src/                        # Pipeline code (mounted into Spark containers)
│   ├── common/                 # spark_factory.py, nessie_manager.py, iceberg_schema.py
│   ├── pipelines/
│   │   ├── bronze/             # ingest_yellow_taxi.py, ingest_zone_lookup.py
│   │   ├── silver/             # clean_yellow_taxi.py, clean_zone_lookup.py
│   │   └── gold/               # build_yellow_taxi_trips.py, aggregate_demand_zone_hour.py
│   ├── ml/                     # train.py (single entry point, dispatched by use case),
│   │                           #   splits.py / pipelines.py / algorithms.py / tracking.py (shared building blocks)
│   │   └── api/                # REST serving — client.py + example payloads + README (model-server, `api` profile)
│   └── notebooks/              # bronze (explore + preprocess + verify), silver (verify), gold (verify), ml ({fare,duration,demand}_diagnostics)
└── docs/                       # Documentation
```

## What the platform does

- Ingests NYC TLC parquet (Q1 of 2023/2024/2025) and the taxi-zone CSV into bronze
- Cleans and conforms it into silver (validity filtering + per-`RatecodeID` IQR outlier
  removal), then curates two gold tables (trip-level and demand-per-zone-per-hour)
- Versions every layer with Nessie branches/tags under a Write-Audit-Publish flow;
  audits are non-raising and quarantine failing months as `review/` tags
- Trains three Spark MLlib regression models (fare, trip duration, demand) with MLflow
  tracking and a Model Registry champion/challenger workflow
- Serves a registered model over REST (`mlflow models serve`, the `model-server` service
  in the `api` profile) — POST feature rows to `:5001/invocations`, get predictions
- Catalogs the gold tables and registered models in OpenMetadata
- Ships per-layer verification notebooks and one diagnostic notebook per ML use case

## Development Workflow

### Python Modules

#### `src/common/` — shared utilities

- `spark_factory.get_spark(app_name)`: SparkSession factory (config from `spark-defaults.conf`)
- `nessie_manager`: Spark-SQL wrappers for Nessie refs (`create_branch`, `merge_branch`,
  `create_tag`, `set_spark_branch`, `wap_branch_name`, `review_tag_name`,
  `ensure_persistent_branches`, ...)
- `iceberg_schema.schema_to_ddl(StructType)`: generates Iceberg column DDL with
  `NOT NULL` constraints from a Spark `StructType`. Each pipeline declares its
  target schema with explicit nullability and creates the table via SQL DDL so
  the constraints land in Iceberg's column metadata.

#### `src/pipelines/bronze/` — bronze ingestion

- `ingest_yellow_taxi.py`: downloads NYC TLC parquet (Q1 of 2023/2024/2025),
  reconciles schema drift, writes `nessie.nyc.yellow_taxi` via WAP per month
- `ingest_zone_lookup.py`: writes the small reference table `nessie.nyc.taxi_zone_lookup`

#### `src/pipelines/silver/` — silver cleaning

- `clean_yellow_taxi.py`: WAP per month against `nessie.nyc.yellow_taxi` on the
  `silver` branch. `clean()` runs validity filtering (NULL datetimes,
  `pickup >= dropoff`, non-positive `fare_amount` / `total_amount`, negative
  distance / tips / tolls, location IDs outside `1..265`, codes off the TLC
  dictionary, `passenger_count` outside `1..6`, duplicates) followed by IQR
  outlier removal — Tukey "far out" fences `[Q1 − 3·IQR, Q3 + 3·IQR]` computed
  **per `RatecodeID`** on `fare_amount` / `total_amount` / `trip_distance` /
  trip duration. Audit gate quarantines any month above 35 % drop ratio to a
  `review/silver-...` tag.
- `clean_zone_lookup.py`: full refresh of `nessie.nyc.taxi_zone_lookup` on `silver`

#### `src/pipelines/gold/` — gold curation

- `build_yellow_taxi_trips.py`: joins silver trips with the zone lookup, derives time
  features, writes `nessie.nyc.yellow_taxi_trips` (partitioned by month) via WAP on `main`
- `aggregate_demand_zone_hour.py`: aggregates silver trips to
  `(pu_location_id, hour_bucket)` and writes `nessie.nyc.pickup_demand_zone_hour`
  (target dataset for the demand model)

#### `src/ml/` — ML training (Spark MLlib + MLflow)

One **unified `train.py`** dispatches by use case (`fare` / `duration` /
`demand`); shared building blocks are split across four focused modules.
All ML code reads only from the gold tables on `main` (strict medallion).

- `splits.py`: data preparation — `make_splits` is a **last-year holdout**:
  `common_test` is every row of `TEST_YEAR = 2025` (fully held out), and
  `train` / `val` are a stratified-by-year random split of `TRAIN_VAL_YEARS
  = [2023, 2024]` at `val_frac = 0.15` per year. The model is
  never exposed to any `TEST_YEAR` row during training or selection, so
  `common_test_rmse` is an unbiased out-of-time generalization estimate.
  Cyclical encoding for hour/dow (sin/cos for GLR; trees keep the
  integer columns). Constants: `TEST_YEAR`, `TRAIN_VAL_YEARS`,
  `SUBSET_A_YEARS = [2023]` (Phase 2 A — half the training pool;
  B = all of `TRAIN_VAL_YEARS`, reused as the Phase-1 winner).
- `pipelines.py`: `build_regression_pipeline` (StringIndexer → optional
  OneHot → numerical scaling → VectorAssembler → optional VectorIndexer →
  estimator). Tree constants `TREE_MAX_BINS = 300` so the ~265-level
  location IDs clear Spark's `buildMetadata` arity check.
- `algorithms.py`: three regression estimators — `glr`
  (`GeneralizedLinearRegression` with family/link supplied per use case via
  `GLR_FAMILY_PRESETS`: gamma/log for fare and duration, poisson/log for
  `n_pickups`), `gbt` (`GBTRegressor`, bias-reducing boosting tree ensemble),
  `rfr` (`RandomForestRegressor`, variance-reducing bagging tree ensemble —
  same tree preprocessing as GBT). `TREE_ALGORITHMS = {"gbt", "rfr"}`
  exposes the tree set so `splits.resolve_numerical_cols` knows to skip
  cyclical encoding. Single grid set `FULL_PARAM_GRIDS` consumed per
  algorithm in Phase 1 Training (4-pt GLR, 4-pt GBT, 4-pt RFR — 4 ×
  `CV_NUM_FOLDS=3` = 12 fits/algorithm; GBT explores depth + one
  stochastic-GBM regularizer probe per Friedman 2002; RFR explores forest
  size + a depth probe per Breiman 2001) plus fast-mode variants.
  Each algorithm is CV-tuned over its full grid in one pass — the old
  Phase-1 screening + Phase-2 winner-only optimization were collapsed
  into a single training step. `fast_mode_enabled()` parses `--fast` /
  `ML_FAST_MODE=1`.
- `tracking.py`: MLflow side — `get_ml_spark` (pins the session to
  `main`/gold), `setup_mlflow`, `evaluate_regression` (rmse/mae/r2 point
  estimates), `train_and_log_run` / `tune_with_cv` (hand-rolled k-fold
  CV, `CV_NUM_FOLDS=3` — feature stages fit once per fold, transformed
  vectors cached and reused across grid points at `CV_PARALLELISM=2`;
  avoids `CrossValidator`'s redundant per-(fold × point) preprocessing
  with no cross-fold leakage),
  `compute_baseline_metrics` (single-split helper, still used by the
  diagnostic notebook) + `log_baseline_run` (fits the per-group means on
  `train`, scores **every** split in `eval_splits`, logs `{split}_{metric}`
  so the baseline carries the same train/val/common_test metric set as the
  model runs), `log_input_datasets` (logs the train/val/test MLflow
  `Dataset`s onto each run so the *Datasets used* field is populated),
  `register_candidates` (registers every candidate from one training
  invocation as a new Model Registry version; the within-run winner gets
  `@champion` only if its `val_rmse` is strictly below the previous
  champion's `val_rmse` — point-estimate improvement — otherwise it stays
  `validation_status="challenger"`; all other candidates always get
  `validation_status="challenger"`; `@challenger` alias → best non-champion
  of the run; every version carries the per-split point metrics
  (`val_rmse`, `common_test_{rmse,mae,r2}`, `baseline_common_test_rmse`)
  plus the reproducibility pins `nessie_main_hash` + `source_snapshot_id`;
  the new @champion also gets `inference_latency_*` tags from a 1000-row
  `common_test` sample; the registry is pruned to the top
  `REGISTRY_MAX_VERSIONS = 20` non-aliased versions at the end of the
  call). `common_test` is held out — its metrics are logged but never
  drive selection or promotion.
- `train.py`: single entry point — `spark-submit train.py {fare|duration|demand} [--fast]`.
  The `USE_CASES` dict at the top carries the per-use-case knobs
  (`source` / `label_col` / `year_col` / `numerical_cols` / `categorical_cols` /
  `glr_family` / `baseline_group_cols` / `redundant_categoricals`). Workflow:
  **Phase 0** drop the `redundant_categoricals` from `categorical_cols`
  (borough is derivable from location_id, so it carries no extra information
  given the IDs) → build one MLflow `Dataset` per split (`from_spark`,
  reused across runs, logged as `training`/`validation`/`testing` inputs on
  every run) → **baseline** (mean(label) per group, scored on
  `train`/`val`/`common_test` like the model runs — mean is the RMSE-optimal
  constant-per-group predictor, floor reference) → **Phase 1 Training** (one CV(k=3) per algorithm over
  its `FULL_PARAM_GRIDS` entry — 4-pt GLR, 4-pt GBT, 4-pt RFR (12 fits/algorithm) — winner
  is whichever algorithm's CV bestModel scores lowest on `val_rmse`;
  collapses the old Phase-1 screening + Phase-2 winner-only optimization
  into one step) → **Phase 2 More-data** (winner refit on
  `SUBSET_A_YEARS` — the earliest training year; B is the Phase-1
  winner trained on all of `TRAIN_VAL_YEARS` and reused without refit;
  both score on the same `common_test = TEST_YEAR` so neither sees the
  held-out year) → **Phase 3 Registration**: every candidate (4 per
  run: 3 training + 1 more-data-A) becomes a new MLflow Model
  Registry version under the experiment name (`fare_prediction`,
  `trip_duration`, `demand_forecast`). The within-run winner (lowest
  `val_rmse`) gets `@champion` only if its `val_rmse` is strictly below
  the previous champion's `val_rmse` (strict point-estimate improvement);
  otherwise it is tagged `validation_status="challenger"`. All other
  candidates always get `validation_status="challenger"`; the `@challenger`
  alias points to the best non-champion version from the run. `common_test`
  is held out and never used to rank candidates — the test metrics on every
  version are an unbiased generalization estimate, not a selection signal.
  Every version is also pinned to the data state at training time
  via `nessie_main_hash` + `source_snapshot_id` tags so the exact
  training input is reproducible later; the new @champion picks up
  `inference_latency_*` tags from a 1000-row `common_test` sample so
  between-champion cost regressions are visible at a glance; the
  registry is pruned to `REGISTRY_MAX_VERSIONS = 20` non-aliased
  versions at the end of the run (aliased versions are always kept).
  `--fast` runs a 1 %-sampled smoke test with tiny grids and CV k=2 —
  completes in minutes; metrics not meaningful.
- **No PCA.** An earlier dim-reduction probe ran PCA on the
  numerical block and was removed: wrong feature-count regime (~7-9
  numericals), interpretability cost ("PC1" replaces named features in
  coefficient/importance reports), no measurable benefit. The drop-redundant-
  categoricals step survives as Phase 0 because the redundancy is provable
  from the schema, not empirical.

### Notebooks

- `bronze/explore.ipynb`: pandas first-look on raw TLC parquet
- `bronze/preprocess.ipynb`: Spark EDA on the full dataset; quantitatively justifies
  the silver cleaning rules
- `bronze|silver|gold/verify_iceberg.ipynb`: layer-specific Iceberg + Nessie audits
- `ml/{fare,duration,demand}_diagnostics.ipynb`: **one diagnostic notebook per use
  case** — each pins `USE_CASE` (analysis body identical across the three).
  Loads `<experiment>@champion` from the MLflow Registry, reconstructs the splits,
  plots the per-year breakdown across `TRAIN_VAL_YEARS` (in-sample-ish) and
  `TEST_YEAR` (held out — the drift signal), predicted vs actual, residuals,
  feature importance, the Phase 2 more-data comparison (both arms scored on
  `common_test = TEST_YEAR`), and the baseline-vs-champion gap. **Does not
  re-train.**

## Technology Stack

| Layer      | Technology                                   | Version       |
| ---------- | -------------------------------------------- | ------------- |
| Storage    | MinIO                                        | 2025-04-22    |
| Metadata   | Nessie (embedded RocksDB)                    | 0.106.0       |
| Format     | Iceberg                                      | 1.10.1        |
| Compute    | Apache Spark                                 | 3.5.8         |
| Query      | Dremio OSS                                   | 25.2.0        |
| Governance | OpenMetadata                                 | 1.12.1        |
| ML Ops     | MLflow                                       | 2.20.3        |
| Databases  | PostgreSQL 16 / Elasticsearch 9.3.0          | -             |
| Container  | Docker & Docker Compose                      | 20.10+ / 2.0+ |

## Installation & Setup

### Requirements

- Docker 20.10+
- Docker Compose 2.0+
- 8GB RAM minimum (16GB recommended)
- 20GB disk space minimum (50GB recommended)

### Quick Start

```bash
# Clone repository
git clone <repository>
cd datalakehouse

# Bring up core + Spark + Dremio + MLflow (the `ml` profile);
# use `--profile governance` for the OpenMetadata stack, `--profile api` to serve a model.
docker compose --profile ml up -d

# Access services
# MinIO: http://localhost:9001
# Spark: http://localhost:8080
# Dremio: http://localhost:9047
# MLflow: http://localhost:5000
# Model REST API (with `--profile api`): http://localhost:5001
```

End-to-end execution sequence (bronze → silver → gold → ML) is documented in
[RUNBOOK.md](RUNBOOK.md).

## Documentation

### Main Documents

1. **[README.md](README.md)** — Complete project guide
2. **[RUNBOOK.md](RUNBOOK.md)** — End-to-end execution sequence
3. **[docs/QUICK_REFERENCE.md](docs/QUICK_REFERENCE.md)** — Command cheatsheet
4. **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** — Architecture decisions and rationale
5. **[docs/DEVELOPMENT.md](docs/DEVELOPMENT.md)** — Developer guide
6. **[CLAUDE.md](CLAUDE.md)** — Codebase conventions for Claude Code (also useful as a quick architectural reference)

## Code Quality

- Type annotations on the shared helpers (`src/common`, `src/ml`)
- Comments explain *why*, not *what*; module docstrings kept to 2–3 lines
- Pipeline progress printed at every WAP step
- Audits are non-raising: a failed gate quarantines the month as a `review/` tag and the
  run continues, reporting failures in an end-of-run summary
- Per-layer verification notebooks under `src/notebooks/*/verify_iceberg.ipynb`

## Limitations

A single-node proof of concept, not a production deployment:

- Single MinIO node; standalone Spark cluster (two workers on one host)
- No Kubernetes, no high availability, no TLS
- Default development credentials; internal Docker network only
- Tuned for a 12 GiB WSL2 / Docker Desktop VM

## References

- Apache Iceberg: https://iceberg.apache.org/
- Project Nessie: https://projectnessie.org/
- Apache Spark: https://spark.apache.org/
- Dremio: https://docs.dremio.com/
- MLflow: https://mlflow.org/docs/
- OpenMetadata: https://docs.open-metadata.org/
- MinIO: https://min.io/docs/
