# Architecture & Design Decisions

## Technical Architecture

This document details the architectural decisions and technical rationale behind the data lakehouse platform.

## 1. Storage Layer: MinIO

### Decision Rationale

- **S3 Compatibility**: MinIO exposes an S3-compatible API, so Spark's S3A connector and the AWS SDK talk to it unchanged
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

- **Semantic layer**: virtual datasets/views over the physical tables
- **Iceberg-native**: reads Iceberg tables directly through the Nessie catalog
- **Distributed query execution**: parallel scans over the warehouse bucket
- **SQL interface**: ANSI SQL plus Arrow Flight / JDBC / REST endpoints

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
src/ml/train.py {fare | duration | demand} [--fast]
    ↓ (reads only the gold tables — nessie.nyc.* on main — strict medallion)
Phase 0 — drop redundant categoricals (borough derivable from location_id)
    ↓
Last-year holdout split (common_test = TEST_YEAR; train / val from TRAIN_VAL_YEARS)
    ↓
Spark MLlib pipeline (StringIndexer → optional OneHot → numerical scaling →
    VectorAssembler → optional VectorIndexer → estimator)
    estimator ∈ { GLR (GLM with use-case family/link),
                  GBT (gradient-boosted trees),
                  RFR (random forest) }
    (tree models use maxBins=300 for the ~265-level location IDs)
    ↓
MLflow (Tracking Server, behind the `ml` and `governance` profiles)
    ├── PostgreSQL (run metadata, params, metrics, tags)
    └── MinIO S3 (PipelineModel artifacts in mlflow-artifacts bucket)
        ↓
    Model Registry (MLflow) — single-metric champion / challenger on val_rmse
                              (common_test held out — reported, never used to select)
        ├── Diagnostics notebook ({fare,duration,demand}_diagnostics.ipynb, one per use
        │   case) loads `<experiment>@champion` and scores it per year — TRAIN_VAL_YEARS
        │   are in-sample-ish, TEST_YEAR is the held-out drift signal.
        └── model-server (`api` profile) — `mlflow models serve` exposes
            `models:/<model>@<alias>` as a REST endpoint at :5001/invocations
```

### Model Serving

A registered model is served over REST by the **`model-server`** service
(`docker-compose.yml`, **`api` profile**). It runs MLflow's built-in inference server
(`mlflow models serve -m models:/${SERVE_MODEL}@${SERVE_ALIAS}`) on the Spark image —
the models are Spark MLlib `PipelineModel`s, so loading one needs a SparkSession; it
runs `local[*]` (driver == executor), which sidesteps the distributed staging gotcha.
The endpoint exposes `/ping`, `/health`, `/version` and `POST /invocations` (a
`dataframe_split` payload of gold feature rows → predictions). The service is isolated
in its own profile because it serves a *registered* model and must start only after one
exists. Client + example payloads: `src/ml/api/` (see its `README.md`).

### ML Use Cases

| # | Use case   | Target                  | Source table                                | GLR family/link | Year column   |
|---|------------|-------------------------|---------------------------------------------|-----------------|---------------|
| 1 | `fare`     | `fare_amount`           | `nessie.nyc.yellow_taxi_trips` (main)       | gamma / log     | `pickup_year` |
| 2 | `duration` | `trip_duration_minutes` | `nessie.nyc.yellow_taxi_trips` (main)       | gamma / log     | `pickup_year` |
| 3 | `demand`   | `n_pickups`             | `nessie.nyc.pickup_demand_zone_hour` (main) | poisson / log   | `hour_year`   |

All three use cases share four focused modules under `src/ml/` (split from
the old monolithic `ml_common.py`):

- **`splits.py`** — `make_splits` is a **last-year holdout**: `common_test`
  is every row of `TEST_YEAR = 2025` (fully held out — the model is never
  exposed to any TEST_YEAR row during training or selection), and `train`
  / `val` are a stratified-by-year random split of `TRAIN_VAL_YEARS =
  [2023, 2024]` at `val_frac = 0.15` per year. Cyclical
  encoding for hour/dow (sin/cos for GLR; trees keep the integer
  columns). Constants: `TEST_YEAR`, `TRAIN_VAL_YEARS`, `SUBSET_A_YEARS =
  [2023]` (Phase 2 A — half the training pool; B = all of
  `TRAIN_VAL_YEARS`, reused as the Phase-1 winner).
- **`pipelines.py`** — `build_regression_pipeline` (StringIndexer → optional
  OneHot → numerical scaling → VectorAssembler → optional VectorIndexer →
  estimator). Tree constants `TREE_MAX_BINS = 300` so the ~265-level
  `pu_location_id` / `do_location_id` clear Spark's `buildMetadata` arity
  check.
- **`algorithms.py`** — three estimators covering a GLM plus two
  complementary tree ensembles: `glr` (`GeneralizedLinearRegression`
  with family/link supplied per use case via `GLR_FAMILY_PRESETS`),
  `gbt` (`GBTRegressor`, bias-reducing boosting), `rfr`
  (`RandomForestRegressor`, variance-reducing bagging — same tree
  preprocessing as GBT). `TREE_ALGORITHMS = {"gbt", "rfr"}` exposes the
  tree set so `splits.resolve_numerical_cols` knows to skip cyclical
  encoding. Single grid set: `FULL_PARAM_GRIDS` (4-pt GLR, 4-pt GBT, 4-pt RFR — 4 × `CV_NUM_FOLDS=3` = 12 fits/algorithm; GBT explores depth + one stochastic-GBM
  `subsamplingRate` / `featureSubsetStrategy` probe per Friedman 2002; RFR
  explores forest size + a depth probe per Breiman 2001). Each algorithm is CV-tuned over its full
  grid in one pass — the old Phase-1 screening + Phase-2 winner-only
  optimization were collapsed into a single training step. Fast-mode
  variants for `--fast` smoke tests (1 % sample, tiny grids, CV k=2).
- **`tracking.py`** — `get_ml_spark` (pins the session to `main`/gold),
  `setup_mlflow`, `evaluate_regression` (rmse/mae/r2 point estimates),
  `train_and_log_run` and `tune_with_cv` (hand-rolled k-fold CV,
  `CV_NUM_FOLDS=3` — fits the shared feature stages once per fold and
  caches the transformed vectors, refitting only the bare estimator per
  grid point at `CV_PARALLELISM=2`, so it skips `CrossValidator`'s
  redundant per-(fold × point) preprocessing without cross-fold leakage),
  `compute_baseline_metrics` (single-split helper, still called by the
  diagnostic notebook) + `log_baseline_run` (fits the per-group means on
  `train`, scores **every** split, logs `{split}_{metric}`),
  `log_input_datasets` (attaches the train/val/test MLflow `Dataset`s to
  each run so the *Datasets used* field is populated everywhere),
  `register_candidates`
  (multi-version registry helper — registers every candidate of one run,
  picks `@champion` on strict point-estimate improvement over the previous
  champion's `val_rmse`, tags the rest `validation_status="challenger"`).

A single unified `train.py` dispatches by use case and runs **four
stages** end-to-end inside one MLflow experiment per use case:

0. **Phase 0 — drop redundant categoricals.** `borough` (and
   `pu_service_zone` for demand) are derivable from `pu_location_id` via the
   silver `taxi_zone_lookup`, so they're dropped up-front for every
   algorithm/phase. Not an A/B probe — the redundancy is provable from the
   zone lookup, not empirical.
1. **Baseline.** Mean-of-label per `(group_cols)`, fit on `train` and
   scored on `train`/`val`/`common_test` (the same metric set as the model
   runs). Floor reference; the per-group mean is the RMSE-optimal
   constant-per-group predictor (median would be MAE-optimal — wrong loss
   for our metric). Any trained model that doesn't beat this on RMSE isn't
   learning anything beyond per-group averages.
2. **Phase 1 — Training.** One `CV(k=3)` per algorithm over its
   `FULL_PARAM_GRIDS` entry (4-pt GLR, 4-pt GBT, 4-pt RFR = 12 fits/algorithm). The
   within-run winner is whichever algorithm's CV bestModel scores lowest
   on `val_rmse`. Each algorithm is registered with its fairly-tuned
   best, so the "losing" algorithms still appear in the registry as
   challengers (not as 3-pt screening points like the old Phase 1
   design). Collapses the previous Phase 1 (screening) + Phase 2
   (winner-only optimization) into a single training step.
3. **Phase 2 — More-data.** Refit the Phase-1 winner on `SUBSET_A_YEARS`
   (the earliest training year, 2023). Subset B is the
   Phase-1 winner trained on all of `TRAIN_VAL_YEARS`
   (2023/2024) and reused without refit. Both score on the
   same `common_test = TEST_YEAR`, so neither sees the held-out year —
   the contrast isolates "does more training data help generalize to a
   future year?".
4. **Phase 3 — Registration.** Every candidate trained in this
   invocation (4 per run: 3 Phase-1 Training + 1 Phase-2 more-data-A)
   is registered as a new MLflow Model Registry version under the
   experiment name (`fare_prediction`, `trip_duration`,
   `demand_forecast`). The within-run winner (lowest `val_rmse`) gets
   `@champion` **only if its `val_rmse` is strictly below the previous
   registry champion's `val_rmse`** (point-estimate improvement, see
   *Promotion policy*); otherwise it is tagged
   `validation_status="challenger"`. Every other candidate is always
   tagged `validation_status="challenger"`. The `@challenger` alias
   points to the best non-champion version from this run. Version tags
   expose algorithm, phase, run_name, per-split RMSE/MAE/R² and (where
   applicable) `more_data_subset` for filtering in the registry UI.

**Dataset & run-field policy.** Every run logs its input data and the same
field set, so an experiment's runs stay comparable in the MLflow UI. Each
split is wrapped once as an MLflow `Dataset` (`mlflow.data.from_spark`,
`table_name` = the gold source, `targets` = the label) and the same objects
are logged on every run via `log_input_datasets`, so the *Datasets used*
field shows `training`/`validation`/`testing` everywhere (more-data-A logs
its own subset-A training dataset). The digest is the DataFrame's
logical-plan `semanticHash()` — no data scan. Run tags share one key set:
`more_data_subset` defaults to `none` (only the Phase-2 A/B comparison
overrides it) and `training_data` is a tag on every run; both the baseline
and the training runs log `train`/`val`/`common_test` metrics. The only
run-type-specific fields are the tuned-run `cv_*` metrics and per-algorithm
`best_<hp>` params (a baseline has none; GLR/GBT/RFR tune different knobs).

**Metric policy.** `val_rmse` drives every selection decision (Phase 1
within-run winner, within-run @champion, cross-run @champion promotion). `common_test`
is held out — its metrics are logged on every version and shown in the
diagnostic notebook as the unbiased generalization estimate, but never
used to rank candidates. Ranking on `common_test` would turn it into a
*selection* set and bias the reported test numbers — the single most
likely methodological objection in a thesis defense.

**Split policy.** Last-year holdout: `common_test` is every row of
`TEST_YEAR = 2025` (fully out-of-time — the model never sees any 2025 row
during training or hyperparameter selection); `train` / `val` are a
stratified-by-year random split of `TRAIN_VAL_YEARS`. Reason: a
per-year random three-way split leaks within-year temporal autocorrelation
— two trips minutes apart in the same zone share weather, demand regime,
and traffic, but a random split routes them to opposite sides of the
train/test boundary. For a model meant to predict *future* trips the
honest evaluation is a chronological out-of-time test, and the last-year
holdout gives that cleanly with zero overlap.

**Promotion policy.** Strict @champion promotion on `val_rmse`: the
within-run contender becomes `@champion` only if its `val_rmse` is
strictly below the previous champion's `val_rmse`. Implemented in
`register_candidates`. An earlier iteration used a bootstrap-CI noise
floor on this comparison; it was removed as over-engineering for a
thesis-scale project. The trade-off is intentional: any improvement —
however small — swaps the alias.

**Reproducibility policy.** Every registered model version carries two
pin tags taken at training time: `nessie_main_hash` (the full Nessie
`main` commit hash, via `nessie_manager.current_ref_hash`) and
`source_snapshot_id` (the latest Iceberg snapshot id of the source gold
table, via `nessie_manager.latest_snapshot_id`). The Nessie hash pins
every table on every branch as of that moment, so a future reader can
`USE REFERENCE <hash> IN nessie` and rebuild the identical training
input — even after gold has rolled forward. The snapshot id is the
finer-grained per-table pin for spot-checks.

**Latency policy.** When promotion fires, the new @champion gets three
tags from a 1000-row sample of `common_test`:
`inference_latency_ms_per_row`, `inference_latency_sample_n`,
`inference_latency_total_s`. Measured by `measure_inference_latency`
with one warm-up pass + a single timed
`transform.select("prediction").collect()`. Numbers are Spark-cluster
latency (full distributed roundtrip), not a production-serving SLO —
they're a regression check between champions ("did v40 suddenly take
5× longer than v39?"). Measured only on promotion, not every candidate.

**Registry retention policy.** At the end of every `register_candidates`
the registry is pruned to `REGISTRY_MAX_VERSIONS = 20` non-aliased
versions; any version carrying `@champion` or `@challenger` is always
kept. Caps the linear growth (~5 versions per `train.py` run) without
losing the audit trail to the current aliases.

**No PCA**. An earlier "dim-reduction" probe (the original Phase 4) ran PCA
on the numerical block but was removed: ~7-9 numericals is the wrong
feature-count regime for PCA; interpretability cost ("PC1" replaces named
features in coefficient/importance reports); no measurable benefit. The
drop-redundant-categoricals step survives as Phase 0 because the redundancy
is provable from the schema, not empirical.

Pre-trip / known-at-pickup features only — no leakage from post-trip amounts.

`--fast` (or `ML_FAST_MODE=1`) runs a smoke test: 1 % sample of gold, tiny
grids, CV k=2 — the full Phase-0 + baseline + Phase-1 Training +
Phase-2 More-data + Phase-3 Registration workflow (with throwaway model
versions) completes in minutes. Metrics are not meaningful in fast mode.

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
1. Read gold (only nessie.nyc.* on `main`)              — src/ml/tracking.py.get_ml_spark
2. Phase 0 — drop redundant categoricals                — train.py applies cfg["redundant_categoricals"]
3. Last-year holdout: common_test = TEST_YEAR (2025);
   train+val = stratified random split of TRAIN_VAL_YEARS  — src/ml/splits.py.make_splits
4. Baseline (mean(label) per group_cols — RMSE-optimal,            — src/ml/tracking.py.log_baseline_run
   scored on train/val/common_test like the model runs)
5. Phase 1 Training: GLR + GBT + RFR — CV(k=3) per algo over its FULL_PARAM_GRIDS,
                                          winner by val_rmse on CV bestModel
6. Phase 2 More-data: refit Phase-1 winner on SUBSET_A_YEARS — B is the Phase-1 winner reused
7. Log every run to MLflow      — params, train/val/test Dataset inputs, metrics (train_*/val_*/common_test_*), PipelineModel artifact
8. Phase 3 Register every candidate as a model version  — best val_rmse → @champion only if it is strictly
                                                          below the previous champion's val_rmse;
                                                          rest tagged validation_status="challenger";
                                                          common_test held out, reported but not used to rank
10. Diagnostics notebook                                — {fare,duration,demand}_diagnostics.ipynb loads <experiment>@champion,
                                                          per-year scoring (TRAIN_VAL = in-sample-ish vs
                                                          TEST_YEAR = held out) = drift signal
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
