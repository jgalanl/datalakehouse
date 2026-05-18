# Development Guide

## Setting Up Your Development Environment

### Prerequisites

- Python 3.9+
- pip or conda
- Docker & Docker Compose
- Git

### 1. Clone Repository

```bash
git clone <repository>
cd datalakehouse
```

### 2. Create Python Virtual Environment

```bash
# Using venv
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Using conda
conda create -n datalakehouse python=3.11
conda activate datalakehouse
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment

```bash
cp .env.example .env
# Edit .env as needed
```

### 5. Start Services

```bash
docker-compose up -d
```

## Python Development

### Project Structure

```
src/
├── common/                     # SparkSession factory + Nessie branch helpers + Iceberg DDL generator
│   ├── spark_factory.py        # get_spark(app_name) — config from spark-defaults.conf
│   ├── nessie_manager.py       # create_branch / merge_branch / create_tag / set_spark_branch / ...
│   └── iceberg_schema.py       # schema_to_ddl(StructType) — generates Iceberg column DDL with NOT NULL
├── pipelines/
│   ├── bronze/                 # Source download + Iceberg ingestion (no separate landing zone)
│   │   ├── ingest_yellow_taxi.py     # NYC TLC yellow taxi → nessie.nyc.yellow_taxi (WAP per month)
│   │   └── ingest_zone_lookup.py     # Taxi zone lookup → nessie.nyc.taxi_zone_lookup
│   ├── silver/                 # Bronze → Silver (clean, conform, deduplicate)
│   │   ├── clean_yellow_taxi.py      # WAP per month, audit on drop ratio, quarantine on fail
│   │   ├── clean_zone_lookup.py      # full refresh, row-count audit
│   │   └── reset_silver.py           # drops all silver refs and rebuilds from main
│   └── gold/                   # Silver → Gold (join, aggregate, ML features)
│       ├── build_yellow_taxi_trips.py        # trip-level enriched + time features (WAP on main)
│       └── aggregate_demand_zone_hour.py     # (zone, hour) demand aggregate (WAP on main)
├── ml/                         # ML training scripts (Spark MLlib + MLflow), gold-only
│   ├── ml_common.py                    # Shared helpers + train_regression() — full GBT skeleton (Spark+MLflow setup,
│   │                                   #   load, optional pre-filter, temporal split, fit, evaluate, log) used by all 3 scripts.
│   │                                   #   Named ml_common (not common) so it doesn't shadow the top-level common package.
│   ├── fare_prediction.py              # Use case 1 — config + main(); calls train_regression
│   ├── trip_duration.py                # Use case 2 — same, with pre_filter to drop trip_duration_minutes <= 0
│   └── demand_forecast.py              # Use case 3 — same, with year_col="hour_year" for the temporal split
└── notebooks/                  # Jupyter notebooks, split per medallion layer
    ├── bronze/                 # explore.ipynb (pandas, parquet original — first look on Jan 2019),
    │                           # preprocess.ipynb (Spark, full-dataset EDA → silver-rule evidence),
    │                           # verify_iceberg.ipynb
    ├── silver/                 # verify_iceberg.ipynb
    ├── gold/                   # verify_iceberg.ipynb (trips + demand audits)
    └── ml/                     # *_diagnostics.ipynb (one per ML use case, loads latest MLflow run)
```

### Code Style

- **PEP 8**: Follow Python Enhancement Proposal 8
- **Type Hints**: Use type annotations for all functions
- **Docstrings**: Follow Google-style docstrings

### Example: Creating a New Module

```python
"""Module description."""

from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


class MyService:
    """Service description.

    Longer description of what the service does.
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize service.

        Args:
            config: Configuration dictionary

        Raises:
            ValueError: If config is invalid
        """
        self.config = config
        logger.info("Service initialized")

    def process(self, data: Any) -> Any:
        """Process data.

        Args:
            data: Input data

        Returns:
            Processed data

        Raises:
            RuntimeError: If processing fails
        """
        try:
            result = self._do_process(data)
            logger.info("Processing completed")
            return result
        except Exception as e:
            logger.error(f"Processing failed: {e}")
            raise RuntimeError(f"Failed to process data: {e}") from e

    def _do_process(self, data: Any) -> Any:
        """Internal processing method."""
        return data
```

## Declaring a Schema in a Pipeline

Every pipeline declares its target Iceberg schema at the top of the file as a
`StructType` with explicit nullability, then creates the table with SQL DDL
generated from that schema. The pattern:

```python
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType, TimestampNTZType,
)
from common.iceberg_schema import schema_to_ddl

# 1. Declare — NOT NULL only where the pipeline provably guarantees it.
TARGET_SCHEMA = StructType([
    StructField("pickup_datetime", TimestampNTZType(), nullable=False),
    StructField("fare_amount",     DoubleType(),       nullable=False),
    StructField("airport_fee",     DoubleType(),       nullable=True),   # year-dependent
    # ...
])

# 2. Create — SQL DDL generated from the StructType.
spark.sql(f"""
    CREATE TABLE {TARGET} (
        {schema_to_ddl(TARGET_SCHEMA)}
    ) USING iceberg
    PARTITIONED BY (months(pickup_datetime))
    TBLPROPERTIES (
        'format-version' = '2',
        'write.spark.check-nullability' = 'false'
    )
""")

# 3. Write — plain append; Iceberg row-level writer enforces NOT NULL on actual values.
df.writeTo(TARGET).append()
```

`write.spark.check-nullability = 'false'` is required because Spark does not
propagate `filter(isNotNull)` / `between(...)` into the DataFrame's schema, so
without it the planner would reject the write into a `NOT NULL` column.

## Verification

This PoC does not ship a unit-test suite. Pipeline correctness is verified
end-to-end through the WAP audit gates (drop-ratio thresholds, row-count parity,
pickup conservation, zero-NULL-zone checks) and through the interactive notebooks:

- [src/notebooks/bronze/verify_iceberg.ipynb](../src/notebooks/bronze/verify_iceberg.ipynb) — Iceberg metadata, snapshots, partitions, data quality for bronze.
- [src/notebooks/silver/verify_iceberg.ipynb](../src/notebooks/silver/verify_iceberg.ipynb) — Nessie refs, schema, snapshots, growth-by-yearly-tag, partitions, cleaning-rule assertions, bronze↔silver retention, zone-lookup orphan detection.
- [src/notebooks/gold/verify_iceberg.ipynb](../src/notebooks/gold/verify_iceberg.ipynb) — schemas, snapshots, partitions, quality assertions, silver↔gold row-count parity, `sum(n_pickups)` conservation.

When the schema changes (e.g. tightening `nullable=False`), re-run the verify
notebook for the affected layer and confirm that `printSchema()` reflects the
new contract.

## Notebooks

### Jupyter Setup

```bash
pip install jupyter jupyterlab

# Start notebook
jupyter notebook
```

### Notebook Conventions

1. One topic per notebook
2. Clear markdown sections
3. Execute cells in order
4. Save notebooks without output (for git)

### Example Notebook Structure

```
# Title

## 1. Setup and Imports
- Load environment
- Initialize services

## 2. Data Loading
- Load data from source
- Display schema

## 3. Exploratory Analysis
- Summary statistics
- Visualizations

## 4. Data Processing
- Transformations
- Validations

## 5. Results and Insights
- Key findings
- Recommendations
```

## Debugging

### Python Debugging

```python
# Using pdb
import pdb; pdb.set_trace()

# Using breakpoint() (Python 3.7+)
breakpoint()

# Using logging
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.debug("Debug message")
```

### Docker Debugging

```bash
# Access container shell
docker exec -it container-name /bin/bash

# View logs
docker logs -f container-name

# Check environment
docker exec container-name env

# Execute Python in container
docker exec -it container-name python3 -c "import sys; print(sys.version)"
```

### Spark Debugging

```python
# Enable detailed logging
spark.sparkContext.setLogLevel("DEBUG")

# Profile job
spark.sql("SELECT * FROM table").explain(extended=True)

# Monitor execution
# View at http://localhost:4040
```

## Git Workflow

### Branching Strategy

```bash
# Create feature branch
git checkout -b feature/new-feature

# Make changes and commit
git add .
git commit -m "feat: add new feature"

# Push to origin
git push origin feature/new-feature

# Create pull request and merge
```

### Commit Message Format

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types**: feat, fix, docs, test, refactor, chore

**Example**:

```
feat(ingestion): add CSV ingestion support

Add support for ingesting CSV files into Iceberg tables with
automatic schema detection and type inference.

Fixes #123
```

## Performance Profiling

### Python Profiling

```python
import cProfile
import pstats

profiler = cProfile.Profile()
profiler.enable()

# Code to profile
function_call()

profiler.disable()
stats = pstats.Stats(profiler)
stats.sort_stats('cumulative')
stats.print_stats(10)
```

### Spark Performance

```python
# Query explanation
spark.sql("SELECT * FROM table").explain(extended=True)

# Collect metrics
df = spark.sql("SELECT * FROM table")
df.write.format("parquet").save("s3a://warehouse/gold/output")

# View Web UI
# http://localhost:4040
```

## Documentation

### Docstring Format

```python
def function(arg1: str, arg2: int) -> bool:
    """Short description.

    Longer description explaining what the function does
    and any important details.

    Args:
        arg1: Description of arg1
        arg2: Description of arg2

    Returns:
        Description of return value

    Raises:
        ValueError: When arg1 is empty
        TypeError: When arg2 is not int

    Examples:
        >>> function("test", 42)
        True
    """
    pass
```

### Code Comments

```python
# Use comments for WHY, not WHAT
# GOOD:
# Convert to uppercase because API requires uppercase input
name = name.upper()

# BAD:
# Convert to uppercase
name = name.upper()
```

## Release Process

### Version Numbering

Follow Semantic Versioning (MAJOR.MINOR.PATCH)

### Release Steps

1. Update version in setup.py, **init**.py
2. Update CHANGELOG.md
3. Create git tag: `git tag v1.0.0`
4. Push tag: `git push origin v1.0.0`
5. Create release notes on GitHub

## Troubleshooting Development Issues

### Import Errors

```bash
# Reinstall packages
pip install -r requirements.txt --force-reinstall

# Clear Python cache
find . -type d -name __pycache__ -exec rm -r {} +
find . -type f -name "*.pyc" -delete
```

### Module Not Found

```python
# Add to path temporarily
import sys
sys.path.insert(0, '/path/to/module')

# Or use PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:/path/to/module"
```

### Connection Issues

```bash
# Test connectivity to services
curl http://localhost:9000/minio/health/live
curl http://localhost:8080

# Check DNS resolution
docker exec container-name nslookup service-name
```

## Resources

- **Python**: https://docs.python.org/3/
- **PySpark**: https://spark.apache.org/docs/latest/api/python/
- **Pytest**: https://docs.pytest.org/
- **Git**: https://git-scm.com/doc

---

**Last Updated**: May 2026
