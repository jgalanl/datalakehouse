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
├── common/                     # Shared utilities (SparkSession, Nessie helpers)
├── etl/
│   ├── bronze/                 # Source download + Iceberg ingestion (no separate landing zone)
│   │   ├── ingest_yellow_taxi.py   # NYC TLC yellow taxi → nessie.bronze.yellow_taxi
│   │   └── ingest_zone_lookup.py   # Taxi zone lookup → nessie.bronze.taxi_zone_lookup
│   ├── silver/                 # Bronze → Silver (clean, conform, deduplicate)
│   └── gold/                   # Silver → Gold (join, aggregate, ML features)
├── ml/                         # ML experiments (MLflow tracking)
└── notebooks/                  # Jupyter notebooks, split per medallion layer
    ├── bronze/                 # explore.ipynb (pandas, parquet original — first look on Jan 2019), preprocess.ipynb (Spark, full-dataset EDA), verify_iceberg.ipynb
    ├── silver/                 # verify_iceberg.ipynb
    └── gold/                   # feature engineering, ML prep
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

## Testing

### Test Structure

```
tests/
├── __init__.py
├── test_core.py
├── test_ingestion.py
├── test_ml_ops.py
└── conftest.py
```

### Writing Tests

```python
import pytest
from src.core import SparkSessionBuilder, CatalogManager


@pytest.fixture
def spark():
    """Provide Spark session for tests."""
    return SparkSessionBuilder.build(
        app_name="test",
        master="local[*]"
    )


@pytest.fixture
def catalog_manager(spark):
    """Provide catalog manager."""
    return CatalogManager(spark)


def test_create_database(catalog_manager):
    """Test database creation."""
    result = catalog_manager.create_database("test_db")
    assert result is True


def test_list_tables_empty(catalog_manager):
    """Test listing empty database."""
    catalog_manager.create_database("empty_db")
    tables = catalog_manager.list_tables("empty_db")
    assert tables == []


@pytest.mark.parametrize("db_name", ["db1", "db2", "db3"])
def test_multiple_databases(catalog_manager, db_name):
    """Test creating multiple databases."""
    result = catalog_manager.create_database(db_name)
    assert result is True
```

### Running Tests

```bash
# Run all tests
pytest

# Run specific test
pytest tests/test_core.py::test_create_database

# Run with verbose output
pytest -v

# Run with coverage
pytest --cov=src

# Run only fast tests
pytest -m "not slow"
```

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

**Last Updated**: January 2026
