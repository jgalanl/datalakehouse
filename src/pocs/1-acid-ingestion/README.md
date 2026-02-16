# POC 1: ACID Ingestion

## Overview

This Proof of Concept (POC) demonstrates that a datalakehouse can behave as a traditional data warehouse by supporting ACID operations (Atomicity, Consistency, Isolation, Durability) on Apache Iceberg tables.

The POC uses:
- **Apache Spark**: Distributed data processing engine
- **Apache Iceberg**: Table format for large analytic datasets
- **MinIO**: S3-compatible object storage
- **Project Nessie**: Git-like catalog for data versioning

## Objectives

1. Ingest data from CSV into an Iceberg table
2. Demonstrate INSERT operations (add new records)
3. Demonstrate UPDATE operations (modify existing records)
4. Demonstrate DELETE operations (remove records)
5. Demonstrate Time Travel (query historical snapshots)

## Dataset

**Amazon Sales Dataset**
- Source: [Kaggle - Amazon Sales Dataset](https://www.kaggle.com/datasets/karkavelrajaj/amazon-sales-dataset)
- Contains product information, pricing, ratings, and reviews
- Downloaded programmatically using `kagglehub`

## Prerequisites

### 1. Docker Environment

Ensure the datalakehouse platform is running:

```bash
# From the project root directory
docker-compose up -d
```

Verify services are healthy:

```bash
docker-compose ps
```

Required services:
- minio (Storage)
- spark-master (Compute)
- spark-worker-1, spark-worker-2 (Workers)
- nessie-server (Catalog)
- iceberg-rest (Iceberg REST API)

### 2. Kaggle API Token

The Kaggle API token is configured in the environment variables:

1. The token is already set in the [.env](.env) file as `KAGGLE_API_TOKEN`
2. If you need to use a different token:
   - Get your token from [kaggle.com](https://www.kaggle.com/settings) (Account Settings > API > Create New API Token)
   - Update the `KAGGLE_API_TOKEN` value in the `.env` file
3. The token is automatically passed to Spark containers via docker-compose

**Note**: The kagglehub library is pre-installed in the Spark Docker image, so no manual installation is required.

## Project Structure

```
1-acid-ingestion/
├── README.md                    # This file
├── 00_download_dataset.py       # Download dataset from Kaggle
├── 01_ingest_data.py           # Ingest data into Iceberg table
├── 02_acid_operations.py       # Demonstrate ACID operations
└── data/                       # Dataset storage (created by script)
    └── amazon.csv              # Downloaded dataset
```

## Execution Steps

### Step 1: Download Dataset

Execute the dataset download script inside the Spark master container:

```bash
# Access Spark master container
docker exec -it datalakehouse-spark-master bash

# Navigate to POC directory
cd /opt/spark/src/pocs/1-acid-ingestion

# Verify Kaggle token is set
echo $KAGGLE_API_TOKEN

# Download dataset
python 00_download_dataset.py
```

Expected output:
- Kaggle authentication configured using API token
- Dataset downloaded from Kaggle
- CSV file(s) copied to `data/` directory
- Confirmation of successful download

### Step 2: Ingest Data

Run the data ingestion script:

```bash
python 01_ingest_data.py
```

This script will:
1. Create a Spark session with Iceberg configuration
2. Load the Amazon Sales CSV data
3. Create an Iceberg table in the Nessie catalog
4. Write data to MinIO storage
5. Display table statistics and sample data

Expected output:
- Table created: `nessie.sales_poc.amazon_sales`
- Initial record count
- Sample data preview
- Table history and snapshots

### Step 3: Perform ACID Operations

Run the ACID operations script:

```bash
python 02_acid_operations.py
```

This script demonstrates:

#### INSERT Operation
- Adds 3 new product records to the table
- Shows before/after record counts
- Displays newly inserted records

#### UPDATE Operation
- Updates `discount_percentage` to 60% for Electronics products with rating < 4.5
- Uses Iceberg MERGE functionality
- Shows modified records

#### DELETE Operation
- Removes records with `product_id` starting with 'P999'
- Shows before/after record counts
- Confirms deletion

#### TIME TRAVEL
- Queries historical snapshots
- Retrieves deleted records from previous snapshot
- Demonstrates data versioning capabilities

## Verification

### 1. Verify Table in MinIO

Access MinIO Console:
- URL: http://localhost:9001
- Username: (from .env MINIO_ROOT_USER)
- Password: (from .env MINIO_ROOT_PASSWORD)

Navigate to the `warehouse` bucket to see Iceberg table files.

### 2. Verify Table in Nessie

Access Nessie API:
```bash
curl http://localhost:19120/api/v2/trees
```

### 3. Query with Spark SQL

Start Spark SQL shell:
```bash
docker exec -it datalakehouse-spark-master spark-sql
```

Query the table:
```sql
-- Show all tables
SHOW TABLES IN nessie.sales_poc;

-- Count records
SELECT COUNT(*) FROM nessie.sales_poc.amazon_sales;

-- Query by category
SELECT category, COUNT(*) as count
FROM nessie.sales_poc.amazon_sales
GROUP BY category
ORDER BY count DESC;

-- View table history
SELECT * FROM nessie.sales_poc.amazon_sales.history;

-- View snapshots
SELECT snapshot_id, committed_at, operation
FROM nessie.sales_poc.amazon_sales.snapshots;
```

## Key Concepts Demonstrated

### ACID Compliance

| Property | Demonstration |
|----------|---------------|
| **Atomicity** | All operations (INSERT/UPDATE/DELETE) complete fully or not at all |
| **Consistency** | Schema validation ensures data integrity |
| **Isolation** | Snapshot isolation prevents read anomalies |
| **Durability** | Changes are persisted to MinIO storage |

### Iceberg Features

1. **Schema Evolution**: Table schema can be modified without rewriting data
2. **Time Travel**: Query historical versions using snapshot IDs
3. **MERGE Support**: Efficient UPDATE operations via MERGE INTO
4. **Partition Evolution**: Partition layout can change over time
5. **Hidden Partitioning**: No need to specify partitions in queries

### Datalakehouse Benefits

1. **Unified Storage**: Single system for both analytics and transactional workloads
2. **Open Formats**: Uses open standards (Iceberg, Parquet)
3. **Cost Effective**: Object storage is cheaper than traditional databases
4. **Scalability**: Horizontal scaling via Spark workers
5. **Version Control**: Git-like versioning via Nessie

## Expected Results

### Initial Ingestion
- Table created with original dataset records
- Snapshot 1: Initial data load

### After INSERT
- 3 new records added
- Snapshot 2: INSERT operation

### After UPDATE
- Electronics products with low ratings have updated discounts
- Snapshot 3: UPDATE operation

### After DELETE
- Test records (P999x) removed from current view
- Snapshot 4: DELETE operation
- Historical snapshots still contain deleted records

## Troubleshooting

### Issue: Kaggle credentials not found
**Solution**: Ensure `kaggle.json` is in the correct location with proper permissions

### Issue: Connection refused to Nessie
**Solution**: Verify Nessie service is running:
```bash
docker-compose ps nessie-server
curl http://localhost:19120/api/v2/config
```

### Issue: S3 access denied
**Solution**: Check AWS credentials in environment variables:
```bash
docker exec datalakehouse-spark-master env | grep AWS
```

### Issue: Out of memory
**Solution**: Increase Spark memory in docker-compose.yml or reduce data volume

## Performance Considerations

- **Data Volume**: This POC uses a small dataset for demonstration
- **Partitioning**: For production, partition tables by date or category
- **Compaction**: Regularly compact small files for better performance
- **Caching**: Nessie provides catalog caching for faster metadata access

## Next Steps

After completing this POC, consider:

1. **Partitioning**: Modify script to partition by category or date
2. **Schema Evolution**: Add/remove columns and observe compatibility
3. **Branching**: Use Nessie branches for isolated development
4. **Dremio Integration**: Query the table via Dremio SQL interface
5. **Metadata Management**: Integrate with OpenMetadata for governance

## References

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Project Nessie Documentation](https://projectnessie.org/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [MinIO Documentation](https://min.io/docs/)
- [Kaggle API Documentation](https://www.kaggle.com/docs/api)

## Author

Data Lakehouse POC
Master's Degree Final Project - Technical Architecture
Date: 2026-02-15
