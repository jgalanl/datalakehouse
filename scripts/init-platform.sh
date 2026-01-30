#!/bin/bash
# ============================================================================
# Data Lakehouse Platform - Initialization Script
# Master's Degree Final Project
# ============================================================================
# This script initializes the data lakehouse platform by:
# 1. Starting Docker Compose services
# 2. Creating MinIO buckets
# 3. Initializing Iceberg catalog
# 4. Setting up databases

set -e

echo "=========================================="
echo "Data Lakehouse Platform Initialization"
echo "=========================================="
echo ""

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '#' | xargs)
    echo "✓ Loaded environment configuration"
else
    echo "⚠ .env file not found, using defaults"
fi

# ============================================================================
# Step 1: Start Docker Compose Services
# ============================================================================
echo ""
echo "Step 1: Starting Docker Compose services..."
echo "---"

docker-compose up -d

echo "Waiting for services to become healthy..."
sleep 20

# Check service health
services=("datalakehouse-minio" "datalakehouse-nessie-postgres" "datalakehouse-nessie" "datalakehouse-spark-master" "datalakehouse-iceberg-postgres" "datalakehouse-iceberg-rest" "datalakehouse-dremio-postgres" "datalakehouse-dremio" "datalakehouse-openmetadata-mysql" "datalakehouse-openmetadata-elasticsearch" "datalakehouse-openmetadata" "datalakehouse-mlflow-postgres" "datalakehouse-mlflow")

for service in "${services[@]}"; do
    if docker ps | grep -q "$service"; then
        echo "✓ $service is running"
    else
        echo "⚠ $service is not running yet"
    fi
done

echo ""

# ============================================================================
# Step 2: Create MinIO Buckets
# ============================================================================
echo "Step 2: Creating MinIO buckets..."
echo "---"

# Wait for MinIO to be ready
echo "Waiting for MinIO to be ready..."
for i in {1..30}; do
    if curl -s http://localhost:9000/minio/health/live > /dev/null; then
        echo "✓ MinIO is ready"
        break
    fi
    echo "  Attempt $i/30..."
    sleep 2
done

# Create buckets
docker exec datalakehouse-minio mc alias set minio http://localhost:9000 ${MINIO_ROOT_USER:-minioadmin} ${MINIO_ROOT_PASSWORD:-minioadmin123} > /dev/null 2>&1 || true

echo "Creating S3 buckets..."
docker exec datalakehouse-minio mc mb minio/warehouse --ignore-existing > /dev/null 2>&1 || true
echo "  ✓ warehouse bucket"

docker exec datalakehouse-minio mc mb minio/mlflow-artifacts --ignore-existing > /dev/null 2>&1 || true
echo "  ✓ mlflow-artifacts bucket"

docker exec datalakehouse-minio mc mb minio/spark-logs --ignore-existing > /dev/null 2>&1 || true
echo "  ✓ spark-logs bucket"

docker exec datalakehouse-minio mc mb minio/dremio-uploads --ignore-existing > /dev/null 2>&1 || true
echo "  ✓ dremio-uploads bucket"

echo ""

# ============================================================================
# Step 3: Verify Services Health
# ============================================================================
echo "Step 3: Verifying service health..."
echo "---"

echo "Service URLs:"
echo "  MinIO Console:        http://localhost:9001"
echo "  MinIO S3 API:         http://localhost:9000"
echo "  Spark Master UI:      http://localhost:8080"
echo "  Spark Worker 1 UI:    http://localhost:8081"
echo "  Spark Worker 2 UI:    http://localhost:8082"
echo "  Nessie API:           http://localhost:19120/api/v2"
echo "  Iceberg REST:         http://localhost:8181"
echo "  Dremio Console:       http://localhost:9047"
echo "  Dremio Flight:        grpc://localhost:31010"
echo "  OpenMetadata:         http://localhost:8585"
echo "  MLflow Tracking:      http://localhost:5000"
echo ""

# ============================================================================
# Step 4: Display Credentials
# ============================================================================
echo "Step 4: Service Credentials"
echo "---"
echo "MinIO:"
echo "  Username: ${MINIO_ROOT_USER:-minioadmin}"
echo "  Password: ${MINIO_ROOT_PASSWORD:-minioadmin123}"
echo ""
echo "OpenMetadata:"
echo "  Email:    ${OPENMETADATA_ADMIN_EMAIL:-admin@example.com}"
echo "  Password: ${OPENMETADATA_ADMIN_PASSWORD:-openmetadata123}"
echo ""
echo "PostgreSQL Databases:"
echo "  Nessie:   nessie:nessie123 (port 5432)"
echo "  Iceberg:  iceberg:iceberg123 (port 5433)"
echo "  Dremio:   dremio:dremio123 (port 5434)"
echo "  MLflow:   mlflow:mlflow123 (port 5435)"
echo ""

echo "=========================================="
echo "✓ Data Lakehouse Platform initialized!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Configure data sources in Dremio (http://localhost:9047)"
echo "2. Create Iceberg tables using Spark"
echo "3. Register models with MLflow"
echo "4. Document data assets in OpenMetadata"
echo ""
