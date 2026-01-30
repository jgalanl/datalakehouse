@echo off
REM ============================================================================
REM Data Lakehouse Platform - Initialization Script (Windows PowerShell)
REM Master's Degree Final Project
REM ============================================================================
REM This script initializes the data lakehouse platform

setlocal enabledelayedexpansion

echo.
echo ==========================================
echo Data Lakehouse Platform Initialization
echo ==========================================
echo.

REM Load environment variables from .env file
if exist .env (
    echo Loading environment configuration...
    for /f "delims== tokens=1,2" %%A in (.env) do (
        if not "%%A"=="" if not "%%A:~0,1%"=="#" (
            set "%%A=%%B"
        )
    )
    echo. Environment loaded
) else (
    echo. .env file not found, using defaults
)

REM ============================================================================
REM Step 1: Start Docker Compose Services
REM ============================================================================
echo.
echo Step 1: Starting Docker Compose services...
echo ---

docker-compose up -d

echo Waiting for services to become healthy...
timeout /t 20 /nobreak

echo.

REM ============================================================================
REM Step 2: Create MinIO Buckets
REM ============================================================================
echo Step 2: Creating MinIO buckets...
echo ---

echo Waiting for MinIO to be ready...
for /L %%i in (1,1,30) do (
    curl -s http://localhost:9000/minio/health/live >nul 2>&1
    if !errorlevel! equ 0 (
        echo. MinIO is ready
        goto :minio_ready
    )
    echo   Attempt %%i/30...
    timeout /t 2 /nobreak
)

:minio_ready

echo Creating S3 buckets...

docker exec datalakehouse-minio mc alias set minio http://localhost:9000 minioadmin minioadmin123 >nul 2>&1

docker exec datalakehouse-minio mc mb minio/warehouse --ignore-existing >nul 2>&1
echo   . warehouse bucket

docker exec datalakehouse-minio mc mb minio/mlflow-artifacts --ignore-existing >nul 2>&1
echo   . mlflow-artifacts bucket

docker exec datalakehouse-minio mc mb minio/spark-logs --ignore-existing >nul 2>&1
echo   . spark-logs bucket

docker exec datalakehouse-minio mc mb minio/dremio-uploads --ignore-existing >nul 2>&1
echo   . dremio-uploads bucket

echo.

REM ============================================================================
REM Step 3: Display Service Information
REM ============================================================================
echo Step 3: Service URLs and Credentials
echo ---
echo.
echo Service URLs:
echo   MinIO Console:        http://localhost:9001
echo   MinIO S3 API:         http://localhost:9000
echo   Spark Master UI:      http://localhost:8080
echo   Spark Worker 1 UI:    http://localhost:8081
echo   Spark Worker 2 UI:    http://localhost:8082
echo   Nessie API:           http://localhost:19120/api/v2
echo   Iceberg REST:         http://localhost:8181
echo   Dremio Console:       http://localhost:9047
echo   Dremio Flight:        grpc://localhost:31010
echo   OpenMetadata:         http://localhost:8585
echo   MLflow Tracking:      http://localhost:5000
echo.
echo MinIO Credentials:
echo   Username: minioadmin
echo   Password: minioadmin123
echo.
echo OpenMetadata Credentials:
echo   Email:    admin@example.com
echo   Password: openmetadata123
echo.

echo ==========================================
echo. Data Lakehouse Platform initialized!
echo ==========================================
echo.

endlocal
