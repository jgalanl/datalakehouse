FROM python:3.11-slim

# ============================================================================
# MLflow Setup
# Reference: https://mlflow.org/docs/latest/
# ============================================================================

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements-mlflow.txt .

RUN pip install --upgrade pip && \
    pip install -r requirements-mlflow.txt

# Create directories for MLflow
RUN mkdir -p /mlflow/artifacts && \
    mkdir -p /mlflow/data

# Expose MLflow tracking server port
EXPOSE 5000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=15s \
    CMD curl -f http://localhost:5000/ || exit 1

# Start MLflow tracking server
CMD ["mlflow", "server", \
    "--backend-store-uri", "${BACKEND_STORE_URI}", \
    "--default-artifact-root", "${DEFAULT_ARTIFACT_ROOT}", \
    "--host", "0.0.0.0", \
    "--port", "5000"]
