FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
    curl \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-mlflow.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements-mlflow.txt

EXPOSE 5000

CMD ["sh", "-c", "mlflow server --backend-store-uri \"${BACKEND_STORE_URI}\" --default-artifact-root \"${DEFAULT_ARTIFACT_ROOT}\" --host 0.0.0.0 --port 5000"]
