FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-mlflow.txt .

RUN pip install --upgrade pip && \
    pip install -r requirements-mlflow.txt

RUN mkdir -p /mlflow/artifacts && \
    mkdir -p /mlflow/data

EXPOSE 5000

# sh -c is needed so the shell expands BACKEND_STORE_URI and DEFAULT_ARTIFACT_ROOT at runtime
CMD ["sh", "-c", "mlflow server --backend-store-uri \"${BACKEND_STORE_URI}\" --default-artifact-root \"${DEFAULT_ARTIFACT_ROOT}\" --host 0.0.0.0 --port 5000"]
