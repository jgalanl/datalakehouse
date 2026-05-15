# Runbook

Secuencia completa para ejecutar el pipeline de extremo a extremo. Cada bloque
es copia-pega directa.

## 0. Reset opcional (Nessie limpio)

Solo si quieres arrancar desde cero (borra todo el catálogo Nessie y los volúmenes).

```bash
docker compose down
rm -rf ./nessie-data
```

## 1. Levantar la plataforma

```bash
# Stack completo (Bronze/Silver/Gold + ML)
docker compose --profile ml up -d

# Verificar healthchecks
docker compose ps
```

> Añade `--profile governance` si vas a usar OpenMetadata.

## 2. Bronze — ingesta cruda

```bash
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/bronze/ingest_yellow_taxi.py
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/bronze/ingest_zone_lookup.py
```

## 3. Silver — limpieza y conformación

```bash
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/silver/clean_yellow_taxi.py
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/silver/clean_zone_lookup.py
```

> Reset selectivo de la capa silver (drop refs `wap/silver-*`, `silver-*`, `review/silver-*`,
> wipe `s3a://warehouse/silver/`, recrea `silver` desde `main`):
> ```bash
> docker exec -it spark-master spark-submit /opt/spark/src/pipelines/silver/reset_silver.py
> ```

## 4. Gold — curación

```bash
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/gold/build_yellow_taxi_trips.py
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/gold/aggregate_demand_zone_hour.py
```

## 5. Entrenamiento ML (requiere `--profile ml` activo)

```bash
docker exec -it spark-master spark-submit /opt/spark/src/ml/fare_prediction.py
docker exec -it spark-master spark-submit /opt/spark/src/ml/trip_duration.py
docker exec -it spark-master spark-submit /opt/spark/src/ml/demand_forecast.py
```

## 6. Verificación interactiva

Jupyter en http://localhost:8888 — abre los notebooks en orden:

- [src/notebooks/bronze/explore.ipynb](src/notebooks/bronze/explore.ipynb)
- [src/notebooks/bronze/preprocess.ipynb](src/notebooks/bronze/preprocess.ipynb)
- [src/notebooks/bronze/verify_iceberg.ipynb](src/notebooks/bronze/verify_iceberg.ipynb)
- [src/notebooks/silver/verify_iceberg.ipynb](src/notebooks/silver/verify_iceberg.ipynb)
- [src/notebooks/gold/verify_iceberg.ipynb](src/notebooks/gold/verify_iceberg.ipynb)
- [src/notebooks/ml/fare_prediction_diagnostics.ipynb](src/notebooks/ml/fare_prediction_diagnostics.ipynb)
- [src/notebooks/ml/trip_duration_diagnostics.ipynb](src/notebooks/ml/trip_duration_diagnostics.ipynb)
- [src/notebooks/ml/demand_forecast_diagnostics.ipynb](src/notebooks/ml/demand_forecast_diagnostics.ipynb)

## 7. UIs

| Servicio          | URL                                  |
|-------------------|--------------------------------------|
| MLflow            | http://localhost:5000                |
| Spark Master      | http://localhost:8080                |
| Spark Worker 1    | http://localhost:8081                |
| Jupyter Notebook  | http://localhost:8888                |
| MinIO Console     | http://localhost:9001                |
| Dremio            | http://localhost:9047                |
| Nessie API (v2)   | http://localhost:19120/api/v2/config |
| OpenMetadata      | http://localhost:8585                |

## 8. Apagar

```bash
docker compose down            # mantiene volúmenes y ./nessie-data
docker compose down -v         # limpieza total (volúmenes nombrados)
```

> `docker compose down -v` no borra `./nessie-data` (es bind mount). Para wipe
> total de Nessie usa el bloque 0.
