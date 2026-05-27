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

## 4. Gold — curación

```bash
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/gold/build_yellow_taxi_trips.py
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/gold/aggregate_demand_zone_hour.py
```

## 5. Entrenamiento ML (requiere `--profile ml` activo)

El split es un **last-year holdout**: `common_test` es toda la fila de `TEST_YEAR=2025` (totalmente reservado — el modelo no ve ninguna fila de 2025 ni durante entrenamiento ni en selección), y `train`/`val` se obtienen por split aleatorio estratificado por año dentro de `TRAIN_VAL_YEARS=[2019, 2020, 2022, 2023]` (`val_frac=0.15`).

Un único script dispatcheado por caso de uso. Cada invocación corre Phase 0 (drop de categóricas redundantes — borough es derivable de location_id) → baseline `mean-por-grupo` (la media es el predictor constante por grupo óptimo para RMSE; la mediana sería óptima para MAE — métrica equivocada) → Phase 1 Training (CV(k=3) por algoritmo sobre su `FULL_PARAM_GRIDS` completo — 5-pt GLR, 8-pt GBT, 7-pt RFR; el ganador es el algoritmo cuyo bestModel del CV obtiene menor `val_rmse`; cada algoritmo queda registrado con su mejor punto correctamente afinado) → Phase 2 More-data (A entrena con `SUBSET_A_YEARS=[2019, 2020]`, B es el ganador de Phase 1 entrenado con todo `TRAIN_VAL_YEARS`; ambos se evalúan en `common_test = TEST_YEAR`) → Phase 3 Registration: cada candidato (4 por invocación: 3 training + 1 more-data-A) se registra como una nueva versión del modelo en el MLflow Model Registry. El ganador de la invocación (menor `val_rmse`) pasa a `@champion` solo si su `val_rmse` es estrictamente menor que el `val_rmse` del campeón anterior (mejora estricta del punto); en caso contrario se etiqueta `validation_status="challenger"`. El resto de candidatos se etiquetan siempre `validation_status="challenger"`, y el alias `@challenger` apunta al mejor de ellos. `common_test` queda *held out* — sus métricas se registran en cada versión pero no se usan para rankear: son la estimación de generalización imparcial que se reporta en el notebook de diagnóstico.

Cada versión del registro lleva además el **hash de Nessie `main`** (`nessie_main_hash`) y el **snapshot_id de Iceberg** de la tabla origen (`source_snapshot_id`) en el momento del entrenamiento — basta con el model card para reconstruir el input exacto más adelante. Cuando hay promoción, el nuevo `@champion` recibe tres tags de latencia (`inference_latency_ms_per_row` / `inference_latency_sample_n` / `inference_latency_total_s`) sobre una muestra de 1000 filas de `common_test` — chequeo de regresión de coste entre campeones, no un SLO de servicio. Al final de cada invocación el registro se poda al top `REGISTRY_MAX_VERSIONS = 20` no-aliadas (las versiones con alias `@champion`/`@challenger` se conservan siempre).

```bash
docker exec -it spark-master spark-submit /opt/spark/src/ml/train.py fare
docker exec -it spark-master spark-submit /opt/spark/src/ml/train.py duration
docker exec -it spark-master spark-submit /opt/spark/src/ml/train.py demand
# Smoke test (1% sample, grids tiny, CV k=2) — corre en minutos:
docker exec -it spark-master spark-submit /opt/spark/src/ml/train.py fare --fast
```

## 6. Verificación interactiva

Jupyter en http://localhost:8888 — abre los notebooks en orden:

- [src/notebooks/bronze/explore.ipynb](src/notebooks/bronze/explore.ipynb)
- [src/notebooks/bronze/preprocess.ipynb](src/notebooks/bronze/preprocess.ipynb)
- [src/notebooks/bronze/verify_iceberg.ipynb](src/notebooks/bronze/verify_iceberg.ipynb)
- [src/notebooks/silver/verify_iceberg.ipynb](src/notebooks/silver/verify_iceberg.ipynb)
- [src/notebooks/gold/verify_iceberg.ipynb](src/notebooks/gold/verify_iceberg.ipynb)
- [src/notebooks/ml/model_diagnostics.ipynb](src/notebooks/ml/model_diagnostics.ipynb) — cambia `USE_CASE` a `fare` / `duration` / `demand` en la primera celda y rerun.

## 7. UIs

| Servicio         | URL                                  |
| ---------------- | ------------------------------------ |
| MLflow           | http://localhost:5000                |
| Spark Master     | http://localhost:8080                |
| Spark Worker 1   | http://localhost:8081                |
| Spark Worker 2   | http://localhost:8082                |
| Jupyter Notebook | http://localhost:8888                |
| MinIO Console    | http://localhost:9001                |
| Dremio           | http://localhost:9047                |
| Nessie API (v2)  | http://localhost:19120/api/v2/config |
| OpenMetadata     | http://localhost:8585                |

## 8. Apagar

```bash
docker compose down            # mantiene volúmenes y ./nessie-data
docker compose down -v         # limpieza total (volúmenes nombrados)
```

> `docker compose down -v` no borra `./nessie-data` (es bind mount). Para wipe
> total de Nessie usa el bloque 0.
