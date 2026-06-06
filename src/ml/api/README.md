# Model serving — REST API

Deploy a registered MLflow model as a REST endpoint and call it for predictions.
The trained models are Spark MLlib `PipelineModel`s; MLflow's built-in inference
server loads the Spark flavor and exposes a standard scoring API.

## Why it runs on the Spark image

The `model-server` service is built from the **Spark image** (`docker/spark`) because
the models are Spark MLlib `PipelineModel`s — loading one needs a SparkSession. It
runs `local[*]` (driver == executor), which sidesteps the "executors can't read the
driver's `/tmp` staging dir" problem that breaks distributed Spark-model loads. It's
defined in `docker-compose.yml` under the **`api` profile**, with port `5001` mapped
to the host, a `/ping` healthcheck, and `restart: on-failure`.

The service is **not** in the core or `ml` profile on purpose: it serves a *registered*
model, so it must start only after one exists — otherwise it restart-loops on a missing
alias. Bring it up with its own profile once a champion is registered.

## Prerequisites

```bash
# A model must be registered with an alias first (training does this automatically).
# Train one if needed, then check: http://localhost:5000 -> Models -> fare_prediction -> @champion
docker exec -it spark-master spark-submit /opt/spark/src/ml/train.py fare
```

Registered models (one per use case, set in `ml/train.py` `USE_CASES`):

| Use case   | Registered model  | Predicts                  | Gold source                     |
| ---------- | ----------------- | ------------------------- | ------------------------------- |
| `fare`     | `fare_prediction` | `fare_amount` ($)         | `nessie.nyc.yellow_taxi_trips`  |
| `duration` | `trip_duration`   | `trip_duration_minutes`   | `nessie.nyc.yellow_taxi_trips`  |
| `demand`   | `demand_forecast` | `n_pickups` per zone-hour | `nessie.nyc.pickup_demand_zone_hour` |

## Deploy

Which model/alias is served is set in `.env` (defaults shown):

```dotenv
SERVE_MODEL=fare_prediction   # or trip_duration, demand_forecast
SERVE_ALIAS=champion
```

Then bring up the `api` profile — it starts the model server plus the MLflow
tracking server it reads from (and the always-on MinIO/Nessie core):

```bash
docker compose --profile api up -d model-server
```

The container waits on its `/ping` healthcheck; `docker compose ps` shows it healthy
once the model has loaded (~60–90 s cold). Logs: `docker compose logs -f model-server`.

To serve a **different** model, edit `SERVE_MODEL`/`SERVE_ALIAS` and recreate:
`docker compose --profile api up -d --force-recreate model-server`. To serve several
models at once, add another service block on a different port (e.g. copy `model-server`
to `model-server-demand` with `5002:5002`).

## Endpoints

| Method | Path           | Purpose                                  |
| ------ | -------------- | ---------------------------------------- |
| GET    | `/ping`        | Liveness (200 once the model is loaded)  |
| GET    | `/health`      | Same as `/ping`                          |
| GET    | `/version`     | MLflow version of the server             |
| POST   | `/invocations` | Score a batch of rows, returns predictions |

## Consume

POST a `dataframe_split` payload to `/invocations`. Two equivalent ways:

```bash
# curl (per the MLflow docs)
curl -s -X POST http://localhost:5001/invocations \
     -H 'Content-Type: application/json' \
     -d @src/ml/api/examples/fare_request.json
# -> {"predictions": [20.99, 8.05, 70.46]}

# or the bundled Python helper (stdlib only — just posts the example file)
python src/ml/api/client.py fare        # duration | demand, or --json <file> / --port <n>
```

## Request format

MLflow's `/invocations` accepts a `dataframe_split` body — `columns` plus a `data`
matrix of one row per prediction
([docs](https://mlflow.org/docs/latest/ml/deployment/deploy-model-locally/)). The
columns are the **gold feature columns** of the use case (see `ml/train.py`
`USE_CASES`), after Phase-0 drops the redundant categoricals (`pu_borough` /
`do_borough` / `pu_service_zone`, derivable from the location id). Example payloads:
[`examples/`](examples/).

The example payloads carry integer `pickup_hour`/`pickup_dow`, which the **tree
champions (GBT, RFR)** consume directly — the current `fare` champion is a GBT. A
**GLR champion** instead expects the cyclical `*_sin`/`*_cos` columns (`ml/splits.py`
derives them from hour/dow at train time); add those fields to the payload if you
promote a GLR model.

## Stop

```bash
docker compose --profile api stop model-server   # or `down` to remove it
```
