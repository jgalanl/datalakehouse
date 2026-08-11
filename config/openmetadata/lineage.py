#!/usr/bin/env python3
"""Declara en OpenMetadata el linaje de la arquitectura medallón y los modelos.

Las aristas se envían como PUT idempotentes, así que se reconstruyen en cada ingesta.
"""
import json
import os
import urllib.error
import urllib.request

OM_HOST = os.environ.get("OM_HOST", "http://localhost:8585").rstrip("/")
JWT = os.environ.get("OM_JWT") or os.environ.get("OM_INGESTION_BOT_JWT")
if not JWT:
    raise SystemExit("Falta OM_JWT / OM_INGESTION_BOT_JWT en el entorno.")
HEADERS = {"Authorization": f"Bearer {JWT}", "Content-Type": "application/json"}

B = "iceberg_bronze.nessie.nyc."
S = "iceberg_silver.nessie.nyc."
G = "iceberg_gold.nessie.nyc."
M = "mlflow_registry."

# Mapeo de columnas silver -> gold yellow_taxi_trips (renombrados y derivadas del timestamp)
TRIPS_FROM_TAXI = [
    ("pickup_datetime", ["tpep_pickup_datetime"]),
    ("dropoff_datetime", ["tpep_dropoff_datetime"]),
    ("pickup_year", ["tpep_pickup_datetime"]),
    ("pickup_month", ["tpep_pickup_datetime"]),
    ("pickup_week", ["tpep_pickup_datetime"]),
    ("pickup_dow", ["tpep_pickup_datetime"]),
    ("pickup_hour", ["tpep_pickup_datetime"]),
    ("trip_duration_minutes", ["tpep_pickup_datetime", "tpep_dropoff_datetime"]),
    ("pu_location_id", ["PULocationID"]),
    ("do_location_id", ["DOLocationID"]),
    ("vendor_id", ["VendorID"]),
    ("ratecode_id", ["RatecodeID"]),
    ("payment_type", ["payment_type"]),
    ("store_and_fwd_flag", ["store_and_fwd_flag"]),
    ("passenger_count", ["passenger_count"]),
    ("trip_distance", ["trip_distance"]),
    ("fare_amount", ["fare_amount"]),
    ("tip_amount", ["tip_amount"]),
    ("tolls_amount", ["tolls_amount"]),
    ("mta_tax", ["mta_tax"]),
    ("extra", ["extra"]),
    ("improvement_surcharge", ["improvement_surcharge"]),
    ("congestion_surcharge", ["congestion_surcharge"]),
    ("airport_fee", ["airport_fee"]),
    ("cbd_congestion_fee", ["cbd_congestion_fee"]),
    ("total_amount", ["total_amount"]),
]
# Columnas de zona (PU y DO) que gold yellow_taxi_trips toma del join con taxi_zone_lookup
TRIPS_FROM_ZONE = [
    ("pu_borough", ["borough"]), ("pu_zone", ["zone"]), ("pu_service_zone", ["service_zone"]),
    ("do_borough", ["borough"]), ("do_zone", ["zone"]), ("do_service_zone", ["service_zone"]),
]
# Mapeo silver -> gold pickup_demand_zone_hour (clave, bucket horario, derivadas y agregados)
DEMAND_FROM_TAXI = [
    ("pu_location_id", ["PULocationID"]),
    ("hour_bucket", ["tpep_pickup_datetime"]),
    ("hour_year", ["tpep_pickup_datetime"]),
    ("hour_month", ["tpep_pickup_datetime"]),
    ("hour_week", ["tpep_pickup_datetime"]),
    ("hour_dow", ["tpep_pickup_datetime"]),
    ("hour_of_day", ["tpep_pickup_datetime"]),
    ("total_revenue", ["total_amount"]),
    ("avg_fare", ["fare_amount"]),
    ("avg_tip", ["tip_amount"]),
    ("avg_distance", ["trip_distance"]),
    ("avg_passengers", ["passenger_count"]),
]
DEMAND_FROM_ZONE = [
    ("pu_borough", ["borough"]), ("pu_zone", ["zone"]), ("pu_service_zone", ["service_zone"]),
]
ZONE_BRONZE_TO_SILVER = [
    ("location_id", ["LocationID"]), ("borough", ["Borough"]),
    ("zone", ["Zone"]), ("service_zone", ["service_zone"]),
]

# Aristas: origen, destino, descripción, mapeo de columnas
#   cols = "IDENTITY" (mismas columnas), lista [(col_destino, [cols_origen])], o None (a nivel de tabla)
EDGES = [
    # bronze -> silver (limpieza)
    (B + "yellow_taxi", S + "yellow_taxi",
     "Silver ETL — clean_yellow_taxi.py: filtrado de validez y outliers IQR (Tukey k=3) por RatecodeID",
     "IDENTITY"),
    (B + "taxi_zone_lookup", S + "taxi_zone_lookup",
     "Silver ETL — clean_zone_lookup.py: refresco completo de la tabla de zonas",
     ZONE_BRONZE_TO_SILVER),
    # silver -> gold (enriquecimiento / agregación)
    (S + "yellow_taxi", G + "yellow_taxi_trips",
     "Gold ETL — build_yellow_taxi_trips.py: enriquecimiento y features temporales",
     TRIPS_FROM_TAXI),
    (S + "taxi_zone_lookup", G + "yellow_taxi_trips",
     "Gold ETL — build_yellow_taxi_trips.py: broadcast join de zonas (PU/DO)",
     TRIPS_FROM_ZONE),
    (S + "yellow_taxi", G + "pickup_demand_zone_hour",
     "Gold ETL — aggregate_demand_zone_hour.py: agregación por (zona, hora)",
     DEMAND_FROM_TAXI),
    (S + "taxi_zone_lookup", G + "pickup_demand_zone_hour",
     "Gold ETL — aggregate_demand_zone_hour.py: broadcast join de zonas",
     DEMAND_FROM_ZONE),
    # gold -> modelos (entrenamiento) — a nivel de tabla
    (G + "yellow_taxi_trips", M + "fare_prediction",
     "Entrenamiento — train.py fare (GBT @champion)", None),
    (G + "yellow_taxi_trips", M + "trip_duration",
     "Entrenamiento — train.py duration (GBT @champion)", None),
    (G + "pickup_demand_zone_hour", M + "demand_forecast",
     "Entrenamiento — train.py demand (GBT @champion)", None),
]


def api(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(OM_HOST + path, data=data, headers=HEADERS, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            raw = resp.read().decode()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()[:300]


def build_columns_lineage(mapping, src_fqn, dst_fqn, cols):
    """Traduce (col_destino, [cols_origen]) a columnsLineage con FQN reales de columna."""
    if mapping is None:
        return None
    src_cols, dst_cols = cols[src_fqn], cols[dst_fqn]
    if mapping == "IDENTITY":
        mapping = [(name, [name]) for name in dst_cols if name in src_cols]
    out = []
    for to_col, from_cols in mapping:
        if to_col not in dst_cols:
            print(f"       [skip col] destino inexistente: {dst_fqn}.{to_col}")
            continue
        froms = [src_cols[c] for c in from_cols if c in src_cols]
        if len(froms) != len(from_cols):
            miss = [c for c in from_cols if c not in src_cols]
            print(f"       [skip col] origen inexistente: {src_fqn}.{miss}")
            continue
        out.append({"fromColumns": froms, "toColumn": dst_cols[to_col]})
    return out or None


def main():
    # id y type por FQN; y FQN de cada columna por (tablaFQN, nombre)
    ref, cols = {}, {}
    _, tpage = api("GET", "/api/v1/tables?fields=columns&limit=1000")
    for t in tpage["data"]:
        ref[t["fullyQualifiedName"]] = {"id": t["id"], "type": "table"}
        cols[t["fullyQualifiedName"]] = {
            c["name"]: c["fullyQualifiedName"] for c in t.get("columns", [])
        }
    _, mpage = api("GET", "/api/v1/mlmodels?limit=1000")
    for m in mpage["data"]:
        ref[m["fullyQualifiedName"]] = {"id": m["id"], "type": "mlmodel"}

    failures = 0
    for src, dst, desc, colmap in EDGES:
        missing = [fqn for fqn in (src, dst) if fqn not in ref]
        if missing:
            print(f"[SKIP] entidad no catalogada: {', '.join(missing)}")
            failures += 1
            continue
        details = {"source": "Manual", "description": desc}
        col_lineage = build_columns_lineage(colmap, src, dst, cols)
        if col_lineage:
            details["columnsLineage"] = col_lineage
        body = {"edge": {"fromEntity": ref[src], "toEntity": ref[dst], "lineageDetails": details}}
        status, resp = api("PUT", "/api/v1/lineage", body)
        ok = status in (200, 201)
        ncols = len(col_lineage) if col_lineage else 0
        print(f"[{'OK ' if ok else 'ERR'} {status}] {src.split('.')[-1]:24s} -> "
              f"{dst.split('.')[-1]:24s} ({ncols} col)")
        if not ok:
            print("       ", resp)
            failures += 1
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
