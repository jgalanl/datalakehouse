# OpenMetadata ingestion image, patched for this stack.
# Two fixes over the stock 1.12.1 ingestion image, both required to ingest our
# MLflow registry (server pinned at MLflow 2.20.3, models managed via aliases):
#   1. The stock image bundles the MLflow *client* 3.6.0, whose registry-search
#      REST contract is incompatible with a 2.20.3 server (search returns empty).
#      Pin the client to the server version.
#   2. OM's MlflowSource picks the latest model version with
#      `ver.last_updated_timestamp == model.last_updated_timestamp`. That equality
#      breaks whenever the registered-model timestamp lags its newest version (it
#      does here, by ~40 ms), yielding no model. Patch it to take the highest
#      version number instead.
FROM docker.getcollate.io/openmetadata/ingestion:1.12.1

# Stay as the default `airflow` user — the base image's pip wrapper refuses to run
# as root, and packages install into /home/airflow/.local owned by this user.

# 1. Match the MLflow client to our tracking/registry server. mlflow-skinny carries
#    the client + entities the connector uses, and (unlike full mlflow) installs
#    cleanly under the image's airflow pip constraints.
RUN pip install --no-cache-dir "mlflow-skinny==2.20.3"

# 2. Make the latest-version selection robust (highest version, not timestamp-equality).
RUN python - <<'PY'
import pathlib
p = pathlib.Path(
    "/home/airflow/.local/lib/python3.10/site-packages/"
    "metadata/ingestion/source/mlmodel/mlflow/metadata.py"
)
src = p.read_text()
old = """            latest_version: Optional[ModelVersion] = next(
                (
                    ver
                    for ver in model.latest_versions
                    if ver.last_updated_timestamp == model.last_updated_timestamp
                ),
                None,
            )"""
new = """            # PATCHED: pick the highest version number; upstream's
            # timestamp-equality check breaks when the model record lags its
            # newest version (alias-managed registries, MLflow 2.x).
            latest_version: Optional[ModelVersion] = (
                max(model.latest_versions, key=lambda v: int(v.version))
                if model.latest_versions
                else None
            )"""
assert old in src, "upstream MlflowSource shape changed; revisit the patch"
p.write_text(src.replace(old, new))
print("patched MlflowSource latest-version selection")
PY
