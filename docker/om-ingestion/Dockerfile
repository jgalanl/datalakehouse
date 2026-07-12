FROM docker.getcollate.io/openmetadata/ingestion:1.12.1

RUN pip install --no-cache-dir "mlflow-skinny==2.22.1"

# Selecciona la última versión por marca de tiempo

RUN python - <<'PY'
import pathlib, re
p = pathlib.Path("/home/airflow/.local/lib/python3.10/site-packages/"
                 "metadata/ingestion/source/mlmodel/mlflow/metadata.py")
patched, n = re.subn(
    r"next\(\s*\(\s*ver\s+for\s+ver\s+in\s+model\.latest_versions\s+"
    r"if\s+ver\.last_updated_timestamp\s*==\s*model\.last_updated_timestamp\s*\),\s*None,\s*\)",
    "(\n"
    "                max(model.latest_versions, key=lambda v: int(v.version))\n"
    "                if model.latest_versions\n"
    "                else None\n"
    "            )",
    p.read_text(),
)
assert n == 1, f"expected 1 match, got {n}; upstream MlflowSource changed"
p.write_text(patched)
PY
