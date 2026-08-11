FROM docker.getcollate.io/openmetadata/ingestion:1.12.1

RUN pip install --no-cache-dir "mlflow-skinny==2.22.1"

# Selección de la última versión de cada modelo registrado.
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
assert n == 1, f"se esperaba 1 coincidencia, hay {n}; MlflowSource ha cambiado aguas arriba"
p.write_text(patched)
PY

# Selección de referencia de Nessie en el conector Iceberg-REST.
RUN python - <<'PY'
import pathlib
p = pathlib.Path("/home/airflow/.local/lib/python3.10/site-packages/"
                 "metadata/ingestion/source/database/iceberg/catalog/rest.py")
src = p.read_text()

old_import = "from pyiceberg.catalog import Catalog, load_rest"
assert src.count(old_import) == 1, "los imports de IcebergRestCatalog han cambiado aguas arriba"
src = src.replace(old_import, "import os\n" + old_import, 1)

anchor = (
    "            \"token\": catalog.connection.token.get_secret_value()\n"
    "            if catalog.connection.token\n"
    "            else None,\n"
    "        }\n"
)
inject = anchor + (
    "\n"
    "        # Nessie multi-referencia: inyecta el prefix Iceberg-REST (rama/etiqueta)\n"
    "        # que OM OSS no expone, para catalogar la capa objetivo.\n"
    "        _nessie_ref = os.environ.get(\"NESSIE_ICEBERG_PREFIX\")\n"
    "        if _nessie_ref:\n"
    "            parameters[\"prefix\"] = _nessie_ref\n"
)
assert src.count(anchor) == 1, "el dict parameters de IcebergRestCatalog ha cambiado aguas arriba"
src = src.replace(anchor, inject, 1)
p.write_text(src)
PY
