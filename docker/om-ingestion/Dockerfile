FROM docker.getcollate.io/openmetadata/ingestion:1.12.1

RUN pip install --no-cache-dir "mlflow-skinny==2.22.1"

# Selección de la versión servida (@champion) de cada modelo registrado
RUN python - <<'PY'
import pathlib, re
p = pathlib.Path("/home/airflow/.local/lib/python3.10/site-packages/"
                 "metadata/ingestion/source/mlmodel/mlflow/metadata.py")
src = p.read_text()

anchor = "class MlflowSource(MlModelServiceSource):"
helper = '''MODEL_ALIAS = "champion"


def pick_model_version(client, model):
    """Versión con alias @champion; si el modelo no lo tiene, la de mayor número."""
    try:
        return client.get_model_version_by_alias(model.name, MODEL_ALIAS)
    except Exception:  # todavía sin alias: registro recién creado o alias retirado
        logger.info(f"{model.name}: sin alias @{MODEL_ALIAS}, se cataloga la última versión")
    versions = model.latest_versions
    return max(versions, key=lambda v: int(v.version)) if versions else None


'''
assert src.count(anchor) == 1, "MlflowSource ha cambiado aguas arriba"
src = src.replace(anchor, helper + anchor, 1)

old_comment = "            # Get the latest version\n"
assert src.count(old_comment) == 1, "el bucle de get_mlmodels ha cambiado aguas arriba"
src = src.replace(old_comment, "            # Versión servida, con la última como respaldo\n", 1)

src, n = re.subn(
    r"next\(\s*\(\s*ver\s+for\s+ver\s+in\s+model\.latest_versions\s+"
    r"if\s+ver\.last_updated_timestamp\s*==\s*model\.last_updated_timestamp\s*\),\s*None,\s*\)",
    "pick_model_version(self.client, model)",
    src,
)
assert n == 1, f"se esperaba 1 coincidencia, hay {n}; MlflowSource ha cambiado aguas arriba"
p.write_text(src)
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
