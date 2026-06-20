from pyspark.sql import Row, SparkSession

PERSISTENT_BRANCHES = ["main", "silver", "bronze"]
DOMAIN = "nyc"
NAMESPACE = f"nessie.{DOMAIN}"


def table_location(layer: str, table: str) -> str:
    return f"s3a://warehouse/{layer}/{DOMAIN}/{table}"


def list_references(spark: SparkSession) -> list[Row]:
    return spark.sql("LIST REFERENCES IN nessie").collect()


def ref_exists(spark: SparkSession, name: str) -> bool:
    return current_ref_hash(spark, name) is not None


def create_branch(spark: SparkSession, name: str, from_ref: str = "main") -> None:
    spark.sql(f"CREATE BRANCH IF NOT EXISTS `{name}` IN nessie FROM `{from_ref}`")


def drop_branch(spark: SparkSession, name: str) -> None:
    if name in PERSISTENT_BRANCHES:
        raise ValueError(f"Cannot drop persistent branch '{name}'")
    spark.sql(f"DROP BRANCH IF EXISTS `{name}` IN nessie")


def merge_branch(spark: SparkSession, source: str, into: str = "main") -> None:
    try:
        spark.sql(f"MERGE BRANCH `{source}` INTO `{into}` IN nessie")
    except Exception as e:
        # Historial de Nessie obsoleto → reset manual (docker compose down -v y reiniciar).
        if "No common ancestor" in str(e):
            raise RuntimeError(
                f"Cannot merge '{source}' into '{into}': no common ancestor. "
                f"Nessie likely has stale history from a previous instance. "
                f"Fix: docker compose down -v (removes the nessie_data volume), then restart."
            ) from e
        raise


def ensure_persistent_branches(spark: SparkSession) -> None:
    """Crea bronze/silver a partir de main si no existen y siembra el namespace en cada rama."""
    for branch in PERSISTENT_BRANCHES:
        if branch == "main":
            continue
        if ref_exists(spark, branch):
            print(f"  Persistent branch '{branch}' already exists")
        else:
            print(f"  Creating persistent branch '{branch}' from main")
            create_branch(spark, branch, from_ref="main")
        set_spark_branch(spark, branch)
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
    set_spark_branch(spark, "main")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")


def create_tag(spark: SparkSession, name: str, from_ref: str = "main") -> None:
    spark.sql(f"CREATE TAG IF NOT EXISTS `{name}` IN nessie FROM `{from_ref}`")


def set_spark_branch(spark: SparkSession, branch: str) -> SparkSession:
    spark.sql(f"USE REFERENCE `{branch}` IN nessie")
    return spark


def wap_branch_name(layer: str, table: str, year: int, month: int) -> str:
    """Nombre de la rama de trabajo para un ciclo WAP."""
    return f"wap/{layer}-{table}-{year}-{month:02d}"


def yearly_tag_name(layer: str, table: str, year: int) -> str:
    """Tag de auditoría anual que se crea en la rama persistente tras fusionar el último mes."""
    return f"{layer}-{table}-{year}"


def review_tag_name(layer: str, table: str, year: int, month: int) -> str:
    """Nombre del tag para un mes puesto en cuarentena por la auditoría (rechazo del WAP)."""
    return f"review/{layer}-{table}-{year}-{month:02d}"


def current_ref_hash(spark: SparkSession, ref: str = "main") -> str | None:
    """Hash del último commit en `ref` (rama o tag), o None si no se conoce. Fija el estado de Nessie en tiempo de ejecución."""
    rows = (
        spark.sql("LIST REFERENCES IN nessie")
        .filter(f"name = '{ref}'")
        .collect()
    )
    if not rows:
        return None
    return rows[0]["hash"]


def latest_snapshot_id(spark: SparkSession, table: str) -> int | None:
    """ID del último snapshot de Iceberg de `table` en el ref actual de Nessie, o None si no tiene snapshots."""
    rows = spark.sql(
        f"SELECT snapshot_id FROM {table}.snapshots "
        f"ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    if not rows:
        return None
    return int(rows[0]["snapshot_id"])


def print_references(spark: SparkSession) -> None:
    refs_df = spark.sql("LIST REFERENCES IN nessie")
    rows = refs_df.collect()
    if not rows:
        print("  No references found.")
        return
    columns = refs_df.columns
    for ref in rows:
        name = ref["name"]
        ref_type = ref["refType"] if "refType" in columns else "?"
        hash_val = ref["hash"][:12] if "hash" in columns and ref["hash"] else "?"
        marker = "*" if name in PERSISTENT_BRANCHES else " "
        print(f"  {marker} {ref_type:<6} {name:<40} {hash_val}")
