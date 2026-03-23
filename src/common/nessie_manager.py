"""Nessie branch management via Spark SQL extensions."""

from pyspark.sql import Row, SparkSession

PERSISTENT_BRANCHES = ["main", "silver", "bronze"]


class NoCommonAncestorError(Exception):
    pass


def list_references(spark: SparkSession) -> list[Row]:
    return spark.sql("LIST REFERENCES IN nessie").collect()


def ref_exists(spark: SparkSession, name: str) -> bool:
    refs = spark.sql("LIST REFERENCES IN nessie")
    return refs.filter(refs.name == name).count() > 0


def create_branch(spark: SparkSession, name: str, from_ref: str = "main") -> None:
    spark.sql(f"CREATE BRANCH IF NOT EXISTS `{name}` IN nessie FROM `{from_ref}`")


def drop_branch(spark: SparkSession, name: str) -> None:
    if name in PERSISTENT_BRANCHES:
        raise ValueError(f"Cannot drop persistent branch '{name}'")
    spark.sql(f"DROP BRANCH IF EXISTS `{name}` IN nessie")


def recreate_branch(spark: SparkSession, name: str, from_ref: str = "main") -> None:
    """Drop + recreate. Used when a branch has divergent history."""
    spark.sql(f"DROP BRANCH IF EXISTS `{name}` IN nessie")
    spark.sql(f"CREATE BRANCH `{name}` IN nessie FROM `{from_ref}`")


def merge_branch(spark: SparkSession, source: str, into: str = "main") -> None:
    try:
        spark.sql(f"MERGE BRANCH `{source}` INTO `{into}` IN nessie")
    except Exception as e:
        # Stale branch from a previous Nessie instance
        if "No common ancestor" in str(e):
            raise NoCommonAncestorError(
                f"Cannot merge '{source}' into '{into}': no common ancestor. "
                f"'{into}' likely has stale history from a previous Nessie instance."
            ) from e
        raise


def ensure_persistent_branches(spark: SparkSession) -> None:
    """Create bronze/silver from main if missing, and seed bronze with a namespace commit.

    Without at least one real commit (with a parent), Nessie's merge algorithm
    can't find a common ancestor when ETL branches try to merge back.
    """
    for branch in PERSISTENT_BRANCHES:
        if branch == "main":
            continue
        if not ref_exists(spark, branch):
            print(f"  Creating persistent branch '{branch}' from main")
            create_branch(spark, branch, from_ref="main")
        else:
            print(f"  Persistent branch '{branch}' already exists")

    # Seed bronze so it has at least one commit with a parent
    set_spark_branch(spark, "bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")
    set_spark_branch(spark, "main")


def create_tag(spark: SparkSession, name: str, from_ref: str = "main") -> None:
    spark.sql(f"CREATE TAG IF NOT EXISTS `{name}` IN nessie FROM `{from_ref}`")


def set_spark_branch(spark: SparkSession, branch: str) -> SparkSession:
    """Switch Nessie ref via USE REFERENCE (nessie-spark-extensions).

    Previous approach (newSession + conf.set) didn't reliably switch the cached catalog ref.
    """
    spark.sql(f"USE REFERENCE `{branch}` IN nessie")
    return spark


def etl_branch_name(layer: str, table: str, year: int, month: int) -> str:
    return f"etl/{layer}-{table}-{year}-{month:02d}"


def etl_tag_name(layer: str, table: str, year: int) -> str:
    return f"{layer}-{table}-{year}"


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
