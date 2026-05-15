"""Reset the silver layer: drop all silver refs, purge table data, recreate branch.

Usage:
    docker exec -it spark-master spark-submit /opt/spark/src/pipelines/silver/reset_silver.py
"""

from common.spark_factory import get_spark
from common import nessie_manager as nessie

TABLE = "nessie.silver.yellow_taxi"
NAMESPACE = "nessie.silver"
SILVER_DATA_PREFIX = "s3a://warehouse/silver/"
SILVER_REF_PREFIXES = (
    "wap/silver-",
    "silver-",
    "review/silver-",
)


def delete_silver_data(spark):
    """Recursively delete s3a://warehouse/silver/ via Hadoop FileSystem.

    DROP TABLE ... PURGE is refused by Iceberg when gc.enabled=false (the Nessie
    default — snapshots may be shared across refs). Since reset drops every
    silver ref before getting here, no branch references these files anymore
    and a manual prefix wipe is safe.
    """
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(SILVER_DATA_PREFIX)
    fs = path.getFileSystem(hconf)
    if fs.exists(path):
        fs.delete(path, True)
        print(f"  Deleted {SILVER_DATA_PREFIX}")
    else:
        print(f"  {SILVER_DATA_PREFIX} already absent")


def collect_silver_refs(spark):
    """Find all non-persistent branches and tags related to the silver layer."""
    rows = nessie.list_references(spark)
    branches, tags = [], []
    for row in rows:
        name = row["name"]
        if name == "silver":
            continue  # handled separately
        if not any(name.startswith(p) for p in SILVER_REF_PREFIXES):
            continue
        # Nessie returns refType as "Branch" / "Tag" (Pascal case)
        if row["refType"].upper() == "TAG":
            tags.append(name)
        else:
            branches.append(name)
    return branches, tags


def main():
    spark = get_spark(app_name="reset-silver")

    # --- Discover silver refs ---
    branches, tags = collect_silver_refs(spark)
    print(f"Found {len(branches)} silver branch(es), {len(tags)} silver tag(s)")

    # --- Drop WAP branches ---
    for b in branches:
        print(f"  Dropping branch '{b}'")
        spark.sql(f"DROP BRANCH IF EXISTS `{b}` IN nessie")

    # --- Drop tags (yearly + review) ---
    for t in tags:
        print(f"  Dropping tag '{t}'")
        spark.sql(f"DROP TAG IF EXISTS `{t}` IN nessie")

    # --- Drop the silver table metadata (data files cleaned separately) ---
    nessie.set_spark_branch(spark, "silver")
    print(f"  Dropping table {TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")

    # --- Wipe orphaned silver data files from MinIO ---
    delete_silver_data(spark)

    # --- Recreate silver branch fresh from main ---
    # Switch away from silver before dropping it
    nessie.set_spark_branch(spark, "main")
    print("  Dropping persistent branch 'silver'")
    spark.sql("DROP BRANCH IF EXISTS `silver` IN nessie")
    print("  Recreating 'silver' from main")
    spark.sql("CREATE BRANCH `silver` IN nessie FROM `main`")
    nessie.set_spark_branch(spark, "silver")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")

    # --- Verify ---
    print("\nNessie references after reset:")
    nessie.print_references(spark)
    spark.stop()
    print("\nSilver layer fully reset.")


if __name__ == "__main__":
    main()
