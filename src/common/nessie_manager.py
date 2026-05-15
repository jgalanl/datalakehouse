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


def merge_branch(spark: SparkSession, source: str, into: str = "main") -> None:
    try:
        spark.sql(f"MERGE BRANCH `{source}` INTO `{into}` IN nessie")
    except Exception as e:
        # Stale branch from a previous Nessie instance — manual reset required
        # (delete ./nessie-data, restart Nessie). See CLAUDE.md "Known Gotchas".
        if "No common ancestor" in str(e):
            raise NoCommonAncestorError(
                f"Cannot merge '{source}' into '{into}': no common ancestor. "
                f"Nessie likely has stale history from a previous instance. "
                f"Fix: stop Nessie, delete ./nessie-data, restart."
            ) from e
        raise


def ensure_persistent_branches(spark: SparkSession) -> None:
    """Create bronze/silver from main if missing, then seed namespaces.

    Safe to call repeatedly: existing branches are left untouched (never
    dropped or recreated). If a branch is truly stale from a prior Nessie
    instance, a WAP merge will raise NoCommonAncestorError with a
    clear message directing to manual reset.

    Also seeds nessie.gold on main itself: gold lives on main (no separate
    gold branch). Seeding gives main a real namespace commit so wap/gold-*
    branches forked from main share a non-trivial ancestor with it. Without
    this seed, the very first gold merge into main throws a spurious
    "No common ancestor" because both refs sit at the initial empty commit.
    """
    for branch in PERSISTENT_BRANCHES:
        if branch == "main":
            continue
        if ref_exists(spark, branch):
            print(f"  Persistent branch '{branch}' already exists")
        else:
            print(f"  Creating persistent branch '{branch}' from main")
            create_branch(spark, branch, from_ref="main")
        # Seed the branch with a namespace commit (idempotent).
        set_spark_branch(spark, branch)
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS nessie.{branch}")
    # Seed the gold namespace on main (gold lives on main).
    set_spark_branch(spark, "main")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")


def create_tag(spark: SparkSession, name: str, from_ref: str = "main") -> None:
    spark.sql(f"CREATE TAG IF NOT EXISTS `{name}` IN nessie FROM `{from_ref}`")


def set_spark_branch(spark: SparkSession, branch: str) -> SparkSession:
    """Switch Nessie ref via USE REFERENCE (nessie-spark-extensions).

    Previous approach (newSession + conf.set) didn't reliably switch the cached catalog ref.
    """
    spark.sql(f"USE REFERENCE `{branch}` IN nessie")
    return spark


def wap_branch_name(layer: str, table: str, year: int, month: int) -> str:
    """Working-branch name for one WAP cycle (Write → Audit → Publish)."""
    return f"wap/{layer}-{table}-{year}-{month:02d}"


def yearly_tag_name(layer: str, table: str, year: int) -> str:
    """Yearly audit tag created on the persistent layer after the last merged month."""
    return f"{layer}-{table}-{year}"


def review_tag_name(layer: str, table: str, year: int, month: int) -> str:
    """Tag name for a month quarantined by the audit step (WAP reject)."""
    return f"review/{layer}-{table}-{year}-{month:02d}"


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
