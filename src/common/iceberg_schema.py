"""Generate Iceberg CREATE TABLE column DDL from a Spark StructType.

Each StructField becomes `name type [NOT NULL]` — the nullable flag becomes
a Iceberg NOT NULL constraint, which the writer enforces at row level.
"""

from pyspark.sql.types import StructType


def schema_to_ddl(schema: StructType) -> str:
    return ",\n            ".join(
        f"{f.name} {f.dataType.simpleString()}{'' if f.nullable else ' NOT NULL'}"
        for f in schema.fields
    )
