from pyspark.sql.types import StructType


def schema_to_ddl(schema: StructType) -> str:
    return ",\n            ".join(
        f"{f.name} {f.dataType.simpleString()}{'' if f.nullable else ' NOT NULL'}"
        for f in schema.fields
    )
