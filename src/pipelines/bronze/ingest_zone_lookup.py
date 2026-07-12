import os
import shutil
import sys
from datetime import date
from pathlib import Path
from urllib.request import urlretrieve

from pyspark.sql.functions import lit
from pyspark.sql.types import (
    IntegerType, StringType, StructField, StructType,
)

from common.spark_factory import get_spark
from common.iceberg_schema import schema_to_ddl
from common import nessie_manager as nessie

ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
TABLE = "nessie.nyc.taxi_zone_lookup"
TABLE_LOCATION = nessie.table_location("bronze", "taxi_zone_lookup")
DOWNLOAD_DIR = Path("/opt/spark/src/.tmp_ingest")  # bind mount compartido
# Esquema bronze de la tabla de zonas. Todos los campos son nullable para permitir la evolución del esquema.
BRONZE_ZONE_SCHEMA = StructType([
    StructField("LocationID", IntegerType(), nullable=True),
    StructField("Borough", StringType(), nullable=True),
    StructField("Zone", StringType(), nullable=True),
    StructField("service_zone", StringType(), nullable=True),
])


def validate_ingestion(spark, expected_count: int) -> None:
    actual = spark.table(TABLE).count()
    if actual != expected_count:
        raise ValueError(f"Se esperaban {expected_count} registros, se obtuvieron {actual}")
    print(f"  Validación superada: {actual} registros")


def main() -> None:
    try:
        DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        dest = DOWNLOAD_DIR / "taxi_zone_lookup.csv"

        print(f"\n[1/2] Descargando {ZONE_LOOKUP_URL}")
        try:
            urlretrieve(ZONE_LOOKUP_URL, dest)
            print(f"  -> {os.path.getsize(dest) / 1024:.1f} KB")
        except OSError as e:
            print(f"  FALLÓ: {e}")
            sys.exit(1)

        print("\n[2/2] Ingestando tabla de zonas (WAP: rama -> validar -> fusionar)\n")
        spark = get_spark(app_name="bronze-ingest-zone-lookup")
        nessie.ensure_persistent_branches(spark=spark)
        spark = nessie.set_spark_branch(spark=spark, branch="bronze")

        today = date.today()
        branch_name = nessie.wap_branch_name(
            layer="bronze", table="taxi_zone_lookup",
            year=today.year, month=today.month,
        )
        tag_name = nessie.yearly_tag_name(
            layer="bronze", table="taxi_zone_lookup", year=today.year,
        )

        print(f"  Creando rama '{branch_name}' desde bronze")
        nessie.create_branch(spark=spark, name=branch_name, from_ref="bronze")
        spark = nessie.set_spark_branch(spark=spark, branch=branch_name)

        try:
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {nessie.NAMESPACE}")

            df = (
                spark.read.option("header", "true")
                .schema(BRONZE_ZONE_SCHEMA)
                .csv(str(dest))
            )
            count = df.count()
            print(f"  Registros: {count}")
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {TABLE} (
                    {schema_to_ddl(BRONZE_ZONE_SCHEMA)}
                ) USING iceberg
                LOCATION '{TABLE_LOCATION}'
                TBLPROPERTIES (
                    'format-version' = '2',
                    'write.spark.check-nullability' = 'false'
                )
            """)
            df.writeTo(TABLE).overwrite(lit(True))

            validate_ingestion(spark=spark, expected_count=count)

            print(f"  Fusionando '{branch_name}' -> bronze")
            nessie.merge_branch(spark=spark, source=branch_name, into="bronze")

            print(f"  Creando tag '{tag_name}'")
            nessie.create_tag(spark=spark, name=tag_name, from_ref="bronze")
            nessie.drop_branch(spark=spark, name=branch_name)

        except Exception:
            nessie.drop_branch(spark=spark, name=branch_name)
            raise
        finally:
            spark = nessie.set_spark_branch(spark=spark, branch="bronze")

        print(f"\nHecho. Tabla de zonas escrita en {TABLE}")
        nessie.print_references(spark=spark)
        spark.stop()
    finally:
        shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)


if __name__ == "__main__":
    main()
