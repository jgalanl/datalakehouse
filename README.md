# Diseño e implementación de una arquitectura Data Lakehouse para el ciclo de vida del dato: ingeniería, aprendizaje automático y gobernanza

Prueba de concepto local y orientada a *batch* de una arquitectura **data lakehouse**,
desarrollada como Trabajo Fin de Máster (TFM). Recorre el ciclo de vida completo del dato
—ingeniería, aprendizaje automático y gobernanza— sobre el conjunto de datos público
**NYC TLC yellow taxi**, siguiendo una arquitectura *medallion* (bronze → silver → gold) y
entrenando tres modelos de regresión con Spark MLlib. Integra siete componentes
*open-source* bajo Docker Compose; nada se despliega en la nube ni en un clúster.

> **Stack:** MinIO · Apache Iceberg · Project Nessie · Apache Spark · Dremio OSS ·
> OpenMetadata · MLflow.

## Tabla de contenidos

- [Diseño e implementación de una arquitectura Data Lakehouse para el ciclo de vida del dato: ingeniería, aprendizaje automático y gobernanza](#diseño-e-implementación-de-una-arquitectura-data-lakehouse-para-el-ciclo-de-vida-del-dato-ingeniería-aprendizaje-automático-y-gobernanza)
  - [Tabla de contenidos](#tabla-de-contenidos)
  - [1. Introducción](#1-introducción)
    - [1.1. Objetivos](#11-objetivos)
  - [2. Arquitectura de la solución](#2-arquitectura-de-la-solución)
    - [2.1. Ingesta: MinIO, Iceberg y Nessie](#21-ingesta-minio-iceberg-y-nessie)
    - [2.2. Consulta y analítica: Dremio OSS](#22-consulta-y-analítica-dremio-oss)
    - [2.3. Computación: Spark](#23-computación-spark)
    - [2.4. Aprendizaje automático: MLflow y API REST](#24-aprendizaje-automático-mlflow-y-api-rest)
    - [2.5. Gobernanza: OpenMetadata](#25-gobernanza-openmetadata)
  - [3. Arquitectura de datos: el patrón medallón](#3-arquitectura-de-datos-el-patrón-medallón)
    - [3.1. Capas bronze, silver y gold](#31-capas-bronze-silver-y-gold)
    - [3.2. Modelo de datos layer-first](#32-modelo-de-datos-layer-first)
    - [3.3. Política de consumo y rigor de esquema](#33-política-de-consumo-y-rigor-de-esquema)
    - [3.4. Limpieza y outliers en silver](#34-limpieza-y-outliers-en-silver)
  - [4. Modelo Write-Audit-Publish (WAP)](#4-modelo-write-audit-publish-wap)
    - [4.1. Ramas persistentes y ramas WAP](#41-ramas-persistentes-y-ramas-wap)
    - [4.2. Tags de auditoría y cuarentena](#42-tags-de-auditoría-y-cuarentena)
    - [4.3. Validación no bloqueante](#43-validación-no-bloqueante)
  - [5. Gestión de modelos](#5-gestión-de-modelos)
    - [5.1. Casos de uso](#51-casos-de-uso)
    - [5.2. Preparación de datos](#52-preparación-de-datos)
    - [5.3. Flujo de entrenamiento](#53-flujo-de-entrenamiento)
    - [5.4. Algoritmos e hiperparámetros](#54-algoritmos-e-hiperparámetros)
    - [5.5. Registro, promoción y políticas](#55-registro-promoción-y-políticas)
    - [5.6. Servicio del modelo (API REST)](#56-servicio-del-modelo-api-rest)
  - [6. Desarrollo](#6-desarrollo)
    - [6.1. Prerrequisitos](#61-prerrequisitos)
    - [6.2. Entorno de desarrollo](#62-entorno-de-desarrollo)
    - [6.3. Estructura de carpetas](#63-estructura-de-carpetas)
    - [6.4. Construcción (build)](#64-construcción-build)
    - [6.5. Despliegue](#65-despliegue)
  - [7. Run book](#7-run-book)
    - [0. Reinicio opcional (Nessie limpio)](#0-reinicio-opcional-nessie-limpio)
    - [1. Levantar la plataforma](#1-levantar-la-plataforma)
    - [2. Bronze — ingesta cruda](#2-bronze--ingesta-cruda)
    - [3. Silver — limpieza y conformación](#3-silver--limpieza-y-conformación)
    - [4. Gold — curación](#4-gold--curación)
    - [5. Entrenamiento ML (requiere `--profile ml` activo)](#5-entrenamiento-ml-requiere---profile-ml-activo)
    - [6. Verificación interactiva](#6-verificación-interactiva)
    - [7. Servir un modelo como REST (perfil `api`)](#7-servir-un-modelo-como-rest-perfil-api)
    - [8. Catalogar en OpenMetadata — datos + modelos (perfil `governance`)](#8-catalogar-en-openmetadata--datos--modelos-perfil-governance)
    - [9. Apagar](#9-apagar)
  - [8. Licencia](#8-licencia)

---

## 1. Introducción

Un *data lakehouse* combina la flexibilidad y el bajo coste de almacenamiento de un *data
lake* con las garantías transaccionales y el modelo tabular de un *data warehouse*. Este
proyecto materializa esa idea con componentes *open-source* sobre almacenamiento de objetos
compatible con S3, sin depender de ningún proveedor de nube.

La plataforma ingiere los viajes de taxi amarillo de la ciudad de Nueva York (NYC TLC, primer
trimestre de 2023, 2024 y 2025) junto con la tabla de referencia de zonas, los procesa a
través de una arquitectura *medallion* de tres capas (bronze → silver → gold) y, sobre la capa
*gold* curada, entrena y registra modelos de regresión. Todas las transformaciones se versionan
con Nessie mediante un flujo **Write-Audit-Publish (WAP)** de estilo Git, y los activos
resultantes (tablas y modelos) se catalogan en OpenMetadata.

El proyecto es una **prueba de concepto de un solo nodo**, no un despliegue de producción:
todo corre localmente bajo Docker Compose, es estrictamente *batch* (no hay *streaming*) y usa
credenciales de desarrollo sin TLS.

### 1.1. Objetivos

El TFM persigue demostrar, de extremo a extremo y sobre infraestructura local, las tres
dimensiones del ciclo de vida del dato:

- **Ingeniería del dato.** Construir una canalización *medallion* reproducible que ingiera datos
  crudos, los limpie y los cure, con control de versiones de datos tipo Git (ramas y *tags* por
  capa) y un patrón **WAP** que garantiza que ningún dato llega a una capa persistente sin pasar
  una auditoría.
- **Aprendizaje automático.** Entrenar, evaluar y registrar modelos de regresión con Spark MLlib
  y MLflow siguiendo una metodología rigurosa (separación *train/val/test* sin fugas, selección
  por validación, *holdout* fuera de tiempo, registro versionado *champion/challenger* y servicio
  del modelo por REST).
- **Gobernanza.** Catalogar las tablas *gold* y los modelos registrados en OpenMetadata, con
  búsqueda e indexación, para dar visibilidad y trazabilidad a los activos de datos y de ML.

Objetivos transversales: usar un **catálogo único** (Nessie) para evitar metadatos divergentes,
integrar siete componentes heterogéneos de forma coherente y mantener todo el sistema
ejecutable en una máquina de desarrollo.

Quedan **explícitamente fuera de alcance**: alta disponibilidad, Kubernetes, TLS/cifrado,
control de acceso por roles, *streaming* y cualquier despliegue en clúster o nube.

---

## 2. Arquitectura de la solución

La plataforma se organiza en capas lógicas, cada una cubierta por uno o varios componentes
*open-source*. El almacenamiento (MinIO) es la base; sobre él, Iceberg aporta el formato de
tabla y Nessie el catálogo y el versionado; Spark ejecuta el cómputo *batch*; Dremio ofrece
consulta SQL; MLflow gestiona el ciclo de vida del ML; y OpenMetadata cataloga y gobierna los
activos.

```
┌─────────────────────────────────────────────────────────────────┐
│                    Aplicaciones cliente                          │
└──────────┬──────────────────────────┬──────────────────┬─────────┘
           │                          │                  │
    ┌──────▼────────┐    ┌───────────▼────────┐   ┌─────▼──────────┐
    │ Spark (batch)  │    │ Dremio (SQL/BI)    │   │ MLflow (ML)    │
    │ Procesamiento  │    │ Motor de consulta  │   │ Operaciones ML │
    └──────┬────────┘    └──────┬─────────────┘   └─────┬──────────┘
           │                     │                       │
    ┌──────▼────────────────────▼───────────────────────▼──────────┐
    │           Capa Iceberg + Nessie                              │
    │  (formato de tabla + versionado + catálogo de metadatos)     │
    └──────┬─────────────────────────────────────────────┬─────────┘
           │                                             │
    ┌──────▼─────────────────────────────────────────────▼──────────┐
    │  Almacenamiento de objetos MinIO (compatible con S3)         │
    │  Buckets:                                                     │
    │  - warehouse (todas las tablas Iceberg)                       │
    │  - mlflow-artifacts (artefactos de modelos, versionados)      │
    │  - spark-logs (event logs de Spark)                           │
    └───────────────────────────────────────────────────────────────┘
```

**Diseño de catálogo único.** Nessie es el **único** catálogo Iceberg de la plataforma. Es una
decisión deliberada: introducir un segundo catálogo (por ejemplo un Iceberg REST catalog)
crearía metadatos divergentes. Dremio y OpenMetadata leen del mismo catálogo Nessie.

**Perfiles de Docker Compose.** Los servicios se activan por perfiles. El **núcleo** (`minio`,
`minio-init`, `nessie-init`, `nessie`) no tiene perfil: siempre se levanta. El resto está
gateado:

| Perfil        | Añade                                                                      |
| ------------- | -------------------------------------------------------------------------- |
| `ingest`      | Spark (master/notebook/workers) + Dremio                                   |
| `ml`          | Spark + Dremio + MLflow (+ su PostgreSQL)                                   |
| `governance`  | MLflow + OpenMetadata (+ PostgreSQL + Elasticsearch)                        |
| `api`         | MLflow + `model-server` (servicio REST de un modelo registrado)            |

Los perfiles se combinan (`--profile ml --profile governance` une ambos conjuntos). Un
`docker compose up` desnudo levanta solo el núcleo, así que **siempre hay que pasar un perfil**
para tener cómputo.

### 2.1. Ingesta: MinIO, Iceberg y Nessie

Esta capa es el cimiento de almacenamiento, formato y catálogo. La ingesta *bronze* descarga
los datos de origen y escribe tablas Iceberg directamente (no hay zona de *landing*
independiente).

**MinIO** — Almacenamiento de objetos compatible con S3, base del *data lake*.

- API S3: `http://localhost:9000` · Consola: `http://localhost:9001`
- Un único bucket `warehouse` con estructura *layer-then-domain*: `bronze/nyc/`, `silver/nyc/`,
  `gold/nyc/`. Cada tabla Iceberg se crea con un `LOCATION` explícito
  `s3a://warehouse/{layer}/nyc/{table}`; dentro de esa carpeta, Iceberg gestiona el *layout*
  físico de los ficheros (nunca se organizan carpetas a mano).
- Buckets auxiliares: `mlflow-artifacts` (artefactos de modelos, versionados) y `spark-logs`
  (*event logs* de Spark, bajo el prefijo `events/`).
- La creación de buckets la hace en exclusiva el contenedor *one-shot* `minio-init`.

**Apache Iceberg** — Formato de tabla abierto que aporta transacciones ACID sobre el
almacenamiento de objetos.

- Evolución de esquema sin rupturas. Se demuestra en *bronze*: el campo `cbd_congestion_fee`
  (añadido por la TLC en enero de 2025) se incorpora con `ALTER TABLE ADD COLUMNS` sobre la rama
  WAP al ingerir el primer fichero de 2025; los datos anteriores devuelven `NULL` automáticamente.
- *Hidden partitioning* (poda de particiones sin exponer columnas de partición), *snapshot
  isolation* y *time travel*.
- Se usa `HadoopFileIO` (no `S3FileIO`, que requeriría el AWS SDK v2 ausente en la imagen de
  Spark); funciona vía el conector S3A de Hadoop (AWS SDK v1).

**Project Nessie** — Versionado tipo Git y catálogo de metadatos (catálogo Iceberg único).

- API: `http://localhost:19120/api/v2/config` · Estado en RocksDB embebido (sin base de datos
  externa), persistido en el volumen `nessie_data`.
- Semántica de ramas y *tags* (ver [§4](#4-modelo-write-audit-publish-wap)); *time travel* y
  transacciones atómicas multi-tabla.
- **Reparto de versiones de API:** Spark usa `/api/v1` (las `nessie-spark-extensions` 0.106.0
  usan internamente el cliente v1 y lanzan `NessieApiCompatibilityException` contra `/api/v2`);
  Dremio se conecta vía `/api/v2`. El servidor 0.106.0 sirve ambos *endpoints*.
- Todas las operaciones de ramas se hacen mediante extensiones SQL de Spark
  (`LIST REFERENCES`, `CREATE BRANCH`, `MERGE BRANCH`, `CREATE TAG`, `SHOW LOG`…), envueltas en
  `src/common/nessie_manager.py`. No se usa la API REST de Nessie directamente (tiene problemas
  de codificación de URL con `@` en los segmentos de ruta).
- Nessie corre como usuario no-root (uid 10000); un contenedor *one-shot* `nessie-init` ajusta
  la propiedad del volumen (`chown 10000:10001`) antes de arrancar.

### 2.2. Consulta y analítica: Dremio OSS

**Dremio OSS** — Motor de consulta SQL con capa semántica para analítica.

- Consola: `http://localhost:9047` · Arrow Flight: `grpc://localhost:31010`
- Lee tablas Iceberg directamente a través del catálogo Nessie (configurado manualmente desde
  la UI de Dremio, vía `/api/v2`). Capa semántica con *datasets*/vistas virtuales sobre las
  tablas físicas.
- Optimización de consultas: *predicate pushdown* a Iceberg, poda de particiones y *columnar
  data skipping*.
- Usa RocksDB embebido para sus metadatos (sin PostgreSQL externo), en el volumen `dremio_data`.
- **`paths.dist` es obligatorio** en `dremio.conf` (con esquema de URI, p. ej.
  `file:///var/lib/dremio/dist`); sin él, las fuentes de sistema fallan con
  `No FileSystem for scheme "null"`.

```sql
-- Consultar silver (vive en la rama `silver` — cualificar con AT BRANCH)
SELECT * FROM nessie.nyc.yellow_taxi AT BRANCH "silver"
WHERE tpep_pickup_datetime >= '2023-01-01'
LIMIT 10;

-- Gold ya incluye columnas de zona desnormalizadas (vive en `main`, rama por defecto)
SELECT pu_zone, COUNT(*) AS trip_count, AVG(fare_amount) AS avg_fare
FROM nessie.nyc.yellow_taxi_trips
GROUP BY pu_zone ORDER BY trip_count DESC LIMIT 20;
```

### 2.3. Computación: Spark

**Apache Spark** — Motor de procesamiento distribuido para las operaciones *batch* y el
entrenamiento ML (Spark MLlib; no hay *stack* de ML separado).

- Master: `spark://spark-master:7077` · UI del master: `http://localhost:8080`
- Topología: **1 master + 2 workers idénticos + 1 driver de notebook**.
  - `spark-master` aloja el *driver* (los *jobs* se lanzan con
    `docker exec spark-master spark-submit`). UI del *driver*: `http://localhost:4040`.
  - `spark-worker-1` (`http://localhost:8081`) y `spark-worker-2` (`http://localhost:8082`).
  - `spark-notebook`: *driver* residente en Jupyter para trabajo interactivo
    (`http://localhost:8888`; UI del *driver* en `http://localhost:4041`).
- Imagen Docker propia con los JARs de Iceberg + Nessie, el conector S3A y BLAS nativo. La
  ingesta escribe tablas Iceberg en MinIO y registra los metadatos en Nessie.
- **Dimensionamiento** (ajustado a una VM WSL2 / Docker Desktop de 13 GiB): `spark-master` 3584M
  / 2 CPU (`spark.driver.memory = 2500m`); dos *workers* de 4G / 5 CPU cada uno
  (`spark.executor.memory = 3g`, `--cores 5`). Total del clúster: **2 ejecutores × 5 núcleos =
  10 núcleos, ~6 GB de memoria de ejecutor** (`spark.default.parallelism = 10`,
  `spark.sql.shuffle.partitions = 20`). Dos *workers* son necesarios para que el CV de
  entrenamiento ejecute dos *fits* en paralelo (`CV_PARALLELISM = 2`), cada uno en su ejecutor.
- Los ficheros descargados en la ingesta deben estar en una ruta visible para *driver* y
  *workers*: se usa `/opt/spark/src/.tmp_ingest/` (*bind mount* `./src` compartido), no `/tmp/`
  (local al contenedor).

### 2.4. Aprendizaje automático: MLflow y API REST

Este apartado describe MLflow y el servidor REST como **componentes de infraestructura**. La
metodología completa de modelado está en [§5. Gestión de modelos](#5-gestión-de-modelos).

**MLflow** — Seguimiento de experimentos y registro de modelos.

- *Tracking server*: `http://localhost:5000`
- *Backend* de seguimiento en PostgreSQL; artefactos en MinIO (bucket `mlflow-artifacts`,
  versionado, con carpeta por experimento: `fare_prediction/`, `trip_duration/`,
  `demand_forecast/`).
- Registro de modelos con versionado y alias (`@champion` / `@challenger`).
- **Alineación de versiones.** El *tracking server* (`docker/mlflow/Dockerfile`) y el cliente de
  Spark (`docker/spark/Dockerfile`) fijan `mlflow` a la **misma** versión (2.22.1); una
  discrepancia ya ha dado problemas. Además, la base Python de la imagen de MLflow sigue al minor
  de la imagen base de Spark (`apache/spark:3.5.8` trae Python 3.10.12 → imagen MLflow
  `python:3.10-slim`), con `numpy`/`pandas`/`pyarrow`/`boto3` fijados a las versiones exactas de
  la imagen de Spark.

**API REST del modelo (`model-server`)** — Sirve un modelo *registrado* por REST con el servidor
integrado de MLflow (`mlflow models serve`), bajo el perfil `api`.

- Endpoint: `http://localhost:5001` · `GET /ping` · `/health` · `/version` ·
  `POST /invocations`.
- Construido desde la imagen de Spark (los modelos son `PipelineModel` de Spark MLlib y requieren
  una `SparkSession`); corre `local[*]` (driver == executor). Modelo/alias configurables en
  `.env` (`SERVE_MODEL` / `SERVE_ALIAS`, por defecto `fare_prediction@champion`).
- Está en su propio perfil a propósito: sirve un modelo *registrado*, así que debe arrancar solo
  después de que exista (si no, entra en bucle de reinicio por alias inexistente).

### 2.5. Gobernanza: OpenMetadata

**OpenMetadata** — Documentación, linaje y gobernanza de datos.

- Consola: `http://localhost:8585` · *Backend* propio en PostgreSQL + Elasticsearch.
- Cataloga las tablas Iceberg *gold* (a través del *endpoint* Iceberg-REST **integrado de
  Nessie**, `http://nessie:19120/iceberg` —el mismo *store*, no un segundo catálogo—) y los
  modelos registrados en MLflow.
- La ingesta de metadatos se ejecuta con el CLI `metadata` desde una imagen de ingesta propia
  (`docker/om-ingestion`), sobre la red de Compose; no hay contenedor Airflow.
- **El conector de datos es Iceberg/Nessie REST, no Dremio**: el conector Dremio de OpenMetadata
  1.12.1 es solo Collate (no está en la imagen OSS).
- **OpenMetadata 1.12 requiere Elasticsearch 9.x.** El cliente Java de ES embebido (9.2.4)
  negocia el protocolo `compatible-with=9`; un servidor ES 8.x lo rechaza con
  `media_type_header_exception` y el índice de búsqueda queda vacío (Explore no muestra nada),
  aunque la ingesta y la API REST funcionen. Por eso se fija **Elasticsearch 9.3.0**.

---

## 3. Arquitectura de datos: el patrón medallón

El modelo de datos sigue el patrón *medallion* centralizado de Strengholt: los datos progresan
por capas de calidad creciente, y cada transición se valida antes de publicarse.

### 3.1. Capas bronze, silver y gold

- **Bronze** — Ingesta cruda, fiel al origen. Descarga el *parquet* de la NYC TLC (Q1 de
  2023/2024/2025) y el CSV de zonas, reconcilia la deriva de esquema entre años y escribe tablas
  Iceberg directamente. **Toda columna es `NULLABLE`** (bronze conserva incluso los valores
  ausentes del origen). No hay zona de *landing*: la ingesta es el punto de entrada.
- **Silver** — Datos limpios y conformados. Aplica reglas de validez y eliminación de *outliers*
  (ver [§3.4](#34-limpieza-y-outliers-en-silver)), tipa, deduplica y declara `NOT NULL` en cada
  columna que la limpieza garantiza no-nula.
- **Gold** — Datos curados, listos para consumo y ML. Enriquece *silver* con zonas
  desnormalizadas y *features* temporales derivadas, y produce dos tablas: trazas de viaje y un
  agregado de demanda por zona y hora.

**Deriva de esquema NYC TLC.** Los *parquet* cambian de esquema entre años (INT vs BIGINT en
`VendorID`/`PULocationID`/`DOLocationID`; BIGINT vs DOUBLE en `passenger_count`/`RatecodeID`;
INT vs DOUBLE en `airport_fee`; mayúscula `Airport_fee` en 2025). `mergeSchema` no reconcilia
tipos incompatibles; *bronze* lo resuelve con un `BRONZE_SCHEMA` canónico y *casts* explícitos.

### 3.2. Modelo de datos layer-first

La capa *medallion* se codifica en la **rama** de Nessie, no en el *namespace*. Todas las tablas
comparten un único *namespace* `nyc` (el dominio de datos), creado en cada rama.

```
rama     identificador de tabla                  — descripción
bronze   nessie.nyc.yellow_taxi                  — ingesta cruda, particionada por months(tpep_pickup_datetime)
bronze   nessie.nyc.taxi_zone_lookup             — referencia, sin particionar (~265 filas)
silver   nessie.nyc.yellow_taxi                  — limpia, tipada, deduplicada
silver   nessie.nyc.taxi_zone_lookup             — nombres de zona estandarizados
main     nessie.nyc.yellow_taxi_trips            — nivel viaje: zonas desnormalizadas + features temporales
main     nessie.nyc.pickup_demand_zone_hour      — agregado (pu_location_id, hour_bucket); objetivo del modelo de demanda
```

- El mismo identificador `nessie.nyc.yellow_taxi` resuelve a una tabla física **distinta** en
  `bronze` y en `silver`, así que un identificador nunca repite el nombre de su capa.
- Cada tabla declara un `LOCATION` explícito (`s3a://warehouse/{layer}/nyc/{table}`); sin él, el
  *namespace* compartido colapsaría todas las capas sobre la misma ruta.
- **Lecturas entre ramas:** una sesión de Spark está ligada a una sola *ref* de Nessie a la vez
  (`USE REFERENCE`). Para leer de `bronze` y escribir en `silver` en la misma sesión hay que
  cambiar de *ref* explícitamente (`nessie.set_spark_branch(...)`), traer a memoria lo necesario
  y volver a cambiar.

### 3.3. Política de consumo y rigor de esquema

**Política de consumo por capas:**

- **Las canalizaciones de ML leen solo de *gold*** (las tablas `nessie.nyc.*` en `main`).
  Cualquier código bajo `src/ml/` consume exclusivamente *gold*: *silver* es interno a la
  plataforma de datos.
- **Ingenieros y científicos de datos** pueden leer de cualquier capa para exploración, EDA o
  justificación de reglas. Para eso existen los *notebooks* de `src/notebooks/bronze/` y
  `src/notebooks/silver/`.
- *Razón:* los consumidores tipo producción (ML) solo ven datos curados y validados, mientras que
  las personas conservan visibilidad completa de las capas inferiores para depurar y diseñar.

**Rigor de esquema.** Cada canalización declara su esquema objetivo como un `StructType` con
nulabilidad explícita y crea la tabla Iceberg vía DDL SQL (`CREATE TABLE ... USING iceberg`)
generada por `src/common/iceberg_schema.py:schema_to_ddl()`, de modo que las restricciones
`NOT NULL` quedan en los metadatos de columna de Iceberg, no solo por convención.

| Capa   | Política de nulabilidad                                                                          |
| ------ | ------------------------------------------------------------------------------------------------ |
| Bronze | Toda columna `NULLABLE` — fiel al origen, incluidos sus valores ausentes.                         |
| Silver | `NOT NULL` en cada columna que `clean()` filtra de forma demostrable (fechas, IDs, importes `> 0` / `>= 0`, `passenger_count` `between(1, 6)`…). Recargos no filtrados y campos dependientes del año (`congestion_surcharge`, `airport_fee`, `cbd_congestion_fee`) siguen `NULLABLE`. |
| Gold   | `NOT NULL` en cada columna trazable a una `NOT NULL` de silver, en cada *feature* temporal derivada y en cada campo de zona que la auditoría garantiza (`pu_borough`, `pu_zone`, `do_borough`, `do_zone`). `pu_service_zone` / `do_service_zone` siguen `NULLABLE`. |

**Propiedad Iceberg `write.spark.check-nullability = false`** en cada tabla: Spark no propaga
`filter(isNotNull)` / `between(...)` al esquema del DataFrame, así que la comprobación de
nulabilidad en *planning* rechazaría la escritura en una columna `NOT NULL`. Desactivarla
salta esa comprobación; la comprobación **a nivel de fila** en el *writer* de Iceberg/Parquet
sigue activa y es la red de seguridad real.

### 3.4. Limpieza y outliers en silver

`clean_yellow_taxi.py:clean()` corre en dos etapas:

1. **Filtrado de validez** — descarta filas *erróneas*: fechas de *pickup*/*dropoff* nulas,
   `pickup >= dropoff`, `trip_distance` / `tip_amount` / `tolls_amount` negativos,
   **`fare_amount` / `total_amount` no positivos** (un 0 es un error de registro, no un viaje
   real), IDs de localización fuera de `1..265`, códigos categóricos fuera del diccionario TLC
   2024 (`VendorID` / `RatecodeID` / `payment_type` / `store_and_fwd_flag`), `passenger_count`
   fuera de `1..6` y duplicados exactos de 20 columnas.
2. **Eliminación de *outliers* por IQR** — recorta **ambas colas** de `fare_amount`,
   `total_amount`, `trip_distance` y la duración del viaje usando vallas de Tukey *"far out"*
   `[Q1 − 3·IQR, Q3 + 3·IQR]` (`IQR_K = 3.0`). Las vallas se calculan **por `RatecodeID`**
   (taxímetro estándar, tarifa plana JFK/Newark, negociada, grupo, desconocida): cada código es
   una población de tarifa distinta, y una valla global estaría dominada por `RatecodeID = 1`
   (~99,5 % de filas) y recortaría viajes legítimos de aeropuerto/negociados.

Implementación: un único `groupBy("RatecodeID").agg(percentile_approx(...))` se `.collect()`-a a
Python y las vallas por código se hornean en un solo `CASE WHEN` por columna, de modo que el
filtro corre en una sola pasada sin *join*.

**Puerta de auditoría** `MAX_DROP_RATIO = 0.3`: pone en cuarentena cualquier mes cuya limpieza
descarte más del 30 % de filas. La canalización de
*preprocess* (`src/notebooks/bronze/preprocess.ipynb`) cuantifica cada regla sobre el conjunto
completo y justifica la valla de Tukey.

---

## 4. Modelo Write-Audit-Publish (WAP)

Cada transición de capa se ejecuta como **Write-Audit-Publish** sobre una rama efímera: se
escribe el resultado, se audita, y solo si pasa se fusiona en la rama persistente. Ningún dato
llega a una capa persistente sin pasar la validación.

### 4.1. Ramas persistentes y ramas WAP

- **Ramas persistentes** (siempre existen, nunca se borran):
  - `bronze` — datos crudos ingeridos
  - `silver` — datos limpios y conformados
  - `main` — capa *gold*, datos curados listos para producción
- **Ramas WAP efímeras** — patrón `wap/{target_layer}-{table}-{YYYY}-{MM}` (p. ej.
  `wap/bronze-yellow_taxi-2025-01`). Se crean desde la rama persistente destino, se validan, se
  fusionan de vuelta y se borran.

**Flujo de la canalización:** rama WAP desde `bronze` → validar → fusionar en `bronze` → rama WAP
desde `silver` (lee de `bronze`) → validar → fusionar en `silver` → rama WAP desde `main` (lee de
`silver`) → validar → fusionar en `main`.

Todos los *commits* de Nessie desde Spark se autorizan como `spark` (configurado en
`spark-defaults.conf`).

### 4.2. Tags de auditoría y cuarentena

- **Tags anuales de auditoría** — `{layer}-{table}-{YYYY}` (p. ej. `bronze-yellow_taxi-2025`).
  Uno por año, creados tras el último mes del año; marcan el punto de auditoría, no cada fusión
  mensual.
- **Tags de revisión (cuarentena)** — `review/{layer}-{table}-{YYYY}-{MM}` (p. ej.
  `review/silver-yellow_taxi-2025-02`). Se crean desde la cabeza de la rama WAP cuando la
  auditoría falla, dejando el *snapshot* rechazado inspeccionable como referencia inmutable. La
  rama WAP se borra después y la canalización continúa con el siguiente mes.
- **Ramas de experimento ML** — `ml/{model}-v{N}`; más longevas, se fusionan solo al validar.

### 4.3. Validación no bloqueante

Las puertas de validación son **no bloqueantes**: `validate()` devuelve `(passed, reason)` en
lugar de lanzar una excepción. Un mes que falla se congela como *tag* `review/...` (creado
*antes* de borrar la rama WAP), la rama WAP se descarta y el bucle continúa. Los fallos se
reportan en un resumen al final de la ejecución, no se elevan.

Reglas de auditoría por capa: *silver* usa el umbral de ratio de descarte (30 %); *gold* exige
paridad exacta de recuento de filas con *silver* (tabla de viajes) y conservación de
`sum(n_pickups)` por mes (tabla de demanda), además de cero zonas `NULL`.

La ingesta *bronze* es la excepción **bloqueante** (sí lanza, conservando la rama WAP para
inspección): valida que el **delta de filas del *append*** —el total de la tabla en la rama WAP
antes y después de escribir— coincida con `num_rows` del parquet de origen. Se mide el delta y
**no** se filtra por `(año, mes)`, porque los parquet de la TLC traen filas sueltas con
*timestamps* fuera de ventana y el total del fichero no coincidiría con esa partición.

> **Nota sobre «No common ancestor».** Si se conserva el volumen `nessie_data` tras una limpieza
> parcial, las ramas persistentes pueden quedar con historia obsoleta y las fusiones fallan con
> `No common ancestor`. Para un reinicio limpio, `docker compose down -v`. Además,
> `ensure_persistent_branches` siembra el *namespace* `nyc` en `main` para darle un *commit* no
> trivial antes de la primera fusión *gold* (Nessie 0.106.0 rechaza fusiones contra un `main` con
> solo el *commit* raíz).

---

## 5. Gestión de modelos

Sobre la capa *gold* curada se entrenan tres modelos de regresión con Spark MLlib, gestionados
con MLflow. Un **único script** `train.py` se despacha por caso de uso y ejecuta el mismo flujo
de extremo a extremo dentro de un experimento de MLflow por caso de uso. Todo el código de ML
lee **solo de las tablas *gold* en `main`** (medallion estricto).

Los bloques compartidos se reparten en cuatro módulos enfocados bajo `src/ml/`: `splits.py`
(preparación de datos), `pipelines.py` (factoría de *pipeline* de Spark ML), `algorithms.py`
(catálogo de algoritmos y *grids*) y `tracking.py` (lado MLflow: sesión, evaluadores, *baseline*,
registro).

### 5.1. Casos de uso

| # | Caso       | Objetivo                | Tabla origen                                | Familia/enlace GLR | Columna de año |
| - | ---------- | ----------------------- | ------------------------------------------- | ------------------ | -------------- |
| 1 | `fare`     | `fare_amount`           | `nessie.nyc.yellow_taxi_trips` (main)       | gamma / log        | `pickup_year`  |
| 2 | `duration` | `trip_duration_minutes` | `nessie.nyc.yellow_taxi_trips` (main)       | gamma / log        | `pickup_year`  |
| 3 | `demand`   | `n_pickups`             | `nessie.nyc.pickup_demand_zone_hour` (main) | poisson / log      | `hour_year`    |

El *dict* `USE_CASES` en `train.py` lleva los parámetros por caso (`source`, `label_col`,
`year_col`, `numerical_cols`, `categorical_cols`, `glr_family`, `baseline_group_cols`,
`label_filter_predicate`, `redundant_categoricals`). Solo `duration` filtra
`trip_duration_minutes > 0` (corrección de viajes sub-segundo que `unix_timestamp` trunca a 0).

### 5.2. Preparación de datos

**Split last-year holdout.** `common_test` es **toda fila de `TEST_YEAR = 2025`** (totalmente
fuera de tiempo: el modelo no ve ninguna fila de 2025 ni en entrenamiento ni en selección),
mientras que `train` / `val` son un *split* aleatorio estratificado por año de
`TRAIN_VAL_YEARS = [2023, 2024]` con `val_frac = 0.15` por año.

*Razón:* un *split* aleatorio de tres vías por año filtra la autocorrelación temporal
intra-año —dos viajes con minutos de diferencia en la misma zona comparten clima, tráfico y
régimen de demanda, pero un *split* aleatorio los reparte a ambos lados de la frontera
*train/test*—. Para un modelo que predice viajes *futuros*, la evaluación honesta es un *test*
cronológico fuera de tiempo, y el *holdout* del último año lo da sin solape.

*Determinismo y caché:* `make_splits` cachea el *DataFrame* padre antes del `randomSplit` (si no,
el plan *lazy* se recalcularía en cada acción y las filas migrarían entre *splits*), materializa
los tres hijos en su propia caché y luego libera el padre, que ya nadie consume. Por eso la
llamada es **eager** y los *splits* que devuelve vienen ya cacheados.

**Muestreo.** Por defecto `SAMPLE_FRACTION = 1.00` (toda la tabla *gold*); el muestreo ocurre
*antes* del *split*, así que `train` / `val` / `common_test` se muestrean proporcionalmente. El
modo `--fast` usa `FAST_SAMPLE_FRACTION = 0.01` para *smoke tests*.

**Codificación cíclica.** *Features* sin/cos de hora y día de la semana para GLR; los árboles
(GBT, RFR) conservan las columnas enteras.

### 5.3. Flujo de entrenamiento

Cada invocación corre un *baseline* más cuatro fases:

- **Fase 0 — eliminar categóricas redundantes.** `borough` (y `pu_service_zone` para `demand`)
  son derivables de `pu_location_id` vía el *zone lookup* de silver (mapeo uno a uno), así que se
  eliminan de antemano para todos los algoritmos. No es un sondeo A/B: la redundancia es
  demostrable a partir del esquema, no empírica. (Se consideró PCA y se **descartó**: régimen
  equivocado para PCA con ~7-9 numéricas, coste de interpretabilidad y sin beneficio medible.)
- **Baseline.** Media de la etiqueta por `(group_cols)`, ajustada en `train` y evaluada en
  `train`/`val`/`common_test` (mismo conjunto de métricas que los *runs* de modelo). La media es
  el predictor constante por grupo óptimo para RMSE (la mediana sería óptima para MAE — métrica
  equivocada). Todo modelo que no la supere en RMSE no aprende nada más allá de medias por grupo.
- **Fase 1 — Entrenamiento.** Un `CV(k=3)` por algoritmo sobre su `FULL_PARAM_GRIDS` (4 puntos
  GLR + 4 GBT + 4 RFR = 12 *fits*/algoritmo). El ganador de la invocación es el algoritmo cuyo
  *bestModel* del CV obtiene menor `val_rmse`.
- **Fase 2 — Más datos.** Reentrena el ganador de Fase 1 sobre `SUBSET_A_YEARS = [2023]` (la mitad
  más antigua del *pool* de entrenamiento); el subconjunto B es el ganador de Fase 1 entrenado con
  todo `TRAIN_VAL_YEARS` y reutilizado sin reajuste. Ambos evalúan sobre el mismo
  `common_test = TEST_YEAR`, así que el contraste aísla «¿ayudan más datos a generalizar a un año
  futuro?».
- **Fase 3 — Registro.** Cada candidato (4 por invocación: 3 de entrenamiento + 1 de más-datos-A)
  se registra como una nueva versión del *Model Registry* (ver [§5.5](#55-registro-promoción-y-políticas)).

### 5.4. Algoritmos e hiperparámetros

`ALGORITHM_SPECS` es la única fuente de verdad que mapea cada algoritmo a su estimador y su
preprocesado:

- **`glr`** — `GeneralizedLinearRegression` con familia/enlace por caso (gamma/log para `fare` y
  `duration`, poisson/log para `n_pickups`). Escalado *standard* (para que la regularización
  tenga sentido). Barre `regParam ∈ {0.1, 0.5, 1.0, 3.0}` (se excluyen `0` y `0.01`: la matriz de
  diseño gamma/log es deficiente de rango bajo OHE de categóricas de alta cardinalidad).
- **`gbt`** — `GBTRegressor` (*ensemble* de árboles por *boosting*, reduce sesgo). Explora
  profundidad (4/5/6) + un sondeo *stochastic-GBM* (`subsamplingRate=0.7`,
  `featureSubsetStrategy="sqrt"`, `maxIter=60`).
- **`rfr`** — `RandomForestRegressor` (*ensemble* por *bagging*, reduce varianza). Explora tamaño
  del bosque (50/100/200) + un sondeo de profundidad (`maxDepth=8`).

`build_regression_pipeline` compone StringIndexer → OneHot opcional → escalado numérico →
VectorAssembler → VectorIndexer opcional → estimador. Los árboles usan `vector_index=True` y
`TREE_MAX_BINS = 300` para que los ~265 IDs de localización superen la comprobación de aridad de
`buildMetadata`.

El CV es **artesanal** (`tune_with_cv`, `CV_NUM_FOLDS = 3`): ajusta las etapas de *features* una
vez por *fold*, cachea los vectores transformados y reajusta solo el estimador por punto del
*grid* (`CV_PARALLELISM = 2`), evitando el preprocesado redundante por *(fold × punto)* de
`CrossValidator` sin fugas entre *folds*. Los *folds* se calculan solo sobre `train`, nunca tocan
`val` ni `common_test`.

### 5.5. Registro, promoción y políticas

Cada invocación de `train.py` registra **todos** sus candidatos como nuevas versiones del *Model
Registry* bajo el nombre del experimento (`fare_prediction`, `trip_duration`, `demand_forecast`).

- **Política de métrica — `val` para seleccionar, `common_test` para reportar.** Toda decisión se
  basa en `val_rmse`. `common_test` queda *held out*: sus métricas se registran en cada versión y
  se muestran en el *notebook* de diagnóstico como estimación de generalización imparcial, pero
  **nunca** se usan para rankear candidatos (rankear sobre `common_test` lo convertiría en un
  conjunto de selección y sesgaría los números reportados — la objeción metodológica más probable
  en una defensa).
- **Política de promoción — estricta sobre `val_rmse`.** El contendiente de la invocación (menor
  `val_rmse`) pasa a `@champion` **solo si** su `val_rmse` es estrictamente menor que el del
  campeón anterior. En caso contrario se etiqueta `validation_status="challenger"`. El resto de
  candidatos se etiquetan siempre `challenger`; el alias `@challenger` apunta al mejor
  no-campeón de la invocación.
- **Política de reproducibilidad.** Cada versión lleva dos *pins* del estado de los datos en el
  momento del entrenamiento: `nessie_main_hash` (hash completo de `main`) y `source_snapshot_id`
  (último *snapshot* Iceberg de la tabla origen). Bastan para reconstruir el input exacto más
  adelante (`USE REFERENCE <hash> IN nessie`).
- **Política de latencia.** Al promocionar, el nuevo `@champion` recibe tres *tags*
  (`inference_latency_ms_per_row` / `..._sample_n` / `..._total_s`) sobre una muestra de 1000
  filas de `common_test` — chequeo de regresión de coste entre campeones, no un SLO de servicio.
- **Política de retención.** Al final de cada registro, el *registry* se poda al top
  `REGISTRY_MAX_VERSIONS = 20` no-aliadas; las versiones con alias `@champion`/`@challenger` se
  conservan siempre.
- **Datasets y uniformidad de *runs*.** Cada *run* (baseline + los 4 candidatos) registra sus
  *splits* como *Dataset* de MLflow (campo *Datasets used*) y comparte el mismo conjunto de
  *tags*/métricas (`train`/`val`/`common_test` × `{rmse, mae, r2}`), de modo que la tabla de
  *runs* sea comparable en la UI.

**Diagnóstico.** Hay un *notebook* de diagnóstico por caso de uso
(`src/notebooks/ml/{fare,duration,demand}_diagnostics.ipynb`): cada uno carga
`<experiment>@champion`, reconstruye los *splits* con la misma semilla, mide el modelo en cada
*split* y desglosa `common_test` por año (la señal de deriva), además de predicho-vs-real,
residuos, importancia de *features* y la comparación de más-datos de la Fase 2. **No reentrena.**

### 5.6. Servicio del modelo (API REST)

Un modelo registrado se expone por REST con el servicio `model-server` (perfil `api`), que corre
el servidor de inferencia integrado de MLflow (`mlflow models serve`) y carga el *flavor* Spark
del modelo. Los modelos entrenados son `PipelineModel` de Spark MLlib, así que cargar uno
necesita una `SparkSession`: por eso `model-server` se construye desde la **imagen de Spark** y
corre `local[*]` (driver == executor), lo que esquiva el problema de que los *executors* no
pueden leer el directorio de *staging* `/tmp` del *driver* en una carga distribuida.

Está en su propio perfil a propósito: sirve un modelo *registrado*, así que debe arrancar **solo
después de que exista uno** (si no, entra en bucle de reinicio por alias inexistente). Por eso no
está en el núcleo ni en el perfil `ml`.

**Modelos registrados** (uno por caso de uso, definidos en `ml/train.py` `USE_CASES`):

| Caso de uso | Modelo registrado | Predice                     | Origen *gold*                        |
| ----------- | ----------------- | --------------------------- | ------------------------------------ |
| `fare`      | `fare_prediction` | `fare_amount` ($)           | `nessie.nyc.yellow_taxi_trips`       |
| `duration`  | `trip_duration`   | `trip_duration_minutes`     | `nessie.nyc.yellow_taxi_trips`       |
| `demand`    | `demand_forecast` | `n_pickups` por zona-hora   | `nessie.nyc.pickup_demand_zone_hour` |

**Despliegue.** Qué modelo/alias se sirve se fija en `.env` (por defecto
`SERVE_MODEL=fare_prediction` / `SERVE_ALIAS=champion`). El perfil `api` levanta el servidor más
el *tracking server* de MLflow del que lee:

```bash
docker compose --profile api up -d model-server
# docker compose ps -> "healthy" (~60-90 s en frío, cuando el modelo ha cargado)
```

Para servir un modelo **distinto**, edita `SERVE_MODEL`/`SERVE_ALIAS` y recrea:
`docker compose --profile api up -d --force-recreate model-server`. Para servir varios modelos a
la vez, añade otro bloque de servicio en otro puerto (p. ej. copia `model-server` a
`model-server-demand` con `5002:5002`).

**Endpoints:**

| Método | Ruta           | Propósito                                       |
| ------ | -------------- | ----------------------------------------------- |
| GET    | `/ping`        | *Liveness* (200 cuando el modelo está cargado)  |
| GET    | `/health`      | Igual que `/ping`                               |
| GET    | `/version`     | Versión de MLflow del servidor                  |
| POST   | `/invocations` | Puntúa un lote de filas, devuelve predicciones  |

**Consumo.** Se hace `POST` de un *payload* `dataframe_split` a `/invocations` — `columns` más una
matriz `data` con una fila por predicción
([docs](https://mlflow.org/docs/latest/ml/deployment/deploy-model-locally/)). Las columnas son
las *features* de *gold* del caso de uso (tras la Fase 0 que elimina las categóricas redundantes
`pu_borough` / `do_borough` / `pu_service_zone`). *Payloads* de ejemplo por caso de uso en
[src/ml/api/examples/](src/ml/api/examples/).

```bash
curl -s -X POST http://localhost:5001/invocations \
  -H 'Content-Type: application/json' -d @src/ml/api/examples/fare_request.json
# -> {"predictions": [20.99, 8.05, 70.46]}   (la fila 3 es tarifa plana JFK ≈ $70)

# O con el cliente Python incluido (solo stdlib — solo postea el fichero de ejemplo):
python src/ml/api/client.py fare        # duration | demand, o --json <file> / --port <n>
```

El campeón de `fare` es un GBT (árbol), así que el *payload* son las columnas crudas de *gold*
**sin** codificación cíclica (`trip_distance`, `passenger_count`,
`pickup_year/month/week/dow/hour`, `pu_location_id`, `do_location_id`, `vendor_id`,
`ratecode_id`, `payment_type`); los campeones de árbol (GBT, RFR) consumen `pickup_hour`/
`pickup_dow` enteros directamente. Un campeón **GLR** esperaría las columnas cíclicas `*_sin`/
`*_cos` (que `ml/splits.py` deriva de hora/día en el entrenamiento); habría que añadirlas al
*payload* si se promociona un modelo GLR.

**Parar:**

```bash
docker compose --profile api stop model-server   # o `down` para eliminarlo
```

---

## 6. Desarrollo

### 6.1. Prerrequisitos

- **Docker** 20.10+ y **Docker Compose** 2.0+.
- **Recursos del sistema:** mínimo 8 GB de RAM y 20 GB de disco; recomendado ~18 GB de RAM y 6
  CPU asignadas a Docker, 50 GB de disco. El dimensionamiento de Spark está ajustado a una **VM
  WSL2 / Docker Desktop de 13 GiB** (configurada en `%USERPROFILE%\.wslconfig`: `memory=13GB`,
  `processors=16`, `swap=3GB`).
- **Puertos** (host): 9000-9001, 5432, 5435-5437, 8080-8082, 8585, 8888, 9047, 19120, 5000, 5001,
  31010, 32010, 45678, 9200, 9300, 4040-4041.
- **Git** y, opcionalmente, Python 3.10+ con `venv`/`conda` para soporte de editor/linting fuera
  de los contenedores.

**Versiones fijadas** (al cambiar una, actualizar **todas** sus ubicaciones):

| Componente               | Versión     | Dónde                                                              |
| ------------------------ | ----------- | ----------------------------------------------------------------- |
| Apache Spark             | 3.5.8       | `docker/spark/Dockerfile`                                         |
| Apache Iceberg           | 1.10.1      | `docker/spark/Dockerfile`                                         |
| Project Nessie           | 0.106.0     | `docker/spark/Dockerfile`, `docker-compose.yml`                  |
| Hadoop                   | 3.3.4       | `docker/spark/Dockerfile`                                         |
| AWS SDK                  | 1.12.262    | `docker/spark/Dockerfile`                                         |
| MLflow                   | 2.22.1      | `docker/spark/Dockerfile`, `docker/mlflow/requirements-mlflow.txt`|
| Dremio OSS               | 25.2.0      | `docker-compose.yml`                                             |
| OpenMetadata             | 1.12.1      | `docker-compose.yml`                                             |
| Elasticsearch            | 9.3.0       | `docker-compose.yml`                                             |
| MinIO                    | 2025-04-22  | `docker-compose.yml`                                             |
| PostgreSQL               | 16          | `docker-compose.yml`                                             |
| Python (imagen MLflow)   | 3.10-slim   | `docker/mlflow/Dockerfile`                                       |

### 6.2. Entorno de desarrollo

Las dependencias de ejecución están horneadas en las imágenes Docker
(`docker/spark/Dockerfile`, `docker/mlflow/requirements-mlflow.txt`): no hay paso de
`pip install` local. Un *venv* solo hace falta para soporte de editor/linting:

```bash
git clone <repository>
cd datalakehouse
cp .env.example .env   # configurar credenciales (hay valores por defecto)

# Levantar el perfil de desarrollo habitual:
docker compose --profile ml up -d
```

**Declarar un esquema en una canalización.** Cada *pipeline* declara su `StructType` y crea la
tabla con DDL SQL generada desde él:

```python
from common.iceberg_schema import schema_to_ddl

TARGET_SCHEMA = StructType([
    StructField("pickup_datetime", TimestampNTZType(), nullable=False),
    StructField("fare_amount",     DoubleType(),       nullable=False),
    StructField("airport_fee",     DoubleType(),       nullable=True),   # dependiente del año
])

spark.sql(f"""
    CREATE TABLE {TARGET} (
        {schema_to_ddl(TARGET_SCHEMA)}
    ) USING iceberg
    PARTITIONED BY (months(pickup_datetime))
    TBLPROPERTIES ('format-version' = '2', 'write.spark.check-nullability' = 'false')
""")
df.writeTo(TARGET).append()
```

> Nota: el *transform* de partición de Iceberg-Spark es `months(...)` (plural), no `month(...)`
> (que es la función Spark que devuelve 1..12).

**Verificación.** La PoC no trae *suite* de tests unitarios: la corrección se verifica de extremo
a extremo con las puertas de auditoría WAP y con los *notebooks* `verify_iceberg.ipynb` de cada
capa (`src/notebooks/{bronze,silver,gold}/`). Al cambiar un esquema, reejecutar el *notebook* de
verificación de la capa afectada y confirmar que `printSchema()` refleja el nuevo contrato.

**Estilo de código.** Python conciso (PEP 8); comentarios que explican el *porqué*, no el *qué*;
*docstrings* de módulo de 2-3 líneas como máximo. YAML/config con divisores `# ---` breves.
Dockerfiles con comentarios mínimos.

### 6.3. Estructura de carpetas

```
datalakehouse/
├── docker-compose.yml          # Orquestación completa (todos los servicios, healthchecks, perfiles)
├── .env / .env.example         # Configuración de entorno (credenciales, modelo a servir…)
├── config/
│   ├── spark/                  # spark-defaults.conf (catálogo + S3/MinIO), log4j2.properties
│   ├── dremio/                 # dremio.conf (internals; fuentes vía UI)
│   ├── mlflow/ · nessie/ · openmetadata/
├── docker/
│   ├── spark/Dockerfile        # Imagen Spark multi-stage (base/app/notebook) con Iceberg + Nessie
│   ├── mlflow/Dockerfile       # Tracking server de MLflow
│   └── om-ingestion/           # Imagen de ingesta parcheada para OpenMetadata
├── src/
│   ├── common/                 # spark_factory.py, nessie_manager.py, iceberg_schema.py
│   ├── pipelines/
│   │   ├── bronze/             # ingest_yellow_taxi.py, ingest_zone_lookup.py
│   │   ├── silver/             # clean_yellow_taxi.py, clean_zone_lookup.py
│   │   └── gold/               # build_yellow_taxi_trips.py, aggregate_demand_zone_hour.py
│   ├── ml/                     # train.py (entrypoint único) + splits/pipelines/algorithms/tracking.py
│   │   └── api/                # Servicio REST: client.py + examples/ + README
│   └── notebooks/
│       ├── bronze/             # explore.ipynb, preprocess.ipynb, verify_iceberg.ipynb
│       ├── silver/             # verify_iceberg.ipynb
│       ├── gold/               # verify_iceberg.ipynb
│       └── ml/                 # {fare,duration,demand}_diagnostics.ipynb
├── LICENSE                     # GNU GPL v3
└── README.md                   # Este documento
```

### 6.4. Construcción (build)

Las imágenes propias se construyen con Docker Compose. La imagen de Spark es **multi-stage** (un
Dockerfile, tres *targets*): `base` (ejecutores/workers: BLAS nativo + JARs S3/Iceberg/Nessie +
numpy/pandas/pyarrow), `app` (driver/serving: `base` + mlflow + boto3) y `notebook` (`app` +
matplotlib/seaborn/jupyter). Cada servicio elige su capa con `build.target`.

```bash
# Reconstruir una imagen tras cambiar su Dockerfile
docker compose build spark-master
docker compose build mlflow

# Imagen de ingesta de OpenMetadata (una sola vez)
docker build -t datalakehouse/om-ingestion:1.12.1 docker/om-ingestion
```

**Alineación Python/MLflow.** La base Python de la imagen de MLflow debe seguir al minor de la
imagen base de Spark. Comprobar con:

```bash
docker run --rm apache/spark:3.5.8 python3 --version   # -> Python 3.10.12
docker compose build mlflow
docker run --rm datalakehouse-mlflow:latest python -c \
  "import mlflow, numpy, pandas, pyarrow, boto3; print(mlflow.__version__)"
```

### 6.5. Despliegue

```bash
# Núcleo (MinIO, minio-init, nessie-init, Nessie) — sin cómputo
docker compose up -d

# Ingesta: núcleo + Spark + Dremio
docker compose --profile ingest up -d

# ML: núcleo + Spark + Dremio + MLflow (+ su PostgreSQL)
docker compose --profile ml up -d

# Gobernanza: núcleo + MLflow + OpenMetadata (+ PostgreSQL + Elasticsearch)
docker compose --profile governance up -d

# API: núcleo + MLflow + model-server (sirve un modelo registrado por REST)
docker compose --profile api up -d model-server

# Combinar perfiles
docker compose --profile ml --profile governance up -d

# Verificar healthchecks
docker compose ps
```

**Endpoints y UIs:**

| Servicio          | URL                                  | Perfil        |
| ----------------- | ------------------------------------ | ------------- |
| MinIO Console     | http://localhost:9001                | núcleo        |
| MinIO S3 API      | http://localhost:9000                | núcleo        |
| Nessie API (v2)   | http://localhost:19120/api/v2/config | núcleo        |
| Spark Master UI   | http://localhost:8080                | `ingest`/`ml` |
| Spark Worker 1    | http://localhost:8081                | `ingest`/`ml` |
| Spark Worker 2    | http://localhost:8082                | `ingest`/`ml` |
| Jupyter Notebook  | http://localhost:8888                | `ingest`/`ml` |
| Dremio UI         | http://localhost:9047                | `ingest`/`ml` |
| Dremio Flight     | grpc://localhost:31010               | `ingest`/`ml` |
| MLflow UI         | http://localhost:5000                | `ml`/`governance` |
| Model REST API    | http://localhost:5001                | `api`         |
| OpenMetadata UI   | http://localhost:8585                | `governance`  |

| Base de datos             | Host      | Puerto | Perfil               |
| ------------------------- | --------- | ------ | -------------------- |
| PostgreSQL (MLflow)       | localhost | 5435   | `ml`, `governance`   |
| PostgreSQL (OpenMetadata) | localhost | 5437   | `governance`         |
| Elasticsearch (OM)        | localhost | 9200   | `governance`         |

---

## 7. Run book

Secuencia completa para ejecutar el *pipeline* de extremo a extremo. Cada bloque es copia-pega
directa.

### 0. Reinicio opcional (Nessie limpio)

Solo si quieres arrancar desde cero (borra todos los volúmenes nombrados: catálogo Nessie, datos
MinIO, Postgres, Elasticsearch…).

```bash
docker compose down -v
```

### 1. Levantar la plataforma

```bash
docker compose --profile ml up -d   # núcleo + Spark + Dremio + MLflow
docker compose ps                    # verificar healthchecks
```

### 2. Bronze — ingesta cruda

```bash
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/bronze/ingest_yellow_taxi.py
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/bronze/ingest_zone_lookup.py
```

### 3. Silver — limpieza y conformación

```bash
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/silver/clean_yellow_taxi.py
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/silver/clean_zone_lookup.py
```

### 4. Gold — curación

```bash
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/gold/build_yellow_taxi_trips.py
docker exec -it spark-master spark-submit /opt/spark/src/pipelines/gold/aggregate_demand_zone_hour.py
```

### 5. Entrenamiento ML (requiere `--profile ml` activo)

Un único script despachado por caso de uso (ver [§5](#5-gestión-de-modelos) para la metodología).

```bash
docker exec -it spark-master spark-submit /opt/spark/src/ml/train.py fare
docker exec -it spark-master spark-submit /opt/spark/src/ml/train.py duration
docker exec -it spark-master spark-submit /opt/spark/src/ml/train.py demand
# Smoke test (1% de muestra, grids pequeños, CV k=2) — corre en minutos:
docker exec -it spark-master spark-submit /opt/spark/src/ml/train.py fare --fast
```

### 6. Verificación interactiva

Jupyter en `http://localhost:8888` — abre los *notebooks* en orden:

- [src/notebooks/bronze/explore.ipynb](src/notebooks/bronze/explore.ipynb) — primer vistazo con pandas al *parquet* original
- [src/notebooks/bronze/preprocess.ipynb](src/notebooks/bronze/preprocess.ipynb) — EDA sobre el dataset completo; justifica las reglas de silver
- [src/notebooks/bronze/verify_iceberg.ipynb](src/notebooks/bronze/verify_iceberg.ipynb)
- [src/notebooks/silver/verify_iceberg.ipynb](src/notebooks/silver/verify_iceberg.ipynb)
- [src/notebooks/gold/verify_iceberg.ipynb](src/notebooks/gold/verify_iceberg.ipynb)
- [src/notebooks/ml/fare_diagnostics.ipynb](src/notebooks/ml/fare_diagnostics.ipynb) · [duration](src/notebooks/ml/duration_diagnostics.ipynb) · [demand](src/notebooks/ml/demand_diagnostics.ipynb)

### 7. Servir un modelo como REST (perfil `api`)

```bash
# El modelo/alias a servir se fija en .env (por defecto fare_prediction / champion)
docker compose --profile api up -d model-server
# docker compose ps -> espera a que aparezca "healthy" (~60-90 s en frío)

curl -s -X POST http://localhost:5001/invocations \
  -H 'Content-Type: application/json' -d @src/ml/api/examples/fare_request.json
# o: python src/ml/api/client.py fare
```

### 8. Catalogar en OpenMetadata — datos + modelos (perfil `governance`)

La ingesta corre con el CLI `metadata` vía una imagen propia (`docker/om-ingestion`), sobre la
red de Compose; no hay contenedor Airflow.

```bash
# 1. Levanta governance (+ ml para tener Spark/Dremio disponibles)
docker compose --profile ml --profile governance up -d

# 2. Construye la imagen de ingesta parcheada (una sola vez)
docker build -t datalakehouse/om-ingestion:1.12.1 docker/om-ingestion

# 3. Guarda el JWT del ingestion-bot en .env como OM_INGESTION_BOT_JWT (gitignored)
#    UI: Settings > Bots > ingestion-bot ; o API /api/v1/users/auth-mechanism/<botId>

# 4. Ingesta completa: datos (Iceberg vía Nessie REST) + modelos (MLflow), en un paso
config/openmetadata/run_ingestion.sh
```

Si tras la ingesta no ves las tablas ni los modelos en **Explore** (pero sí responden por API),
el problema es el índice de Elasticsearch, no la ingesta — relanza el *reindex*:

```bash
TOKEN=$(cat config/openmetadata/.om-token | tr -d '\r\n')
curl -s -X POST -H "Authorization: Bearer $TOKEN" \
  http://localhost:8585/api/v1/apps/trigger/SearchIndexingApplication
# estado del job (sinkStats debe terminar en failedRecords: 0):
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8585/api/v1/apps/name/SearchIndexingApplication/status?limit=1"
```

(También en UI: Settings → Applications → Search Indexing → Run.)

### 9. Apagar

```bash
docker compose down            # mantiene los volúmenes nombrados
docker compose down -v         # limpieza total (borra todos los volúmenes — reset real)
```

---

## 8. Licencia

Este proyecto se distribuye bajo la **Licencia Pública General de GNU, versión 3 (GNU GPL v3)**.
El texto completo está en el fichero [LICENSE](LICENSE).

GPL v3 es una licencia *copyleft*: garantiza la libertad de usar, estudiar, compartir y modificar
el software, con la condición de que las obras derivadas se distribuyan bajo los mismos términos.
