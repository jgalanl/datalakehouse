# Spark image with Iceberg and Nessie support.
#
# Before building, verify that the Hadoop version bundled in the base image
# still matches HADOOP_VERSION. Run:
#   docker run --rm apache/spark:3.5.8 ls /opt/spark/jars/ | grep hadoop-common
# If it differs from 3.3.4, update HADOOP_VERSION and AWS_SDK_VERSION to match.

ARG SPARK_VERSION=3.5.8
ARG HADOOP_VERSION=3.3.4
ARG ICEBERG_VERSION=1.10.1
ARG NESSIE_VERSION=0.106.0
# AWS SDK version must align with hadoop-aws 3.3.4 — check the dependency tree at
# https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/3.3.4
ARG AWS_SDK_VERSION=1.12.262

FROM apache/spark:${SPARK_VERSION}

# ARGs need to be redeclared after FROM to be in scope for the rest of the build
ARG SPARK_VERSION
ARG HADOOP_VERSION
ARG ICEBERG_VERSION
ARG NESSIE_VERSION
ARG AWS_SDK_VERSION

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# All Python versions pinned for reproducibility
RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir \
    pyspark==${SPARK_VERSION} \
    pyarrow==17.0.0 \
    pandas==2.2.3 \
    numpy==1.26.4 \
    kagglehub==0.3.6 \
    jupyter==1.1.1 \
    notebook==7.2.2 \
    mlflow==2.20.3 \
    boto3==1.35.92

# JARs: Iceberg runtime, Nessie extensions, Hadoop-AWS connector, AWS SDK bundle
RUN mkdir -p /opt/spark/jars
WORKDIR /opt/spark/jars

RUN curl -fSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
    -o iceberg-spark-runtime.jar

# Nessie extensions add CREATE BRANCH, MERGE BRANCH, SHOW LOG to Spark SQL
RUN curl -fSL https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/${NESSIE_VERSION}/nessie-spark-extensions-3.5_2.12-${NESSIE_VERSION}.jar \
    -o nessie-spark-extensions.jar

RUN curl -fSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    -o aws-java-sdk-bundle.jar

# hadoop-aws version must match the Hadoop bundled in the Spark base image
RUN curl -fSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar \
    -o hadoop-aws.jar

# No PostgreSQL JDBC driver needed — Spark talks to Nessie over REST, not JDBC

RUN mkdir -p /home/spark/logs /home/spark/.kaggle && \
    chown -R spark:spark /home/spark

USER spark

WORKDIR /opt/spark

ENV HOME=/home/spark
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

CMD ["spark-class", "org.apache.spark.deploy.master.Master"]
