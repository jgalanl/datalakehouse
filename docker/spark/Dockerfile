ARG SPARK_VERSION=3.5.8
ARG HADOOP_VERSION=3.3.4
ARG ICEBERG_VERSION=1.10.1
ARG NESSIE_VERSION=0.106.0
ARG AWS_SDK_VERSION=1.12.262

FROM apache/spark:${SPARK_VERSION} AS base

ARG SPARK_VERSION
ARG HADOOP_VERSION
ARG ICEBERG_VERSION
ARG NESSIE_VERSION
ARG AWS_SDK_VERSION

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    git \
    libgomp1 \
    libgfortran5 \
    libopenblas-dev \
    && rm -rf /var/lib/apt/lists/*

RUN update-alternatives --install /usr/lib/x86_64-linux-gnu/libblas.so.3 \
    libblas.so.3-x86_64-linux-gnu /usr/lib/x86_64-linux-gnu/libopenblas.so.0 100 && \
    update-alternatives --install /usr/lib/x86_64-linux-gnu/liblapack.so.3 \
    liblapack.so.3-x86_64-linux-gnu /usr/lib/x86_64-linux-gnu/libopenblas.so.0 100

RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir \
    pyarrow==17.0.0 \
    pandas==2.2.3 \
    numpy==1.26.4

# JARs (una sola capa): runtime de Iceberg, extensiones SQL de Nessie, bundle del AWS SDK
# y hadoop-aws
WORKDIR /opt/spark/jars
RUN curl -fSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar -o iceberg-spark-runtime.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/${NESSIE_VERSION}/nessie-spark-extensions-3.5_2.12-${NESSIE_VERSION}.jar -o nessie-spark-extensions.jar && \
    curl -fSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -o aws-java-sdk-bundle.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -o hadoop-aws.jar

RUN mkdir -p /home/spark/logs && chown -R spark:spark /home/spark

ENV HOME=/home/spark
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH="/opt/spark/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip"
ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER spark
WORKDIR /opt/spark
CMD ["spark-class", "org.apache.spark.deploy.master.Master"]

# --- app: spark-master y model-server
FROM base AS app
USER root
RUN pip install --no-cache-dir \
    mlflow==2.22.1 \
    boto3==1.35.92
USER spark

# --- notebook: spark-notebook
FROM app AS notebook
USER root
RUN pip install --no-cache-dir \
    matplotlib==3.9.4 \
    seaborn==0.13.2 \
    jupyter==1.1.1 \
    notebook==7.2.2
USER spark
