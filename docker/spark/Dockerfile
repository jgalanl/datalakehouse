ARG SPARK_VERSION=3.5.2
ARG HADOOP_VERSION=3

FROM apache/spark:3.5.2

# ============================================================================
# Spark Iceberg and Dependencies Installation
# Reference: https://iceberg.apache.org/docs/latest/getting-started/
# ============================================================================

# Redefine args after FROM for use in subsequent stages
ARG SPARK_VERSION=3.5.2
ARG HADOOP_VERSION=3

USER root

# Install curl for health checks and dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages for Spark jobs
RUN pip install --upgrade pip && pip install \
    pyspark==3.5.2 \
    pyarrow>=14.0.0 \
    pandas>=2.0.0 \
    numpy>=1.24.0

# ============================================================================
# Iceberg and Related Libraries
# ============================================================================

# Create jars directory
RUN mkdir -p /opt/spark/jars

WORKDIR /opt/spark/jars

# Download Iceberg Spark runtime JAR
RUN curl -L https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_VERSION}_2.13/1.4.2/iceberg-spark-runtime-${SPARK_VERSION}_2.13-1.4.2.jar -o iceberg-spark-runtime.jar

# Download Nessie Spark Extensions
RUN curl -L https://repo1.maven.org/maven2/org/projectnessie/nessie-spark-extensions-${SPARK_VERSION}_2.13/0.82.0/nessie-spark-extensions-${SPARK_VERSION}_2.13-0.82.0.jar -o nessie-spark-extensions.jar

# Download AWS SDK for S3 access
RUN curl -L https://repo1.maven.org/maven2/software/amazon/awssdk/bundles/aws-java-sdk-bundle/1.12.565/aws-java-sdk-bundle-1.12.565.jar -o aws-java-sdk-bundle.jar

# Download Hadoop AWS for S3 support
RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}.13.3/hadoop-aws-${HADOOP_VERSION}.13.3.jar -o hadoop-aws.jar

# Download PostgreSQL JDBC driver for Nessie catalog
RUN curl -L https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar -o postgresql-jdbc.jar

# Create logs directory
RUN mkdir -p /home/spark/logs && chmod -R 777 /home/spark/logs

# Switch back to spark user
USER spark

WORKDIR /opt/spark

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Default command
CMD ["spark-class", "org.apache.spark.deploy.master.Master"]
