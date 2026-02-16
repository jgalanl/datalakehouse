ARG SPARK_VERSION=3.5.2
ARG HADOOP_VERSION=3.3.4

FROM apache/spark:3.5.2

# ============================================================================
# Spark Iceberg and Dependencies Installation
# Reference: https://iceberg.apache.org/docs/latest/getting-started/
# ============================================================================

# Redefine args after FROM for use in subsequent stages
ARG SPARK_VERSION=3.5.2
ARG HADOOP_VERSION=3.3.4

USER root

# Install curl for health checks and dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages for Spark jobs and Jupyter
RUN pip install --upgrade pip && pip install \
    pyspark==3.5.2 \
    pyarrow>=14.0.0 \
    pandas>=2.0.0 \
    numpy>=1.24.0 \
    kagglehub>=0.2.0 \
    jupyter \
    notebook

# ============================================================================
# Iceberg and Related Libraries
# ============================================================================

# Create jars directory
RUN mkdir -p /opt/spark/jars

WORKDIR /opt/spark/jars

# Download Iceberg Spark runtime JAR (must match Scala 2.12 bundled in Spark image)
RUN curl -fL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar -o iceberg-spark-runtime.jar

# Download Nessie Spark Extensions (must match Scala 2.12)
RUN curl -fL https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.82.0/nessie-spark-extensions-3.5_2.12-0.82.0.jar -o nessie-spark-extensions.jar

# Download AWS SDK for S3 access
RUN curl -fL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.565/aws-java-sdk-bundle-1.12.565.jar -o aws-java-sdk-bundle.jar

# Download Hadoop AWS for S3 support (must match bundled Hadoop version)
RUN curl -fL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -o hadoop-aws.jar

# Download PostgreSQL JDBC driver for Nessie catalog
RUN curl -fL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar -o postgresql-jdbc.jar

# Create home directories with proper permissions for spark user
RUN mkdir -p /home/spark/logs /home/spark/.kaggle && \
    chown -R spark:spark /home/spark

# Switch back to spark user
USER spark

WORKDIR /opt/spark

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Default command
CMD ["spark-class", "org.apache.spark.deploy.master.Master"]
