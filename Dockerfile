# Use an official OpenJDK image as base
FROM openjdk:8-jdk-slim

# Set Spark version
ENV SPARK_VERSION=2.0.0
ENV HADOOP_VERSION=2.7

# Install dependencies
RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

# Download and extract Spark
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | \
    tar zx -C /opt/

# Set environment variables
ENV SPARK_HOME=/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ENV PATH=$PATH:$SPARK_HOME/bin

WORKDIR /workspace

# Default command
CMD ["/bin/bash"]

# docker build --no-cache --pull -t spark-image .
# docker run -it -p 4040:4040 -v “/apache-spark-intro:/workspace" spark-image
# spark-submit --class org.example.CountNonBoringWords --master local[*] spark-app.jar
