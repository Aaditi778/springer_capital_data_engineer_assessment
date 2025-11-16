FROM openjdk:11-slim

# Install Python and dependencies
RUN apt-get update && apt-get install -y python3 python3-pip wget curl && \
    pip3 install pyspark pandas && \
    apt-get clean

# Install Spark
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
RUN wget https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH="$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin"

# Copy your script
WORKDIR /app
COPY script.py /app/script.py

# Entry point
ENTRYPOINT ["spark-submit", "/app/script.py"]
