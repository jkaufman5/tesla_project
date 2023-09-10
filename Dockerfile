# FROM python:3.8-slim
# FROM apache/spark-py:v3.4.0
FROM python:3.8-buster

# Install Java and set JAVA_HOME
RUN apt-get update && apt-get install -y openjdk-11-jdk && \
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 && \
    export PATH=$JAVA_HOME/bin:$PATH

# Set container working dir
WORKDIR /app

# Copy all contents in the current folder into the container under /app/
COPY . /app

# Install Python
# RUN apt-get update && apt-get install -y python3

# Install necessary packages
RUN pip install pyspark

# Define env variable for PySpark
# ENV PYSPARK_PYTHON=python

# Submit Spark job
CMD ["spark-submit", "ingest_roof.py"]
