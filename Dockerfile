FROM python:3.11-buster

# Install Java and set JAVA_HOME
RUN apt-get update && apt-get install -y openjdk-11-jdk && \
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 && \
    export PATH=$JAVA_HOME/bin:$PATH

# Set container working dir
WORKDIR /app

# Copy all contents in the current folder into the container under /app/
COPY . /app

# Install required packages
# RUN pip install pyspark
RUN pip install -r requirements.txt

# Submit Spark job
CMD ["spark-submit", "ingest_roof.py"]
