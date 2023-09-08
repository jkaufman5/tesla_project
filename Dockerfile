#!/bin/bash

FROM python:3.10-slim

WORKDIR /app

# TODO Install Java
RUN apt-get update && apt-get install -y openjdk-8-jdk

# Copy all contents in the current folder into the container under /app/
COPY . /app

# Install necessary packages (in this case, pyspark)
RUN pip install -r requirements.txt

ENV PYSPARK_PYTHON=python

# Submit Spark job
CMD ["spark-submit", "ingest_roof.py"]
