#!/bin/bash

IMAGE="ingest_roof_image"
# CONTAINER="ingest_roof_container"

# Build Docker image
docker build -t $IMAGE .

# Run Docker container
docker run -v "$(pwd)":/app $IMAGE
# docker run --name $CONTAINER -d $IMAGE
