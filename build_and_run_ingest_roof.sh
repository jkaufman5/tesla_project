#!/bin/bash

IMAGE="ingest_roof_image"

# Build Docker image
docker build -t $IMAGE .

# Run Docker container
docker run -v "$(pwd)":/app $IMAGE
