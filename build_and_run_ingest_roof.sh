#!/bin/bash

# Build
docker build -t ingest-roof-container .

# Run
docker run -v "$(pwd)":/app ingest-roof-container
