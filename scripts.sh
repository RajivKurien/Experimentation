#!/bin/bash

# Debugging, Prevent uninitialized variables, exit immediately on non-zero return code
set -o nounset \
    -o errexit \
    -o xtrace

# Version
KAFKA_VERSION=3.7.0

# Start podman
#podman machine init
#podman machine start


# https://github.com/apache/kafka/blob/trunk/docker/resources/common-scripts/configureDefaults#L20
# 5L6g3nShT-eMCtK--X86sw
# Generate 20character ClusterID. Somehow the docker container doesn't see this ???
CLUSTER_ID=$(openssl rand -base64 20 | tr -dc 'a-zA-Z0-9' | head -c 20)

# Run container image
docker run \
  --publish 9092:9092 \
  --env CLUSTER_ID:"${CLUSTER_ID}" \
  --env KAFKA_CLUSTER_ID:"${CLUSTER_ID}" \
  apache/kafka:${KAFKA_VERSION}