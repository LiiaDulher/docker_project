#!/bin/bash

docker network create dulher-project-network
docker run --name cassandra-node --network dulher-project-network -p 9042:9042 -d cassandra:latest
sleep 70s
echo "Creating keyspace and tables"
docker build -t ddl_image -f Dockerfile1 .
docker run -it --network dulher-project-network --rm ddl_image
