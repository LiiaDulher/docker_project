#!/bin/bash

docker build -t cassandra_app -f Dockerfile4 .

docker run --name cassandra-flask --network dulher-project-network -p 8080:8080 -d cassandra_app
