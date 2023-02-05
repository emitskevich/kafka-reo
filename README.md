[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![dockerhub-deploy](https://github.com/emitskevich/kafka-reo/workflows/dockerhub-deploy/badge.svg)](https://github.com/emitskevich/kafka-reo/actions/workflows/dockerhub-deploy.yml)

## What is it
Apache **Kafka** **R**eplicator **E**xactly-**O**nce - the tool to replicate data between 
**different** Apache Kafka **clusters** with **exactly-once** delivery.

## Quickstart
Set your bootstrap servers and topic name for both clusters and run a container from DockerHub:
```
docker run \
    -e KAFKA_CLUSTERS_SOURCE_BOOTSTRAP_SERVERS=source-kafka-cluster:9092 \
    -e KAFKA_CLUSTERS_SOURCE_TOPIC=source-topic \
    -e KAFKA_CLUSTERS_DESTINATION_BOOTSTRAP_SERVERS=destination-kafka-cluster:9092 \
    -e KAFKA_CLUSTERS_DESTINATION_TOPIC=destination-topic \
    emitskevich/kafka-reo
```

## Launch options
#### Docker run
```
docker run \
    -e KAFKA_CLUSTERS_SOURCE_BOOTSTRAP_SERVERS=source-kafka-cluster:9092 \
    -e KAFKA_CLUSTERS_SOURCE_TOPIC=source-topic \
    -e KAFKA_CLUSTERS_DESTINATION_BOOTSTRAP_SERVERS=destination-kafka-cluster:9092 \
    -e KAFKA_CLUSTERS_DESTINATION_TOPIC=destination-topic \
    emitskevich/kafka-reo
```
Or firstly build it from sources:
```
./gradlew check installDist
docker build . -t emitskevich/kafka-reo --build-arg MODULE=replicator
docker run \
    -e KAFKA_CLUSTERS_SOURCE_BOOTSTRAP_SERVERS=source-kafka-cluster:9092 \
    -e KAFKA_CLUSTERS_SOURCE_TOPIC=source-topic \
    -e KAFKA_CLUSTERS_DESTINATION_BOOTSTRAP_SERVERS=destination-kafka-cluster:9092 \
    -e KAFKA_CLUSTERS_DESTINATION_TOPIC=destination-topic \
    emitskevich/kafka-reo
```

#### Docker compose
Replace env vars in `docker-compose.yml`, then run:
```
docker-compose up
```
Or firstly build it from sources:
```
./gradlew check installDist
docker-compose build
docker-compose up
```

#### Kubernetes
Replace env vars in `k8s-deployment.yml`, then run:
```
kubectl apply -f k8s-deployment.yml
```
