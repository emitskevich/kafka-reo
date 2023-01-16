######Docker run
```
docker run -e APPLICATION_NAME=replicator-1 -e KAFKA_CLUSTERS_SOURCE_BOOTSTRAP_SERVERS=source-kafka-cluster:9092 -e KAFKA_CLUSTERS_SOURCE_TOPIC=source-topic -e KAFKA_CLUSTERS_DESTINATION_BOOTSTRAP_SERVERS=destination-kafka-cluster:9092 -e KAFKA_CLUSTERS_DESTINATION_TOPIC=destination-topic kafka-replicator-exactly-once    
```
Or firstly build it from sources:
```
./gradlew check installDist
docker build . -t kafka-replicator-exactly-once --build-arg MODULE=replicator
docker run -e APPLICATION_NAME=replicator-1 -e KAFKA_CLUSTERS_SOURCE_BOOTSTRAP_SERVERS=source-kafka-cluster:9092 -e KAFKA_CLUSTERS_SOURCE_TOPIC=source-topic -e KAFKA_CLUSTERS_DESTINATION_BOOTSTRAP_SERVERS=destination-kafka-cluster:9092 -e KAFKA_CLUSTERS_DESTINATION_TOPIC=destination-topic kafka-replicator-exactly-once
```

######Docker compose
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
