[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![dockerhub-deploy](https://github.com/emitskevich/kafka-reo/workflows/dockerhub-deploy/badge.svg)](https://github.com/emitskevich/kafka-reo/actions/workflows/dockerhub-deploy.yml)

## What is it
**Kafka** **R**eplicator **E**xactly-**O**nce - the tool to replicate data between 
**different** Apache Kafka **clusters** with **exactly-once** delivery.

See [launch options](https://github.com/emitskevich/kafka-reo#launch-options) 
below for quick start.



## The problem

### Why we need replication between clusters
There are different needs, especially for multi-region systems:
1. Making a copy of data for disaster recovery purposes.
1. Gathering data from different regions to the central one for aggregation. 
1. Data sharing between organizations.
1. ...

There are more in [Confluent docs](https://docs.confluent.io/platform/current/multi-dc-deployments/cluster-linking/index.html#use-cases-and-architectures).

### What tools exist for cross-cluster replication
1. [MirrorMaker](https://github.com/apache/kafka/tree/trunk/connect/mirror) from Apache Kafka.
1. [Replicator](https://docs.confluent.io/platform/current/multi-dc-deployments/replicator/replicator-quickstart.html) from Confluent.
1. Simple self-made "consume-produce in the loop" application.

All these tools can provide only either at-most-once or at-least-once delivery. 

### What about exactly-once
Apache Kafka has [transactional API](https://www.confluent.io/blog/transactions-apache-kafka/), 
which can be used for exactly-once delivery. The fundamental idea is to commit consumer offset 
and producer records in a single transaction. Kafka Streams uses it to provide high-level 
abstraction and easy access to exactly-once benefits.
It [may be enabled literally without code change](https://www.confluent.io/blog/enabling-exactly-once-kafka-streams/), 
with one config option. 

This works only within the same Kafka cluster.

### What's not enough
Replication tools from the list above are not compatible with exactly-once delivery.
The reason is in such case consumer offsets and producer records live in different clusters. 
Apache Kafka can't wrap operations with different clusters in one transaction.
There is [Kafka Improvement Proposal](https://cwiki.apache.org/confluence/display/KAFKA/KIP-656%3A+MirrorMaker2+Exactly-once+Semantics) how to make it possible 
and [good reading about it](https://towardsdatascience.com/exactly-once-semantics-across-multiple-kafka-instances-is-possible-20bf900c29cf), 
but it exists from 2020 and nothing of it is implemented in 2023.



## The solution

### Theory
1. Replicate messages to destination cluster with at-least-once guarantee. 
Wrap the messages with some metadata and apply repartitioning.
1. Apply deduplication, unwrap and restore initial partitioning, 
using exactly-once delivery within the destination cluster.

### Design schema
![img.png](design-schema.png)
There is also `design-schema.drawio` in the project root.



## Launch options

### Docker run
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

### Docker compose
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

### Kubernetes
Replace env vars in `k8s-deployment.yml`, then run:
```
kubectl apply -f k8s-deployment.yml
```



## Best practices
Launch this app as close to destination cluster as possible. It will make notable 
performance boost, since the step of deduplication uses transactional API 
of destination cluster and is therefore latency-sensible process.
