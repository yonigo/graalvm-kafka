# graalvm-kafka
GraalVM (Java - Node) Based Kafka client

## Overview 

This repo contains a GraalVm based node.js application using a native Java Kafka client
All services should be run using docker-compose.

## Instructions

### Compile Java client

mvn package
(This will create a target folder containing an Uber-Jar for your Java based kafka client)

### Run node app 

docker-compose run graalvm bash
(This will download and start all required kafka dependencies - kafka broker & zookeeper, and put you inside the container )

node --polyglot --jvm --jvm.cp=code/target/uber-kafka-client-1.0.jar --experimental-worker code/node/services/kafka-user/index.js 
(This will set the appropriate classpath to the jvm, and then run your app)

### Run localy 

1. Install graalvm on your local machine (https://www.graalvm.org/docs/getting-started/ )
2. Update kafka brokers-list to docker exposed local ports (localhost:9092)
3. Run node/services/kafka-user/index with your favorite sdk (adding --polyglot --jvm --jvm.cp=code/target/uber-kafka-client-1.0.jar --experimental-worker to run command )
