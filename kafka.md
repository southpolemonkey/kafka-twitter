# Kafka

get some idea of volumn of data
spotify 700K events per second, future 2M /sec


## Reading

[spotify migrate to pubsub](https://labs.spotify.com/2016/03/10/spotifys-event-delivery-the-road-to-the-cloud-part-iii/)


## Basics

kafka installation

> To have launchd start kafka now and restart at login:
  brew services start kafka
Or, if you don't want/need a background service you can just run:
  zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties & kafka-server-start /usr/local/etc/kafka/server.properties


where does homebrew store kafka configurations: `/usr/local/etc/kafka`

zookeeper default port `2181`

kafka default port

broker default port `9092`

change `advertisted.listen` in server.properties to access remote machine

## Useful commands

```bash
kafka-topics --create --zookeeper 127.0.0.1:2181 --replication-factor 1 --partitions 1 --topic <topic_name>
kafka-topics --list --bootstrap-server PLAINTEXT://127.0.0.1:9092
```

## Java basics

```bash
mvn clean package
java -jar <path_to_jar>

```
## Best practice

create topics before produce data to them.

consumer flags

-- group

-- from-beginin

maven - kafka-client


## kafka stream

[kafka-streams-course-code-repo](https://github.com/simplesteph/kafka-streams-course/tree/2.0.0)

`application.id` important config for stream application, used as `Consumer.group.id = application.id`

some concepts:
- topology
- internal topics

## kafka connect