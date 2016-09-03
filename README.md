# Logs Analzyer

To compile this code, use maven:
```
% mvn package
```

Kafka basics:
```
/usr/local/Cellar/kafka/0.10.0.0/bin [ master*]$ zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties
/usr/local/Cellar/kafka/0.10.0.0/bin [ master*]$ kafka-server-start /usr/local/etc/kafka/server.properties

/usr/local/Cellar/kafka/0.10.0.0/bin [ master*]$ kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
Created topic "test".

/usr/local/Cellar/kafka/0.10.0.0/bin [ master*]$ kafka-console-producer --broker-list localhost:9092 --topic test
/usr/local/Cellar/kafka/0.10.0.0/bin [ master*]$ kafka-console-consumer --zookeeper localhost:2181 --topic test --from-beginning

To run the program, you can use spark-submit program:
```
Run SparkStreamingKafkaLogGenerator

Run SparkStreamingKafkaLogAnalyzer

```