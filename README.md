brew services stop kafka
brew services stop zookeeper

zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties
kafka-server-start /opt/homebrew/etc/kafka/server.properties

kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic iplscores

python producer.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 consumer.py
