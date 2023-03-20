from kafka import KafkaProducer
from pyspark.streaming.kafka import KafkaUtils

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

import json

# Set up Spark and Streaming contexts
sc = SparkContext(appName='PUlocationID popularity')
ssc = StreamingContext(sc, 5)  # batch interval of 5 seconds

# Set up Kafka input streams
kafkaParams = {'metadata.broker.list': 'localhost:9092'}
rides_green_stream = KafkaUtils.createDirectStream(ssc, ['rides_green'], kafkaParams)
rides_fhv_stream = KafkaUtils.createDirectStream(ssc, ['rides_fhv'], kafkaParams)

# Combine the streams and extract pickup location IDs
rides_stream = rides_green_stream.union(rides_fhv_stream).map(lambda x: json.loads(x[1]))
PUlocations = rides_stream.map(lambda x: (x['PUlocationID'], 1))

# Apply aggregations to find most popular pickup location
popularity = PUlocations.reduceByKey(lambda x, y: x + y)
most_popular = popularity.transform(lambda rdd: rdd.sortBy(lambda x: x[1], False)).map(lambda x: x[0])

# Write the results to the rides_all topic
most_popular.foreachRDD(lambda rdd: rdd.foreachPartition(lambda partition: publish_to_kafka(partition, 'rides_all')))

# Helper function to publish messages to Kafka


def publish_to_kafka(partition, topic):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    for message in partition:
        producer.send(topic, message)


# Start the streaming context
ssc.start()
ssc.awaitTermination()
