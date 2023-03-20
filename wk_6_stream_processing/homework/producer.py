from kafka import KafkaProducer
import csv

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Read green trip data file and publish to Kafka
with open('green_tripdata_2019-01.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        message = row  # modify this to serialize data as needed
        producer.send('rides_green', message)

# Read fhv trip data file and publish to Kafka
with open('fhv_tripdata_2019-01.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        message = row  # modify this to serialize data as needed
        producer.send('rides_fhv', message)
