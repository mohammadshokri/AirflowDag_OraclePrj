import json
from pykafka import KafkaClient
from kafka import KafkaConsumer
from pymongo import MongoClient
import logging
from mongo_pkg import *
import sys

DOC_NAME = 'event_gap'
IP = '10.40.195.153'
db = MongoDB()
collection = db.get_coll(DOC_NAME)

# Specify the starting timestamp to fill the gap from
starting_timestamp = < timestamp >

# Initialize consumer variable and set property for JSON decode
client = KafkaClient(hosts='10.40.195.158:9092')
consumer = client.topics[b'EventTopic'].get_simple_consumer(consumer_group='my-group', reset_offset_on_start=True)

# Read data from Kafka history
for message in consumer:
    try:
        json_data = json.loads(message.value.decode('utf-8'))
        if "@timestamp" in json_data:
            timestamp = json_data["@timestamp"]
            if timestamp >= starting_timestamp:
                collection.insert_one(json_data)
        else:
            collection.insert_one(json_data)
    except json.decoder.JSONDecodeError as e:
        err = "Error decoding JSON:", e
        logging.info("Error decoding JSON:" + str(e) + message.value)
    except:
        print("Error in inserting")
        logging.info("Error in inserting" + message.value)

sys.exit()
