import json
from kafka import KafkaConsumer, TopicPartition
from pymongo import MongoClient
import logging
from mongo_pkg import *
from datetime import datetime
from pykafka import KafkaClient
import  sys

DOC_NAME = 'event_gap'
# IP = '10.40.195.153'
db = MongoDB()
collection = db.get_coll(DOC_NAME)
dop_sequence = 0
result = collection.find_one(sort=[("dop_sequence", -1)])
if result:
    dop_sequence = int(result["dop_sequence"]) +1

log_path = 'kafka-event-service_gap.log'

logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


start_time = str(datetime.strptime('2024-02-24 08:38:00', '%Y-%m-%d %H:%M:%S').timestamp() )
end_time = str(datetime.strptime('2024-02-24 14:00:00', '%Y-%m-%d %H:%M:%S').timestamp() )
client = KafkaClient(hosts='10.40.195.158:9092')
consumer = client.topics[b'EventTopic'].get_simple_consumer(consumer_group='my-group', reset_offset_on_start=True)

for message in consumer:
    try:
        json_data = json.loads(message.value.decode('utf-8'))
        if "@timestamp" in json_data:
            timestamp = json_data["@timestamp"]
            if end_time>= timestamp >= start_time:
                if "eventType" in json_data and json_data["eventType"] != 'START':
                    json_data["dop_sequence"] = dop_sequence
                    collection.insert_one(json_data)
                    dop_sequence += 1
                elif "eventType" not in json_data:
                    json_data["dop_sequence"] = dop_sequence
                    collection.insert_one(json_data)
                    dop_sequence += 1
    except json.decoder.JSONDecodeError as e:
        err = "Error decoding JSON:", e
        logging.info("Error decoding JSON:"+ str(e)+ message.value)
    except:
        print("Error in inserting")
        logging.info("Error in inserting" + message.value)

sys.exit()
