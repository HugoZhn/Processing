from process import process
from confluent_kafka import Consumer
from confluent_kafka import KafkaError
from elasticsearch import Elasticsearch
import sys
import time

if __name__ == "__main__":

    topic_name = sys.argv[1]
    consumer_group_id = sys.argv[2]
    index_name = sys.argv[3]

    print("topic_name : ", sys.argv[1])
    print("consumer_group_id : ", sys.argv[2])
    print("index_name : ", sys.argv[3])

    es = Elasticsearch(hosts="localhost:9200")

    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': consumer_group_id,
        'default.topic.config': {'auto.offset.reset': 'earliest',
                                 'enable.auto.commit': True}
    })

    consumer.subscribe([topic_name, ])

    msgs = []
    nb_messages = 0
    Running = True

    while Running:
        msg = consumer.poll(-1)

        if msg:
            if not msg.error():
                res = es.index(index=index_name, doc_type='_doc', body=process(msg.value()))
                print(res['result'])
                print("WAITING")
                time.sleep(5)

            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
                Running = False

        Running = False
    consumer.close()  # On ferme le consumer