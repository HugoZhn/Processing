from process import process
from confluent_kafka import Consumer
from confluent_kafka import KafkaError
from elasticsearch import Elasticsearch
import sys

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

    Running = True

    while Running:
        msg = consumer.poll()

        if msg:
            if not msg.error():
                #res = es.index(index=index_name, doc_type='tweet', body=process(msg.value()))
                print("NTM")

            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
                Running = False

    consumer.close()  # On ferme le consumer
