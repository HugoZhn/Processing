from process import process
from confluent_kafka import Consumer
from confluent_kafka import KafkaError
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import sys

def set_data(topic_name, consumer_group_id, index_name):
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': consumer_group_id,
        'default.topic.config': {'auto.offset.reset': 'earliest',
                                 'enable.auto.commit': True}
    })

    consumer.subscribe([topic_name, ])

    running = True

    while running:

        message = consumer.poll()

        if not msg.error():
            to_send = process(message.value())

            yield {
                "_index": index_name,
                "_type": "tweet",
                "_source": to_send
            }

        elif message.error().code() != KafkaError._PARTITION_EOF:
            print(message.error())
            running = False

    consumer.close()


if __name__ == "__main__":

    topic_name = sys.argv[1]
    consumer_group_id = sys.argv[2]
    index_name = sys.argv[3]

    print("topic_name : ", sys.argv[1])
    print("consumer_group_id : ", sys.argv[2])
    print("index_name : ", sys.argv[3])

    es = Elasticsearch(hosts="localhost:9200")
    success, _ = bulk(es, set_data(topic_name, consumer_group_id, index_name))
