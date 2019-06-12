from TweetProcessor import TweetProcessor
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

    if topic_name == "tweets_amazon_help":
        processor = TweetProcessor(response_time=True)
    else:
        processor = TweetProcessor()

    es = Elasticsearch(hosts="localhost:9200")

    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': consumer_group_id,
        'default.topic.config': {'auto.offset.reset': 'smallest',
                                 'enable.auto.commit': True}
    })

    consumer.subscribe([topic_name, ])

    Running = True

    while Running:
        msg = consumer.poll()

        if msg:
            if not msg.error():
                res = es.index(index=index_name, doc_type='tweet', body=processor.process_tweet(msg.value()))
                print(res['result'])

            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
                Running = False

    consumer.close()  # On ferme le consumer
