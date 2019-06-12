import sys
from confluent_kafka import Consumer
from confluent_kafka import KafkaError
from elasticsearch import Elasticsearch
import json
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords

if __name__ == "__main__":
    topic_name = sys.argv[1]
    consumer_group_id = sys.argv[2]
    index_name = sys.argv[3]

    es = Elasticsearch(hosts="localhost:9200")

    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': consumer_group_id,
        'default.topic.config': {'auto.offset.reset': 'smallest',
                                 'enable.auto.commit': True}
    })

    consumer.subscribe([topic_name, ])

    stopwords = stopwords.words('english')
    tokenizer = RegexpTokenizer(r'\w+')

    Running = True

    while Running:
        msg = consumer.poll()

        if msg:
            if not msg.error():
                data = json.loads(msg.value())
                text = data["extended_tweet"]["full_text"] if data['truncated'] else data["text"]
                tokenized = tokenizer.tokenize(text)
                words = [word for word in tokenized if word not in stopwords]
                for word in words:
                    res = es.index(index=index_name, doc_type='tweet', body={"word": word})
                    print(res['result'])

            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
                Running = False

    consumer.close()  # On ferme le consumer