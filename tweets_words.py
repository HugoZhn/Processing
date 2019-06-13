import sys
from confluent_kafka import Consumer
from confluent_kafka import KafkaError
from elasticsearch import Elasticsearch
import json
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
import datetime


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
    custom_stopwords = ["https", "co", "rt", "get", "let", "amazon", "amazonhelp", "via", "amp", "us", "000", "one",
                        "hi", "6280346856", "vv", "vie01lj9nj", "ni"]
    for i in range(2000):
        custom_stopwords.append(str(i))

    tokenizer = RegexpTokenizer(r'\w+')

    Running = True

    while Running:
        msg = consumer.poll()

        if msg:
            if not msg.error():
                data = json.loads(msg.value())
                if (topic_name == "tweets_amazon_global") or (data["user"]["id"] != 85741735):
                    text = data["extended_tweet"]["full_text"] if data['truncated'] else data["text"]
                    tokenized = tokenizer.tokenize(text)
                    words = [word.lower() for word in tokenized if word.lower() not in stopwords]
                    words = [word for word in words if word not in custom_stopwords]
                    for word in words:
                        res = es.index(index=index_name, doc_type='word', body={"word": word, "timestamp":
                            int(datetime.datetime.strptime(data["created_at"], "%a %b %d %H:%M:%S %z %Y").timestamp()*1000)})

            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
                Running = False

    consumer.close()  # On ferme le consumer
