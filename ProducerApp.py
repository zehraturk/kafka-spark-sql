import json

import ShoppingBasket
import time
from kafka import KafkaProducer

class ProducerApp:

    bootstrap_servers = ['localhost:9092']
    topicName = 'test-6'
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, retries=5,
                             value_serializer=lambda m: json.dumps(m).encode('ascii'))


    while True:
            producer.send(topicName,ShoppingBasket.RandomShoppingBasket())
            time.sleep(1)
            print("ok.")
