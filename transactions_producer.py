from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))

while True:
    transaction = {
        "user_id": random.randint(1, 100),
        "transaction_timestamp_millis": int(time.time() * 1000),
        "amount": random.uniform(-1000, 1000),
        "currency": "USD",
        "counterpart_id": random.randint(1, 100)
    }
    producer.send('transactions', value=transaction)
    time.sleep(1)

