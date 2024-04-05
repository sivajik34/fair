from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('transactions',
                         bootstrap_servers=['localhost:9092'],
                         group_id='transactions-backup-group')

for message in consumer:
    with open('transactions_backup.txt', 'a') as f:
        f.write(json.dumps(json.loads(message.value)) + '\n')

