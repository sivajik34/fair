from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('transactions',
                         bootstrap_servers=['localhost:9092'],
                         group_id='transactions-ml-features-group')

user_transactions = {}

for message in consumer:
    transaction = json.loads(message.value)
    user_id = transaction["user_id"]
    if user_id in user_transactions:
        user_transactions[user_id] += 1
    else:
        user_transactions[user_id] = 1
    # Output results to file
    with open('transactions_ml_features.txt', 'a') as f:
        f.write(json.dumps({
            "user_id": user_id,
            "total_transactions_count": user_transactions[user_id]
        }) + '\n')
    # Output results to key-value store (e.g., Redis) for real-time ML predictions
    # (Code not provided, you need to implement this part)

