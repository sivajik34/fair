#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2020 Confluent Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import argparse
import pandas as pd
import redis
from collections import defaultdict
from confluent_kafka import Consumer, Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer


class Transaction(object):
    """
    Transaction record

    Args:
        user_id (int): The id of the user making the transaction
        transaction_timestamp_millis (int): The timestamp, in milliseconds since epoch, of the transaction
        amount (float): The amount of the transaction. Negative is a debit, positive is a credit.
        currency (str): A 3 letters symbol indicating the currency of the transaction
        counterpart_id (int): The id of the counterpart of this transaction
    """

    def __init__(self, user_id=None, transaction_timestamp_millis=None, amount=None, currency=None, counterpart_id=None):
        self.user_id = user_id
        self.transaction_timestamp_millis = transaction_timestamp_millis
        self.amount = amount
        self.currency = currency
        self.counterpart_id = counterpart_id


def dict_to_transaction(obj, ctx):
    """
    Converts object literal(dict) to a Transaction instance.

    Args:
        obj (dict): Object literal(dict)
        ctx (SerializationContext): Metadata pertaining to the serialization operation.
    """

    if obj is None:
        return None

    return Transaction(user_id=obj['user_id'],
                       transaction_timestamp_millis=obj['transaction_timestamp_millis'],
                       amount=obj['amount'],
                       currency=obj['currency'],
                       counterpart_id=obj['counterpart_id'])


def main():
    topic = "transactions"
    schema_registry_url = "http://localhost:8081"
    consumer_group = "transactions-ml-features-job-group"
    output_file = "transactions_ml_features.parquet"
    redis_host = "localhost"
    redis_port = 6379

    path = os.path.realpath(os.path.dirname(__file__))
    schema_file = "transaction_specific.avsc"  # Adjust if needed

    with open(f"{path}/avro/{schema_file}") as f:
        schema_str = f.read()

    sr_conf = {'url': schema_registry_url}
    schema_registry_client = SchemaRegistryClient(sr_conf)

    avro_deserializer = AvroDeserializer(schema_registry_client,
                                         schema_str,
                                         dict_to_transaction)

    consumer_conf = {'bootstrap.servers': 'localhost:9092',
                     'group.id': consumer_group,
                     'auto.offset.reset': "earliest"}

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    transactions_count = defaultdict(int)

    # Connect to Redis
    r = redis.Redis(host=redis_host, port=redis_port, db=0)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            transaction = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            if transaction is not None:
                # Count transactions per user
                transactions_count[transaction.user_id] += 1

                # Output the results to file based storage
                # For now, let's just print the count
                print("User ID:", transaction.user_id, "Total Transactions Count:", transactions_count[transaction.user_id])

                # Output the results to Redis
                r.set(transaction.user_id, transactions_count[transaction.user_id])

    except KeyboardInterrupt:
        pass

    finally:
        # Write the results to file based storage in Parquet format
        df = pd.DataFrame.from_dict(transactions_count, orient='index', columns=['total_transactions_count'])
        df.index.name = 'user_id'
        df.reset_index(inplace=True)
        df.to_parquet(output_file, index=False)

        consumer.close()


if __name__ == '__main__':
    main()

