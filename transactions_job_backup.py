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
from confluent_kafka import Consumer
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
    consumer_group = "transactions-job-backup-group"
    output_file = "transactions_backup.parquet"

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

    try:
        # Create an empty DataFrame
        df = pd.DataFrame(columns=['user_id', 'transaction_timestamp_millis', 'amount', 'currency', 'counterpart_id'])

        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            transaction = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            if transaction is not None:
                #print("There is a new transaction")
                # Create a DataFrame with the current transaction
                transaction_df = pd.DataFrame({
                    'user_id': [transaction.user_id],
                    'transaction_timestamp_millis': [transaction.transaction_timestamp_millis],
                    'amount': [transaction.amount],
                    'currency': [transaction.currency],
                    'counterpart_id': [transaction.counterpart_id]
                })

                if not df.empty and not transaction_df.empty:
                    df = pd.concat([df, transaction_df], ignore_index=True)
                elif not transaction_df.empty:
                    df = transaction_df.copy()

                # Check if DataFrame exceeds a certain size, and if so, write it to Parquet file
                if df.memory_usage(deep=True).sum() > 1024 * 1024:  # Example threshold: 1MB
                    if not os.path.exists(output_file):
                        df.to_parquet(output_file, index=False, compression='snappy')
                    else:
                        existing_df = pd.read_parquet(output_file)
                        df = pd.concat([existing_df, df], ignore_index=True)
                        df.to_parquet(output_file, index=False, compression='snappy')
                    df = pd.DataFrame(columns=['user_id', 'transaction_timestamp_millis', 'amount', 'currency', 'counterpart_id'])

    except KeyboardInterrupt:
        pass

    finally:
        # Write remaining DataFrame to Parquet file before exiting
        if not df.empty:
            if not os.path.exists(output_file):
                df.to_parquet(output_file, index=False, compression='snappy')
            else:
                existing_df = pd.read_parquet(output_file)
                df = pd.concat([existing_df, df], ignore_index=True)
                df.to_parquet(output_file, index=False, compression='snappy')

        consumer.close()


if __name__ == '__main__':
    main()

