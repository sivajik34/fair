#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
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


# A simple example demonstrating use of AvroSerializer.
import os
from uuid import uuid4
from faker import Faker
import random
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


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

    def __init__(self, user_id, transaction_timestamp_millis, amount, currency, counterpart_id):
        self.user_id = user_id
        self.transaction_timestamp_millis = transaction_timestamp_millis
        self.amount = amount
        self.currency = currency
        self.counterpart_id = counterpart_id


def transaction_to_dict(transaction, ctx):
    """
    Returns a dict representation of a Transaction instance for serialization.

    Args:
        transaction (Transaction): Transaction instance.
        ctx (SerializationContext): Metadata pertaining to the serialization operation.

    Returns:
        dict: Dict populated with transaction attributes to be serialized.
    """
    return dict(
        user_id=transaction.user_id,
        transaction_timestamp_millis=transaction.transaction_timestamp_millis,
        amount=transaction.amount,
        currency=transaction.currency,
        counterpart_id=transaction.counterpart_id
    )


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """
    if err is not None:
        print("Delivery failed for Transaction record {}: {}".format(msg.key(), err))
        return
    print('Transaction record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def generate_fake_transaction(fake):
    """
    Generate fake transaction data using Faker library.

    Args:
        fake (Faker): Faker instance.

    Returns:
        Transaction: Fake transaction object.
    """
    user_id = fake.random_int(min=1, max=1000)
    transaction_timestamp_millis = fake.date_time_between(start_date='-1y', end_date='now').timestamp() * 1000
    amount = round(random.uniform(-1000, 1000), 2)
    currency = fake.currency_code()
    counterpart_id = fake.random_int(min=1, max=1000)
    return Transaction(user_id, transaction_timestamp_millis, amount, currency, counterpart_id)


def main():
    topic = "transactions"
    is_specific = "true"

    if is_specific:
        schema = "transaction_specific.avsc"
    else:
        schema = "transaction_generic.avsc"

    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/avro/{schema}") as f:
        schema_str = f.read()

    schema_registry_conf = {'url': 'http://localhost:8081'}  # Change the URL according to your environment
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str,
                                     transaction_to_dict)

    string_serializer = StringSerializer('utf_8')

    producer_conf = {'bootstrap.servers': 'localhost:9092'}  # Change the bootstrap servers according to your environment

    producer = Producer(producer_conf)

    fake = Faker()

    print("Producing fake transaction records to topic {}. ^C to exit.".format(topic))
    try:
        while True:
            # Serve on_delivery callbacks from previous calls to produce()
            producer.poll(0.0)
            transaction = generate_fake_transaction(fake)
            producer.produce(topic=topic,
                             key=string_serializer(str(uuid4())),
                             value=avro_serializer(transaction, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)
    except KeyboardInterrupt:
        pass

    print("\nFlushing records...")
    producer.flush()


if __name__ == '__main__':
    main()

