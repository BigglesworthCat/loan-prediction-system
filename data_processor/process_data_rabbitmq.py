import json
import os
import time

import pandas as pd
from dotenv import load_dotenv
from pika import PlainCredentials, ConnectionParameters, BlockingConnection, exceptions, BasicProperties
from pika.exchange_type import *


load_dotenv()

host = os.getenv("RABBITMQ_HOST")
port = int(os.getenv('RABBITMQ_AMQP_PORT'))
username = os.getenv('RABBITMQ_USER')
password = os.getenv('RABBITMQ_PASSWORD')

attempt_to_connect = 10
attempts_timeout = 5

pd_exchange = 'process_data'
pd_exchange_dlx = pd_exchange + '_dlx'

pd_queue = 'process_data_queue'
pd_queue_dlq = pd_queue + '_dlx'
message_ttl = 3000
pd_queue_arguments = {'x-message-ttl': message_ttl,
                      'x-dead-letter-exchange': pd_exchange_dlx,
                      'x-dead-letter-routing-key': pd_exchange_dlx}

request_time_limit_seconds = 30


class RabbitMQConnection:
    _instance = None

    def __new__(cls, host="localhost", port=5672, username="admin", password="admin"):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, host="localhost", port=5672, username="admin", password="admin"):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.connection = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        retries = 0
        while retries < 10:
            try:
                credentials = PlainCredentials(self.username, self.password)
                parameters = ConnectionParameters(host=self.host, port=self.port, credentials=credentials)
                self.connection = BlockingConnection(parameters)
                print("Connected to RabbitMQ")
                return
            except exceptions.AMQPConnectionError as e:
                print("Failed to connect to RabbitMQ:", e)
                retries += 1
                wait_time = attempts_timeout
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)

        print("Exceeded maximum number of connection retries. Stopping the code.")

    def is_connected(self):
        return self.connection is not None and self.connection.is_open

    def close(self):
        if self.is_connected():
            self.connection.close()
            self.connection = None
            print("Closed RabbitMQ connection")

    def get_channel(self):
        if self.is_connected():
            return self.connection.channel()

        return None


def start_processing_requests(data_transformer):
    def process_profile_json(profile_json):
        profile = pd.read_json(profile_json, orient='index').T
        transformed_profile = data_transformer.transform(profile, is_dataset=False)
        return json.dumps(transformed_profile.to_dict(orient='records')[0])

    def on_request(ch, method, props, body):
        print('Received:', body.decode())
        response = process_profile_json(body.decode())
        print('Sent:', response, '\n')

        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=BasicProperties(correlation_id=props.correlation_id),
                         body=response)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    with RabbitMQConnection(host=host, port=port, username=username, password=password) as connection:
        channel = connection.get_channel()

        channel.exchange_declare(exchange=pd_exchange, exchange_type=ExchangeType.fanout)
        channel.queue_declare(queue=pd_queue, arguments=pd_queue_arguments)
        channel.queue_bind(exchange=pd_exchange, queue=pd_queue)

        channel.exchange_declare(exchange=pd_exchange_dlx, exchange_type=ExchangeType.fanout)
        channel.queue_declare(queue=pd_queue_dlq)
        channel.queue_bind(exchange=pd_exchange_dlx, queue=pd_queue_dlq)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=pd_queue, on_message_callback=on_request)

        print('Waiting for requests...')
        channel.start_consuming()
