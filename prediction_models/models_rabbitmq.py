import json
import os
import time
from dotenv import load_dotenv

from pika import PlainCredentials, ConnectionParameters, BlockingConnection, exceptions, BasicProperties
from pika.exchange_type import *

from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier

import pandas as pd

load_dotenv()

host = os.getenv("RABBITMQ_HOST")
port = int(os.getenv('RABBITMQ_AMQP_PORT'))
username = os.getenv('RABBITMQ_USER')
password = os.getenv('RABBITMQ_PASSWORD')

attempt_to_connect = 10
attempts_timeout = 5

message_ttl = 3000
request_time_limit_seconds = 30

pm_exchange = 'prediction_models'
pm_exchange_dlx = pm_exchange + '_dlx'

pm_queue = 'prediction_models_queue'
pm_queue_dlq = pm_queue + '_dlx'

pm_queue_arguments = {'x-message-ttl': message_ttl,
                      'x-dead-letter-exchange': pm_exchange_dlx,
                      'x-dead-letter-routing-key': pm_exchange_dlx}


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
        while retries < attempt_to_connect:
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

def start_processing_requests(models_dict, scores_json):
    def process_request_json(request_json):
        request = json.loads(request_json)
        request_variant = request.pop('request')
        if request_variant == 'predict':
            model = request.pop('model')
            profile = pd.DataFrame([request])

            model_result = models_dict[model].predict(profile)

            if model_result == 1:
                response_json = json.dumps({"result": "Accepted"})
            else:
                response_json = json.dumps({"result": "Denied"})
        elif request_variant == 'scores':
            response_json = scores_json
        return response_json

    def on_request(ch, method, props, body):
        print('Received:', body.decode())
        response = process_request_json(body.decode())
        print('Sent:', response, '\n')

        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=BasicProperties(correlation_id=props.correlation_id),
                         body=response)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    with RabbitMQConnection(host=host, port=port, username=username, password=password) as connection:
        channel = connection.get_channel()

        channel.exchange_declare(exchange=pm_exchange, exchange_type=ExchangeType.fanout)
        channel.queue_declare(queue=pm_queue, arguments=pm_queue_arguments)
        channel.queue_bind(exchange=pm_exchange, queue=pm_queue)

        channel.exchange_declare(exchange=pm_exchange_dlx, exchange_type=ExchangeType.fanout)
        channel.queue_declare(queue=pm_queue_dlq)
        channel.queue_bind(exchange=pm_exchange_dlx, queue=pm_queue_dlq)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=pm_queue, on_message_callback=on_request)

        print('Waiting for requests...')
        channel.start_consuming()
