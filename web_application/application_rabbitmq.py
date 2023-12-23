import uuid
import os
import time

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

message_ttl = 3000
request_time_limit_seconds = 30

# Process Data
pd_exchange = 'process_data'
pd_exchange_dlx = pd_exchange + '_dlx'

pd_queue = 'process_data_queue'
pd_queue_dlq = pd_queue + 'dlx'
pd_queue_arguments = {'x-message-ttl': message_ttl,
                      'x-dead-letter-exchange': pd_exchange_dlx,
                      'x-dead-letter-routing-key': pd_exchange_dlx}

# Models
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


class ProcessDataClient(object):

    def __init__(self, connection):
        self.connection = connection
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=pd_exchange, exchange_type=ExchangeType.fanout)
        self.channel.queue_declare(queue=pd_queue, arguments=pd_queue_arguments)
        self.channel.queue_bind(exchange=pd_exchange, queue=pd_queue)

        self.channel.exchange_declare(exchange=pd_exchange_dlx, exchange_type=ExchangeType.fanout)
        self.channel.queue_declare(queue=pd_queue_dlq)
        self.channel.queue_bind(exchange=pd_exchange_dlx, queue=pd_queue_dlq)

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(queue=self.callback_queue, on_message_callback=self.on_response, auto_ack=True)

        self.response = None
        self.correlation_id = None

    def on_response(self, ch, method, props, body):
        if self.correlation_id == props.correlation_id:
            self.response = body.decode()

    def call(self, body):
        self.response = None
        self.correlation_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='',
                                   routing_key=pd_queue,
                                   properties=BasicProperties(reply_to=self.callback_queue,
                                                                   correlation_id=self.correlation_id),
                                   body=body)
        self.connection.process_data_events(time_limit=request_time_limit_seconds)

        return self.response


class PredictModelsClient(object):

    def __init__(self, connection):
        self.connection = connection
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=pm_exchange, exchange_type=ExchangeType.fanout)
        self.channel.queue_declare(queue=pm_queue, arguments=pm_queue_arguments)
        self.channel.queue_bind(exchange=pm_exchange, queue=pm_queue)

        self.channel.exchange_declare(exchange=pm_exchange_dlx, exchange_type=ExchangeType.fanout)
        self.channel.queue_declare(queue=pm_queue_dlq)
        self.channel.queue_bind(exchange=pm_exchange_dlx, queue=pm_queue_dlq)

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(queue=self.callback_queue, on_message_callback=self.on_response, auto_ack=True)

        self.response = None
        self.correlation_id = None

    def on_response(self, ch, method, props, body):
        if self.correlation_id == props.correlation_id:
            self.response = body.decode()

    def call(self, body):
        self.response = None
        self.correlation_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='',
                                   routing_key=pm_queue,
                                   properties=BasicProperties(reply_to=self.callback_queue,
                                                                   correlation_id=self.correlation_id),
                                   body=body)
        self.connection.process_data_events(time_limit=request_time_limit_seconds)

        return self.response


def prepare_profile(profile_json: str) -> str:
    response = ''
    with RabbitMQConnection(host=host, port=port, username=username, password=password) as rmq_connection:
        pdc = ProcessDataClient(rmq_connection.connection)
        pdc.call(profile_json)
        response = pdc.response
    return response


def model_request(request_json: str) -> str:
    response = ''
    with RabbitMQConnection(host=host, port=port, username=username, password=password) as rmq_connection:
        pmc = PredictModelsClient(rmq_connection.connection)
        pmc.call(request_json)
        response = pmc.response
    return response
