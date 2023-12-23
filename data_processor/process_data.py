from process_data_rabbitmq import *

import pickle

with open('resources/data_transformer.pkl', 'rb') as file:
    data_transformer = pickle.load(file)

start_processing_requests(data_transformer)