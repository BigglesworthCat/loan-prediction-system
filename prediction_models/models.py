from models_rabbitmq import *

import os
import joblib

models_path = os.getcwd() + '/models'

scores_path = models_path + '/scores.json'

ending = '.joblib'

models_dict = {}
scores_json = ''

for file_name in os.listdir(models_path):
    if file_name.endswith(ending):
        file_path = os.path.join(models_path, file_name)

        loaded_model = joblib.load(file_path)

        key_without_extension = os.path.splitext(file_name)[0]
        models_dict[key_without_extension] = loaded_model

    with open(scores_path, 'r') as file:
        scores_json = file.read()

start_processing_requests(models_dict, scores_json)
