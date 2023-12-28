from utilities import *
from application_rabbitmq import *

import json

from flask import Flask, render_template, request, redirect, url_for

app = Flask(__name__)
app.secret_key = 'your_secret_key'


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/credit_application/profile', methods=['GET', 'POST'])
def credit_application_profile():
    if request.method == 'POST':
        profile = Profile(request.form)
        model = map_prediction_model(request.form['model'])

        process_data_json = profile.get_dictionary()
        process_data_json.pop('first_name')
        process_data_json.pop('last_name')
        predict_json = json.loads(prepare_profile(json.dumps(process_data_json)))

        predict_json['request'] = 'predict'
        predict_json['model'] = model
        prediction_result = model_request(json.dumps(predict_json))
        print(prediction_result)

        result = json.loads(prediction_result)['result']

        db_profile = profile.get_dictionary()
        db_profile['prediction_result'] = result
        save_prediction(db_profile)

        return redirect(url_for('result', result=result))
    else:
        return render_template('credit_application/client_profile.html', home_ownership_enum=HomeOwnership,
                               loan_intent_enum=LoanIntent, loan_grade_enum=LoanGrade,
                               previous_default_enum=PreviousDefault, prediction_model_enum=PredictionModel)


@app.route('/credit_application/result')
def result():
    return render_template('credit_application/prediction_result.html',
                           result=request.args.get('result', 'Something went wrong...'))


@app.route('/credit_predictions')
def credit_predictions():
    credit_predictions = get_credit_predictions()
    # credit_predictions_table = credit_predictions.to_html(classes="entityTable")
    return render_template('/credit_predictions.html', table_columns=credit_predictions.columns,
                           table_rows=credit_predictions.values)


@app.route('/models_weights')
def models_weights():
    request_json = {'request': 'models_weights'}
    models_weights_json = json.loads(model_request(json.dumps(request_json)))
    print(models_weights_json)
    return render_template('/models_weights.html', models_weights_json=models_weights_json)


@app.route('/models_scores')
def models_scores():
    request_json = {'request': 'scores'}
    scores_json = json.loads(model_request(json.dumps(request_json)))
    print(scores_json)
    return render_template('/models_scores.html', scores_json=scores_json)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
