from enums import *
import pandas as pd

from application_rabbitmq import *
from database_connection import *


def map_prediction_model(prediction_model: str) -> str:
    model_mapping = {
        str(PredictionModel.LOGISTIC_REGRESSION.name): str(PredictionModel.LOGISTIC_REGRESSION.name).lower(),
        str(PredictionModel.DECISION_TREE.name): str(PredictionModel.DECISION_TREE.name).lower(),
        str(PredictionModel.RANDOM_FOREST.name): str(PredictionModel.RANDOM_FOREST.name).lower(),
        str(PredictionModel.GRADIENT_BOOSTING_MACHINES.name): str(
            PredictionModel.GRADIENT_BOOSTING_MACHINES.name).lower(),
    }
    return model_mapping.get(prediction_model, "Unknown model")


class Profile:

    def __init__(self, form):
        self.first_name = form['first_name']
        self.last_name = form['last_name']
        self.age = int(form['age'])
        self.month_income = int(form['month_income'])
        self.employment_length = int(form['employment_length'])
        self.home_ownership = form['home_ownership']
        self.loan_intent = form['loan_intent']
        self.loan_amount = int(form['loan_amount'])
        self.loan_interest_rate = int(form['loan_interest_rate'])
        self.previous_loans = int(form['previous_loans'])
        self.previous_default = form['previous_default']
        self.loan_grade = form['loan_grade']

    def get_dictionary(self):
        dictionary = {
            'first_name': self.first_name,
            'last_name': self.last_name,
            'age': self.age,
            'month_income': self.month_income,
            'employment_length': self.employment_length,
            'home_ownership': self.home_ownership,
            'loan_intent': self.loan_intent,
            'loan_amount': self.loan_amount,
            'loan_interest_rate': self.loan_interest_rate,
            'previous_loans': self.previous_loans,
            'previous_default': self.previous_default,
            'loan_grade': self.loan_grade
        }
        return dictionary


def get_credit_predictions() -> pd.DataFrame:
    with DatabaseConnection() as db_connection:
        return pd.read_sql(f'SELECT * FROM {DatabaseConnection.schema}.{DatabaseConnection.predictions_table};',
                           DatabaseConnection.engine)


def save_prediction(record: dict) -> int:
    record_df = pd.DataFrame([record])
    return record_df.to_sql(DatabaseConnection.predictions_table, DatabaseConnection.engine,
                            schema=DatabaseConnection.schema,
                            if_exists='append', index=False)
