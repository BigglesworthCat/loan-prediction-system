from data_transformator import *
from database_connection import *
from process_data_rabbitmq import *


class DatasetMetadata:
    def __init__(self):
        self.target_feature = 'loan_status'
        self.input_features = ['age', 'month_income', 'employment_length', 'home_ownership', 'loan_intent',
                               'loan_amount', 'loan_interest_rate', 'previous_loans', 'previous_default', 'loan_grade']
        self.replace_dict = {
            'previous_default': {'YES': 1, 'NO': 0},
            'home_ownership': {'OTHER': 0, 'RENT': 0, 'MORTGAGE': 1, 'OWN': 2},
            'loan_grade': {'G': 0, 'F': 1, 'E': 2, 'D': 3, 'C': 4, 'B': 5, 'A': 6}
        }
        self.features_to_encode = ['loan_intent']


dt = DataTransformer(DatasetMetadata())

with DatabaseConnection() as db_connection:
    df = pd.read_sql(f'SELECT * FROM {DatabaseConnection.schema}.{DatabaseConnection.original_table};',
                     DatabaseConnection.engine)

    df_transformed = dt.transform(df)

    result = df_transformed.to_sql(DatabaseConnection.transformed_table, DatabaseConnection.engine,
                                   schema=DatabaseConnection.schema,
                                   if_exists='replace', index=False)

start_processing_requests(dt)