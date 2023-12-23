from sklearn.preprocessing import StandardScaler
from category_encoders import TargetEncoder

import pandas as pd


class DatasetMetadata:
    def __init__(self):
        self.features = ['age', 'month_income', 'employment_length', 'home_ownership', 'loan_intent',
                               'loan_amount', 'loan_interest_rate', 'previous_loans', 'previous_default', 'loan_grade', 'loan_status']
        self.target_feature = 'loan_status'
        self.input_features = [f for f in self.features if f != self.target_feature]
        self.replace_dict = {
            'previous_default': {'YES': 1, 'NO': 0},
            'home_ownership': {'OTHER': 0, 'RENT': 0, 'MORTGAGE': 1, 'OWN': 2},
            'loan_grade': {'G': 0, 'F': 1, 'E': 2, 'D': 3, 'C': 4, 'B': 5, 'A': 6}
        }
        self.features_to_encode = ['loan_intent']


class DataTransformer:
    def __init__(self, dataset_metadata):
        self.ds = dataset_metadata

        self.target_encoder = TargetEncoder(cols=self.ds.features_to_encode)
        self.standard_scaler = StandardScaler()

    def transform(self, X, is_dataset=True):
        X_copy = X.copy()

        for column, mapping in self.ds.replace_dict.items():
            X_copy[column] = X_copy[column].replace(mapping)

        if is_dataset:
            self.target_encoder.fit(X_copy[self.ds.input_features], X_copy[self.ds.target_feature])
            X_copy[self.ds.input_features] = self.target_encoder.transform(X_copy[self.ds.input_features])

            self.standard_scaler.fit(X_copy[self.ds.input_features])
            X_copy[self.ds.input_features] = self.standard_scaler.transform(X_copy[self.ds.input_features])
        else:
            X_copy = self.target_encoder.transform(X_copy)
            X_copy[self.ds.input_features] = self.standard_scaler.transform(X_copy)

        return X_copy
