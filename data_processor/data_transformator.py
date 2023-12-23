from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import StandardScaler
from category_encoders import TargetEncoder

import pandas as pd


class MyTargetEncoder(BaseEstimator, TransformerMixin):
    def __init__(self, features_to_encode):
        self.target_encoder = TargetEncoder()
        self.features_to_encode = features_to_encode

    def fit(self, X, y):
        self.target_encoder.fit(X[self.features_to_encode], y)
        return self

    def transform(self, X):
        encoded_features = self.target_encoder.transform(X[self.features_to_encode])
        X = pd.concat([X.drop(self.features_to_encode, axis=1), encoded_features], axis=1)
        return X


class DataTransformer:
    def __init__(self, dataset_metadata):
        self.ds = dataset_metadata

        self.target_encoder = TargetEncoder(self.ds.features_to_encode)
        self.standard_scaler = StandardScaler()

    def transform(self, X, is_dataset=True):
        for column, mapping in self.ds.replace_dict.items():
            X[column] = X[column].replace(mapping)

        if is_dataset:
            self.target_encoder.fit(X[self.ds.input_features], X[self.ds.target_feature])
            X[self.ds.input_features] = self.target_encoder.transform(X[self.ds.input_features])

            self.standard_scaler.fit(X[self.ds.input_features])
            X[self.ds.input_features] = self.standard_scaler.transform(X[self.ds.input_features])
        else:
            X = self.target_encoder.transform(X)
            X[self.ds.input_features] = self.standard_scaler.transform(X)
        return X


def process_profile_json(profile_json):
    profile = pd.read_json(profile_json, orient='index').T
