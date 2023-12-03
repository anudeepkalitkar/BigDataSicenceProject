import numpy as np
from tqdm import tqdm
from itertools import combinations


from xgboost import XGBClassifier
from lightgbm import LGBMClassifier
from catboost import CatBoostClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.base import BaseEstimator, ClassifierMixin

from keras.models import Sequential
from keras.layers import Conv1D, LSTM, Dense, Flatten


def CreateEnsembleCombinations(MLModelsNames: list, minimumModels: int):
    EnsembleCombinations = []
    for length in tqdm(range(minimumModels, len(MLModelsNames) + 1)):
        for combo in combinations(MLModelsNames, length):
            EnsembleCombinations.append(list(combo))

    return EnsembleCombinations


def CreateXGBClassifier(
    parameters: dict = {"tree_method": "hist", "device": "cpu", "verbosity": 1}
):
    XGBModel = XGBClassifier(**parameters)
    return XGBModel


def CreateLGBClassifier(parameters: dict = {"device": "gpu", "verbosity": 1}):
    LGBModel = LGBMClassifier(**parameters)
    return LGBModel


def CreateCBClassifier(
    parameters: dict = {
        "task_type": "GPU",
        "devices": "0:1",
        "verbose": 1,
        "iterations": 100,
    }
):
    CBModel = CatBoostClassifier(**parameters)
    return CBModel


def CreateLRClassifier(
    parameters: dict = {
        "n_jobs": -1,
    }
):
    LRModel = LogisticRegression(**parameters)
    return LRModel


class CNNClassifier(BaseEstimator, ClassifierMixin):
    def __init__(
        self,
        inputShape,
        numClasses,
        epochs,
        batchSize,
        lossFunction,
        optimizer,
        metrics,
        verbose,
        callbacks,
    ):
        self.verbose = verbose
        self.lossFunction = lossFunction
        self.optimizer = optimizer
        self.metrics = metrics
        self.inputShape = inputShape
        self.numClasses = numClasses
        self.epochs = epochs
        self.batchSize = batchSize
        self.callbacks = callbacks
        self.classes_ = np.arange(self.numClasses)
        self.model = self.CreateCNNModel()

    def fit(self, X: np.ndarray, y: np.ndarray):
        X = X.reshape((X.shape[0], self.inputShape[0], self.inputShape[1]))

        self.model.fit(
            X,
            y,
            epochs=self.epochs,
            batch_size=self.batchSize,
            verbose=self.verbose,
            # callbacks=self.callbacks,
            # validation_split=0.1,
        )
        return self

    def predict(self, X: np.ndarray):
        X = X.reshape((X.shape[0], self.inputShape[0], self.inputShape[1]))

        predictions = self.model.predict(X)
        return (predictions > 0.5).astype("int32")

    def predict_proba(self, X: np.ndarray):
        return self.model.predict(X)

    def CreateCNNModel(self):
        model = Sequential()
        model.add(
            Conv1D(
                filters=64,
                kernel_size=3,
                activation="relu",
                input_shape=self.inputShape,
            )
        )
        model.add(Flatten())
        model.add(Dense(50, activation="relu"))
        model.add(Dense(self.numClasses, activation="sigmoid"))
        model.compile(
            loss=self.lossFunction,
            optimizer=self.optimizer,
            metrics=self.metrics,
        )
        return model


class LSTMClassifier(BaseEstimator, ClassifierMixin):
    def __init__(
        self,
        inputShape,
        numClasses,
        epochs,
        batchSize,
        lossFunction,
        optimizer,
        metrics,
        verbose,
        callbacks,
    ):
        self.verbose = verbose
        self.lossFunction = lossFunction
        self.optimizer = optimizer
        self.metrics = metrics
        self.inputShape = inputShape
        self.numClasses = numClasses
        self.epochs = epochs
        self.batchSize = batchSize
        self.callbacks = callbacks
        self.classes_ = np.arange(self.numClasses)
        self.model = self.CreateLSTMModel()

    def fit(self, X: np.ndarray, y: np.ndarray):
        X = X.reshape((X.shape[0], self.inputShape[0], self.inputShape[1]))
        self.model.fit(
            X,
            y,
            epochs=self.epochs,
            batch_size=self.batchSize,
            verbose=self.verbose,
            # callbacks=self.callbacks,
            # validation_split=0.1,
        )
        return self

    def predict(self, X: np.ndarray):
        X = X.reshape((X.shape[0], self.inputShape[0], self.inputShape[1]))
        predictions = self.model.predict(X)
        return (predictions > 0.5).astype("int32")

    def predict_proba(self, X: np.ndarray):
        return self.model.predict(X)

    def CreateLSTMModel(self):
        model = Sequential()
        model.add(LSTM(50, return_sequences=True, input_shape=self.inputShape))
        model.add(LSTM(50))
        model.add(Dense(50, activation="relu"))
        model.add(Dense(self.numClasses, activation="sigmoid"))
        model.compile(
            loss=self.lossFunction,
            optimizer=self.optimizer,
            metrics=self.metrics,
        )
        return model
