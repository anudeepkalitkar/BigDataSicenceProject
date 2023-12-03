import os
import pickle
import warnings
import datetime
import pandas as pd
from itertools import combinations

warnings.filterwarnings("ignore")

from sklearn.metrics import accuracy_score
from sklearn.ensemble import StackingClassifier
from sklearn.model_selection import train_test_split
from sklearn.multioutput import MultiOutputClassifier
from sklearn.preprocessing import MultiLabelBinarizer

import tensorflow as tf
from keras.callbacks import (
    EarlyStopping,
    ReduceLROnPlateau,
    TensorBoard,
)

from .ModelCreation import *
from .Bicycle import *
from .Weather import *

print("tensorflow version:", tf.__version__)
physicalDevices = tf.config.list_physical_devices("GPU")
print(physicalDevices)

if len(physicalDevices) > 0:
    tf.config.experimental.set_memory_growth(physicalDevices[0], True)


logsDir = "logs/fit/" + datetime.datetime.now().strftime("%Y%m%d-%H")
tensorboardCBK = TensorBoard(log_dir=logsDir, histogram_freq=1)
earlyStoppingCBK = EarlyStopping(monitor="val_loss", patience=10, verbose=0, mode="min")
reduceLRPlateauCBK = ReduceLROnPlateau(
    monitor="val_loss", factor=0.1, patience=7, verbose=1, mode="min"
)
callbacks = [earlyStoppingCBK, reduceLRPlateauCBK, tensorboardCBK]


bicycleDatasetFolderPath = "Dataset/Bicycle Dataset/"
bicycleMetaDataFilepath = "Dataset/Bicycle Dataset/metadata/metadata.json"
weatherDatasetFolderPath = "Dataset/Weather Dataset/JsonFiles/"


def CreateEnsembleModel(
    inputShape: tuple,
    numClasses: int,
    lossFunction: str,
    optimizer: str,
    metrics: list,
    epochs: int = 100,
    batchSize: int = 32,
    verbose: int = 1,
    XGBParameters: dict = {"tree_method": "hist", "device": "cuda", "verbosity": 1},
):
    CNNModel = CNNClassifier(
        inputShape=inputShape,
        numClasses=numClasses,
        epochs=epochs,
        batchSize=batchSize,
        lossFunction=lossFunction,
        optimizer=optimizer,
        metrics=metrics,
        verbose=verbose,
        callbacks=callbacks,
    )
    LSTMModel = LSTMClassifier(
        inputShape=inputShape,
        numClasses=numClasses,
        epochs=epochs,
        batchSize=batchSize,
        lossFunction=lossFunction,
        optimizer=optimizer,
        metrics=metrics,
        verbose=verbose,
        callbacks=callbacks,
    )
    XGBModel = MultiOutputClassifier(CreateXGBClassifier(XGBParameters))
    LGBModel = MultiOutputClassifier(CreateLGBClassifier())
    CBModel = MultiOutputClassifier(CreateCBClassifier())
    LRModel = MultiOutputClassifier(CreateLRClassifier())

    # ensembleClassifer = StackingClassifier(
    #     estimators=[("CNNModel", CNNModel), ("LSTMModel", LSTMModel)],
    #     verbose=1,
    #     final_estimator=XGBModel,
    # )
    ensembleClassifer = StackingClassifier(
        estimators=[("XGBModel", XGBModel), ("LGBModel", LGBModel), ("CBModel", CBModel)],
        verbose=1,
        final_estimator=LRModel,
    )
    return ensembleClassifer


def LoadDataset(
    bicycleClass: BicycleDataset,
    weatherClass: WeatherDataset,
    fromFile: bool = False,
    metadataFilepath: str = None,
    weatherDatasetFolderPath: str = None,
):
    bicycleDataFrame = bicycleClass.LoadDataset(fromFile, metadataFilepath)
    weatherDataFrame = weatherClass.LoadDataset(fromFile, weatherDatasetFolderPath)

    finalDataFrame = pd.merge(
        bicycleDataFrame,
        weatherDataFrame,
        on=["day", "Zipcode"],
        how="outer"
        # bicycleDataFrame,
        # weatherDataFrame,
        # on=["day"],
        # how="outer",
    )
    # finalDataFrame = finalDataFrame.drop(columns=["day", "Zipcode_y", "Zipcode_x"])

    return finalDataFrame


def PreProcessDataset(
    weatherClass: WeatherDataset,
    X: pd.DataFrame,
    y: pd.DataFrame = None,
    windDirectionEncoder: LabelEncoder = None,
    climateEncoder: LabelEncoder = None,
    MlBinarizer: MultiLabelBinarizer = None,
):
    X = X.dropna()
    X, windDirectionEncoder, climateEncoder = weatherClass.PreprocessDataset(
        X, windDirectionEncoder, climateEncoder
    )
    if type(y) != type(None):
        if MlBinarizer == None:
            MlBinarizer = MultiLabelBinarizer()
            MlBinarizer = MlBinarizer.fit(y)
            print(MlBinarizer.classes_)
            print(np.unique(y.values))
            
        y = MlBinarizer.transform(y)
    return X, y, windDirectionEncoder, climateEncoder, MlBinarizer


def CustomAccuracy(y_test, y_pred, MlBinarizer: MultiLabelBinarizer):
    correctValues = 0
    for ypred, yacc in zip(
        MlBinarizer.inverse_transform(y_pred), MlBinarizer.inverse_transform(y_test)
    ):
        if any(label in yacc for label in ypred):
            correctValues += 1
    accuracy = correctValues / len(y_test)
    return accuracy


def PredictModel(
    ensembleClassifer: StackingClassifier,
    X_test: pd.DataFrame,
    windDirectionEncoder: LabelEncoder,
    climateEncoder: LabelEncoder,
    MlBinarizer: MultiLabelBinarizer,
):
    weatherClass = WeatherDataset()
    directionMapping = BicycleDataset().directionsMapping
    

    (
        X_test,
        y_test,
        windDirectionEncoder,
        climateEncoder,
        MlBinarizer,
    ) = PreProcessDataset(
        weatherClass, X_test, None, windDirectionEncoder, climateEncoder, MlBinarizer
    )
    y_pred = ensembleClassifer.predict(X_test.to_numpy())
    y_pred = MlBinarizer.inverse_transform(y_pred)
    predictedDirections = []
    for directions in y_pred:
        for direction in directions:
            predictedDirection =[]
            for key,value in directionMapping.items():
                if(value == direction):
                    predictedDirection.append(key)
        predictedDirections.append(predictedDirection)    
    return predictedDirections


def TrainModel():
    bicycleClass = BicycleDataset()
    weatherClass = WeatherDataset()

    finalDataFrame = LoadDataset(bicycleClass, weatherClass, False)
    finalDataFrame = finalDataFrame.dropna()
    # print(finalDataFrame.columns)
    # print(finalDataFrame.dtypes)

    y = finalDataFrame["BestDirections"]
    # print(y.values)
    finalDataFrame = finalDataFrame.drop(columns=["BestDirections"])
    finalDataFrame = finalDataFrame.drop(columns=["day", "Zipcode"])

    X_train, X_test, y_train, y_test = train_test_split(
        finalDataFrame, y, test_size=0.2, random_state=42
    )
    (
        X_train,
        y_train,
        windDirectionEncoder,
        climateEncoder,
        MlBinarizer,
    ) = PreProcessDataset(weatherClass, X_train, y_train)
    print("X_train shape", X_train.shape)
    print("y_train shape", y_train.shape)

    (
        X_test,
        y_test,
        windDirectionEncoder,
        climateEncoder,
        MlBinarizer,
    ) = PreProcessDataset(
        weatherClass, X_test, y_test, windDirectionEncoder, climateEncoder, MlBinarizer
    )
    print("X_test shape", X_test.shape)
    print("y_test shape", y_test.shape)

    lossFunction = "binary_crossentropy"
    optimizer = "adam"
    metrics = ["accuracy"]

    inputShape = (X_train.shape[1], 1)
    numClasses = y_train.shape[1]

    ensembleClassifer = CreateEnsembleModel(
        inputShape=inputShape,
        numClasses=numClasses,
        lossFunction=lossFunction,
        optimizer=optimizer,
        metrics=metrics,
        epochs=1,
    )
    ensembleClassifer.fit(X_train.to_numpy(), y_train)
    y_pred = ensembleClassifer.predict(X_test.to_numpy())
    # accuracy = accuracy_score(y_test, y_pred)
    customAccuracy = CustomAccuracy(y_test, y_pred, MlBinarizer)
    return (
        ensembleClassifer,
        windDirectionEncoder,
        climateEncoder,
        MlBinarizer,
        customAccuracy,
    )
