import os
import json
import pandas as pd
import numpy as np
from tqdm import tqdm
from sklearn.preprocessing import LabelEncoder
from .MongoRetreival import *


class WeatherDataset:
    def __init__(self):
        self.replaceDirection = {
            "ESE": "E",
            "SSE": "S",
            "WSW": "W",
            "NNE": "N",
            "ENE": "E",
            "NNE": "N",
            "SSW": "S",
            "WNW": "W",
            "NNW": "N",
        }

    def LoadDataset(self, fromFiles: bool, weatherDatasetFolderPath: str = None):
        if fromFiles:
            if not os.path.isdir(weatherDatasetFolderPath):
                print("enter a valid metadata Filepath")
                return None
            weatherDataFrame = self.LoadDatasetFromFolder(weatherDatasetFolderPath)
            weatherDataFrame = weatherDataFrame.drop(columns=['_id'])
            return weatherDataFrame
        cursor = RunQueryonMongodb("WeatherData")
        data = list(cursor)
        weatherDataFrame = pd.DataFrame(data)
        weatherDataFrame["day"] = pd.to_datetime(weatherDataFrame["day"])
        weatherDataFrame = weatherDataFrame.drop(columns=['_id'])
        return weatherDataFrame

    def LoadDatasetFromFolder(self, weatherDatasetFolderPath: str):
        weatherDataFrame = []
        fileList = os.listdir(weatherDatasetFolderPath)
        for fileName in tqdm(fileList):
            with open(weatherDatasetFolderPath + fileName, "r") as jsonFile:
                fileData = json.load(jsonFile)
            for date, weather in fileData.items():
                fileData = {}
                fileData["day"] = date
                for key, value in weather.items():
                    fileData[key] = value
                weatherDataFrame.append(fileData)

        weatherDataFrame = pd.DataFrame(weatherDataFrame)
        weatherDataFrame["Zipcode"] = 80309
        weatherDataFrame["day"] = pd.to_datetime(weatherDataFrame["day"])
        return weatherDataFrame

    def PreprocessDataset(
        self,
        weatherDataFrame: pd.DataFrame,
        windDirectionEncoder: LabelEncoder = None,
        climateEncoder: LabelEncoder = None,
    ):
        weatherDataFrame.replace(self.replaceDirection, inplace=True)
        windDirectionColumns = [
            columnName
            for columnName in weatherDataFrame.columns
            if columnName.__contains__("windDir")
        ]

        if windDirectionEncoder == None:
            windDirectionEncoder = LabelEncoder()
            uniquewindDirectionValues = []
            for column in windDirectionColumns:
                uniquewindDirectionValues += list(weatherDataFrame[column].unique())
            windDirectionEncoder = windDirectionEncoder.fit(uniquewindDirectionValues)

        for column in windDirectionColumns:
            weatherDataFrame[column] = windDirectionEncoder.transform(
                weatherDataFrame[column]
            )

        climateColumns = [
            columnName
            for columnName in weatherDataFrame.columns
            if columnName.__contains__("weather")
        ]

        if climateEncoder == None:
            climateEncoder = LabelEncoder()
            uniqueClimateValues = []
            for column in climateColumns:
                uniqueClimateValues += list(weatherDataFrame[column].unique())
            climateEncoder = climateEncoder.fit(uniqueClimateValues)

        for column in climateColumns:
            weatherDataFrame[column] = climateEncoder.transform(
                weatherDataFrame[column]
            )

        return weatherDataFrame, windDirectionEncoder, climateEncoder
