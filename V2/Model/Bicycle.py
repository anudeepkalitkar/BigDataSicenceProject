import os
import json
import pandas as pd
import numpy as np
from tqdm import tqdm
from .MongoRetreival import *


# replace filename with filepath in metadata
class BicycleDataset:
    def __init__(
        self,
        columnsRetain: list = ["day", "Total"],
    ):
        self.columnsRetain = columnsRetain
        self.directionsMapping = {
            "NorthBound": 1,
            "SouthBound": 2,
            "WestBound": 3,
            "EastBound": 4,
        }

    def DropColumns(
        self,
        dataFrame: pd.DataFrame,
        renameColumns: dict,
    ):
        dataFrame = dataFrame[self.columnsRetain]
        dataFrame = dataFrame.rename(columns=renameColumns)
        return dataFrame

    def ConvertDaytoDateTime(self, dataFrame: pd.DataFrame):
        dataFrame["day"] = pd.to_datetime(dataFrame["day"])
        dataFrame = dataFrame.sort_values(by="day")
        return dataFrame

    def Get1HrIntervals(self, dataFrame: pd.DataFrame, columnName: str):
        dataFrame = dataFrame.resample("1H", on=columnName).sum().reset_index()
        return dataFrame

    def ConcatDataFrames(self, dataFrames: list):
        dataFrame = pd.concat(dataFrames, axis=1)
        retainColumns = ~dataFrame.columns.duplicated()
        dataFrame = dataFrame.loc[:, retainColumns]
        return dataFrame

    def FindBestDirections(self, row: np.ndarray):
        maxValue = row.max()
        return [
            self.directionsMapping[direction]
            for direction in row.index
            if row[direction] == maxValue
        ]

    def LoadDataset(self, fromFile: bool, metadataFilepath: str = None):
        if fromFile:
            if not os.path.exists(metadataFilepath):
                print("enter a valid metadata Filepath")
                return None

            bicycleDataFrame = self.LoadDatasetFromFile(metadataFilepath)
            bicycleDataFrame = bicycleDataFrame.drop(columns=['_id'])
            bicycleDataFrame = self.PreprocessDataset(bicycleDataFrame)
            
            return bicycleDataFrame

        cursor = RunQueryonMongodb("BicycleData")
        data = list(cursor)
        bicycleDataFrame = pd.DataFrame(data)
        bicycleDataFrame = bicycleDataFrame.drop(columns=['_id'])
        bicycleDataFrame = self.PreprocessDataset(bicycleDataFrame)

        return bicycleDataFrame

    def LoadDatasetFromFile(self, metadataFilepath: str):
        metadata = json.load(open(metadataFilepath, "r"))
        bicycleDataFrame = None
        for data in tqdm(metadata):
            dataFrame = pd.read_csv(data["filename"], index_col=None, header=0)
            dataFrame = self.ConvertDaytoDateTime(dataFrame)
            dataFrame = self.Get1HrIntervals(dataFrame, "day")
            dataFrame = self.DropColumns(dataFrame, renameColumns=data["renameColumns"])

            dataFrame["Zipcode"] = data["Zipcode"]
            DFColumns = list(dataFrame.columns)
            columnsRearrange = [DFColumns[0], DFColumns[-1]] + DFColumns[1:-1]
            dataFrame = dataFrame[columnsRearrange]

            if type(bicycleDataFrame) == type(None):
                bicycleDataFrame = dataFrame

            else:
                bicycleDataFrame = pd.merge(
                    bicycleDataFrame, dataFrame, on=["day", "Zipcode"], how="outer"
                )
        return bicycleDataFrame

    def PreprocessDataset(self, bicycleDataFrame: pd.DataFrame):
        bicycleDataFrame["day"] = pd.to_datetime(bicycleDataFrame["day"])
        bicycleDataFrame = bicycleDataFrame.dropna()
        # bicycleDataFrame["EastBound"] = 0
        bicycleDataFrame["BestDirections"] = bicycleDataFrame[
            ["NorthBound", "SouthBound", "WestBound"]
        ].apply(self.FindBestDirections, axis=1)

        bicycleDataFrame = bicycleDataFrame.drop(
            columns=["NorthBound", "SouthBound", "WestBound"]
        )

        return bicycleDataFrame
