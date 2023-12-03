import requests
import pandas as pd
from datetime import datetime, timedelta

# # COand30NB = "https://services.arcgis.com/ePKBjXrBZ2vEEgWd/arcgis/rest/services/Bicycle_Traffic_Counts_at_Colorado_and_30th_(Northbound)/FeatureServer/0/query"
# # COand30SB = "https://services.arcgis.com/ePKBjXrBZ2vEEgWd/arcgis/rest/services/Bicycle_Traffic_Counts_at_Colorado_and_30th_(Southbound)/FeatureServer/0/query"
# # COand30WB = "https://services.arcgis.com/ePKBjXrBZ2vEEgWd/arcgis/rest/services/Bicycle_and_Pedestrian_Traffic_Counts_at_Colorado_and_30th_Westbound/FeatureServer/0/query"

# bicycleDataBaseUrls = [
#     "https://services.arcgis.com/ePKBjXrBZ2vEEgWd/arcgis/rest/services/Bicycle_Traffic_Counts_at_Colorado_and_30th_(Northbound)/FeatureServer/0/query",
#     "https://services.arcgis.com/ePKBjXrBZ2vEEgWd/arcgis/rest/services/Bicycle_Traffic_Counts_at_Colorado_and_30th_(Southbound)/FeatureServer/0/query",
#     "https://services.arcgis.com/ePKBjXrBZ2vEEgWd/arcgis/rest/services/Bicycle_and_Pedestrian_Traffic_Counts_at_Colorado_and_30th_Westbound/FeatureServer/0/query",
# ]


bicycleMetadata = [
    {
        "filename": "Colorado_and_30th_Northbound.csv",
        "Zipcode": 80309,
        "renameColumns": {"Total": "NorthBound"},
        "baseURL": "https://services.arcgis.com/ePKBjXrBZ2vEEgWd/arcgis/rest/services/Bicycle_Traffic_Counts_at_Colorado_and_30th_(Northbound)/FeatureServer/0/query",
    },
    {
        "filename": "Colorado_and_30th_Southbound.csv",
        "Zipcode": 80309,
        "renameColumns": {"Total": "SouthBound"},
        "baseURL": "https://services.arcgis.com/ePKBjXrBZ2vEEgWd/arcgis/rest/services/Bicycle_Traffic_Counts_at_Colorado_and_30th_(Southbound)/FeatureServer/0/query",
    },
    {
        "filename": "Colorado_and_30th_Westbound.csv",
        "Zipcode": 80309,
        "renameColumns": {"Total": "WestBound"},
        "baseURL": "https://services.arcgis.com/ePKBjXrBZ2vEEgWd/arcgis/rest/services/Bicycle_and_Pedestrian_Traffic_Counts_at_Colorado_and_30th_Westbound/FeatureServer/0/query",
    },
]





def GenerateQuery(dateTime: datetime):
    year = dateTime.year
    month = dateTime.month
    day = dateTime.day
    hour = dateTime.hour
    minute = dateTime.minute
    query = f"?where=day%20%3D%20'{year}-{month}-{day:02d}%20{hour:02d}%3A{minute:02d}%3A00'&outFields=*&outSR=4326&f=json"
    return query


def GetBicycleData(baseUrl: str, dateTime: datetime):
    url = baseUrl + GenerateQuery(dateTime)
    response = requests.get(url)
    if response.status_code == 200:
        try:
            bicycleData = response.json()["features"][0]["attributes"]
            return bicycleData
        except:
            return None
    else:
        print(f"Failed to get data. Status code: {response.status_code}")
        return None


def ConvertDaytoDateTime(dataFrame: pd.DataFrame):
    dataFrame["day"] = pd.to_datetime(dataFrame["day"])
    dataFrame = dataFrame.sort_values(by="day")
    return dataFrame


def Get1HrIntervals(dataFrame: pd.DataFrame, columnName: str):
    dataFrame = dataFrame.resample("1H", on=columnName).sum().reset_index()
    return dataFrame


def DropColumns(
    dataFrame: pd.DataFrame, renameColumns: dict, columnsRetain: list = ["day", "Total"]
):
    dataFrame = dataFrame[columnsRetain]
    dataFrame = dataFrame.rename(columns=renameColumns)
    return dataFrame
