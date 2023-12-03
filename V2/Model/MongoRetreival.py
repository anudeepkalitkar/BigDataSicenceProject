from pymongo. cursor  import Cursor
from pymongo import MongoClient

def RunQueryonMongodb(topic: str,filter: object ={}):
    client = MongoClient(
            "mongodb+srv://bicycledirectionprediction:Bigdataproject@cluster0.wmtrnrn.mongodb.net/?retryWrites=true&w=majority")
    collection = client.bicycledirectionpredicition[topic]
    cursor = collection.find(filter) 
    return cursor

def GetLatestEntries(cursor: Cursor, numberofEntries: int = 1):
    cursor = cursor.sort('_id', -1).limit(numberofEntries)
    dataList = list(cursor)
    return dataList
