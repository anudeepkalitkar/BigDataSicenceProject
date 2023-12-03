from pymongo import MongoClient

def InjectToMongodb( topic: str, data: dict = None, dataList: list = None):
    try:
        client = MongoClient(
            "mongodb+srv://<username>:<password>@cluster0.wmtrnrn.mongodb.net/?retryWrites=true&w=majority")
        collection = client.bicycledirectionpredicition[topic]
        if(type(data) == type(None)):
            for data in dataList:
                if(not collection.find_one(data)):
                    collection.insert_one(data)
        else:
            if(not collection.find_one(data)):
                collection.insert_one(data)
        return True
    except Exception as e:
        print(e)
        return False
    