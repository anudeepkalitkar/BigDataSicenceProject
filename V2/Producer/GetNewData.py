from datetime import datetime
from .MongoRetreival import *
from .GetBicycleData import *
from .MongoInjection import *
from .GetWeatherData import *
from tqdm import tqdm



def GenerateTimeSequence(startDateTime: datetime, timeDifferenceinMinutes: int):
    currentDateTime = datetime.now()
    currentDateTime = currentDateTime.replace(second=0, microsecond=0)
    timeSequence = []

    while startDateTime <= currentDateTime:
        timeSequence.append(startDateTime)
        startDateTime += timedelta(minutes=timeDifferenceinMinutes)

    return timeSequence

def GetLatestEntryDateFromMongo(topic: str):
    cursor = RunQueryonMongodb(topic)
    dataList = GetLatestEntries(cursor)    
    fromDate = dataList[0]['day']
    fromDate = datetime.strptime(fromDate, '%Y-%m-%d %H:%M:%S') 
    return fromDate
        
def FetchBicycleData(fromDate: datetime, bicycleMetadata: list = bicycleMetadata):
    timeSeq = GenerateTimeSequence(fromDate, 15)
    bicycleDataFrame = None
    for metadata in bicycleMetadata:
        bicycleData = []
        for time in tqdm(timeSeq):
            data = GetBicycleData(metadata["baseURL"],time)
            if(data!=None):
                bicycleData.append(data)
        dataFrame = pd.DataFrame(bicycleData)
        dataFrame = ConvertDaytoDateTime(dataFrame)
        dataFrame = Get1HrIntervals(dataFrame, "day")
        dataFrame = DropColumns(dataFrame, renameColumns=metadata["renameColumns"])

        dataFrame["Zipcode"] = metadata["Zipcode"]
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

def PushBicycleData():
    fromDate = GetLatestEntryDateFromMongo("BicycleData")
    print("fetching Bicycle Data")
    
    bicycleDataFrame = FetchBicycleData(fromDate)
    print("pushing bicycle Data")
    
    for i, row in tqdm(bicycleDataFrame.iterrows()):
        data = row.to_dict()
        data['day'] = data['day'].strftime("%Y-%m-%d %H:%M:%S")
        sucess = InjectToMongodb('BicycleData', data)
        if not sucess:
            print("Failed to push at ", i)
            break


def FetchWeatherData(fromDate: datetime, zipCode: int = 80309):
    timeSeq = GenerateTimeSequence(fromDate,60)
    newData = []
    for index in tqdm(range(len(timeSeq)-1)):
        data = GetWeatherData(zipCode, timeSeq[index].strftime("%Y-%m-%d %H:%M:%S"), timeSeq[index+1].strftime("%Y-%m-%d %H:%M:%S"))
        for day, values in data.items():
            formatedData = {"day": day}
            for key, value in values.items():
                formatedData[key]= value
            newData.append(formatedData)
    weatherDataFrame = pd.DataFrame(newData)
    if(len(newData)):
        weatherDataFrame = weatherDataFrame.drop(columns=['Zipcode_North', 'Zipcode_South', 'Zipcode_East','Zipcode_West'])
        weatherDataFrame = weatherDataFrame.rename(columns={'Zipcode_currentLocation': 'Zipcode'})
    return weatherDataFrame

def PushWeatherData():
    fromDate = GetLatestEntryDateFromMongo('WeatherData')
    print("fetching Weather Data")
    weatherDataFrame = FetchWeatherData(fromDate)
    print("pushing Weather Data")
    for i, row in tqdm(weatherDataFrame.iterrows()):
        data = row.to_dict()
        sucess = InjectToMongodb('WeatherData', data)
        if not sucess:
            print("Failed to push at ", i)
            break
    
    
# PushBicycleData()
# PushWeatherData()
    
    