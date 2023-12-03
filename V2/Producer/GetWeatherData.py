import json
import math
import requests
import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta
from .Creds import *

def CalculateCompassAngle(
    fromLat: float, fromLong: float, toLat: float, toLong: float
) -> int:
    fromLat = math.radians(fromLat)
    toLat = math.radians(toLat)
    longDiff = math.radians(toLong - fromLong)

    x = math.sin(longDiff) * math.cos(toLat)
    y = math.cos(fromLat) * math.sin(toLat) - (
        math.sin(fromLat) * math.cos(toLat) * math.cos(longDiff)
    )
    actualAngle = math.atan2(x, y)

    actualAngle = math.degrees(actualAngle)
    compassAngle = (actualAngle + 360) % 360

    return int(compassAngle)


def GetCompassDirection(angle: int) -> str:
    directions = [
        "North",
        "Northeast",
        "East",
        "Southeast",
        "South",
        "Southwest",
        "West",
        "Northwest",
    ]
    return directions[int((angle + 22.5) % 360 / 45)]


def GetNearbyZipCodes(
    presentZipcode: int,
    geonamesUsername: str,
    radius: int = 30,
    maxRows: int = 100,
    country: str = "US",
    minimumDistance: int = 10,
) -> dict:
    geonamesURL = f"http://api.geonames.org/findNearbyPostalCodesJSON?postalcode={presentZipcode}&maxRows={maxRows}&country={country}&radius={radius}&username={geonamesUsername}"
    allZipcodes = []
    responseJson = {}
    nearestZipCodes = {"North": None, "East": None, "South": None, "West": None}
    nearestDistances = {
        "North": minimumDistance,
        "East": minimumDistance,
        "South": minimumDistance,
        "West": minimumDistance,
    }

    response = requests.get(geonamesURL)
    if response.status_code == 200:
        responseJson = response.json()
    else:
        return {"Error": str(response.status_code)}

    locationsDF = pd.DataFrame(responseJson["postalCodes"])
    locationsDF = locationsDF[["lat", "lng", "distance", "postalCode"]]
    locationsDF = locationsDF.astype(
        {"lat": "float", "lng": "float", "distance": "float", "postalCode": "float"},
        errors="ignore",
    )
    fromLocation = locationsDF[locationsDF["postalCode"] == presentZipcode].values[0]
    nearByLocations = locationsDF[locationsDF["postalCode"] != presentZipcode].values

    for location in nearByLocations:
        compassAngle = CalculateCompassAngle(
            fromLocation[0], fromLocation[1], location[0], location[1]
        )
        direction = GetCompassDirection(compassAngle)
        if direction in ["North", "East", "South", "West"]:
            allZipcodes.append(
                {
                    "zipCode": location[3],
                    "direction": direction,
                    "distance": location[2],
                }
            )

    for zipcode in allZipcodes:
        if zipcode["distance"] > nearestDistances[zipcode["direction"]]:
            nearestDistances[zipcode["direction"]] = zipcode["distance"]
            nearestZipCodes[zipcode["direction"]] = int(zipcode["zipCode"])
    nearestZipCodes["currentLocation"] = presentZipcode
    return nearestZipCodes


def GetWeatherDatafromZipCode(
    zipcode: int,
    fromDateStr: str,
    toDateStr: str,
    aerisClientID: str,
    aerisClientSecret: str,
):
    aerisWeatherURL = f"https://api.aerisapi.com/conditions/{zipcode}?format=json&from={fromDateStr}&to={toDateStr}&plimit=24&filter=1hr&fields=periods.tempF,periods.dateTimeISO,periods.dewpointF,periods.windSpeedMPH,periods.windDir,periods.weather&client_id={aerisClientID}&client_secret={aerisClientSecret}"
    response = requests.get(aerisWeatherURL)
    if response.status_code == 200:
        weatherReport = response.json()["response"][0]["periods"]
        for weather in weatherReport:
            weather["Zipcode"] = zipcode
        
        return weatherReport
    else:
        return [{"Error": str(response.status_code)}]


def GetWeatherData(
    userZipcode: int,
    fromDateStr: str,
    toDateStr: str,
    geonamesUsername: str = GeoNamesUsername,
    aerisCreds: list= AerisCreds,
):
    nearestZipCodes = GetNearbyZipCodes(userZipcode, geonamesUsername)
    weatherDetails = {}
    aerisCredsindex = 0

    for direction, zipcode in nearestZipCodes.items():
        weatherReport = GetWeatherDatafromZipCode(
            zipcode, fromDateStr, toDateStr, aerisCreds[aerisCredsindex]['clientID'], aerisCreds[aerisCredsindex]['clientSecret']
        )
        while weatherReport[0].get("Error") is not None:
            print(weatherReport[0]["Error"])
            aerisCredsindex += 1
            weatherReport = GetWeatherDatafromZipCode(
            zipcode, fromDateStr, toDateStr, aerisCreds[aerisCredsindex]['clientID'], aerisCreds[aerisCredsindex]['clientSecret']
        )

        for weather in weatherReport:
            recoredDate = weather["dateTimeISO"]
            recoredDate = datetime.fromisoformat(recoredDate)
            recoredDate = recoredDate.strftime("%Y-%m-%d %H:%M:%S")

            if weatherDetails.get(recoredDate) is None:
                weatherDetails[recoredDate] = {}
            for key, value in weather.items():
                if key == "dateTimeISO":
                    pass
                else:
                    weatherDetails[recoredDate][f"{key}_{direction}"] = value
    return weatherDetails


