from flask import Flask
import os
from flask_restful import Api
from apispec import APISpec
from flask_cors import CORS
from apispec.ext.marshmallow import MarshmallowPlugin
from flask_apispec.extension import FlaskApiSpec
from flask_restful import Resource, fields
from flask_apispec import marshal_with, doc, use_kwargs
from marshmallow import Schema, fields
from flask_apispec.views import MethodResource
from flask_cors import cross_origin
from Producer.GetNewData import PushBicycleData, PushWeatherData, FetchWeatherData
from Model.EnsembleLearning import TrainModel, PredictModel
from datetime import datetime, timedelta
import pickle

flaskApp = Flask(__name__)
CORS(flaskApp)

flaskApp.config.update(
    {
        "APISPEC_SPEC": APISpec(
            title="My RestAPIs",
            version="v1",
            plugins=[MarshmallowPlugin()],
            openapi_version="2.0.0",
        ),
        "APISPEC_SWAGGER_URL": "/documentationJson",
        "APISPEC_SWAGGER_UI_URL": "/docs",
    }
)

# Create RestAPI
restAPI = Api(flaskApp)
ensembleClassifer = None
windDirectionEncoder = None
climateEncoder = None
MlBinarizer = None
ModelDir = "./SavedModels/"
if not os.path.exists(ModelDir):
    os.mkdir(ModelDir)



class TrainReqParams(Schema):
    train = fields.String(required=False, metadata={"description": "Dummy Paramater"})


class TrainResParams(Schema):
    accuracy = fields.Number(
        required=True, metadata={"description": "Training accuracy"}
    )
    error = fields.String(required=False, metadata={"description": "Error"})


class Train(MethodResource, Resource):
    @doc(description="", tags=["Train Model"])
    @use_kwargs(TrainReqParams, location=("json"))
    @marshal_with(TrainResParams)
    @cross_origin()
    def post(self, **args):
        # global ensembleClassifer, windDirectionEncoder, climateEncoder, MlBinarizer
        try:
            (
                ensembleClassifer,
                windDirectionEncoder,
                climateEncoder,
                MlBinarizer,
                customAccuracy,
            ) = TrainModel()
            with open(ModelDir + "ensembleClassifer.pkl", "wb") as pklFile:
                pickle.dump(ensembleClassifer, pklFile)
            with open(ModelDir + "windDirectionEncoder.pkl", "wb") as pklFile:
                pickle.dump(windDirectionEncoder, pklFile)
            with open(ModelDir + "climateEncoder.pkl", "wb") as pklFile:
                pickle.dump(climateEncoder, pklFile)
            with open(ModelDir + "MlBinarizer.pkl", "wb") as pklFile:
                pickle.dump(MlBinarizer, pklFile)
    
            
            return {"accuracy": int(customAccuracy*100)}
        except Exception as e:
            error = f"Error {e}"
            return {"accuracy": 0, "error": error}


class PredictReqParams(Schema):
    UserZipcode = fields.Number(
        required=True, metadata={"description": "ZipCode of the Users Location"}
    )


class PredictResParams(Schema):
    directions = fields.String(
        required=True, metadata={"description": "Predicted Direction by the model"}
    )
    error = fields.String(required=False, metadata={"description": "Error"})


class Predict(MethodResource, Resource):
    @doc(description="", tags=["Predict Model"])
    @use_kwargs(PredictReqParams, location=("json"))
    @marshal_with(PredictResParams)
    @cross_origin()
    def post(self, **args):

    
            
        if os.path.exists(ModelDir + "ensembleClassifer.pkl"):
            with open(ModelDir +"ensembleClassifer.pkl", "rb") as pklFile:
                ensembleClassifer = pickle.load(pklFile)
                print("loaded ensembleClassifer")

        if os.path.exists(ModelDir + "windDirectionEncoder.pkl"):
            with open(ModelDir +"windDirectionEncoder.pkl", "rb") as pklFile:
                windDirectionEncoder = pickle.load(pklFile)
                print("loaded windDirectionEncoder")

        if os.path.exists(ModelDir + "climateEncoder.pkl"):
            with open(ModelDir +"climateEncoder.pkl", "rb") as pklFile:
                climateEncoder = pickle.load(pklFile)
                print("loaded climateEncoder")

        if os.path.exists(ModelDir + "MlBinarizer.pkl"):
            with open(ModelDir +"MlBinarizer.pkl", "rb") as pklFile:
                MlBinarizer = pickle.load(pklFile)
                print("loaded MlBinarizer")
                
        userZipcode = int(args["UserZipcode"])
        currentDateTime = datetime.now()
        currentDateTime -= timedelta(hours=2)
        print(currentDateTime)
        try:
            weatherDataFrame = FetchWeatherData(currentDateTime, userZipcode)
            weatherDataFrame = weatherDataFrame.drop(columns=["Zipcode","day"])
            print(weatherDataFrame.head())
            predictions = PredictModel(
                ensembleClassifer,
                weatherDataFrame,
                windDirectionEncoder,
                climateEncoder,
                MlBinarizer,
            )
            predictions = predictions[-1]
            predictions = ",".join(predictions)
            return {"directions": predictions}
        
        except Exception as e:
            error = f"Error {e}"
            return {
                "directions": "There is some issue with the Model Please try later!!",
                "error": error,
            }


class GenerateNewDataReqParams(Schema):
    fromDate = fields.Date(
        required=False, metadata={"description": "Date from when to gather data"}
    )


class GenerateNewDataResParams(Schema):
    sucess = fields.Boolean(
        required=True, metadata={"description": "data Retrived sucess"}
    )
    error = fields.String(required=False, metadata={"description": "Error"})


class GenerateNewData(MethodResource, Resource):
    @doc(description="", tags=["Get New Data"])
    @use_kwargs(GenerateNewDataReqParams, location=("json"))
    @marshal_with(GenerateNewDataResParams)
    @cross_origin()
    def post(self, **args):
        try:
            PushBicycleData()
            PushWeatherData()
            return {"sucess": True}
        except Exception as e:
            error = f"Error {e}"
            return {"sucess": False, "error": error}


# Add paths
restAPI.add_resource(Train, "/train")
restAPI.add_resource(Predict, "/predict")
restAPI.add_resource(GenerateNewData, "/generatenewdata")

# Add Docs
docs = FlaskApiSpec(flaskApp)
docs.register(Train, endpoint="train")
docs.register(Predict, endpoint="predict")
docs.register(GenerateNewData, endpoint="generatenewdata")

if __name__ == "__main__":
    try:
        flaskApp.run()
    except:
        print("Email Rest API Failed")
