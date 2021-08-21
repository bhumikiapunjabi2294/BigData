import boto3
import numpy
import s3fs
import h5py
from fastapi import FastAPI
import jwt
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from passlib.hash import bcrypt
from tortoise import fields 
from tortoise.contrib.fastapi import register_tortoise
from tortoise.contrib.pydantic import pydantic_model_creator
from tortoise.models import Model 
from app.api.api_v1.api import router as api_router1
from app.api.api_v2.api import router as api_router2
from mangum import Mangum
from enum import Enum

app = FastAPI(title='SEVIR Dataset',
    description='Predicting future weater forecast and Storm Events')

oauth2_scheme = OAuth2PasswordBearer(tokenUrl='token')

@app.post('/token')
async def token(form_data: OAuth2PasswordRequestForm = Depends()):
    return {'access_token' : form_data.username + 'token'}

@app.get('/')
async def index(token: str = Depends(oauth2_scheme)):
    return {'the_token' : token}

@app.get("/SEVIR")
async def root(token: str = Depends(oauth2_scheme)):
    return {"message": "Welcome to Swagger to explore SEVIR Dataset!"}
    
@app.get("/Dataset")
async def root(token: str = Depends(oauth2_scheme)):
    return {"message`": "Get Synrad or Nowcast Dataset to explore!"}
    
@app.get("/Index")
async def root(token: str = Depends(oauth2_scheme)):
    return {"message`": "Get Index of Algorithm of your choice and do Inference!"}

    
client = boto3.client('s3', aws_access_key_id='AKIA2LOOS4MMLZHPVD3R', aws_secret_access_key='pf0BC0Du3mXUIihYVm1ZTeNihyENqh35czCH4WB')
client._request_signer.sign = (lambda *args, **kwargs: None)

s3 = boto3.resource('s3')
bucket=s3.Bucket('seviringestion')

app.include_router(api_router1, prefix="/api/v1")
app.include_router(api_router2, prefix="/api/v2")
#handler = Mangum(app)

def handler(event, context):
    print(event)

    asgi_handler = Mangum(app)
    response = asgi_handler(event, context) # Call the instance with the event arguments

    return response
