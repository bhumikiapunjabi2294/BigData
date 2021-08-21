from fastapi import APIRouter
from enum import Enum
import boto3
import numpy
import s3fs
import h5py
#import pandas as pd
from fastapi import FastAPI, Depends
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
router = APIRouter()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl='token')

@router.get("/")
async def root(token: str = Depends(oauth2_scheme)):
    return {"message": "Get Nowcast Dataset Algorithm!"}
    
@router.get("/Nowcast/{Index}")
async def read_now_index(Index: int, token: str = Depends(oauth2_scheme)):
    return {"Index": Index}


class NowcastModelName(str, Enum):
    mse_model = "mse_model"
    style_model = "style_model"
    mse_style_model = "mse_style_model"
    gan_model = "gan_model"

    
@router.get("/Nowcast_models/{model_name}")
async def get_now_model(model_name: NowcastModelName, token: str = Depends(oauth2_scheme)):
    if model_name == NowcastModelName.mse_model:
        return {"model_name": model_name, "message": "mse_model Learning FTW!"}

    if model_name.value == "style_model":
        return {"model_name": model_name, "message": "style_model all the images"}
        
    if model_name.value == "mse_style_model":
        return {"model_name": model_name, "message": "mse_style_model all the images"}

    return {"model_name": model_name, "message": "Have some residuals"}


nowcast_DATA = ('s3://seviringestion/data/nowcast_testing.h5')
    
def nowcast_read_data(filename, rank=0, size=1, end=None, dtype=numpy.float32, MEAN=33.44, SCALE=47.54):
    x_keys = ['IN']
    y_keys = ['OUT']
    s = numpy.s_[rank:end:size]
    H5PY_DEFAULT_READONLY=1
    s3 = s3fs.S3FileSystem()
    with s3.open(filename,'rb') as s3file:
        with h5py.File(s3file, 'r') as hf:
            IN  = hf['IN'][s]
            OUT = hf['OUT'][s]
    IN = (IN.astype(dtype)-MEAN)/SCALE
    OUT = (OUT.astype(dtype)-MEAN)/SCALE
    return IN,OUT
    
x_test,y_test = nowcast_read_data(nowcast_DATA,end=50)


@router.get("/Nowcast_Input_at_Index/{Input_Output_size}") 
async def get_output_size(idx: int, token: str = Depends(oauth2_scheme)):
    class persistence:
        def predict(self,x_test):
            return x_test[:,:,:,-1:],[1,1,1,12]

    norm = {'scale':47.54,'shift':33.44}

    pers = persistence().predict(x_test[idx:idx+1])
    #pers = pers*norm['scale']+norm['shift']
    x = x_test[idx:idx+1]
    y = y_test[idx:idx+1]*norm['scale']+norm['shift']

    return {"Input": x.shape,
            "Output":y.shape}
            
@router.get("/Nowcast_Output_at_Index") 
async def get_output(idx: int, token: str = Depends(oauth2_scheme)):
    class persistence:
        def predict(self,x_test):
            return x_test[:,:,:,-1:],[1,1,1,12]

    norm = {'scale':47.54,'shift':33.44}

    pers = persistence().predict(x_test[idx:idx+1])
    #pers = pers*norm['scale']+norm['shift']
    x = x_test[idx:idx+1]
    y = str(y_test[idx:idx+1]*norm['scale']+norm['shift'])

    #df = pd.Dataframe(y)
    #df.to_csv('out.csv')
    return {"Output":y}
            

