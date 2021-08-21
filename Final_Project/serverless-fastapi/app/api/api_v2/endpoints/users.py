from fastapi import APIRouter
from enum import Enum
import boto3
import numpy
import s3fs
import h5py
from fastapi import FastAPI, Depends
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
router = APIRouter()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl='token')

@router.get("/")
async def root(token: str = Depends(oauth2_scheme)):
    return {"message": "Get Synrad Dataset Algorithm!"}


class SynradModelName(str, Enum):
    mse_model = "mse_model"
    mse_vgg_model = "mse_vgg_model"
    gan_model = "gan_model"

def synrad_read_data(filename, rank=0, size=1, end=None,dtype=numpy.float32):
    x_keys = ['ir069','ir107','lght']
    y_keys = ['vil']
    s = numpy.s_[rank:end:size]
    H5PY_DEFAULT_READONLY=1
    s3 = s3fs.S3FileSystem()
    with s3.open(filename,'rb') as s3file:
        with h5py.File(s3file, 'r') as hf:
            IN  = {k:hf[k][s].astype(numpy.float32) for k in x_keys}
            OUT = {k:hf[k][s].astype(numpy.float32) for k in y_keys}
    return IN,OUT
 
synrad_DATA = ('s3://seviringestion/data/synrad_testing.h5')

@router.get("/Synrad/{Index}")
async def read_syn_index(Index: int, token: str = Depends(oauth2_scheme)):
    return {"Index": Index}
    
@router.get("/Synrad_models/{model_name}")
async def get_syn_model(model_name: SynradModelName, token: str = Depends(oauth2_scheme)):
    if model_name == SynradModelName.mse_model:
        return {"model_name": model_name, "message": "Deep Learning FTW!"}

    if model_name.value == "mse_vgg_model":
        return {"model_name": model_name, "message": "LeCNN all the images"}

    return {"model_name": model_name, "message": "Have some residuals"}
    
@router.get("/Synrad_Input_at_Index/{Input_Output_size}") 
async def get_output_size(idx: int, token: str = Depends(oauth2_scheme)):    
    X,Y = synrad_read_data(synrad_DATA,end=1000)
    return {"Input_IR069": X['ir069'][idx,:,:,0].shape,
            "Input_IR071":X['ir107'][idx,:,:,0].shape,
            "Input_lght":X['lght'][idx,:,:,0].shape,
            "Output":Y['vil'][idx,:,:,0].shape} 

@router.get("/Synrad_Output/{Input_Output}") 
async def get_output(idx: int, token: str = Depends(oauth2_scheme)):    
    X,Y = synrad_read_data(synrad_DATA,end=1000)
    return {"Output":Y['vil'][idx,:,:,0]}               
    