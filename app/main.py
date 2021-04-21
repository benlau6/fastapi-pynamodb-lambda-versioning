from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum

from .api.v1.api import router as api_v1_router
from .api.graphql.api import router as gql_router
from app.core.config import settings

import os

STAGE = os.environ['STAGE']

app = FastAPI(root_path=f'/{STAGE}')

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_v1_router, prefix=settings.API_V1_STR)
app.include_router(gql_router, prefix='/graphql', tags=['graphql'])

@app.get("/")
def read_root():
    return {"Hello": "World"}

handler = Mangum(app)