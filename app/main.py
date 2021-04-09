from fastapi import FastAPI
from mangum import Mangum

from .routers import items, graphql
from fastapi.middleware.cors import CORSMiddleware

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


app.include_router(items.router)
app.include_router(graphql.router)

@app.get("/")
def read_root():
    return {"Hello": "World"}


handler = Mangum(app)