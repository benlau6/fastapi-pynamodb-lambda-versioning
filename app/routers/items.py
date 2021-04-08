from fastapi import APIRouter
from typing import Optional
from pydantic import BaseModel

from datetime import datetime

import http.client as httplib
from pynamodb.exceptions import DoesNotExist, DeleteError
from .db_model import ItemModel
import json

router = APIRouter(
    prefix="/items",
    tags=["items"]
)

fake_items_db = [
    {
        "item_id": 1, 
        "name": 'Jack', 
        "image": "https://helpx.adobe.com/content/dam/help/en/photoshop/using/convert-color-image-black-white/jcr_content/main-pars/before_and_after/image-before/Landscape-Color.jpg",
        "status": -1,
        "created_at": datetime.now().astimezone(),
        "updated_at": datetime.now().astimezone()

    },
    {
        "item_id": 2, 
        "name": 'Ben', 
        "image": "https://helpx.adobe.com/content/dam/help/en/photoshop/using/convert-color-image-black-white/jcr_content/main-pars/before_and_after/image-before/Landscape-Color.jpg",
        "status": -1,
        "created_at": datetime.now().astimezone(),
        "updated_at": datetime.now().astimezone()
    }
]


class Item(BaseModel):
    name: str
    image: str
    status: int


@router.get("/{item_id}")
def read_item(item_id: int, q: Optional[str] = None):
    return {"item_id": item_id, "q": q}


@router.put("/{item_id}")
def update_item(item_id: int, item: Item):
    return {"item_name": item.name, "item_id": item_id}


@router.get("/")
def read_items(q: Optional[str] = None):
    try:
        res = ItemModel.scan()
    except DoesNotExist:
        return {
            'statusCode': httplib.NOT_FOUND,
            'body': {
                'error_message': 'not found'
            }
        }

    results = [dict(result) for result in res]

    return {
        "data": results
    }


@router.post("/")
def insert_item(item: Item):
    return {
        "message": "Created",
        "item_id": 1, 
        "name": item.name, 
        "image": item.image,
        "status": item.status
    }