from fastapi import APIRouter
from typing import Optional
from pydantic import BaseModel

from .db_model import ItemModel
from pynamodb.exceptions import DoesNotExist, DeleteError
import http.client as httplib

router = APIRouter(
    prefix="/items",
    tags=["items"]
)


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