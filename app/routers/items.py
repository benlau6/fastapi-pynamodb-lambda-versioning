import http.client as httplib
import uuid
from typing import List, Optional

from fastapi import APIRouter
from pydantic import BaseModel
from pynamodb.exceptions import DeleteError, DoesNotExist

from .db_model import ItemModel
from .utils import get_updated_at

router = APIRouter(
    prefix="/items",
    tags=["items"]
)


class ItemBase(BaseModel):
    name: str
    image: str
    status: str
    created_at: str
    updated_at: str

class ItemIn(ItemBase):
    item_id: str = uuid.uuid4()

class ItemOut(ItemBase):
    item_id: str

class ItemOutAntd(BaseModel):
    data: List[ItemOut]

class ItemUpdate(BaseModel):
    status: str

class ItemSort(BaseModel):
    updated_at: str


@router.get("/{item_id}", response_model=ItemOut)
def read_item(item_id: str, q: Optional[str] = None):
    item = ItemModel.get(item_id)
    return dict(item)


@router.patch("/{item_id}", response_model=ItemOut)
def update_item(item_id: str, new_item:ItemUpdate):
    item = ItemModel.get(item_id)
    item.update(
        actions=[
            ItemModel.status.set(new_item.status),
            ItemModel.updated_at.set(get_updated_at())
        ]
    )
    return dict(item)


@router.get("/", response_model=ItemOutAntd)
def read_items(created_at: Optional[str] = None, updated_at: Optional[str] = None):
    items = ItemModel.scan()
    items = [dict(item) for item in items]
    
    if updated_at:
        if updated_at == 'ascend':
            items = sorted(items, key=lambda k: k['updated_at'])
        elif updated_at == 'descend':
            items = sorted(items, key=lambda k: k['updated_at'], reverse=True)
    elif created_at:
        if created_at == 'ascend':
            items = sorted(items, key=lambda k: k['created_at'])
        elif created_at == 'descend':
            items = sorted(items, key=lambda k: k['created_at'], reverse=True)

    return {'data': items}
    #return {'data': [dict(item) for item in items]}


@router.post("/", response_model=ItemOut)
def insert_item(item: ItemIn):
    return item
