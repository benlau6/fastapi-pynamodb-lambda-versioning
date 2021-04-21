import http.client as httplib
import uuid
from typing import List, Optional

from fastapi import APIRouter
from pynamodb.exceptions import DeleteError, DoesNotExist

from app import crud, schemas
from app.utils import get_updated_at



router = APIRouter()

@router.get("/{item_id}", response_model=schemas.ItemOut)
def read_item(item_id: str, q: Optional[str] = None):
    item = crud.item.get(item_id)
    return dict(item)


@router.patch("/{item_id}", response_model=schemas.ItemOut)
def update_item(item_id: str, new_item:schemas.ItemUpdate):
    item = crud.item.get(item_id)
    item.update(
        actions=[
            crud.item.status.set(new_item.status),
            crud.item.updated_at.set(get_updated_at())
        ]
    )
    return dict(item)


@router.get("/", response_model=schemas.ItemOutAntd)
def read_items(created_at: Optional[str] = None, updated_at: Optional[str] = None):
    items = crud.item.scan()
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


@router.post("/", response_model=schemas.ItemOut)
def insert_item(item: schemas.ItemIn):
    return item
