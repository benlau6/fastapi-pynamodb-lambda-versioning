import uuid
from typing import List, Optional

from pydantic import BaseModel

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