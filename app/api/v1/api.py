from fastapi import APIRouter
from .endpoints import login, users, utils, items

router = APIRouter()
#router.include_router(login.router, tags=['login'])
router.include_router(users.router, prefix='/users', tags=['users'])
#router.include_router(utils.router, prefix='/utils', tags=['utils'])
router.include_router(items.router, prefix='/items', tags=['items'])