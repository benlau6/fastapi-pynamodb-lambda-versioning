import http.client as httplib
from pynamodb.exceptions import DoesNotExist, DeleteError
from .db_model import ItemModel
from log_cfg import logger
from lambda_decorators import cors_headers, dump_json_body

def info(event, context):
    logger.debug('event: {}'.format(event))
    
    try:
        data = ItemModel.scan()
    except DoesNotExist:
        return {
            'statusCode': httplib.NOT_FOUND,
            'body': {
                'error_message': 'not found'
            }
        }

    return {
        "statusCode": httplib.OK,
        "body": data
    }
