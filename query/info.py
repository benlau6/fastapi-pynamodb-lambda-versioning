import http.client as httplib
from pynamodb.exceptions import DoesNotExist, DeleteError
from query.query_model import KeyModel
from log_cfg import logger
import json
from lambda_decorators import cors_headers, dump_json_body

@cors_headers
@dump_json_body
def info(event, context):
    logger.debug('event: {}'.format(event))
    
    try:
        api_key = event['requestContext']['identity']['apiKey']
        key = KeyModel.get(hash_key=api_key)
    except DoesNotExist:
        return {
            'statusCode': httplib.FORBIDDEN,
            'body': {
                'error_message': 'API KEY {} not found'.format(api_key)
            }
        }

    info = key.get_attribute()

    return {
        "statusCode": httplib.OK,
        "body": info
    }
