import http.client as httplib
import os
import json
from pynamodb.exceptions import DoesNotExist
from query.query_model import KeyModel, QueryModel, State
from log_cfg import logger
from query.util import check_api_key, check_permission, get_query_record, create_query_record
import urllib.parse
import time
from lambda_decorators import cors_headers, dump_json_body

QUERY_CONCURRENT_LIMIT = int(os.environ['QUERY_CONCURRENT_LIMIT'])
QUERY_DEFAULT_WAITING_TIME = int(os.environ['QUERY_DEFAULT_WAITING_TIME'])

@cors_headers
@dump_json_body
def query(event, context):
    logger.debug('event: {}, context: {}'.format(event, context))

    try:
        key = check_api_key(event)
    except DoesNotExist as e:
        return {
            'statusCode': httplib.FORBIDDEN,
            'body': {
                'error_message': str(e)
            }
        }           

    try:
        check_permission(event, key)
    except DoesNotExist as e:
        return {
            'statusCode': httplib.FORBIDDEN,
            'body': {
                'error_message': str(e)
            }            
        }

    api_key = event['requestContext']['identity']['apiKey']

    try:
        query = get_query_record(event)
    except DoesNotExist:
        count = QueryModel.view_index.count(api_key, QueryModel.state==State.RUNNING.name)
        if count > QUERY_CONCURRENT_LIMIT:
            return {
                'statusCode': httplib.TOO_MANY_REQUESTS,
                'body': {
                    'error_message': 'Exceed concurrent query limit of {}, please wait existing queries to finish.'.format(count)
                }
            }
        else:
            query = create_query_record(event)
    query.api_key = api_key

    try:
        timeout = time.time() + 20
        finished_states = [State.SUCCEEDED.name, State.FAILED.name, State.CANCELLED.name]
        while (query.state not in finished_states) or (query.is_expired()):
            query.update_state()
            time.sleep(QUERY_DEFAULT_WAITING_TIME)

            if time.time() > timeout:
                return {
                    "statusCode": httplib.GATEWAY_TIMEOUT,
                    "body": {
                        'error_message': 'API timeout, query state: {}. please send the request again.'.format(query.state)
                    }
                }

        url = query.get_result()
        return {
            "statusCode": httplib.CREATED,
            "body": {
                'download_url': url,
                'query': query.query
            }
        }
    except Exception as e:
        return {
            "statusCode": httplib.UNPROCESSABLE_ENTITY,
            "body": {
                'error_message': str(e)
            }
        }