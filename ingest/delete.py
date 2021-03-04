import http.client as httplib
from pynamodb.exceptions import DoesNotExist, DeleteError
from ingest.asset_model import AssetModel
from log_cfg import logger
from ingest.util import check_api_key, check_permission, param_to_s3_path
from lambda_decorators import cors_headers, dump_json_body

@cors_headers
@dump_json_body
def delete(event, context):
    logger.debug('event: {}'.format(event))
    
    res = check_api_key(event)
    if isinstance(res, dict):
        return res
    key = res

    res = check_permission(event, key)
    if isinstance(res, dict):
        return res

    res = param_to_s3_path(event)
    if isinstance(res, dict):
        return res
    s3_path = res

    api_key = event['requestContext']['identity']['apiKey']

    try:
        asset = AssetModel.get(hash_key=api_key, range_key=s3_path)
    except DoesNotExist:
        return {
            'statusCode': httplib.NOT_FOUND,
            'body': {
                'error_message': 'asset {} not exist'.format(s3_path)
            }
        }
    
    try:
        asset.mark_deleted()
    except DeleteError:
        return {
            'statusCode': httplib.BAD_REQUEST,
            'body': {
                'error_message': 'Unable to delete ASSET {}'.format(asset)
            }
        }

    return {'statusCode': httplib.NO_CONTENT}