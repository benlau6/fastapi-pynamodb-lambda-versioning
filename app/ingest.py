import http.client as httplib
from pynamodb.exceptions import DoesNotExist
from ingest.asset_model import AssetModel
from log_cfg import logger
from ingest.util import check_api_key, check_permission, param_to_s3_path, s3_path_to_key
from lambda_decorators import cors_headers, dump_json_body

@cors_headers
@dump_json_body
def ingest(event, context):
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
    method = event['pathParameters']['method']
    file_format = event['pathParameters']['file-format']

    try:
        asset = AssetModel.get(hash_key=api_key, range_key=s3_path)
        if asset.state in ['DELETED', 'CREATED']:
            raise DoesNotExist
        else:
            return {
                'statusCode': httplib.CONFLICT,
                'body': {
                    'error_message': 'object {} exist, please delete it if want to update'.format(s3_path_to_key(s3_path))
                }
            }
    except DoesNotExist:
        asset = AssetModel()
        asset.api_key = api_key
        asset.s3_path = s3_path
        asset.source = 'api'
        
        if method == 'json':
            if method != file_format:
                return {
                    "statusCode": httplib.NOT_FOUND,
                    "body": {
                        'method': method,
                        'file_format': file_format
                    }
                }    
            asset.save()
            asset.upload_json(json_object = event['body'])
            return {
                "statusCode": httplib.CREATED,
            }
        elif method == 'url':
            asset.save()
            upload_url = asset.get_upload_url()  # No timeout specified here, use member param default

            return {
                "statusCode": httplib.CREATED,
                "body": {
                    'upload_url': upload_url,
                    'file_name': asset.get_file_name()
                }
            }
        else:
            return {
                "statusCode": httplib.NOT_FOUND,
                "body": {
                    'method': f'{method} is not allowed. Please use url or json.'
                }
            }  
