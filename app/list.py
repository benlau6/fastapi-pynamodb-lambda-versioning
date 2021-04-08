import http.client as httplib
from ingest.asset_model import AssetModel, PathModel
from log_cfg import logger
import datetime as dt
from ingest.util import check_api_key, check_permission, param_to_s3_path, query_s3_path_to_key
from lambda_decorators import cors_headers, dump_json_body

@cors_headers
@dump_json_body
def asset_list(event, context):
    logger.debug('event: {}, context: {}'.format(event, context))

    res = check_api_key(event)
    if isinstance(res, dict):
        return res
    key = res

    res = check_permission(event, key, check_file_format=False)
    if isinstance(res, dict):
        return res

    res = param_to_s3_path(event, to_prefix=True)

    if isinstance(res, dict):
        return res
    else:
        prefix = res

    date = event['queryStringParameters']['date']
    source = event['queryStringParameters']['source'].lower()

    date_start = dt.datetime.strptime(date, '%Y-%m-%d').replace(tzinfo=dt.timezone.utc) - dt.timedelta(hours=8)
    date_end = date_start + dt.timedelta(hours=24)

    hash_cond = event['requestContext']['identity']['apiKey']
    range_cond = AssetModel.created_at.between(date_start, date_end)
    filter_cond = (AssetModel.s3_path.contains(prefix)) & (AssetModel.source==source)

    results = AssetModel.view_index.query(hash_cond, range_cond, filter_cond)

    return {
        'statusCode': httplib.OK,
        'body': {
            'items': [query_s3_path_to_key(dict(result)) for result in results]
        }
    }