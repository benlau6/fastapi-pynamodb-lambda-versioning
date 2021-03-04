import http.client as httplib
import os
from pynamodb.exceptions import DoesNotExist, DeleteError, UpdateError
from ingest.asset_model import AssetModel
from log_cfg import logger
import boto3


def event(event, context):
    """
    Triggered by s3 object create events, 
    """
    logger.debug('event: {}'.format(event))
    event_name = event['Records'][0]['eventName']
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key'].replace('%3D', '=')
    md5 = event['Records'][0]['s3']['object']['eTag']

    s3 = boto3.client('s3')
    response = s3.get_object_tagging(
            Bucket=bucket,
            Key=key,
        )

    tag_set = response.get("TagSet")
    for tag in tag_set:
        if tag['Key'] == 'api-key':
            api_key = tag['Value']
    logger.debug('api_key: {}'.format(api_key))
    for tag in tag_set:
        if tag['Key'] == 'bucket':
            bucket_dest = tag['Value']
    logger.debug('bucket: {}'.format(bucket_dest))
    try:
        if 'ObjectCreated:Put' == event_name:
            try:
                asset = AssetModel.get(hash_key=api_key, range_key=f's3://{bucket_dest}/{key}')
                asset.mark_received()
            except UpdateError:
                return {
                    'statusCode': httplib.BAD_REQUEST,
                    'body': {
                        'error_message': 'Unable to update ASSET'}
                }
    except DoesNotExist:
        return {
            'statusCode': httplib.NOT_FOUND,
            'body': {
                'error_message': 'ASSET {} not found'.format(key)
            }
        }

    # save md5 in dynamodb
    asset.md5 = md5
    asset.bucket = None
    asset.save()
    
    # copy object from tmp bucket to dest
    s3 = boto3.resource('s3')
    dest = s3.Bucket(bucket_dest)
    source= { 'Bucket' : bucket, 'Key': key}
    dest.copy(source, key)
    s3.Object(bucket, key).delete()

    return {'statusCode': httplib.ACCEPTED}