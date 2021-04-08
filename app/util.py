import http.client as httplib
from pynamodb.exceptions import DoesNotExist
from ingest.asset_model import AssetModel, KeyModel, PathModel

def check_api_key(event):
    try:
        api_key = event['requestContext']['identity']['apiKey']
        key = KeyModel.get(hash_key=api_key)
        return key
    except DoesNotExist:
        return {
            'statusCode': httplib.FORBIDDEN,
            'body': {
                'error_message': 'API KEY {} does not exist'.format(api_key)
            }
        }   

def check_permission(event, key, check_file_format=True):
    project = event['pathParameters']['project']
    dataset = event['pathParameters']['dataset']
    file_format = event['pathParameters']['file-format'] if check_file_format else 'tmp'
    project, dataset, file_format = key.check_path(project, dataset, file_format)

    check_list = [project, dataset]
    res_body = {
        'project': project,
        'dataset': dataset
    }

    if check_file_format:
        check_list.append(file_format)
        res_body['file_format'] = file_format

    if not all(check_list):
        return {
            "statusCode": httplib.FORBIDDEN,
            "body": res_body
        }         
    else:
        event['pathParameters']['project'] = project
        event['pathParameters']['dataset'] = dataset
        if check_file_format:
            event['pathParameters']['file-format'] = file_format
        return True

def param_to_s3_path(event, to_prefix=False): 
    try:      
        project = event['pathParameters']['project']
        dataset = event['pathParameters']['dataset']
        path = PathModel.get(hash_key=project, range_key=dataset)

        if to_prefix:
            return path.prefix
        else:
            s3_path = path.get_s3_path(event['pathParameters']['file-name'], 
                                        event['pathParameters']['file-format'], 
                                        event['pathParameters']['year'], 
                                        event['pathParameters']['month'], 
                                        event['pathParameters']['day'])
            return s3_path
    except DoesNotExist:
        return {
            'statusCode': httplib.NOT_FOUND,
            'body': {
                'error_message': 'project{} & dataset{} not found in path database'.format(project, dataset)
            }
        }

def s3_path_to_key(path: str) -> str:
    return path.replace('s3://', '').split('/', 1)[-1]

def query_s3_path_to_key(result: dict) -> str:
    result['s3_key'] = result.pop('s3_path').replace('s3://', '').split('/', 1)[-1]
    return result