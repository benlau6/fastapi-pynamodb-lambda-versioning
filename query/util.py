import http.client as httplib
from pynamodb.exceptions import DoesNotExist
from query.query_model import QueryModel, KeyModel
import urllib.parse

def check_api_key(event):
    try:
        api_key = event['requestContext']['identity']['apiKey']
        key = KeyModel.get(hash_key=api_key)
        return key
    except DoesNotExist:
        raise DoesNotExist(f'API KEY {api_key} does not exist')

def check_permission(event, key):
    database = event['pathParameters']['database']
    table = event['pathParameters']['table']
    database, table = key.check_table(database, table)

    db_dict = {
        'staging': 'mtr-hk-dev-staging-database',
        'processed': 'mtr-hk-dev-processed-database',
        'archived': 'mtr-hk-dev-archived-database',
    }

    try:
        database = db_dict[database]
    except KeyError:
        raise DoesNotExist(f'Database {database} does not exist')

    event['pathParameters']['database'] = database
    event['pathParameters']['table'] = table     

def param_to_query(event): 
    database = event['pathParameters']['database']
    table = event['pathParameters']['table']   
    query = f'select * from "{database}"."{table}"'

    try:
        where = event['queryStringParameters']['where']
        where = urllib.parse.unquote(where)
        query += f' where {where}'
    except KeyError:
        pass
    
    return query

def get_query_record(event):  
    try:
        query_string = param_to_query(event)
        query = QueryModel.get(hash_key=query_string)
        return query
    except DoesNotExist:
        raise DoesNotExist(f'Query {query_string} is not found')


def create_query_record(event):
    query = QueryModel()
    query.api_key = event['requestContext']['identity']['apiKey']
    query.database = event['pathParameters']['database']
    query.table = event['pathParameters']['table']  
    try: 
        query.where = event['queryStringParameters']['where']
    except KeyError:
        pass
    query.query = param_to_query(event)
    query.save()
    return query