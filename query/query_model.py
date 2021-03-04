from datetime import datetime, timedelta
from pynamodb.exceptions import DoesNotExist
from enum import Enum
import time
import boto3
import json
import os
from pynamodb.indexes import GlobalSecondaryIndex, KeysOnlyProjection
from pynamodb.attributes import UnicodeAttribute, UTCDateTimeAttribute, MapAttribute, ListAttribute
from pynamodb.models import Model
from log_cfg import logger

INDEX = os.environ['DYNAMODB_INDEX_QUERY']
BUCKET = os.environ['S3_BUCKET_QUERY']

URL_DEFAULT_TTL = int(os.environ['URL_DEFAULT_TTL'])
QUERY_DEFAULT_TTL = int(os.environ['QUERY_DEFAULT_TTL'])

class ViewIndex(GlobalSecondaryIndex):
    """
    This class represents a global secondary index
    """
    class Meta:
        index_name = INDEX
        # All attributes are projected
        projection = KeysOnlyProjection()
    api_key = UnicodeAttribute(hash_key=True)
    state = UnicodeAttribute(null=False)

class KeyModel(Model):
    class Meta:
        table_name = os.environ['DYNAMODB_TABLE_QUERY_KEY']
        if 'ENV' in os.environ:
            host = 'http://localhost:8000'
        else:
            region = os.environ['REGION']
            host = os.environ['DYNAMODB_HOST']
            # 'https://dynamodb.us-east-1.amazonaws.com'

    api_key = UnicodeAttribute(hash_key=True)
    vendor = UnicodeAttribute(null=False)
    permission = MapAttribute(of=ListAttribute)

    def __str__(self):
        return 'api_key:{}, vendor:{}'.format(self.api_key, self.vendor)

    def __iter__(self):
        for name, attr in self._get_attributes().items():
            yield name, attr.serialize(getattr(self, name))

    def get_attribute(self):
        return {"vendor": self.vendor, "permission": self.permission.as_dict()}

    def check_table(self, database, table):
        for k1, v1 in self.permission.as_dict().items():
            if database.lower() in k1.lower():
                database = k1
                for v2 in v1:
                    if table.lower() in v2.lower():
                        table = v2
                        break
                else:
                    raise DoesNotExist(f'table {table} does not exist')
                break
        else: 
            raise DoesNotExist(f'database {database} does not exist')
        return database, table

class State(Enum):
    """
    Manage asset states in dynamo with a string field
    Could have used an int as well, or used a custom serializer which is a bit cleaner.
    """
    CREATED = 1
    QUEUED = 2
    RUNNING = 3
    SUCCEEDED = 4
    FAILED = 5
    CANCELLED = 6
        
class QueryModel(Model):
    class Meta:
        table_name = os.environ['DYNAMODB_TABLE_QUERY']
        if 'ENV' in os.environ:
            host = 'http://localhost:8000'
        else:
            region = os.environ['REGION']
            host = os.environ['DYNAMODB_HOST']
            # 'https://dynamodb.us-east-1.amazonaws.com'
        billing_mode = 'PAY_PER_REQUEST'

    query = UnicodeAttribute(hash_key=True)
    database = UnicodeAttribute(null=False)
    table = UnicodeAttribute(null=False)
    where = UnicodeAttribute(null=True)
    query_id = UnicodeAttribute(null=True)
    view_index = ViewIndex()
    api_key = UnicodeAttribute(null=False)
    result_path = UnicodeAttribute(null=True)
    state = UnicodeAttribute(null=False, default=State.CREATED.name)
    reason = UnicodeAttribute(null=True)
    updated_at = UTCDateTimeAttribute(null=True, default=datetime.now().astimezone())

    def __str__(self):
        return 'database:{}, table:{}, query:{}'.format(self.database, self.table, self.query)

    def __iter__(self):
        for name, attr in self._get_attributes().items():
            yield name, attr.serialize(getattr(self, name))

    def save(self, conditional_operator=None, **expected_values):
        try:
            logger.debug('saving: {}'.format(self))
            super(QueryModel, self).save()
        except Exception as e:
            logger.error('save {} failed: {}'.format(self.__str__, e), exc_info=True)
            raise e

    def is_expired(self):
        ttl = timedelta(seconds=QUERY_DEFAULT_TTL)
        if datetime.now().astimezone() - self.updated_at > ttl:
            return True
        else:
            return False

    def update_state(self):
        exec_states = [State.QUEUED.name, State.RUNNING.name]

        if self.state in exec_states:
            self.get_query()
        else:
            self.start_query()

    def get_result(self):
        finished_states = [State.SUCCEEDED.name, State.FAILED.name, State.CANCELLED.name]
        if self.state not in finished_states:
            raise AssertionError('Incorrect state {} to get result'.format(self.state))
        
        if self.state == State.SUCCEEDED.name: 
            return self.get_download_url()
        elif self.state in [State.FAILED.name, State.CANCELLED.name]:
            raise AssertionError('Query failed since {}'.format(self.reason))

    def start_query(self):
        client = boto3.client('athena')
        try:
            res = client.start_query_execution(
                QueryString=self.query,
                ResultConfiguration={
                    'OutputLocation': f's3://{BUCKET}',
                },
            )
            self.query_id = res['QueryExecutionId']
        except Exception as e:
            self.state = State.FAILED.name
            self.reason = str(e)
            self.save()
            raise Exception('Query failed, please check database: {}, table: {}, where clause: {}'.format(self.database, self.table, self.where))
        self.get_query()

    def get_query(self):
        client = boto3.client('athena')
        res = client.get_query_execution(
            QueryExecutionId=self.query_id
        )
        res = res['QueryExecution']

        self.state = res['Status']['State']

        try:
            self.result_path = res['ResultConfiguration']['OutputLocation'] 
        except KeyError:
            self.result_path = None   

        try:
            self.reason = res['Status']['StateChangeReason']
        except KeyError:
            self.reason = None     

        try:
            self.updated_at = res['Status']['CompletionDateTime']
        except KeyError:
            self.updated_at = res['Status']['SubmissionDateTime']

        self.save()

    def get_download_url(self, ttl=URL_DEFAULT_TTL):
        """
        :param ttl: url duration in seconds
        :return: a temporary presigned download url
        """
        if self.state != State.SUCCEEDED.name:
            raise AssertionError(
                'Query_id {} is marked as {}, must be marked {} to retrieve.'.format(
                    self.query_id, self.state, State.SUCCEEDED.name
                )
            )

        bucket, key = self.result_path.replace('s3://', '').split('/', 1)
        s3 = boto3.client('s3', config=boto3.session.Config(s3={'addressing_style': 'virtual'}, signature_version='s3v4'))
        get_url = s3.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': bucket,
                'Key': key,
            },
            ExpiresIn=ttl,
            HttpMethod='GET'
        )
        logger.debug('download URL: {}'.format(get_url))
        return get_url