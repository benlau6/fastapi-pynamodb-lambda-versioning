from datetime import datetime
from enum import Enum
import boto3
import json
import os
from pynamodb.indexes import LocalSecondaryIndex, AllProjection
from pynamodb.attributes import UnicodeAttribute, UTCDateTimeAttribute, MapAttribute, ListAttribute
from pynamodb.models import Model
from log_cfg import logger

BUCKET = os.environ['S3_BUCKET']
INDEX = os.environ['DYNAMODB_INDEX']
URL_DEFAULT_TTL = int(os.environ['URL_DEFAULT_TTL'])

class DatasetMap(MapAttribute):
    file_format = ListAttribute()

class ViewIndex(LocalSecondaryIndex):
    """
    This class represents a local secondary index
    """
    class Meta:
        index_name = INDEX
        # All attributes are projected
        projection = AllProjection()
    api_key = UnicodeAttribute(hash_key=True)
    created_at = UTCDateTimeAttribute(range_key=True)
    source = UnicodeAttribute(null=False)
    s3_path = UnicodeAttribute(null=False)

class State(Enum):
    """
    Manage asset states in dynamo with a string field
    Could have used an int as well, or used a custom serializer which is a bit cleaner.
    """
    CREATED = 1
    RECEIVED = 2
    DELETED = 3

class AssetModel(Model):
    class Meta:
        table_name = os.environ['DYNAMODB_TABLE']
        if 'ENV' in os.environ:
            host = 'http://localhost:8000'
        else:
            region = os.environ['REGION']
            host = os.environ['DYNAMODB_HOST']
        billing_mode = 'PAY_PER_REQUEST'

    api_key = UnicodeAttribute(hash_key=True)
    s3_path = UnicodeAttribute(range_key=True)
    md5 = UnicodeAttribute(null=True)
    view_index = ViewIndex()
    source = UnicodeAttribute(null=False)
    state = UnicodeAttribute(null=False, default=State.CREATED.name)
    created_at = UTCDateTimeAttribute(null=False, default=datetime.now().astimezone())
    updated_at = UTCDateTimeAttribute(null=False, default=datetime.now().astimezone())

    def __str__(self):
        return 'api_key:{}, s3_path:{}'.format(self.api_key, self.s3_path)

    def __iter__(self):
        for name, attr in self._get_attributes().items():
            yield name, attr.serialize(getattr(self, name))

    def save(self, conditional_operator=None, **expected_values):
        try:
            self.update_at = datetime.now().astimezone()
            logger.debug('saving: {}'.format(self))
            super(AssetModel, self).save()
        except Exception as e:
            logger.error('save {} failed: {}'.format(self.get_file_name(), e), exc_info=True)
            raise e

    def get_bucket(self):
        return self.s3_path.replace('s3://', '').split('/', 1)[0]

    def get_s3_key(self):
        return self.s3_path.replace('s3://', '').split('/', 1)[-1]

    def get_file_name(self):
        return self.s3_path.rsplit('/',1)[-1]

    def get_upload_url(self, ttl=URL_DEFAULT_TTL):
        """
        :param ttl: url duration in seconds
        :return: a temporary presigned PUT url
        """
        s3 = boto3.client('s3', config=boto3.session.Config(s3={'addressing_style': 'virtual'}, signature_version='s3v4'))
        put_url = s3.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': BUCKET,
                'Key': self.get_s3_key(),
                'Tagging': f'api-key={self.api_key}&bucket={self.get_bucket()}'
            },
            ExpiresIn=ttl,
            HttpMethod='PUT'
        )
        logger.debug('upload URL: {}'.format(put_url))
        return put_url
    
    def upload_json(self, json_object=None):
        s3 = boto3.client('s3')
        s3.put_object(
            Body=str(json.dumps(json_object)),
            Bucket=BUCKET,
            Key=self.get_s3_key(),
            Tagging=f'api-key={self.api_key}&bucket={self.get_bucket()}'
        )

    def mark_received(self):
        """
        Mark asset as having been received via the s3 objectCreated:Put event
        """
        self.state = State.RECEIVED.name
        logger.debug('mark asset received: {}'.format(self.get_file_name()))
        self.save()

    def mark_uploaded(self):
        """
        Mark asset as having been uploaded via a PUT to the asset's REST path
        """
        uploaded_states = [State.RECEIVED.name, State.UPLOADED.name]
        if self.state not in uploaded_states:
            raise AssertionError('State: \"{}\" must be one of {}'.format(self.state, uploaded_states))
        self.state = State.UPLOADED.name
        logger.debug('mark asset uploaded: {}'.format(self.get_file_name()))
        self.save()

    def mark_deleted(self):
        """
        Mark asset as deleted
        """
        if self.state == 'DELETED':
            return
        s3 = boto3.client('s3')
        s3.delete_object(
            Bucket=self.get_bucket(),
            Key=self.get_s3_key()
        )
        self.state = State.DELETED.name
        logger.debug('mark asset deleted: {}'.format(self.get_file_name()))
        self.save()



class KeyModel(Model):
    class Meta:
        table_name = os.environ['DYNAMODB_TABLE_KEY']
        if 'ENV' in os.environ:
            host = 'http://localhost:8000'
        else:
            region = os.environ['REGION']
            host = os.environ['DYNAMODB_HOST']
            # 'https://dynamodb.us-east-1.amazonaws.com'

    api_key = UnicodeAttribute(hash_key=True)
    vendor = UnicodeAttribute(null=False)
    permission = MapAttribute(of=DatasetMap)

    def __str__(self):
        return 'api_key:{}, vendor:{}'.format(self.api_key, self.vendor)

    def get_attribute(self):
        return {"vendor": self.vendor, "permission": self.permission.as_dict()}

    def check_path(self, project, dataset, file_format):
        for k1, v1 in self.permission.as_dict().items():
            if project.lower() in k1.lower():
                project = k1
                for k2, v2 in v1.items():
                    if dataset.lower() in k2.lower():
                        dataset = k2
                        for v3 in v2:
                            if file_format.lower() in v3.lower():
                                file_format = v3
                                break
                        else:
                            file_format = False
                        break
                else:
                    dataset = False
                break
        else: 
            project = False
        return project, dataset, file_format

    def __iter__(self):
        for name, attr in self._get_attributes().items():
            yield name, attr.serialize(getattr(self, name))

class PathModel(Model):
    class Meta:
        table_name = os.environ['DYNAMODB_TABLE_PATH']
        if 'ENV' in os.environ:
            host = 'http://localhost:8000'
        else:
            region = os.environ['REGION']
            host = os.environ['DYNAMODB_HOST']
            # 'https://dynamodb.us-east-1.amazonaws.com'

    project = UnicodeAttribute(hash_key=True)
    dataset = UnicodeAttribute(range_key=True)
    bucket = UnicodeAttribute(null=False)
    prefix = UnicodeAttribute(null=False)

    def __str__(self):
        return 'project:{}, dataset:{}'.format(self.project, self.dataset)

    def __iter__(self):
        for name, attr in self._get_attributes().items():
            yield name, attr.serialize(getattr(self, name))

    def get_s3_path(self, file_name, file_format, year, month, day):
        year = year.lstrip('0')
        month = month.lstrip('0')
        day = day.lstrip('0')
        return f's3://{self.bucket}/{self.prefix}year={year}/month={month}/day={day}/{file_name}.{file_format}'