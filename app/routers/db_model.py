from datetime import datetime
from datetime import timedelta
from enum import Enum
import boto3
import json
import os
from pynamodb.attributes import UnicodeAttribute, UTCDateTimeAttribute, ListAttribute
from pynamodb.models import Model
from log_cfg import logger

class State(Enum):
    """
    Manage asset states in dynamo with a string field
    Could have used an int as well, or used a custom serializer which is a bit cleaner.
    """
    undetermined = 1
    normal = 2
    fault = 3

class ItemModel(Model):
    class Meta:
        table_name = os.environ['DYNAMODB_TABLE']
        if 'ENV' in os.environ:
            host = 'http://localhost:8000'
        else:
            region = os.environ['REGION']
            host = os.environ['DYNAMODB_HOST']
        billing_mode = 'PAY_PER_REQUEST'

    item_id = UnicodeAttribute(hash_key=True)
    name = UnicodeAttribute(null=False)
    image = UnicodeAttribute(null=False)
    status = UnicodeAttribute(null=False, default=State.undetermined.name)
    created_at = UnicodeAttribute(null=False, default=(datetime.now()+timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S'))
    updated_at = UnicodeAttribute(null=False, default=(datetime.now()+timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S'))

    def __str__(self):
        return 'item_id:{}, name:{}'.format(self.item_id, self.name)

    def __iter__(self):
        for name, attr in self._get_attributes().items():
            yield name, attr.serialize(getattr(self, name))

    def save(self, conditional_operator=None, **expected_values):
        try:
            self.update_at = datetime.now().astimezone()
            logger.debug('saving: {}'.format(self))
            super(AssetModel, self).save()
        except Exception as e:
            logger.error('save {} failed: {}'.format(self.item_id, e), exc_info=True)
            raise e