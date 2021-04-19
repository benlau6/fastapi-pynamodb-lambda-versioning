from datetime import datetime, timedelta
import time

datetime_format = '%Y-%m-%d %H:%M:%S'

def get_updated_at():
    return (datetime.now() + timedelta(hours=8)).strftime(datetime_format)