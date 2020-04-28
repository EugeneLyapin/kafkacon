import json
import time
import datetime
import yaml
import boto3
from botocore.exceptions import ClientError, ParamValidationError, EndpointConnectionError, ConnectTimeoutError
from debug import errx, debug, trace

class aws(object):
    """aws boto3 class"""
    name = "aws"

    ErrHandler = {}
    def __init__(self, conf=None):
        self.name = self.getname()
        self.conf = conf
        self.region = region
        self.client = self.connect()

    def connect(self):
        return boto3.client(self.service, region_name=self.region)

    def decrypt(self):
        return {}

    def getname(self):
        return self.__class__.__name__

    def json_datetime_serialize(self, o):
        if isinstance(o, (datetime.date, datetime.datetime)):
            return o.isoformat()

    def json_datetime_convert(self, data):
        return json.loads(json.dumps(data, default=self.json_datetime_serialize))

    def botoHandler(self, call=None, key=None, **kwargs):
        default = { key: {} }
        data = self.boto_method_handler(call=call, **kwargs)
        try:
            items = data[key]
        except:
            data = default

        return data

    def boto_method_handler(self, call=None, **kwargs):
        data = {}

        trace({
            'timestamp': time.time(),
            'call': call,
            'args': kwargs
        })

        try:
            data = call(**kwargs)
        except ConnectTimeoutError as error:
            debug(level=1, service=self.name, region=self.region, error=error)

        except EndpointConnectionError as error:
            debug(level=1, service=self.name, region=self.region, error=error)

        except ClientError as error:
            try:
                errcode = error.response['Error']['Code']
                data['ErrorData'] = self.ErrHandler.get(errcode, errx)(error)
            except:
                debug(level=1, service=self.name, region=self.region)
                errx(error)

        except (ValueError, TypeError, ParamValidationError) as error:
            debug(level=1, service=self.name, region=self.region)
            errx(error)

        try:
            del data['ResponseMetadata']
        except:
            pass

        return data

class KMS(aws):
    """KMS boto3 class"""
    name = "KMS"
    service = "kms"

    def decrypt(self):
        return self.botoHandler(call=)
