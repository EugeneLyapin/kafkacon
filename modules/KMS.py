import json
import time
import datetime
import yaml
import boto3
import base64
from botocore.exceptions import ClientError, ParamValidationError, EndpointConnectionError, ConnectTimeoutError
from debug import errx, debug, trace

class aws(object):
    """aws boto3 class"""
    name = "aws"
    region = None

    conf = {
        'EncryptionAlgorithm': 'SYMMETRIC_DEFAULT'
    }

    ErrHandler = {}

    def __init__(self, conf=None):
        self.name = self.getname()
        self.getconf(conf)
        self.client = self.connect()
        
    def getconf(self, conf):
        pass

    def connect(self):
        try:
            self.region = self.conf['region']
        except:
            errx('Option region is not defined')

        return boto3.client(self.service, region_name=self.region)

    def getname(self):
        return self.__class__.__name__

    def json_datetime_serialize(self, o):
        if isinstance(o, (datetime.date, datetime.datetime)):
            return o.isoformat()

    def json_datetime_convert(self, data):
        return json.loads(json.dumps(data, default=self.json_datetime_serialize))

    def botoHandler(self, call=None, key=None, **kwargs):
        items = { key: {} }
        data = self.boto_method_handler(call=call, **kwargs)
        try:
            items = data[key]
        except:
            pass

        return items

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
                raise

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

    def getconf(self, conf):
        try:
            self.conf.update(conf['services']['KMS'])
        except:
            errx("No KMS configuration found")

        self.conf.update(conf['parser'])
        trace(self.conf)

    def base64decode(self, data):
        return base64.b64decode(data)

    def decrypt(self, CiphertextBlob=None):
        try:
            KeyId = self.conf['KeyId']
        except:
            errx("No KeyId found in configuration file")

        DecodedCiphertextBlob = self.base64decode(CiphertextBlob)
        return self.botoHandler(call=self.client.decrypt, key='Plaintext',
                                CiphertextBlob=DecodedCiphertextBlob, KeyId=KeyId,
                                EncryptionAlgorithm=self.conf['EncryptionAlgorithm'])
