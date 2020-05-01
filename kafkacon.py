# -*- coding: utf-8 -*-
import json
import sys
import os
import time
import datetime
import yaml
import pathlib
basepath = pathlib.Path(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(str(pathlib.Path(basepath, "modules")))
import config
from debug import errx, debug, trace
from Kafka import KafkaConsumer
from KMS import KMS

def main():
    consumer = KafkaConsumer(conf=conf)
    Blob = consumer.readMessageByPartitionOffsetAvro()

    # Dict value example to decrypt
    CiphertextBlob = Blob['customerIdentity']['email']

    kms = KMS(conf=conf)
    debug(level=1, obj=kms, service=kms.service)
    data = kms.decrypt(CiphertextBlob=CiphertextBlob)
    print(data)

if __name__ == '__main__':
    conf = config.getConf(sys.argv[1:])
    exit(main())
