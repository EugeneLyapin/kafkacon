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
    CiphertextBlob = consumer.readMessageByPartitionOffsetAvro()
    trace(CiphertextBlob)
    CiphertextBlob = 'AQICAHhJQblkxQwd25zGDR3lbyJ2t+DNP98Mw8bSKctqHqSS7AGdEYy39rHwqpkcPDzXy2QIAAAAbTBrBgkqhkiG9w0BBwagXjBcAgEAMFcGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMC7BlNXPlD1H00MRtAgEQgCoj1FXHltlKtwOsZ5lSC9IHjd18A2nc66R/G8Gk0p39wEnqwa0U4MmonGY='
    kms = KMS(conf=conf)
    debug(level=1, obj=kms, service=kms.service)
    data = kms.decrypt(CiphertextBlob=CiphertextBlob)
    debug(level=1, service=kms.service, data=data)

if __name__ == '__main__':
    conf = config.getConf(sys.argv[1:])
    exit(main())
