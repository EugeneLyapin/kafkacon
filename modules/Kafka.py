import threading
import logging
import time
import multiprocessing
from kafka import KafkaConsumer, TopicPartition
from schema_registry.client import SchemaRegistryClient, schema
from schema_registry.serializers import MessageSerializer
from debug import errx, debug, trace

class Consumer(multiprocessing.Process):

    conf = {
        'auto_offset_reset': 'earliest',
        'consumer_timeout_ms': 1000
    }

    def __init__(self, conf=None):
        self.getconf(conf)
        logging.basicConfig( format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s', level=logging.INFO)
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()

    def stop(self):
        self.stop_event.set()

    def getconf(self, conf):
        try:
            self.conf.update(conf['data']['Kafka'])
        except:
            errx("No Kafka configuration found")

        self.conf.update(conf['parser'])
        trace(self.conf)

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=self.conf['brokers'],
                                 auto_offset_reset=self.conf['auto_offset_reset'],
                                 consumer_timeout_ms=self.conf['consumer_timeout_ms'])
        consumer.subscribe([ self.conf['topic'] ])

        while not self.stop_event.is_set():
            for message in consumer:
                print(message)
                if self.stop_event.is_set():
                    break
        consumer.close()

    def readMessageByPartitionOffset(self):
        consumer = KafkaConsumer(bootstrap_servers=self.conf['brokers'],
                                 auto_offset_reset=self.conf['auto_offset_reset'],
                                 consumer_timeout_ms=self.conf['consumer_timeout_ms'])

        partition = TopicPartition(self.conf['topic'], self.conf['partition'])
        start = self.conf['offset']
        consumer.assign([partition])
        consumer.seek(partition, start)
        for msg in consumer:
            if msg.offset == start:
                return msg.value

    def readMessageByPartitionOffsetAvro(self):
        try:
            schema_registry = self.conf['schema_registry']
        except:
            errx("Option 'schema_registry' not found")

        client = SchemaRegistryClient(schema_registry)
        message_serializer = MessageSerializer(client)
        debug(level=1, MessageSerializer=MessageSerializer)
        try:
            security_protocol = self.conf['security_protocol']
        except:
            pass

        if security_protocol == 'SASL_SSL':
            try:
                pre_defined_kwargs = {
                    'sasl_plain_username': self.conf['sasl_plain_username'],
                    'sasl_mechanism': self.conf['sasl_mechanism'],
                    'sasl_plain_password': self.conf['sasl_plain_password']
                }
            except:
                errx("SASL_SSL options are not defined")

        consumer = KafkaConsumer(bootstrap_servers=self.conf['brokers'],
                                 auto_offset_reset=self.conf['auto_offset_reset'],
                                 consumer_timeout_ms=self.conf['consumer_timeout_ms'], **pre_defined_kwargs)

        partition = TopicPartition(self.conf['topic'], self.conf['partition'])
        start = self.conf['offset']
        consumer.assign([partition])
        consumer.seek(partition, start)
        for message_encoded in consumer:
            if message_encoded.offset == start:
                assert len(message_encoded) > 5
                assert isinstance(message_encoded, bytes)
                message_decoded = message_serializer.decode_message(message_encoded)
                return message_decoded
