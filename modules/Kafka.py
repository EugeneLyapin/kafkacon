import threading, logging, time
import multiprocessing
from kafka import KafkaConsumer, KafkaProducer
from debug import errx, debug, trace

class Consumer(multiprocessing.Process):

    broker = None
    consumer_timeout_ms = 1000
    auto_offset_reset = 'earliest'

    def __init__(self, conf=None):
        self.conf = conf
        self.getconf()
        logging.basicConfig( format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s', level=logging.INFO)
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()

    def stop(self):
        self.stop_event.set()

    def getconf(self):
        try:
            myconf = self.conf['data']['Kafka']
        except:
            pass

        self.broker = self.conf['broker']
        self.topic = self.conf['topic']
        self.offset = self.conf['offset']

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=self.broker,
                                 auto_offset_reset=self.auto_offset_reset,
                                 consumer_timeout_ms=self.consumer_timeout_ms)
        consumer.subscribe([ self.topic ])

        while not self.stop_event.is_set():
            for message in consumer:
                print(message)
                if self.stop_event.is_set():
                    break
        consumer.close()
