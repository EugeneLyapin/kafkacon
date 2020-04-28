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
from Kafka import Consumer
from KMS import KMS

def main():
    tasks = [
        Consumer(conf=conf)
    ]

    for t in tasks:
        t.start()

    time.sleep(10)
    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()

if __name__ == '__main__':
    conf = config.getConf(sys.argv[1:])
    exit(main())
