import argparse
import json
import sys
import os
import yaml

from debug import errx, debug, trace

def getConf(args):
    global debug

    default_filename = 'config.yaml'
    parser = argparse.ArgumentParser()
    parser.add_argument('--broker', type=str, required=True, action='store')
    parser.add_argument('--offset', type=str, required=True, action='store')
    parser.add_argument('--topic', type=str, required=True, action='store')
    parser.add_argument('--filename', type=str, action='store', default=default_filename)
    parser.add_argument('--debug', type=int, action='store', default=0)
    options = parser.parse_args(args)

    filename = getattr(options, 'filename')
    debug = getattr(options, 'debug')

    try:
        fd = open(filename)
    except IOError as error:
        filename = default_filename
        fd = open(filename)

    data = yaml.safe_load(fd)

    conf = {
      'data': data
    }

    for key in ['broker', 'offset', 'topic', 'debug']:
        val = getattr(options, key)
        if val is not None:
            conf[key] = val

    return conf
